import sys
import asyncio
import logging
from pathlib import Path
import yaml
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler
import httpx
from wtpy.WtCoreDefs import WTSBarStruct

# 设置日志
console = Console()
logger = logging.getLogger("futures_bars")
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)

class FuturesBarsDownloader:
    """币安永续合约K线数据下载器"""
    
    def __init__(self, config: dict):
        self.config = config
        self.network_config = config["network"]
        self.binance_config = config["markets"]["crypto"]["exchanges"]["binance"]["futures"]
        self.base_url = self.binance_config["base_url"]
        
        # 设置请求限制
        self.rate_limit = 1000  # 每分钟请求数
        self.batch_size = 1000  # 每次请求数据量
        
        # 设置存储路径
        self.base_path = Path(config["base_path"])
        self.storage_path = self.base_path / "storage"
        
    async def _fetch_data(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """发送API请求"""
        try:
            async with httpx.AsyncClient(
                proxies=self.network_config["proxy"],
                timeout=self.network_config["timeout"],
                verify=False
            ) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                await asyncio.sleep(60/self.rate_limit)  # 控制请求速率
                return response.json()
        except Exception as e:
            logger.error(f"请求失败: {url}, 错误: {str(e)}")
            return None

    def _convert_to_wtpy_bars(self, klines: List[List], symbol: str) -> List[WTSBarStruct]:
        """转换为wtpy的K线格式"""
        bars = []
        for k in klines:
            bar = WTSBarStruct()
            dt = datetime.fromtimestamp(k[0]/1000)
            bar.date = int(dt.strftime('%Y%m%d'))
            bar.time = int(dt.strftime('%H%M%S'))
            bar.open = float(k[1])
            bar.high = float(k[2])
            bar.low = float(k[3])
            bar.close = float(k[4])
            bar.vol = float(k[5])      # 成交量
            bar.money = float(k[7])    # 成交额
            bars.append(bar)
        return bars

    def _save_bars(self, bars: List[WTSBarStruct], symbol: str, period: str = "1m"):
        """保存K线数据"""
        try:
            # 创建保存目录
            save_path = self.storage_path / "csv" / "crypto" / "futures" / period
            save_path.mkdir(parents=True, exist_ok=True)
            
            # 转换为DataFrame
            df = pd.DataFrame([{
                'date': bar.date,
                'time': bar.time,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.vol,
                'turnover': bar.money
            } for bar in bars])
            
            # 保存数据
            file_path = save_path / f"{symbol}.csv"
            df.to_csv(file_path, index=False)
            logger.info(f"已保存 {symbol} 的K线数据到 {file_path}")
            
            return True
        except Exception as e:
            logger.error(f"保存数据失败: {symbol}, 错误: {str(e)}")
            return False

    def validate_data(self, file_path: Path) -> bool:
        """验证数据完整性"""
        try:
            df = pd.read_csv(file_path)
            
            # 检查必要的列
            required_columns = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'turnover']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"{file_path}: 缺少必要的列")
                return False
            
            # 检查数据类型
            df['date'] = pd.to_numeric(df['date'])
            df['time'] = pd.to_numeric(df['time'])
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            df[numeric_columns] = df[numeric_columns].astype(float)
            
            # 检查数据有效性
            if df['high'].lt(df['low']).any():
                logger.error(f"{file_path}: 发现无效的高低点数据")
                return False
            
            return True
        except Exception as e:
            logger.error(f"验证文件 {file_path} 时发生错误: {e}")
            return False

    async def download_bars(self, 
                          symbol: str, 
                          start_time: Optional[int] = None,
                          end_time: Optional[int] = None,
                          period: str = "1m") -> bool:
        """下载K线数据"""
        try:
            # 如果未指定时间，默认下载最近30天数据
            if end_time is None:
                end_time = int(datetime.now().timestamp() * 1000)
            if start_time is None:
                start_time = end_time - (30 * 24 * 60 * 60 * 1000)
            
            url = f"{self.base_url}/fapi/v1/klines"
            all_klines = []
            current_start = start_time
            
            while current_start < end_time:
                current_end = min(current_start + (self.batch_size * 60 * 1000), end_time)
                
                params = {
                    "symbol": symbol,
                    "interval": period,
                    "startTime": current_start,
                    "endTime": current_end,
                    "limit": self.batch_size
                }
                
                klines = await self._fetch_data(url, params)
                if not klines:
                    break
                
                all_klines.extend(klines)
                current_start = klines[-1][0] + 1
                
                logger.info(f"已下载 {symbol} 从 "
                          f"{datetime.fromtimestamp(current_start/1000)} 到 "
                          f"{datetime.fromtimestamp(current_end/1000)} 的数据")
            
            if all_klines:
                # 转换为wtpy格式并保存
                bars = self._convert_to_wtpy_bars(all_klines, symbol)
                if self._save_bars(bars, symbol, period):
                    logger.info(f"{symbol} 数据下载完成")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"下载 {symbol} 数据时发生错误: {e}")
            return False

async def main():
    try:
        # 加载配置
        config_path = Path(__file__).parent.parent / "config.yaml"
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        # 加载交易对
        pairs_file = Path(config["base_path"]) / "storage/common/futures_pairs.json"
        with open(pairs_file, "r") as f:
            pairs_data = json.load(f)
        
        # 按24小时成交额排序
        pairs = pairs_data["pairs"]
        pairs.sort(key=lambda x: float(x.get("quote_volume_24h", 0)), reverse=True)
        
        # 创建下载器
        downloader = FuturesBarsDownloader(config)
        
        # 处理前20个交易量最大的交易对
        for pair in pairs[:20]:
            symbol = pair["symbol"]
            logger.info(f"开始下载 {symbol} 的K线数据")
            await downloader.download_bars(symbol)
        
        logger.info("所有数据下载完成")
        
    except Exception as e:
        logger.exception("程序执行出错")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())