import sys
from pathlib import Path
import asyncio
import logging
from datetime import datetime
import json
import yaml
from rich.console import Console
from rich.logging import RichHandler
import httpx
from typing import Optional, Dict, List

# Windows平台特定配置
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 设置Rich控制台输出
console = Console()
logger = logging.getLogger("pairs_manager")
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)

# 禁用 httpx 的请求日志
logging.getLogger("httpx").setLevel(logging.WARNING)

class CryptoDataManager:
    """数据管理器"""
    def __init__(self, config: dict):
        self.config = config
        self.network_config = config["network"]
        self.binance_config = config["markets"]["crypto"]["exchanges"]["binance"]
        
        # 设置基础路径
        self.base_path = Path(config["base_path"])
        self.common_path = self.base_path / "storage/common"
        self.common_path.mkdir(parents=True, exist_ok=True)
        
    async def _fetch_data(self, url: str, params: dict = None) -> Optional[Dict]:
        """发送API请求"""
        try:
            async with httpx.AsyncClient(
                proxies=self.network_config["proxy"],
                timeout=self.network_config["timeout"],
                verify=False
            ) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"请求失败: {url}, 错误: {str(e)}")
            return None

    async def get_active_pairs(self, market_type: str = "spot") -> List[Dict]:
        """获取活跃交易对"""
        base_url = self.binance_config[market_type]["base_url"]
        url = f"{base_url}/{'api/v3' if market_type == 'spot' else 'fapi/v1'}/exchangeInfo"
        
        logger.info(f"正在获取{market_type}市场交易对信息...")
        data = await self._fetch_data(url)
        
        if not data or "symbols" not in data:
            logger.error(f"获取{market_type}交易对信息失败")
            return []
        
        pairs = []
        filters = self.binance_config[market_type]["symbol_filters"]
        
        for symbol in data["symbols"]:
            # 基本过滤条件
            if (symbol["status"] != "TRADING" or
                symbol["quoteAsset"] not in filters["quote_assets"] or
                (market_type == "futures" and symbol.get("contractType") != "PERPETUAL")):
                continue
            
            # 检查排除模式
            should_exclude = any(
                pattern.replace("*", "") in symbol["symbol"]
                for pattern in filters["exclude_patterns"]
            )
            if should_exclude:
                continue
            
            # 提取所有filters信息
            symbol_filters = {f["filterType"]: f for f in symbol.get("filters", [])}
            
            # 构建基础交易对信息
            pair_info = {
                "symbol": symbol["symbol"],
                "baseAsset": symbol["baseAsset"],
                "quoteAsset": symbol["quoteAsset"],
                "status": symbol["status"],
                "market_type": market_type,
                
                # 精度和限制信息
                "price_filter": symbol_filters.get("PRICE_FILTER", {}),
                "lot_size": symbol_filters.get("LOT_SIZE", {}),
                "min_notional": symbol_filters.get("MIN_NOTIONAL", {}).get("minNotional", "0"),
                
                # 交易规则
                "orderTypes": symbol.get("orderTypes", []),
                "timeInForce": symbol.get("timeInForce", []),
                "permissions": symbol.get("permissions", []),
                
                # 计算后的精度信息
                "price_precision": next((int(f["tickSize"].find('1') - 1) 
                                      for f in symbol.get("filters", [])
                                      if f["filterType"] == "PRICE_FILTER"), 8),
                "qty_precision": next((int(f["stepSize"].find('1') - 1)
                                    for f in symbol.get("filters", [])
                                    if f["filterType"] == "LOT_SIZE"), 8),
            }
            
            # 添加合约特有信息
            if market_type == "futures":
                # 获取杠杆档位信息
                leverage_brackets = symbol.get("leverageBracket", [])
                max_leverage = max([bracket.get("initialLeverage", 1) for bracket in leverage_brackets]) if leverage_brackets else symbol.get("leverage", 20)  # 默认20倍
                
                pair_info.update({
                    "contractType": symbol.get("contractType"),
                    "deliveryDate": symbol.get("deliveryDate", 0),
                    "onboardDate": symbol.get("onboardDate", 0),
                    "maintMarginPercent": symbol.get("maintMarginPercent", "0"),
                    "requiredMarginPercent": symbol.get("requiredMarginPercent", "0"),
                    "baseCommissionRate": symbol.get("baseCommissionRate", "0"),
                    "takerCommissionRate": symbol.get("takerCommissionRate", "0"),
                    "makerCommissionRate": symbol.get("makerCommissionRate", "0"),
                    "max_leverage": max_leverage,  # 最大杠杆倍数
                    "leverage_brackets": leverage_brackets  # 完整的杠杆档位信息
                })
            
            pairs.append(pair_info)
        
        logger.info(f"获取到 {len(pairs)} 个{market_type}市场交易对")
        return pairs

    async def update_pairs(self):
        """更新所有交易对信息"""
        try:
            # 并发获取现货和合约交易对
            spot_task = self.get_active_pairs("spot")
            futures_task = self.get_active_pairs("futures")
            
            spot_pairs, futures_pairs = await asyncio.gather(spot_task, futures_task)
            
            # 分别保存现货和合约交易对信息
            spot_file = self.common_path / "crypto_spot_pairs.json"
            futures_file = self.common_path / "crypto_futures_pairs.json"
            
            # 保存现货交易对
            with open(spot_file, "w", encoding="utf-8") as f:
                json.dump({
                    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "market_type": "spot",
                    "total_count": len(spot_pairs),
                    "pairs": spot_pairs
                }, f, indent=2, ensure_ascii=False)
            
            # 保存合约交易对
            with open(futures_file, "w", encoding="utf-8") as f:
                json.dump({
                    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "market_type": "futures",
                    "total_count": len(futures_pairs),
                    "pairs": futures_pairs
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"交易对更新完成:")
            logger.info(f"- 现货交易对: {len(spot_pairs)}个，已保存至: {spot_file}")
            logger.info(f"- 合约交易对: {len(futures_pairs)}个，已保存至: {futures_file}")
            
            return {
                "spot": spot_pairs,
                "futures": futures_pairs
            }
            
        except Exception as e:
            logger.exception("更新交易对时发生错误")
            return None

async def main():
    """主函数"""
    try:
        # 加载配置
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        # 更新交易对
        manager = CryptoDataManager(config)
        await manager.update_pairs()
        
    except Exception as e:
        logger.exception("程序执行出错")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())