"""
币安期货K线数据下载器

此模块用于从币安期货市场下载1分钟K线数据。主要功能包括：
1. 支持多交易对并发下载
2. 自动选择主要交易对
3. 支持断点续传
4. 文件完整性验证
5. 优雅退出机制
"""

import os
import sys
import time
import json
import signal
import logging
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
import pytz
import yaml
from pathlib import Path

# 设置日志
logger = logging.getLogger("futures_downloader")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 全局变量，用于控制程序退出和下载状态
exit_event = threading.Event()  # 退出事件标志
download_lock = threading.Lock()  # 下载状态锁
active_downloads = set()  # 当前活跃的下载任务集合

class FuturesDownloader:
    def __init__(self, config_file: str = "config.yaml"):
        """
        初始化下载器
        
        Args:
            config_file: 配置文件路径，默认为'config.yaml'
        
        配置文件包含以下主要内容：
        - base_path: 数据存储基础路径
        - timezone: 时区设置
        - storage: 存储路径模板
        - network: 网络配置（代理、超时等）
        - markets: 市场配置（交易对选择规则等）
        """
        # 加载配置文件
        self.config = self._load_config(config_file)
        
        # 基础配置
        base_path = Path(self.config.get("base_path", "G:/trading"))
        self.tz = pytz.timezone(self.config.get("timezone", "Asia/Shanghai"))
        
        # 构建存储路径
        storage_template = self.config["storage"]["csv"]
        relative_path = storage_template.format(market="crypto", market_type="futures/1m")
        self.base_path = base_path / relative_path
        
        # 状态文件路径
        status_template = self.config["storage"]["status"]
        status_path = base_path / status_template.format(market="crypto", market_type="futures")
        status_path.mkdir(parents=True, exist_ok=True)
        self.status_file = status_path / "download_crypto_futures_status.json"
        
        # 创建必要的目录
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # 网络配置
        network_config = self.config.get("network", {})
        self.proxies = {"http": network_config.get("proxy"), "https": network_config.get("proxy")} if network_config.get("proxy") else None
        self.timeout = network_config.get("timeout", 30)
        self.max_retries = network_config.get("max_retries", 3)
        self.retry_delay = network_config.get("retry_delay", 5)
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"初始化完成，使用配置:")
        logger.info(f"- 基础路径: {self.base_path}")
        logger.info(f"- 状态文件: {self.status_file}")
        logger.info(f"- 时区: {self.tz}")
        if self.proxies:
            logger.info(f"- 代理: {self.proxies['http']}")

    def _load_config(self, config_file: str) -> dict:
        """
        加载配置文件
        
        Args:
            config_file: 配置文件路径
            
        Returns:
            dict: 配置信息字典
            
        Raises:
            Exception: 配置文件加载失败时抛出异常
        """
        try:
            config_path = Path(__file__).parent.parent / config_file
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """
        信号处理函数，用于处理程序退出信号
        
        处理流程：
        1. 设置退出标志
        2. 等待当前活跃的下载任务完成
        3. 安全退出程序
        
        Args:
            signum: 信号编号
            frame: 当前栈帧
        """
        logger.info("接收到退出信号，正在优雅退出...")
        exit_event.set()
        
        # 等待当前活跃的下载完成
        with download_lock:
            active_symbols = list(active_downloads)
        
        if active_symbols:
            logger.info(f"等待当前活跃的下载完成: {', '.join(active_symbols)}")
            while True:
                with download_lock:
                    if not active_downloads:
                        break
                time.sleep(1)
        
        logger.info("程序已安全退出")
        sys.exit(0)

    def verify_file(self, file_path: Path, date_str: str) -> bool:
        """
        验证下载文件的完整性和时间戳连续性
        
        验证内容：
        1. 文件是否存在且可读
        2. 数据列是否完整
        3. 时间戳是否连续且在指定日期范围内
        4. 数值列是否有效
        
        Args:
            file_path: 文件路径
            date_str: 日期字符串（格式：YYYY-MM-DD，基于北京时间）
            
        Returns:
            bool: 验证是否通过
        """
        try:
            if not file_path.exists():
                return False
                
            # 读取CSV文件
            try:
                df = pd.read_csv(file_path)
            except Exception as e:
                logger.error(f"读取文件 {file_path} 失败: {e}")
                return False
            
            # 检查必要的列
            required_columns = ['open_time', 'open', 'high', 'low', 'close', 'volume',
                              'quote_volume', 'trades', 'taker_buy_volume',
                              'taker_buy_quote_volume', 'taker_sell_volume', 'taker_sell_quote_volume']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"{file_path} 缺少必要的列")
                return False
            
            try:
                # 转换时间列为北京时间
                df['open_time'] = pd.to_datetime(df['open_time'])
                if not df['open_time'].dt.tz:  # 如果时间没有时区信息，添加时区
                    df['open_time'] = df['open_time'].dt.tz_localize(self.tz)
            except Exception as e:
                logger.error(f"{file_path} 时间列格式错误: {e}")
                return False
            
            # 获取日期所在的月份范围
            date = datetime.strptime(date_str, "%Y-%m-%d")
            month_start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if date.month == 12:
                month_end = date.replace(year=date.year + 1, month=1, day=1)
            else:
                month_end = date.replace(month=date.month + 1, day=1)
            
            month_start = self.tz.localize(month_start)
            month_end = self.tz.localize(month_end)
            
            # 检查数据是否在月份范围内
            df_month_data = df[
                (df['open_time'] >= month_start) &
                (df['open_time'] < month_end)
            ]
            
            if len(df_month_data) == 0:
                logger.error(f"{file_path} 不包含 {date_str} 所在月份的数据")
                return False
            
            # 确保数据按时间排序
            df_month_data = df_month_data.sort_values('open_time')
            
            # 检查时间连续性
            time_diff = df_month_data['open_time'].diff()[1:]  # 计算相邻时间点的差值，跳过第一个NaN
            expected_diff = timedelta(minutes=1)
            
            # 检查是否存在不连续的时间点
            invalid_diffs = time_diff != expected_diff
            if invalid_diffs.any():
                invalid_times = df_month_data['open_time'][1:][invalid_diffs]
                logger.error(f"{file_path} 存在时间不连续的点: {invalid_times.tolist()}")
                return False
            
            # 检查数值列是否有效
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 
                             'quote_volume', 'trades', 'taker_buy_volume', 
                             'taker_buy_quote_volume']
            for col in numeric_columns:
                if not pd.to_numeric(df_month_data[col], errors='coerce').notna().all():
                    logger.error(f"{file_path} 列 {col} 包含无效数值")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"验证文件 {file_path} 失败: {e}")
            return False

    def _remove_invalid_file(self, file_path: Path):
        """
        删除无效的数据文件
        
        Args:
            file_path: 要删除的文件路径
        """
        try:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"已删除无效文件: {file_path}")
        except Exception as e:
            logger.error(f"删除文件 {file_path} 失败: {e}")

    def load_download_status(self) -> dict:
        """
        加载下载状态记录
        
        Returns:
            dict: 包含各交易对下载状态的字典
        """
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载下载状态失败: {e}")
        return {}

    def save_download_status(self, status: dict):
        """
        保存下载状态记录
        
        Args:
            status: 包含各交易对下载状态的字典
        """
        try:
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=4)
        except Exception as e:
            logger.error(f"保存下载状态失败: {e}")

    def get_symbol_listing_date(self, symbol: str) -> str:
        """
        获取合约交易对的上市时间
        
        Args:
            symbol: 交易对名称（如'BTCUSDT'）
            
        Returns:
            str: 上市日期（格式：YYYY-MM-DD）或None（获取失败）
        """
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = requests.get(url, proxies=self.proxies, verify=False)
            response.raise_for_status()
            data = response.json()
            
            for symbol_info in data.get('symbols', []):
                if symbol_info['symbol'] == symbol:
                    listing_time = int(symbol_info.get('onboardDate', 0))
                    if listing_time > 0:
                        listing_date = datetime.fromtimestamp(listing_time / 1000).replace(tzinfo=pytz.UTC)
                        listing_date = listing_date.astimezone(self.tz)
                        return listing_date.strftime("%Y-%m-%d")
            
            return None
        except Exception as e:
            logger.error(f"获取 {symbol} 上市时间失败: {e}")
            return None

    def _check_missing_periods(self, df: pd.DataFrame, start_time: datetime, end_time: datetime) -> list:
        """
        检查DataFrame中缺失的时间段
        
        Args:
            df: 数据DataFrame
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            list: 缺失时间段列表，每个元素为(start, end)元组
        """
        if df.empty:
            return [(start_time, end_time)]
            
        # 确保时间列格式正确，统一转换为tz-aware
        df['open_time'] = pd.to_datetime(df['open_time'])
        if not df['open_time'].dt.tz:
            df['open_time'] = df['open_time'].dt.tz_localize(self.tz)
        else:
            df['open_time'] = df['open_time'].dt.tz_convert(self.tz)
            
        df = df.sort_values('open_time')
        
        # 创建完整的时间索引（1分钟间隔）
        full_range = pd.date_range(start=start_time, end=end_time, freq='1min', tz=self.tz)
        
        # 找出现有数据中的时间点
        existing_times = set(df['open_time'])
        
        # 找出缺失的时间点
        missing_times = [t for t in full_range if t not in existing_times]
        
        if not missing_times:
            return []
            
        # 将连续的缺失时间点组合成时间段
        missing_periods = []
        period_start = missing_times[0]
        prev_time = missing_times[0]
        
        for curr_time in missing_times[1:]:
            if curr_time - prev_time > timedelta(minutes=1):
                missing_periods.append((period_start, prev_time))
                period_start = curr_time
            prev_time = curr_time
            
        missing_periods.append((period_start, prev_time))
        return missing_periods

    def download_future(self, symbol: str, date_str: str) -> bool:
        """
        下载单个交易对指定日期的分钟K线数据
        
        主要步骤：
        1. 检查现有数据
        2. 确定需要补充的时间段
        3. 只下载缺失的数据
        4. 合并并保存数据
        
        Args:
            symbol: 交易对名称
            date_str: 日期字符串（格式：YYYY-MM-DD）
            
        Returns:
            bool: 下载是否成功
        """
        try:
            # 解析日期获取年月
            date = datetime.strptime(date_str, "%Y-%m-%d")
            year_month = date.strftime("%Y-%m")
            
            # 设置当月的北京时间范围
            date = datetime.strptime(date_str, "%Y-%m-%d")
            cn_start_time = self.tz.localize(date.replace(day=1, hour=0, minute=0, second=0, microsecond=0))
            if date.month == 12:
                cn_end_time = self.tz.localize(date.replace(year=date.year + 1, month=1, day=1))
            else:
                cn_end_time = self.tz.localize(date.replace(month=date.month + 1, day=1))
            
            # 转换为UTC时间用于API请求
            utc_start_time = cn_start_time.astimezone(pytz.UTC)
            utc_end_time = cn_end_time.astimezone(pytz.UTC)
            
            # 构建保存路径
            save_path = self.base_path / symbol
            save_path.mkdir(parents=True, exist_ok=True)
            file_path = save_path / f"{symbol}_1m_{year_month}.csv"
            
            # 读取现有数据（如果存在）
            existing_df = pd.DataFrame()
            if file_path.exists():
                try:
                    existing_df = pd.read_csv(file_path)
                    # 确保时间列有正确的时区信息
                    existing_df['open_time'] = pd.to_datetime(existing_df['open_time'])
                    if not existing_df['open_time'].dt.tz:
                        existing_df['open_time'] = existing_df['open_time'].dt.tz_localize(self.tz)
                except Exception as e:
                    logger.warning(f"读取现有文件失败: {e}，将重新下载完整数据")
                    existing_df = pd.DataFrame()
            
            # 检查缺失的时间段（使用北京时间）
            missing_periods = self._check_missing_periods(existing_df, cn_start_time, cn_end_time)
            
            if not missing_periods:
                logger.info(f"{symbol} {date_str} 数据已完整")
                return True
            
            # 下载缺失的数据
            all_new_data = []
            # 按时间顺序处理缺失段
            missing_periods.sort(key=lambda x: x[0])
            
            for start, end in missing_periods:
                logger.info(f"下载 {symbol} 从 {start} 到 {end} 的缺失数据")
                
                # 转换为UTC时间戳
                start_ts = int(start.astimezone(pytz.UTC).timestamp() * 1000)
                end_ts = int(end.astimezone(pytz.UTC).timestamp() * 1000)
                
                url = "https://fapi.binance.com/fapi/v1/klines"
                params = {
                    "symbol": symbol,
                    "interval": "1m",
                    "limit": 1500,
                    "startTime": start_ts,
                    "endTime": end_ts
                }
                
                try:
                    response = requests.get(url, params=params, proxies=self.proxies, verify=False)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data:
                        df_new = pd.DataFrame(data, columns=[
                            'open_time', 'open', 'high', 'low', 'close', 'volume',
                            'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                            'taker_buy_quote_volume', 'ignore'
                        ])
                        
                        # 处理时间格式，确保有正确的时区信息
                        df_new['open_time'] = pd.to_datetime(df_new['open_time'], unit='ms', utc=True)
                        df_new['open_time'] = df_new['open_time'].dt.tz_convert(self.tz)
                        
                        all_new_data.append(df_new)
                    else:
                        logger.warning(f"时间段 {start} - {end} 未获取到数据")
                        
                except Exception as e:
                    logger.error(f"下载时间段 {start} - {end} 失败: {e}")
                    continue
                
                # 添加小延迟避免触发频率限制
                time.sleep(0.5)
            
            if not all_new_data:
                logger.error(f"未能获取到任何新数据")
                return False
            
            # 合并所有新数据
            df_new = pd.concat(all_new_data, ignore_index=True)
            
            # 删除不需要的列
            df_new = df_new.drop(['ignore', 'close_time'], axis=1)
            
            # 转换数值列
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 
                             'quote_volume', 'trades', 'taker_buy_volume', 
                             'taker_buy_quote_volume']
            for col in numeric_columns:
                df_new[col] = pd.to_numeric(df_new[col])
            
            # 计算主动卖单成交量和成交额
            df_new['taker_sell_volume'] = df_new['volume'] - df_new['taker_buy_volume']
            df_new['taker_sell_quote_volume'] = df_new['quote_volume'] - df_new['taker_buy_quote_volume']
            
            # 合并新旧数据
            if not existing_df.empty:
                df_final = pd.concat([existing_df, df_new])
                df_final = df_final.drop_duplicates(subset=['open_time'], keep='last')
                df_final = df_final.sort_values('open_time')
            else:
                df_final = df_new
            
            # 最后移除时区信息并格式化时间列
            df_final['open_time'] = df_final['open_time'].dt.tz_localize(None)
            df_final['open_time'] = df_final['open_time'].dt.strftime('%Y-%m-%d %H:%M')
            
            # 使用临时文件保存
            temp_file = file_path.with_suffix('.csv.tmp')
            try:
                df_final.to_csv(temp_file, index=False)
                if temp_file.exists():
                    if file_path.exists():
                        file_path.unlink()
                    temp_file.rename(file_path)
                    logger.info(f"成功保存 {symbol} {date_str} 的数据")
                    return True
                else:
                    logger.error(f"临时文件创建失败")
                    return False
                    
            except Exception as e:
                logger.error(f"保存数据失败: {e}")
                if temp_file.exists():
                    temp_file.unlink()
                return False
            
        except Exception as e:
            logger.error(f"下载 {symbol} {date_str} 失败: {e}")
            return False

    def download_worker(self, symbol: str, start_time: str, end_time: str):
        """
        下载工作线程，负责下载指定时间范围内的数据
        
        主要功能：
        1. 加载已下载状态
        2. 按日期顺序下载数据
        3. 更新下载状态
        4. 处理退出信号
        
        Args:
            symbol: 交易对名称
            start_time: 开始日期（格式：YYYY-MM-DD）
            end_time: 结束日期（格式：YYYY-MM-DD）
        """
        try:
            # 加载下载状态
            status = self.load_download_status()
            symbol_status = status.get(symbol, {})
            
            # 解析日期
            start_date = datetime.strptime(start_time, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_time, "%Y-%m-%d").date()
            
            # 获取上市时间
            listing_date_str = self.get_symbol_listing_date(symbol)
            if listing_date_str:
                listing_date = datetime.strptime(listing_date_str, "%Y-%m-%d").date()
                # 如果上市时间晚于请求的开始时间，调整开始时间
                if listing_date > start_date:
                    start_date = listing_date
                    start_time = start_date.strftime("%Y-%m-%d")
                    logger.info(f"{symbol} 上市时间为 {listing_date_str}，调整下载起始日期为 {start_time}")
            
            # 生成日期序列
            current_date = start_date
            dates = []
            while current_date <= end_date:
                dates.append(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)
            
            # 按日期顺序下载数据
            for date_str in dates:
                # 检查是否需要退出
                if exit_event.is_set():
                    logger.info(f"检测到退出信号，停止下载 {symbol}")
                    break
                
                # 检查是否已下载
                if date_str in symbol_status.get("downloaded_dates", []):
                    logger.debug(f"{symbol} {date_str} 已下载，跳过")
                    continue
                
                # 下载数据
                success = self.download_future(symbol, date_str)
                
                if success:
                    # 更新下载状态
                    if symbol not in status:
                        status[symbol] = {"downloaded_dates": []}
                    if "downloaded_dates" not in status[symbol]:
                        status[symbol]["downloaded_dates"] = []
                    status[symbol]["downloaded_dates"].append(date_str)
                    status[symbol]["downloaded_dates"].sort()  # 保持日期有序
                    status[symbol]["last_update"] = datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S")
                    self.save_download_status(status)
                else:
                    logger.error(f"{symbol} {date_str} 下载失败")
            
            logger.info(f"{symbol} 下载完成")
            
        except Exception as e:
            logger.error(f"{symbol} 下载过程出错: {e}")
        finally:
            # 从活跃下载集合中移除
            with download_lock:
                active_downloads.discard(symbol)

    def get_date_range(self, symbol: str = None) -> tuple:
        """
        获取需要下载的日期范围
        
        规则：
        1. 默认下载近一年的数据
        2. 特殊交易对可在配置文件中指定年数
        3. 结束日期为昨天
        4. 如果有上市时间，开始日期不早于上市时间
        
        Args:
            symbol: 交易对名称（可选）
            
        Returns:
            tuple: (开始日期, 结束日期)，格式均为YYYY-MM-DD
        """
        try:
            # 获取昨天的日期（北京时间）
            end_date = datetime.now(self.tz).date() - timedelta(days=1)
            
            # 默认下载近一年的数据
            start_date = end_date - timedelta(days=365)
            
            # 检查配置文件中是否有特殊设置
            if symbol and "special_pairs" in self.config:
                special_pairs = self.config.get("special_pairs", {})
                if symbol in special_pairs:
                    # 如果是特殊交易对，使用配置的年数
                    years = special_pairs[symbol]
                    start_date = end_date - timedelta(days=365 * years)
                    logger.info(f"{symbol} 使用特殊配置: 下载{years}年数据")
            
            # 如果指定了交易对，检查其上市时间
            if symbol:
                listing_date_str = self.get_symbol_listing_date(symbol)
                if listing_date_str:
                    listing_date = datetime.strptime(listing_date_str, "%Y-%m-%d").date()
                    # 如果上市时间晚于计划的开始时间，使用上市时间作为开始时间
                    if listing_date > start_date:
                        start_date = listing_date
                        logger.info(f"{symbol} 上市时间为 {listing_date_str}，调整下载起始日期")
            
            return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        except Exception as e:
            logger.error(f"计算日期范围失败: {e}")
            # 返回默认值：近一年
            end_date = datetime.now(self.tz).date() - timedelta(days=1)
            start_date = end_date - timedelta(days=365)
            return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def download_all_futures(self, max_workers: int = 3):
        """
        并发下载所有选定的期货交易对数据
        
        主要步骤：
        1. 获取交易对列表
        2. 创建线程池
        3. 提交下载任务
        4. 等待所有任务完成
        
        Args:
            max_workers: 最大工作线程数，默认为3
        """
        try:
            # 获取交易对列表
            symbols = self.get_top_futures()
            if not symbols:
                logger.error("未获取到交易对列表")
                return
            
            logger.info(f"开始下载 {len(symbols)} 个交易对的数据")
            
            # 创建线程池
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                # 提交下载任务
                for symbol in symbols:
                    # 获取该交易对的日期范围
                    start_date, end_date = self.get_date_range(symbol)
                    logger.info(f"计划下载 {symbol} 从 {start_date} 到 {end_date} 的数据")
                    
                    # 提交任务
                    future = executor.submit(self.download_worker, symbol, start_date, end_date)
                    futures.append(future)
                    
                    # 记录活跃下载
                    with download_lock:
                        active_downloads.add(symbol)
                
                # 等待所有任务完成
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"下载任务失败: {e}")
            
            logger.info("所有下载任务完成")
            
        except Exception as e:
            logger.error(f"下载所有期货数据失败: {e}")
        finally:
            # 清理活跃下载记录
            with download_lock:
                active_downloads.clear()

    def calculate_symbol_score(self, ticker_data: dict, weights: dict) -> float:
        """
        计算交易对的综合得分，用于选择主要交易对
        
        评分指标：
        1. 24小时成交量
        2. 价格变化幅度
        3. 成交笔数
        4. 价格波动率
        
        Args:
            ticker_data: 24小时行情数据
            weights: 各指标的权重配置
            
        Returns:
            float: 综合得分
        """
        try:
            score = 0.0
            
            # 交易量得分
            if 'volume_weight' in weights:
                volume = float(ticker_data['quoteVolume'])
                score += volume * weights['volume_weight']
            
            # 价格变化百分比得分
            if 'price_change_weight' in weights:
                price_change = abs(float(ticker_data['priceChangePercent']))
                score += price_change * weights['price_change_weight']
            
            # 成交笔数得分
            if 'trades_weight' in weights:
                trades = float(ticker_data['count'])
                score += trades * weights['trades_weight']
            
            # 波动率得分 (最高价与最低价的差值百分比)
            if 'volatility_weight' in weights:
                high = float(ticker_data['highPrice'])
                low = float(ticker_data['lowPrice'])
                if low > 0:
                    volatility = ((high - low) / low) * 100
                    score += volatility * weights['volatility_weight']
            
            return score
        except Exception as e:
            logger.warning(f"计算{ticker_data.get('symbol', 'unknown')}得分失败: {e}")
            return 0.0

    def get_top_futures(self) -> list:
        """
        获取主要合约交易对列表
        
        选择方式：
        1. 优先处理BTCUSDT、ETHUSDT和配置文件指定的交易对
        2. 其他交易对根据筛选条件自动选择：
           - 只选择USDT永续合约
           - 排除UP/DOWN等特殊合约
           - 根据综合得分排序（成交量、价格变化、成交笔数、波动率）
        
        Returns:
            list: 选定的交易对列表
        """
        try:
            # 1. 获取配置
            futures_config = self.config.get("markets", {}).get("crypto", {}).get("exchanges", {}).get("binance", {}).get("futures", {})
            symbols_config = futures_config.get("symbols", {})
            
            # 2. 获取交易所合约信息
            exchange_info_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = requests.get(exchange_info_url, proxies=self.proxies, verify=False)
            response.raise_for_status()
            exchange_info = response.json()
            
            # 获取所有永续合约
            perpetual_symbols = set()
            for symbol_info in exchange_info['symbols']:
                if (symbol_info['symbol'].endswith('USDT') and 
                    symbol_info['contractType'] == 'PERPETUAL' and  # 确保是永续合约
                    symbol_info['status'] == 'TRADING' and
                    'UP' not in symbol_info['symbol'] and 
                    'DOWN' not in symbol_info['symbol']):
                    perpetual_symbols.add(symbol_info['symbol'])
            
            if not perpetual_symbols:
                logger.error("未找到符合条件的永续合约")
                return []
            
            # 3. 获取24小时行情数据
            ticker_url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
            response = requests.get(ticker_url, proxies=self.proxies, verify=False)
            response.raise_for_status()
            ticker_data = response.json()
            
            # 4. 筛选永续合约的行情数据
            valid_tickers = []
            for item in ticker_data:
                if item['symbol'] in perpetual_symbols:
                    valid_tickers.append(item)
            
            # 5. 设置权重配置
            weights = {
                "volume_weight": 1.0,      # 成交量权重
                "price_change_weight": 0.3, # 价格变化权重
                "trades_weight": 0.3,       # 成交笔数权重
                "volatility_weight": 0.4    # 波动率权重
            }
            
            # 6. 优先处理的交易对
            priority_pairs = ["BTCUSDT", "ETHUSDT"]  # 默认优先
            config_pairs = symbols_config.get("include", [])  # 配置文件指定的优先交易对
            if config_pairs:
                priority_pairs.extend([p for p in config_pairs if p not in priority_pairs])
            
            # 确保优先交易对都是有效的永续合约
            priority_pairs = [p for p in priority_pairs if p in perpetual_symbols]
            
            # 7. 计算其他交易对的得分
            scored_pairs = []
            for item in valid_tickers:
                symbol = item['symbol']
                if symbol in priority_pairs:
                    continue
                
                score = self.calculate_symbol_score(item, weights)
                scored_pairs.append((symbol, score))
            
            # 8. 按得分排序
            scored_pairs.sort(key=lambda x: x[1], reverse=True)
            
            # 9. 组合最终结果：优先交易对 + 得分排名靠前的交易对（总共不超过50个）
            remaining_slots = 60 - len(priority_pairs)
            selected_pairs = priority_pairs + [p[0] for p in scored_pairs[:remaining_slots]]
            
            # 10. 输出详细信息
            logger.info(f"优先交易对: {', '.join(priority_pairs)}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("得分排名前10交易对详情:")
                for symbol, score in scored_pairs[:10]:
                    symbol_data = next(item for item in ticker_data if item['symbol'] == symbol)
                    logger.debug(f"\n{symbol}:")
                    logger.debug(f"  总得分: {score:.2f}")
                    logger.debug(f"  24h成交量: {float(symbol_data['quoteVolume']):.2f} USDT")
                    logger.debug(f"  价格变化: {abs(float(symbol_data['priceChangePercent'])):.2f}%")
                    logger.debug(f"  成交笔数: {symbol_data['count']}")
                    high = float(symbol_data['highPrice'])
                    low = float(symbol_data['lowPrice'])
                    volatility = ((high - low) / low * 100) if low > 0 else 0
                    logger.debug(f"  波动率: {volatility:.2f}%")
            
            logger.info(f"最终选择 {len(selected_pairs)} 个交易对")
            return selected_pairs
            
        except Exception as e:
            logger.error(f"获取交易对列表失败: {e}")
            return []

    def run(self):
        """
        启动下载器的主入口
        
        执行流程：
        1. 下载所有选定的期货交易对数据
        2. 处理异常情况
        3. 确保程序可以优雅退出
        """
        try:
            # 获取交易对列表
            symbols = self.get_top_futures()
            if not symbols:
                logger.error("获取交易对列表失败")
                return
                
            logger.info(f"获取到 {len(symbols)} 个交易对")
            
            # 下载所有期货数据
            self.download_all_futures()
            
        except Exception as e:
            logger.error(f"程序执行失败: {e}")
        finally:
            exit_event.set()

if __name__ == "__main__":
    downloader = FuturesDownloader()
    downloader.run()