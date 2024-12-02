from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional
import logging
from rich.logging import RichHandler
from rich.console import Console

console = Console()
logger = logging.getLogger("data_downloader")

class BaseDataDownloader(ABC):
    """数据下载器基类"""
    
    def __init__(self, config: dict):
        self.config = config
        self.base_path = Path(config["base_path"])
    
    @abstractmethod
    async def download_bars(self, symbol: str, period: str) -> bool:
        """下载K线数据"""
        pass
    
    @abstractmethod
    async def download_ticks(self, symbol: str) -> bool:
        """下载Tick数据"""
        pass
    
    @abstractmethod
    def validate_data(self, file_path: Path) -> bool:
        """验证数据完整性"""
        pass

class BinanceDownloader(BaseDataDownloader):
    """币安数据下载器"""
    
    def __init__(self, config: dict, market_type: str, data_type: str):
        super().__init__(config)
        self.market_type = market_type
        self.data_type = data_type
        self.downloader_config = config["downloaders"]["crypto"]["binance"]
        
        # 设置请求限制
        self.rate_limit = self.downloader_config["rate_limits"][market_type][data_type]
        
        # 初始化存储路径
        self.storage_path = self.base_path / "storage"
        self.storage_path.mkdir(parents=True, exist_ok=True)