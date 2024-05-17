import json
import pandas as pd
from datetime import datetime
from pandas.core.api import DataFrame as DataFrame
from baseloader import BaseDataLoader
from enum import Enum
import logging

# Налаштування логування
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Створення форматера
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Створення хендлера для запису у файл
file_handler = logging.FileHandler('coinbase_loader.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Додавання хендлера до логера
logger.addHandler(file_handler)

class Granularity(Enum):
    ONE_MINUTE = 60
    FIVE_MINUTES = 300
    FIFTEEN_MINUTES = 900
    ONE_HOUR = 3600
    SIX_HOURS = 21600
    ONE_DAY = 86400

class CoinbaseLoader(BaseDataLoader):

    def __init__(self, endpoint="https://api.exchange.coinbase.com"):
        super().__init__(endpoint)
        logger.info("CoinbaseLoader initialized with endpoint %s", endpoint)

    def get_pairs(self) -> pd.DataFrame:
        logger.debug("Fetching trading pairs")
        try:
            data = self._get_req("/products")
            df = pd.DataFrame(json.loads(data))
            df.set_index('id', drop=True, inplace=True)
            logger.info("Successfully fetched trading pairs")
            return df
        except Exception as e:
            logger.error("Error fetching trading pairs: %s", e)
            raise

    def get_stats(self, pair: str) -> pd.DataFrame:
        logger.debug("Fetching stats for pair %s", pair)
        try:
            data = self._get_req(f"/products/{pair}")
            logger.info("Successfully fetched stats for pair %s", pair)
            return pd.DataFrame(json.loads(data), index=[0])
        except Exception as e:
            logger.error("Error fetching stats for pair %s: %s", pair, e)
            raise

    def get_historical_data(self, pair: str, begin: datetime, end: datetime, granularity: Granularity) -> DataFrame:
        logger.debug("Fetching historical data for pair %s from %s to %s with granularity %s",
                     pair, begin, end, granularity)
        try:
            params = {
                "start": begin.isoformat(),
                "end": end.isoformat(),
                "granularity": granularity.value
            }
            # retrieve needed data from Coinbase
            data = self._get_req("/products/" + pair + "/candles", params)
            # parse response and create DataFrame from it
            df = pd.DataFrame(json.loads(data),
                              columns=("timestamp", "low", "high", "open", "close", "volume"))
            # use timestamp column as index
            df.set_index('timestamp', drop=True, inplace=True)
            logger.info("Successfully fetched historical data for pair %s", pair)
            return df
        except Exception as e:
            logger.error("Error fetching historical data for pair %s: %s", pair, e)
            raise

if __name__ == "__main__":
    loader = CoinbaseLoader()
    data = loader.get_pairs()
    print(data)
    data = loader.get_stats("btc-usdt")
    print(data)
    data = loader.get_historical_data("btc-usdt", datetime(2023, 1, 1), datetime(2023, 6, 30), granularity=Granularity.ONE_DAY)
    print(data.head(5))
