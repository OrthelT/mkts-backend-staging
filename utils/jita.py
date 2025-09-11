import requests
import pandas as pd
from config.config import DatabaseConfig

class JitaPrice:
    def __init__(self, type_id: int, price_data: dict):
        self.type_id = type_id
        self.buy_percentile = float(price_data['buy']['percentile'])
        self.buy_median = float(price_data['buy']['median'])
        self.buy_min = float(price_data['buy']['min'])
        self.sell_percentile = float(price_data['sell']['percentile'])
        self.sell_median = float(price_data['sell']['median'])
        self.sell_max = float(price_data['sell']['max'])
        self.sell_min = float(price_data['sell']['min'])
        self.sell_volume = float(price_data['sell']['volume'])
        self.buy_volume = float(price_data['buy']['volume'])
        self.buy_weightedAverage = float(price_data['buy']['weightedAverage'])

    def get_price_data(self) -> dict:
        return {
            'type_id': self.type_id,
            'sell_percentile': self.sell_percentile,
            'buy_percentile': self.buy_percentile
        }


# def get_fuzz_prices(station_id: int, type_ids: list[int]) -> dict:
#     fuzz_url = f"https://market.fuzzwork.co.uk/aggregates/?station={station_id}&types={','.join(map(str, type_ids))}"
#     response = requests.get(fuzz_url)
#     return response.json()

# def get_jita_prices(type_ids: list[int]) -> dict:
#     jita_prices = get_fuzz_prices(type_ids)
#     print(jita_prices)



if __name__ == "__main__":
    with __name__ == "__main__":
        pass
