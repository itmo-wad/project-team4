# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
import pandas as pd
import pytz
from datetime import datetime
import asyncio, json
import websockets
import config
from pymongo import MongoClient

# from telegram import InlineKeyboardButton, InlineKeyboardMarkup

DEVELOPER_ID = '-1001786996731'
PUBLIC_ID = '@chuyentienspb'
CTV_ID = '-1001503353838'
TIMEZONE = pytz.timezone('Europe/Moscow')
USER_DB_DIR = config.USER_DB_DIR
DATABASE_URI = config.DATABASE_URI


async def broadcast(data):
    async with websockets.connect("ws://localhost:8000/price/") as websocket:
        await websocket.send(data)
        await websocket.recv()

def broadcast_to_ws(data):
    asyncio.run(broadcast(json.dumps(data)))

class AutoexchangeratePipeline:
    data = []
    prices = {}

    def open_spider(self, spider):
        # Connect to database
        client = MongoClient(DATABASE_URI)
        db = client['project']
        self.db_price = db['price']
        self.vnd2rub_profit = 0.2
        self.rub2vnd_profit = 0.2
        logging.info('Connected to database!')

    def process_item(self, item, spider):
        # print(item)
        self.data.append(item)
        # return item

    def get_ctv_price(self):
        # Định nghĩa
        base_SELL_USD_RUB = self.prices[('VTBank24', 'SELL', 'USD', 'RUB')]['Price']
        base_BUY_USD_RUB = self.prices[('VTBank24', 'BUY', 'USD', 'RUB')]['Price']
        base_SELL_USD_VND = self.prices[('VietcomBank', 'BUY', 'USD', 'VND')]['Price']
        base_BUY_USD_VND = self.prices[('VietcomBank', 'SELL', 'USD', 'VND')]['Price']
        binance_SELL_USDT_RUB = self.prices[('binance', 'SELL', 'USDT', 'RUB')]['Price']
        binance_BUY_USDT_RUB = self.prices[('binance', 'BUY', 'USDT', 'RUB')]['Price']
        binance_SELL_USDT_VND = self.prices[('binance', 'SELL', 'USDT', 'VND')]['Price']
        binance_BUY_USDT_VND = self.prices[('binance', 'BUY', 'USDT', 'VND')]['Price']

        # Giá giao dịch
        vnd2rub_vnd = base_BUY_USD_VND + 250
        vnd2rub_rub = vnd2rub_vnd/binance_BUY_USDT_VND*binance_SELL_USDT_RUB*(1-self.vnd2rub_profit/100)

        rub2vnd_vnd = base_SELL_USD_VND
        rub2vnd_rub = rub2vnd_vnd*binance_BUY_USDT_RUB/binance_SELL_USDT_VND/(1-self.rub2vnd_profit/100)

        return vnd2rub_vnd, vnd2rub_rub, rub2vnd_vnd, rub2vnd_rub

    def get_data(self):
        data = {
            'BUY': {
                'RUB': {},
                'VND': {}
            },
            'SELL': {
                'RUB': {},
                'VND': {}
            },
            'vnd2rub': {},
            'rub2vnd': {}
        }
        for key in self.prices.keys():
            data[key[1]][key[3]][key[0]] = self.prices[key]['Price']

        vnd2rub_vnd, vnd2rub_rub, rub2vnd_vnd, rub2vnd_rub = self.get_ctv_price()
        data['vnd2rub']['vnd'] = vnd2rub_vnd
        data['vnd2rub']['rub'] = vnd2rub_rub
        data['rub2vnd']['vnd'] = rub2vnd_vnd
        data['rub2vnd']['rub'] = rub2vnd_rub
        
        data['timestamp'] = datetime.now().strftime("%d %m %Y, %H:%M:%S")
        return data

    def close_spider(self, spider):
        dataframe = pd.DataFrame(self.data)

        grouped_dataframe = dataframe.groupby(['exchange', 'tradeType', 'asset', 'fiat'])
        # print(grouped_dataframe.groups)
        for key in grouped_dataframe.groups:
            ind = grouped_dataframe.groups[key]
            data = dataframe.loc[ind]
            month_order_count = data['monthOrderCount'].quantile(0.5)
            data = data[(data['monthOrderCount'] >= month_order_count) & (data['monthFinishRate'] >= 0.9)]

            self.prices[key] = {
                "Min Price": data['price'].min(),
                "25% Price": data['price'].quantile(0.25),
                "50% Price": data['price'].quantile(0.5),
                "75% Price": data['price'].quantile(0.75),
                "Max Price": data['price'].max(),
                "Price": data['price'].nsmallest(30).max() if key[1] == 'BUY' else data['price'].nlargest(30).min()
            }


        data = self.get_data()
        
        logging.info('Scraped successfully!!!')
        # Disconnect from database
        logging.info('Disconnected from database!')

        # Broadcast to websocket
        
        print(data)
        broadcast_to_ws(data)
        self.db_price.insert_one(data)


    