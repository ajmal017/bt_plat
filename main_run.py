import Backtest.platform_core as bt
import Backtest.Settings as Settings
from Backtest.indicators import SMA
from auto_trading import automated_trading as at
from Backtest.data_reader import DataReader

from ibapi.contract import Contract
from ibapi.order import Order

import threading
import json
from datetime import datetime as dt
import time

def cancelOpenPositions():
    app.reqPositions()

    while not app.open_positions_received:
        time.sleep(0.5)

    for ix, pos in app.open_positions.iterrows():
        app.placeOrder(app.nextOrderId(), at.IBContract.forex(file["forex"][pos["symbol_currency"]]), at.IBOrder.MarketOrder("SELL", pos["quantity"]))

def submit_orders(trades):
    app.reqPositions()
    app.reqOpenOrders()
    bt_orders = trades[trades["Date_exit"] == "Open"]

    while (not app.open_orders_received) and (not app.open_positions_received):
        time.sleep(0.5)
        
    current_orders = app.open_orders
    current_positions = app.open_positions[app.open_positions["quantity"] != 0] # also shows closed positions with quantity 0 for some reason 

    if len(bt_orders) == 0:
        # close all positions and orders
        app.reqGlobalCancel() #Cancels all active orders. This method will cancel ALL open orders including those placed directly from TWS. 
        cancelOpenPositions()
    else:
        # buy logic
        for ix, order in bt_orders.iterrows():
            # _buy_or_sell = "BUY" if order["Direction"] == "Long" else "SELL"
            asset = order["Symbol"]
            # _asset = order["Symbol"].split(".")
            # _asset = "".join(_asset)
            
            # simple check to see if current order has already been submitted
            if (asset not in current_orders["symbol_currency"].values) and (asset not in current_positions["symbol_currency"].values):
                app.placeOrder(app.nextOrderId(), at.IBContract.forex(file["forex"][asset]), at.IBOrder.MarketOrder("BUY", order["Position_value"]))
        # sell logic
        for order in current_positions.iterrows():
            asset = order["Symbol"]
            if (asset not in bt_orders["Symbol"].values) or (asset not in current_orders["symbol_currency"].values):
                app.placeOrder(app.nextOrderId(), at.IBContract.forex(file["forex"][asset]), at.IBOrder.MarketOrder("SELL", order["quantity"]))

def run_every_min(data):
    prev_min = None
    while True:
        now = dt.now()
        recent_min = now.minute
        if now.second == 5 and recent_min != prev_min:
            prev_min = recent_min
            # TODO: replaced hardcoded Strategy. Gotta find a way to invoke new object each run/appen to previoius one (prob better)
            strat = Strategy("Test_SMA") # gotta create new object, otherwise it duplicates previous results
            print("Running strategy")
            strat.run(data)
            submit_orders(strat.trade_list)


if __name__ == "__main__":
    # Settings.read_from_csv_path = r"E:\Windows\Documents\bt_plat\stock_data\XOM.csv"
    # Settings.read_from = "csvFile"
    
    # Settings.buy_delay = 0
    # Settings.sell_delay = 0

    class Strategy(bt.Backtest):
        def logic(self, current_asset):
            
            sma5 = SMA(current_asset, ["Close"], 5)
            sma25 = SMA(current_asset, ["Close"], 25)

            buyCond = sma5() > sma25()
            sellCond = sma5() < sma25()
            
            shortCond = None
            coverCond = None

            return buyCond, sellCond, shortCond, coverCond
    
    # s = Strategy("Test_SMA")
    # s.run()
    # s.trade_list = s.trade_list.round(2)
    # s.trade_list.to_csv("trades.csv")
    # s.port.value.round(2).to_csv("avail_amount.csv")

    # data = DataReader()

    with open(Settings.path_to_mapping, "r") as f:
        file = json.loads(f.read())

    app = at.IBApp()
    app.connect("127.0.0.1", 7497, 0) #4002 for gateway, 7497 for TWS
    app.start()
    #app.reqAccountSummary(9006, "All", "AccountType")

    # app.reqIds(-1)
    #app.placeOrder(app.nextOrderId(), at.IBContract.EurGbpFx(), at.IBOrder.MarketOrder("BUY", 500))
    # app.nextOrderId()
    # app.placeOrder(app.nextOrderId(), at.IBContract.EurGbpFx(), at.IBOrder.LimitOrder("BUY", 500, 0.85))
    # app.placeOrder(app.nextOrderId(), at.IBContract.EurGbpFx(), at.IBOrder.Stop("BUY", 500, 0.87))
    # print(app.nextValidOrderId)
    # app.reqOpenOrders()
    # app.cancelOrder(10)
    # app.reqPositions()
    app.reqHistoricalData(reqId=app.nextOrderId(), 
                        contract=at.IBContract.forex(file["forex"]["EUR.GBP"]),
                        #endDateTime="20191125 00:00:00",
                        endDateTime="",
                        durationStr="1 D", 
                        barSizeSetting="1 min",
                        whatToShow="MIDPOINT",
                        useRTH=1,
                        formatDate=1,
                        keepUpToDate=True,
                        chartOptions=[]
                        )
    app.reqHistoricalData(reqId=app.nextOrderId(), 
                        contract=at.IBContract.forex(file["forex"]["EUR.USD"]),
                        #endDateTime="20191125 00:00:00",
                        endDateTime="",
                        durationStr="1 D", 
                        barSizeSetting="1 min",
                        whatToShow="MIDPOINT",
                        useRTH=1,
                        formatDate=1,
                        keepUpToDate=True,
                        chartOptions=[]
                        )
    # print(app.data_tracker)
    # print(app.data)
    # s.run(app.data)
    run_every_min(app.data)
