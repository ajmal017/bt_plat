import pandas as pd
import numpy as np
import os
import abc
import logging
import traceback

# own files
from Backtest.indicators import SMA
from Backtest.data_reader import DataReader
from auto_trading.automated_trading import _setup_log
# import database_stuff as db
from Backtest import config
from Backtest import Settings

# for testing
from datetime import datetime as dt

#############################################
# Data reading
# Construct indicator
# Generate signals
# Find entries and exits
# Calculate portfolio value
# Generate portfolio statistics
#############################################

# TODO
# change how self.port.invested is implemented. Currently invests given ammount without round downs
# find common index for portflio. Should be a range of datetimes. Currently useing self.agg_trans_prices.priceFluctuation_dollar.index
# need to set/pass configs

_setup_log("Backtester")
#############################################
# Core starts
#############################################
class Backtest(abc.ABC):

    def __init__(self, name):
        self.name = name
        # self.data = DataReader()  #data_reader.DataReader()
        self.data = {}
        self.runs_at = dt.now() # for logging and data prep purposes. Gets updated when self.run() is called
        self.port = Portfolio()
        self.agg_trade_signals = Agg_TradeSingal()
        self.agg_trans_prices = Agg_TransPrice()
        self.agg_trades = Agg_Trades()
        self.trade_list = None
        self.settings = Settings
        self.log = logging.getLogger("Backtester")
        self.log.info("Backtester started!")
        self.in_trade = {"long":0, "short":0}
        self.universe_ranking = pd.DataFrame()

    def preprocessing(self, data):
        """
        Called once before running through the data.
        Should be used to generate values that need to be known prior running logic such as ranking among asset classes.
        For example, if we want to know top 10 momentum stocks every month or stocks above 200 MA, this data can be generated at this stage.

        Inputs: self + all data that will be used for the backtest
        """
        pass

    def postprocessing(self, data):
        """
        Called right after logic.
        Should be used to generated values that depend on buy/sell/short/cover signal such as stops/take profits.

        Inputs: self + all data that will be used for the backtest
        """
        pass

    def run(self, data):
        try:   
            self.runs_at = dt.now()
            self._prepare_data(data)
            self.preprocessing(data)
            self._run_portfolio()
        except Exception as e:
            print(e)
            traceback.print_exc()
            self.log.error(e, stack_info=True)
            

    @abc.abstractmethod
    def logic(self, current_asset):
        pass

    def _prepare_data(self, data):
        self.log.info(f"Preparing data for {self.runs_at}")
        for name in data:
            temp = pd.DataFrame(columns=data[name].columns)
            temp.index.name = "Date"
            temp["Open"] = data[name]["Open"].groupby("Date").nth(0)
            temp["High"] = data[name]["High"].groupby("Date").max()
            temp["Low"] = data[name]["Low"].groupby("Date").min()
            temp["Close"] = data[name]["Close"].groupby("Date").nth(-1)

            # TODO:
            # volume need to be change for forex, etc cuz gives volume of -1
            # because of that, summing volume will produce wrong result
            temp["Volume"] = data[name]["Volume"].groupby("Date").sum()

            # getting all but last candle. This is done to avoid incomplete bars at runtime
            if pd.Timestamp(self.runs_at.replace(second=0, microsecond=0)) == temp.iloc[-1].name:
                temp = temp.loc[:self.runs_at].iloc[:-1]
                self.log.warning(f"Last bars for {name} {self.runs_at} were cut during data prep")
            self.data[name] = temp     
            

    def _prepricing(self, data):
        """
        Loop through files
        Generate signals
        Find transaction prices
        Match buys and sells
        Save them into common classes agg_*
        """
                                                           
        # for name in self.data:
        current_asset = self.data[name]
        
        # strategy logic
        # buyCond, sellCond, shortCond, coverCond = self.logic(current_asset)
        self.cond = Cond()
        # self.logic(name)
        # self.postprocessing(name)
        self.cond.buy.name, self.cond.sell.name, self.cond.short.name, self.cond.cover.name = ["Buy", "Sell", "Short", "Cover"]
        self.cond._combine() # combine all conds into all
        # if buyCond is None and shortCond is None:
        #     raise Exception("You have to specify buy or short condition. Neither was specified.")
        ################################

        rep = Repeater(current_asset, name, self.cond.all)

        # find trade_signals and trans_prices for an asset
        trade_signals = TradeSignal(rep)
        trans_prices = TransPrice(rep, trade_signals)
        trades_current_asset = Trades(rep, trade_signals, trans_prices)

        # save trade_signals for portfolio level
        # self.agg_trade_signals.buys = self._aggregate(self.agg_trade_signals.buys, trade_signals.buyCond)
        # self.agg_trade_signals.sells = self._aggregate(self.agg_trade_signals.sells, trade_signals.sellCond)
        # self.agg_trade_signals.shorts = self._aggregate(self.agg_trade_signals.shorts, trade_signals.shortCond)
        # self.agg_trade_signals.covers = self._aggregate(self.agg_trade_signals.covers, trade_signals.coverCond)
        # self.agg_trade_signals.all = self._aggregate(self.agg_trade_signals.all, trade_signals.all)

        # save trans_prices for portfolio level
        self.agg_trans_prices.buyPrice = self._aggregate(self.agg_trans_prices.buyPrice, trans_prices.buyPrice)
        self.agg_trans_prices.sellPrice = self._aggregate(self.agg_trans_prices.sellPrice, trans_prices.sellPrice)
        self.agg_trans_prices.shortPrice = self._aggregate(self.agg_trans_prices.shortPrice, trans_prices.shortPrice)
        self.agg_trans_prices.coverPrice = self._aggregate(self.agg_trans_prices.coverPrice, trans_prices.coverPrice)
        self.agg_trades.priceFluctuation_dollar = self._aggregate(self.agg_trades.priceFluctuation_dollar,
                                                                trades_current_asset.priceFluctuation_dollar)
        self.agg_trades.trades = self._aggregate(self.agg_trades.trades, trades_current_asset.trades, ax=0)
        # self.agg_trades.inTradePrice = self._aggregate(self.agg_trades.inTradePrice, trades_current_asset.inTradePrice)

    @staticmethod
    def _aggregate(agg_df, df, ax=1):
        return pd.concat([agg_df, df], axis=ax)        

    def _run_portfolio(self):
        """
        Calculate profit and loss for the stretegy
        """
        # prepare data for portfolio
        self._prepricing()

        # prepare portfolio level
        # copy index and column names for weights
        self.port.weights = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=self.agg_trades.priceFluctuation_dollar.columns)
        self.port.weights.iloc[0] = 0  # set starting weight to 0
        self.port.weights = self.port.weights.astype(np.float)

        # nan in the beg cuz of .shift while finding priceFluctuation
        # to avoid nan in the beg
        self.agg_trades.priceFluctuation_dollar.iloc[0] = 0

        # Fill with 0s, otherwise results in NaN for self.port.avail_amount
        # self.agg_trades.priceChange.fillna(0, inplace=True)

        # prepare value, avail amount, invested
        # copy index and column names for portfolio change
        self.port.value = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
             columns=["Portfolio value"])
        self.port.value.iloc[0] = self.settings.start_amount

        # copy index and set column name for avail amount
        self.port.avail_amount = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=["Available amount"])
        self.port.avail_amount.iloc[0] = self.settings.start_amount
        # self.port.avail_amount.ffill(inplace=True)

        # copy index and column names for invested amount
        self.port.invested = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=self.port.weights.columns)
        self.port.invested.iloc[0] = 0

        self.agg_trades.in_trade_price_fluc = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=self.port.weights.columns
        )
        # put trades in chronological order
        # self.agg_trades.trades.sort_values("Date_entry", inplace=True)
        # self.agg_trades.trades.reset_index(drop=True, inplace=True)
        # trades_current_asset.trades.sort_values("Date_entry", inplace=True)
        # trades_current_asset.trades.reset_index(drop=True, inplace=True)

        # ! doesnt do anything
        # change column names to avoid error
        # self.agg_trans_prices.buyPrice.columns = self.port.weights.columns
        # self.agg_trans_prices.sellPrice.columns = self.port.weights.columns

        # run portfolio level
        # allocate weights
        for current_bar, row in self.port.avail_amount.iterrows():
            # weight = self.port value / entry
            prev_bar = self.port.avail_amount.index.get_loc(current_bar) - 1

            # not -1 cuz it will replace last value
            if prev_bar != -1:
                # update avail amount (roll)
                _roll_prev_value(self.port.avail_amount, current_bar, prev_bar)

                # update port value (roll)
                _roll_prev_value(self.port.value, current_bar, prev_bar)

                # update invested amount (roll)
                _roll_prev_value(self.port.invested, current_bar, prev_bar)

                # update weight anyway cuz if buy, the wont roll for other stocks (roll)
                _roll_prev_value(self.port.weights, current_bar, prev_bar)

            # if there was an entry on that date
            # allocate weight
            # update avail amount (subtract)
            self._execute_trades(current_bar)            

            # POST STEPS
            # record unrealized gains/losses
            self.agg_trades.in_trade_price_fluc.loc[current_bar] = (self.agg_trades.priceFluctuation_dollar.loc[
                current_bar] * self.port.weights.loc[current_bar])

            # update avail amount for day's gain/loss            
            self._update_for_fluct(self.port.avail_amount, self.agg_trades.in_trade_price_fluc, current_bar, prev_bar)
            self._update_for_fluct(self.port.value, self.agg_trades.in_trade_price_fluc, current_bar, prev_bar)

        self._generate_equity_curve()
        self._generate_trade_list()

    def _execute_buy(self, current_bar):
        self.in_trade["long"] = 1
        # find amount to be invested
        # to_invest = self.port.avail_amount.loc[
        #     current_bar, "Available amount"] * self.settings.pct_invest
        to_invest = self.port.value.loc[current_bar, "Portfolio value"
            ] * self.settings.pct_invest

        # find assets that need allocation
        # those that dont have buyPrice for that day wil have NaN
        # drop them, keep those that have values
        affected_assets = self.agg_trans_prices.buyPrice.loc[
            current_bar].dropna().index.values

        # find current bar, affected assets
        # allocate shares to all assets = invested amount/buy price
        rounded_weights = to_invest / self.agg_trans_prices.buyPrice.loc[
            current_bar, affected_assets]
        rounded_weights = rounded_weights.mul(
            10**self.settings.round_to_decimals).apply(np.floor).div(
                10**self.settings.round_to_decimals)
        self.port.weights.loc[current_bar, affected_assets] = rounded_weights

        # find actualy amount invested
        # TODO: adjust amount invested. Right now assumes all intended amount is allocated.
        # ? avail amount doesnt get adjusted for daily fluc?
        actually_invested = self.port.weights.loc[
            current_bar, affected_assets] * self.agg_trans_prices.buyPrice.loc[
                current_bar, affected_assets]

        # update portfolio invested amount
        self.port.invested.loc[current_bar, affected_assets] = actually_invested

        # update portfolio avail amount -= sum of all invested money that day
        self.port.avail_amount.loc[current_bar] -= self.port.invested.loc[current_bar, affected_assets].sum()

    def _execute_sell(self, current_bar):
        """
        if there was an exit on that date
        set weight to 0
        update avail amount
        """
        self.in_trade["long"] = 0
        # prob need to change this part for scaling implementation

        # find assets that need allocation
        # those that dont have sellPrice for that day wil have NaN
        # drop them, keep those that have values
        affected_assets = self.agg_trans_prices.sellPrice.loc[
            current_bar].dropna().index.values
        # amountRecovered = self.port.weights.loc[current_bar, affected_assets] * self.agg_trans_prices.buyPrice2.loc[current_bar, affected_assets]
        self.port.avail_amount.loc[current_bar] += (self.port.weights.loc[
                current_bar, affected_assets] * self.agg_trans_prices.sellPrice.loc[
                current_bar, affected_assets]).sum()

        # set invested amount of the assets to 0
        self.port.invested.loc[current_bar, affected_assets] = 0

        # set weight to 0
        self.port.weights.loc[current_bar, affected_assets] = 0

    def _execute_short(self, current_bar):
        self.in_trade["short"] = 1
        # find amount to be invested
        # to_invest = self.port.avail_amount.loc[
        #     current_bar, "Available amount"] * self.settings.pct_invest
        to_invest = self.port.value.loc[current_bar, "Portfolio value"
            ] * self.settings.pct_invest

        # find assets that need allocation
        # those that dont have shortPrice for that day wil have NaN
        # drop them, keep those that have values
        affected_assets = self.agg_trans_prices.shortPrice.loc[
            current_bar].dropna().index.values

        # find current bar, affected assets
        # allocate shares to all assets = invested amount/buy price
        rounded_weights = to_invest / self.agg_trans_prices.shortPrice.loc[
            current_bar, affected_assets]
        rounded_weights = rounded_weights.mul(
            10**self.settings.round_to_decimals).apply(np.floor).div(
                10**self.settings.round_to_decimals)
        self.port.weights.loc[current_bar, affected_assets] = -rounded_weights

        # find actualy amount invested
        # TODO: adjust amount invested. Right now assumes all intended amount is allocated.
        # ? avail amount doesnt get adjusted for daily fluc?
        actually_invested = self.port.weights.loc[
            current_bar, affected_assets] * self.agg_trans_prices.shortPrice.loc[
                current_bar, affected_assets]

        # update portfolio invested amount
        self.port.invested.loc[current_bar, affected_assets] = actually_invested

        # update portfolio avail amount -= sum of all invested money that day
        self.port.avail_amount.loc[current_bar] += self.port.invested.loc[current_bar, affected_assets].sum()

    def _execute_cover(self, current_bar):
        """
        if there was an exit on that date
        set weight to 0
        update avail amount
        """
        self.in_trade["short"] = 0
        # prob need to change this part for scaling implementation

        # find assets that need allocation
        # those that dont have coverPrice for that day wil have NaN
        # drop them, keep those that have values
        affected_assets = self.agg_trans_prices.coverPrice.loc[
            current_bar].dropna().index.values
        # amountRecovered = self.port.weights.loc[current_bar, affected_assets] * self.agg_trans_prices.buyPrice2.loc[current_bar, affected_assets]
        self.port.avail_amount.loc[current_bar] += (self.port.weights.loc[
                current_bar, affected_assets] * self.agg_trans_prices.coverPrice.loc[
                current_bar, affected_assets]).sum()

        # set invested amount of the assets to 0
        self.port.invested.loc[current_bar, affected_assets] = 0

        # set weight to 0
        self.port.weights.loc[current_bar, affected_assets] = 0

    def _execute_trades(self, current_bar):
        if (current_bar in self.agg_trans_prices.buyPrice.index) and (self.in_trade["long"]==0):
            self._execute_buy(current_bar)            

        if (current_bar in self.agg_trans_prices.sellPrice.index) and (self.in_trade["long"]==1):
            self._execute_sell(current_bar)

        if (current_bar in self.agg_trans_prices.shortPrice.index) and (self.in_trade["short"]==0):
            self._execute_short(current_bar)

        if (current_bar in self.agg_trans_prices.coverPrice.index) and (self.in_trade["short"]==1):
            self._execute_cover(current_bar)

    def _check_trade_list(self):
        pass

    def _update_for_fluct(self, df, in_trade_adjust, current_bar, prev_bar):
        """
        Update for today's gains and losses
        """        
        # By default does not record daily's P&L when stock position is closed that day
        # This happens because sell/cover execution comes before _update_for_fluct
        # Hence in_trade_adjust.loc[current_bar].sum() == 0 for the stocks that were closed
        # because of this we need to manually adj P&L for that day
        df.loc[current_bar] += in_trade_adjust.loc[current_bar].sum()

        if current_bar in self.agg_trans_prices.buyPrice.index:
            # if buy_on close, then should not record today's gains/losses
            if Settings.buy_on.capitalize()=="Close":            
                # find assets that were entered today
                affected_assets = self.agg_trans_prices.buyPrice.loc[current_bar].dropna().index.values
                # deduct the amount for that asset
                df.loc[current_bar] -= in_trade_adjust.loc[current_bar, affected_assets].sum()

        if current_bar in self.agg_trans_prices.shortPrice.index:
            # if short_on close, then should not record today's gains/losses
            if Settings.short_on.capitalize()=="Close":
                # find assets that were entered today
                affected_assets = self.agg_trans_prices.shortPrice.loc[current_bar].dropna().index.values
                # deduct the amount for that asset
                df.loc[current_bar] -= in_trade_adjust.loc[current_bar, affected_assets].sum()

        # for position close (sell/cover) we add daily_adj instead of subtracting because of signs of the values 
        # that we get needs is different from what is stored in in_trade_adjust.loc[current_bar, affected_assets]
        if current_bar in self.agg_trans_prices.sellPrice.index:
            # if sell_on close, then should record today's gains/losses
            if Settings.sell_on.capitalize()=="Close":
                # find assets that were entered today
                affected_assets = self.agg_trans_prices.sellPrice.loc[current_bar].dropna().index.values
                # deduct the amount for that asset
                daily_adj = (self.port.weights[affected_assets].iloc[prev_bar] * 
                                self.agg_trades.priceFluctuation_dollar.loc[current_bar, affected_assets]).sum()
                df.loc[current_bar] += daily_adj

        if current_bar in self.agg_trans_prices.coverPrice.index:            
            # if cover_on open, then should record today's gains/losses
            if Settings.cover_on.capitalize()=="Close": 
                # find assets that were entered today
                affected_assets = self.agg_trans_prices.coverPrice.loc[current_bar].dropna().index.values
                # deduct the amount for that asset
                daily_adj = (self.port.weights[affected_assets].iloc[prev_bar] * 
                                self.agg_trades.priceFluctuation_dollar.loc[current_bar, affected_assets]).sum()
                df.loc[current_bar] += daily_adj

    def _generate_trade_list(self):
        self.trade_list = self.agg_trades.trades.copy()
        self.trade_list["Date_exit"] = self.trade_list["Date_exit"].astype(str)
        self.trade_list.sort_values(by=["Date_exit", "Date_entry", "Symbol"], inplace=True)
        self.trade_list.reset_index(drop=True, inplace=True)

        # assign weights
        self.trade_list["Weight"] = np.NAN
        weight_list = self.trade_list.Symbol.unique()
        for asset in weight_list:
            # find all entry dates for an asset
            dates = self.trade_list[self.trade_list["Symbol"] ==
                                    asset]["Date_entry"]
            # save index of the dates in trade_list for further concat
            idx = self.trade_list[self.trade_list["Symbol"] ==
                                  asset]["Date_entry"].index
            # grab all weights for the asset on entry date
            # ? might have probems with scaling (probably will)
            weights = self.port.weights[asset].loc[dates]

            self.trade_list.loc[idx, "Weight"] = weights.values
        # change values to display positive for short trades (isntead of negative shares)
        self.trade_list["Weight"] = np.where(self.trade_list.Direction=="Long", 
                                    self.trade_list["Weight"], -self.trade_list["Weight"])

        # $ change
        self.trade_list["Dollar_change"] = self.trade_list["Exit_price"] - self.trade_list["Entry_price"]

        # % change
        self.trade_list["Pct_change"] = (self.trade_list["Exit_price"] -
                                        self.trade_list["Entry_price"]) / self.trade_list["Entry_price"]

        # $ profit
        self.trade_list["Dollar_profit"] = self.trade_list["Weight"] * self.trade_list["Dollar_change"]
        self.trade_list["Dollar_profit"] = np.where(self.trade_list.Direction=="Long", 
                                        self.trade_list["Dollar_profit"], -self.trade_list["Dollar_profit"])
        
        # % profit
        self.trade_list["Pct_profit"] = np.where(self.trade_list.Direction=="Long", 
                                        self.trade_list["Pct_change"], -self.trade_list["Pct_change"])
        # cum profit
        self.trade_list["Cum_profit"] = self.trade_list["Dollar_profit"].cumsum()

        # Port value
        self.trade_list["Portfolio_value"] = self.trade_list["Dollar_profit"].cumsum()
        self.trade_list["Portfolio_value"] += self.settings.start_amount

        # Position value
        self.trade_list["Position_value"] = self.trade_list["Weight"] * self.trade_list["Entry_price"]

        # # bars held
        temp = pd.to_datetime(self.trade_list["Date_exit"], errors="coerce")
        self.trade_list["Trade_duration"] = temp - self.trade_list["Date_entry"]
        self.trade_list["Trade_duration"].fillna("Open", inplace=True)

    def _generate_equity_curve(self):
        # Fillna cuz
        self.agg_trades.priceFluctuation_dollar.fillna(0, inplace=True)

        # find daily fluc per asset
        self.port.profit_daily_fluc_per_asset = self.port.weights * \
            self.agg_trades.priceFluctuation_dollar

        # find daily fluc for that day for all assets (sum of fluc for that day)
        self.port.equity_curve = self.port.profit_daily_fluc_per_asset.sum(1)

        # set starting amount
        self.port.equity_curve.iloc[0] = self.settings.start_amount

        # apply fluctuation to equity curve
        self.port.equity_curve = self.port.equity_curve.cumsum()
        self.port.equity_curve.name = "Equity"

class TradeSignal:
    """
    Find trade signals for current asset
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results

    Inputs:
        - buyCond (from repeater): raw, contains all signals
        - sellCond (from repeater): raw, contains all signals
    Output:
        - buyCond: buy results where signals switch from 1 to 0, 0 to 1, etc
        - sellCond: sell results where signals switch from 1 to 0, 0 to 1, etc
        - all: all results with Buy and Sell
    """

    # not using self because the need to pass buy and sell cond
    # buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
    # sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)

    def __init__(self, rep):
        # buy/sell/short/cover/all signals
        self.buyCond = _find_signals(rep.allCond["Buy"])
        self.shortCond = _find_signals(rep.allCond["Short"])

        # keeping it here for now
        from Backtest.indicators import ATR
        atr = ATR(rep.data, 14)

        self._apply_stop("buy", self.buyCond, rep, atr()*2)
        self._apply_stop("short", self.shortCond, rep, atr()*2)

        self.sellCond = _find_signals(rep.allCond["Sell"])
        self.coverCond = _find_signals(rep.allCond["Cover"])

        # self.buyCond.name = "Buy"
        # self.sellCond.name = "Sell"
        # self.shortCond.name = "Short"
        # self.coverCond.name = "Cover"

        # delay implementation
        self._buy_shift = self.buyCond.shift(Settings.buy_delay)
        self._sell_shift = self.sellCond.shift(Settings.sell_delay)
        self._short_shift = self.shortCond.shift(Settings.short_delay)
        self._cover_shift = self.coverCond.shift(Settings.cover_delay)

        self.all = pd.concat([self._buy_shift, self._sell_shift, self._short_shift, 
                            self._cover_shift], axis=1)
        self.all.index.name = "Date"
        # might be a better solution cuz might not create copy - need to test it
        # taken from https://stackoverflow.com/questions/53608501/numpy-pandas-remove-sequential-duplicate-values-equivalent-of-bash-uniq-withou?noredirect=1&lq=1
        #         self.buyCond2 = rep.buyCond.where(rep.buyCond.ne(rep.buyCond.shift(1).fillna(rep.buyCond[0]))).shift(1)
        #         self.sellCond2 = rep.sellCond.where(rep.sellCond.ne(rep.sellCond.shift(1).fillna(rep.sellCond[0]))).shift(1)
        # ! In case of buy and sell signal occuring on the same candle, Sell/Cover signal is prefered over Buy/Short
        # ! might create signal problems in the future
        cond = [(self._sell_shift == 1), (self._buy_shift == 1)]
        out = ["Sell", "Buy"]
        self.long = self._merge_signals(cond, out, rep, self._buy_shift, "Long")

        cond = [(self._cover_shift == 1), (self._short_shift == 1)]
        out = ["Cover", "Short"]
        self.short = self._merge_signals(cond, out, rep, self._short_shift, "Short")

        self.all_merged = pd.concat([self.long, self.short], axis=1)
        self.all_merged.index.name = "Date"
    
        # # remove all extra signals
        # # https://stackoverflow.com/questions/19463985/pandas-drop-consecutive-duplicates
        # # alternative, possibly faster solution ^
        # # or using pd.ne()
        # # or self.all = self.all[self.all != self.all.shift()]
        # self.all = _remove_dups(self.all)
    
    def _apply_stop(self, buy_or_short, cond, rep, ind):
        if buy_or_short == "buy":
            temp_ind = rep.data["Close"] - ind 
        elif buy_or_short == "short":
            temp_ind = rep.data["Close"] + ind 

        stops = pd.DataFrame(index=rep.data.index)
        temp = temp_ind[cond==1]
        temp.name = "ATR"
        stops = stops.join(temp)
        stops = stops.ffill()
        stops = stops["ATR"]

        # update sell/cover cond
        if buy_or_short == "buy":
            temp_cond = rep.data.Close < stops
            rep.allCond["Sell"] = temp_cond | rep.allCond["Sell"]
        elif buy_or_short == "short":
            temp_cond = rep.data.Close > stops
            rep.allCond["Cover"] = temp_cond | rep.allCond["Cover"]
        
        
    @staticmethod
    def _merge_signals(cond, out, rep, entry, col_name):        
        df = np.select(cond, out, default=0)
        df = pd.DataFrame(df, index=rep.data.index, columns=[col_name])
        df = df.replace("0", np.NAN)

        # find where first buy occured
        temp = entry.dropna()
        if not temp.empty:
            first_entry = temp.index[0]

            df = df[first_entry:]
            df = _remove_dups(df)
        
        return df
            


class Agg_TradeSingal:
    """
    Aggregate version of TradeSignal that keeps trade signals for all stocks
    """

    def __init__(self):
        self.buys = pd.DataFrame()
        self.sells = pd.DataFrame()
        self.shorts = pd.DataFrame()
        self.covers = pd.DataFrame()
        self.all = pd.DataFrame()


class TransPrice:
    """
    Looks up transaction price for current asset

    Inputs:
        - time series (from repeater)
        - trade_singals: DataFrame containing all buys and sells
        - buyOn (optional): column that should be looked up when buy occurs
        - sellOn (optional): column that should be looked up when sell occurs

    Output:
        - buyPrice: used in Trades to generate trade list
        - sellPrice: used in Trades to generate trade list 
        - buyIndex: dates of buyPrice
        - sellIndex: dates of sellPrice
    """

    def __init__(self, rep, trade_signals):
        self.all = trade_signals.all_merged

        # to get rid of duplicates
        # self.all = self.all.dropna()
        # self.all = _remove_dups(self.all)
        # self.all = self.all.where(self.all != self.all.shift(1))

        self.buyIndex = self.all[self.all["Long"]=="Buy"].index
        self.sellIndex = self.all[self.all["Long"]=="Sell"].index
        self.shortIndex = self.all[self.all["Short"]=="Short"].index
        self.coverIndex = self.all[self.all["Short"]=="Cover"].index

        self.buyPrice = rep.data[Settings.buy_on][self.buyIndex]
        self.sellPrice = rep.data[Settings.sell_on][self.sellIndex]
        self.shortPrice = rep.data[Settings.short_on][self.shortIndex]
        self.coverPrice = rep.data[Settings.cover_on][self.coverIndex]

        self.buyPrice.name = rep.name
        self.sellPrice.name = rep.name
        self.shortPrice.name = rep.name
        self.coverPrice.name = rep.name


class Agg_TransPrice:
    """
    Aggregate version of TransPrice that keeps transaction price for all stocks
    """

    def __init__(self):
        self.buyPrice = pd.DataFrame()
        self.sellPrice = pd.DataFrame()
        self.shortPrice = pd.DataFrame()
        self.coverPrice = pd.DataFrame()

class Trades:
    """
    Generates DataFrame with trade entries, exits, and transaction prices

    Inputs:
        - trade_signals: Raw buys and sells to find trade duration
        - trans_prices: Transaction prices that need to be matched
    Outputs:
        - trades: DataFrame with trade entry, exits, and transaction prices
        - inTrade: DataFrame that shows time spent in trade for current asset
        - inTradePrice: Close price for the time while inTrade
    """

    def __init__(self, rep, trade_signals, trans_prices):
        self.trades = pd.DataFrame()
        self.inTrade = pd.DataFrame()
        self.inTradePrice = pd.DataFrame()

        self.inTrade = trade_signals.all_merged #long, short
        self.inTrade = self.inTrade.ffill()
        self.inTrade = self.inTrade[(self.inTrade["Long"] == "Buy") | (self.inTrade["Short"] == "Short")]

        long = trans_prices.buyPrice.reset_index()
        sell = trans_prices.sellPrice.reset_index()
        short = trans_prices.shortPrice.reset_index()
        cover = trans_prices.coverPrice.reset_index()

        long["Direction"] = "Long"
        short["Direction"] = "Short"

        # self.trades = long.join(sell, how="outer", lsuffix="_entry", rsuffix="_exit")
        long = long.join(sell, how="outer", lsuffix="_entry", rsuffix="_exit")
        short = short.join(cover, how="outer", lsuffix="_entry", rsuffix="_exit")

        self.trades = pd.concat([long, short])
        # NAs should only be last values that are still open
        # self.trades["Date_exit"].fillna(rep.data.iloc[-1].name, inplace=True)
        self.trades["Date_exit"].fillna("Open", inplace=True)

        # hardcoded Close cuz if still in trade, needs latest quote
        self.trades[trans_prices.sellPrice.name + "_exit"].fillna(
            rep.data.iloc[-1]["Close"], inplace=True)
        # alternative way
        #         u = self.trades.select_dtypes(exclude=['datetime'])
        #         self.trades[u.columns] = u.fillna(4)
        #         u = self.trades.select_dtypes(include=['datetime'])
        #         self.trades[u.columns] = u.fillna(5)

        self.trades["Symbol"] = rep.name

        # changing column names so it's easier to concat in self.agg_trades
        self.trades.rename(
            columns={
                rep.name + "_entry": "Entry_price",
                rep.name + "_exit": "Exit_price",
            },
            inplace=True)

        self.inTradePrice = rep.data["Close"].loc[self.inTrade.index]
        self.inTradePrice.name = rep.name

        # finding dollar price change
        # ? use inTradePrice - inTradePrice.shift(1) ?
        self.priceFluctuation_dollar = rep.data["Close"] - rep.data["Close"].shift()
        self.priceFluctuation_dollar.name = rep.name


class Agg_Trades:
    """
    Aggregate version of Trades. Contains trades, weights, inTradePrice, priceFluctuation in dollars
    """

    def __init__(self):
        self.trades = pd.DataFrame()
        # self.inTrade = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.inTradePrice = pd.DataFrame()
        self.priceFluctuation_dollar = pd.DataFrame()
        self.in_trade_price_fluc = pd.DataFrame()


# ! not used
class Returns(TransPrice):
    """
    Calculates returns for the strategy
    """

    def __init__(self, rep):
        trans_prices = TransPrice(rep)
        self.index = rep.data.index
        self.returns = pd.DataFrame(index=self.index, columns=[rep.name])
        # might result in errors tradesignal/execution is shifted
        self.returns[rep.name].loc[
            trans_prices.buyPrice.index] = trans_prices.buyPrice
        self.returns[rep.name].loc[
            trans_prices.sellPrice.index] = trans_prices.sellPrice
        self.returns = self.returns.dropna().pct_change()
        # works for now
        for i in self.returns.index:
            if trans_prices.inTrade.loc[i][0] == "Buy":
                self.returns.loc[i] = -self.returns.loc[i]


# ! not used
class Stats:
    """
    Calculate various trade statistics based on returns
    """

    def __init__(self, rep):
        r = Returns(rep)
        self.posReturns = r.returns[r.returns > 0].dropna()
        self.negReturns = r.returns[r.returns < 0].dropna()
        self.posTrades = len(self.posReturns)
        self.negTrades = len(self.negReturns)
        self.meanReturns = r.returns.mean()
        self.hitRatio = self.posTrades / (self.posTrades + self.negTrades)
        self.totalTrades = self.posTrades + self.negTrades


class Portfolio:
    """
    Initial settings and what to calculate
    """

    def __init__(self):
        self.weights = pd.DataFrame()
        self.value = pd.DataFrame()
        self.profit = pd.DataFrame()
        self.invested = pd.DataFrame()
        self.fees = pd.DataFrame()
        self.ror = pd.DataFrame()
        self.capUsed = pd.DataFrame()
        self.equity_curve = pd.DataFrame()

        self.start_amount = Settings.start_amount
        self.avail_amount = self.start_amount


class Repeater:
    """
    Common class to avoid repetition
    """

    def __init__(self, data, name, allCond):
        self.data = data
        # self.buyCond = buyCond
        # self.sellCond = sellCond
        # self.shortCond = shortCond
        # self.coverCond = coverCond
        self.name = name
        self.allCond = allCond

class Cond:
    def __init__(self):
        self.buy = pd.DataFrame()
        self.sell = pd.DataFrame()
        self.short = pd.DataFrame()
        self.cover = pd.DataFrame()
        self.all = pd.DataFrame()

    def _combine(self):
        for df in [self.buy, self.sell, self.short, self.cover]:
            self.all = self.all.append(df)
        self.all = self.all.T

        for df in [self.buy, self.sell, self.short, self.cover]:
            if df.name not in self.all.columns:
                self.all[df.name] = False

        # self.all = pd.concat([self.buy, self.sell, self.short, self.cover], axis=1)


# ? ##################
# ? Helper functions #
# ? ##################
def _roll_prev_value(df, current_bar, prev_bar):
    # might wanna return, just to be sure?
    df.loc[current_bar] = df.iloc[prev_bar]
    

def _remove_dups(data):
    data = data.ffill()
    data = data.where(data != data.shift(1))
    return data

def _find_signals(df):
    return df.where(df != df.shift(1).fillna(df[0])).shift(0)

if __name__ == "__main__":
    print("=======================")
    print("Run from main.py file!")
    print("=======================")
    Settings.read_from_csv_path = r"E:\Windows\Documents\bt_plat\stock_data\AA.csv"
    Settings.read_from = "csvFile"
    Settings.buy_delay = 0
    Settings.sell_delay = 0

    class Strategy(Backtest):
        def logic(self, current_asset):
            
            sma5 = SMA(current_asset, ["Close"], 5)
            sma25 = SMA(current_asset, ["Close"], 25)

            buyCond = sma5() > sma25()
            sellCond = sma5() < sma25()
            
            shortCond = sma5() < sma25()
            coverCond = sma5() > sma25()

            return buyCond, sellCond, shortCond, coverCond
    
    s = Strategy("name")
    data = DataReader()
    data.readCSV(Settings.read_from_csv_path)
    s.run(data.data)
    # s.trade_list.to_csv("test.csv")
    # print(s.trade_list)
    # b = Backtest("Strategy 1")
    # # b.read_from_db
    # # strategy logic
    # b.run()
    # print(b.trade_list)
    # # b.show_results
