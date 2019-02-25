import pandas as pd
import numpy as np
import os
import abc

#############################################
# Data reading
#############################################


class DataReader:
    def __init__(self):
        self.data = {}

    def csvFile(self, path):
        assert os.path.isfile(path), "You need to specify a file."
        self.data = pd.read_excel(path, index_col="Date", nrows=100,
                                  names=["Open", "High", "Low", "Close", "Volume"])

    def readFiles(self, path):
        assert os.path.isdir(path), "You need to specify a folder."
        for file in os.listdir(path)[:2]:
            self._fileName = file.split(".txt")[0]
            _temp = pd.read_csv(path+file, nrows=100, index_col="Date/Time")
            _temp.index = pd.to_datetime(_temp.index)
            self.data[self._fileName] = _temp


data = DataReader()

data.readFiles("D:/AmiBackupeSignal/")

# data.data["AAAP"].index

#############################################
# Other
#############################################
# class Benchmark:
#     def __init__(self, data):
#         self.data = data
#         self.dailyRet = data["Close"].pct_change()
#         self.ror = self.dailyRet.cumsum()


# bench = Benchmark(data.data["AAAP"])

#############################################
# Indicators
#############################################

class Indicator(metaclass=abc.ABCMeta):
    """
    Abstract class for an indicator. 
    Requires cols (of data) to be used for calculations
    """

    def __init__(self, cols):
        pass

    @abc.abstractmethod
    def __call__(self):
        pass


class SMA(Indicator):
    """Implementation of Simple Moving Average"""

    def __init__(self, ts, cols, period):
        self.data = ts[cols]
        self.period = period

    def __call__(self):
        self.result = self.data.rolling(self.period).mean()
        # fillna cuz NaNs result from mean() are strings
        self.result.fillna(np.NaN, inplace=True)
        # need to convert dataframe to series for comparison with series
        return pd.Series(self.result["Close"], self.result.index)

#############################################
# Core starts
#############################################


class TradeSignal:
    """
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results
    """
    # not using self because the need to pass buy and sell cond
    # buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
    # sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)

    def __init__(self, rep):
        rep = rep
        # buy/sell/all signals
        self.buyCond = rep.buyCond.where(
            rep.buyCond != rep.buyCond.shift(1).fillna(rep.buyCond[0])).shift(1)
        self.sellCond = rep.sellCond.where(
            rep.sellCond != rep.sellCond.shift(1).fillna(rep.sellCond[0])).shift(1)

        # might be a better solution cuz might not create copy - need to test it
        # taken from https://stackoverflow.com/questions/53608501/numpy-pandas-remove-sequential-duplicate-values-equivalent-of-bash-uniq-withou?noredirect=1&lq=1
#         self.buyCond2 = rep.buyCond.where(rep.buyCond.ne(rep.buyCond.shift(1).fillna(rep.buyCond[0]))).shift(1)
#         self.sellCond2 = rep.sellCond.where(rep.sellCond.ne(rep.sellCond.shift(1).fillna(rep.sellCond[0]))).shift(1)

        cond = [
            (self.buyCond == 1),
            (self.sellCond == 1)
        ]
        out = ["Buy", "Sell"]
        self.all = np.select(cond, out, default=0)
        self.all = pd.DataFrame(
            self.all, index=rep.d.index, columns=[rep.name])
        self.all = self.all.replace("0", np.NAN)

        # find where first buy occured
        first_buy = self.buyCond.dropna().index[0]

        # drop all sell signals that come before first buy
        self.all = self.all[first_buy:]

        # remove all extra signals
        # https://stackoverflow.com/questions/19463985/pandas-drop-consecutive-duplicates
        # alternative, possibly faster solution ^
        # or using pd.ne()
        self.all = self.all[self.all != self.all.shift()]


class Agg_TradeSingal:
    """
    Aggregate version of TradeSignal that keeps trade signals for all stocks
    """

    def __init__(self):
        self.buys = pd.DataFrame()
        self.sells = pd.DataFrame()
        self.all = pd.DataFrame()


class TransPrice:
    def __init__(self, rep, ts, buyOn="Close", sellOn="Close"):
        rep = rep
        self.all = ts.all
        self.buyCond = ts.buyCond
        self.sellCond = ts.sellCond

        buyIndex = self.all[self.all[rep.name] == "Buy"].index
        sellIndex = self.all[self.all[rep.name] == "Sell"].index

        self.buyPrice = rep.d[buyOn][buyIndex]
        self.sellPrice = rep.d[sellOn][sellIndex]

        self.buyPrice.name = rep.name
        self.sellPrice.name = rep.name

        cond = [
            (self.buyCond == 1),
            (self.sellCond == 1)
        ]
        out = ["Buy", "Sell"]
        self.inTrade = np.select(cond, out, default=0)
        self.inTrade = pd.DataFrame(
            self.inTrade, index=rep.d.index, columns=[rep.name])
        self.inTrade = self.inTrade.replace("0", np.NAN)
        self.inTrade = self.inTrade.ffill().dropna()
        self.inTrade = self.inTrade[self.inTrade == "Buy"]

        self.buyPrice.name = "Entry"
        self.sellPrice.name = "Exit"

        df1 = self.buyPrice.reset_index()
        df2 = self.sellPrice.reset_index()

        self.trades = df1.join(
            df2, how="outer", lsuffix="_entry", rsuffix="_exit")

        # self.trades
        # replace hardcoded "Date/Time_exit"
        self.trades["Date/Time_exit"].fillna(rep.d.iloc[-1].name, inplace=True)
        self.trades[self.sellPrice.name].fillna(
            rep.d.iloc[1][sellOn], inplace=True)
        # alternative way
#         u = self.trades.select_dtypes(exclude=['datetime'])
#         self.trades[u.columns] = u.fillna(4)
#         u = self.trades.select_dtypes(include=['datetime'])
#         self.trades[u.columns] = u.fillna(5)

        self.trades["Symbol"] = rep.name

        self.inTradePrice = rep.d["Close"].loc[self.inTrade.index]
        self.inTradePrice.name = rep.name

# # old version
# class TransPrice_1(TradeSignal):
#     # inheriting from tradeSingal cuz of inTrade
#     """
#     Raw transaction price meaning only initial buy and sell prices are recorded without forward fill
#     """

#     def __init__(self, rep, buyOn="Close", sellOn="Close"):
#         # buy price & sell price
#         rep = rep
#         super().__init__(rep)
#         self.buyPrice = rep.d[buyOn][self.buyCond == 1]
#         self.sellPrice = rep.d[sellOn][self.sellCond == 1]

#         self.buyPrice.name = rep.name
#         self.sellPrice.name = rep.name

#         cond = [
#             (self.buyCond == 1),
#             (self.sellCond == 1)
#         ]
#         out = ["Buy", "Sell"]
#         self.inTrade = np.select(cond, out, default=0)
#         self.inTrade = pd.DataFrame(
#             self.inTrade, index=rep.d.index, columns=[rep.name])
#         self.inTrade = self.inTrade.replace("0", np.NAN)
#         self.inTrade = self.inTrade.ffill().dropna()
#         self.inTrade = self.inTrade[self.inTrade == "Buy"]

#         self.inTradePrice = rep.d["Close"].loc[self.inTrade.index]
#         self.inTradePrice.name = rep.name


class Agg_TransPrice:
    """
    Aggregate version of TransPrice that keeps trans price for all stocks
    """

    def __init__(self):
        self.buyPrice = pd.DataFrame()
        self.sellPrice = pd.DataFrame()
#         self.inTrade = pd.DataFrame()
#         self.trades = pd.DataFrame()


class Trades:
    def __init__(self):
        self.trades = pd.DataFrame()
#         self.inTrade = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.inTradePrice = pd.DataFrame()


class Returns(TransPrice):
    """
    Calculates returns for the strategy
    """

    def __init__(self, rep):
        rep = rep
        tp = TransPrice(rep)
        self.index = rep.d.index
        self.returns = pd.DataFrame(index=self.index, columns=[rep.name])
        # might result in errors tradesignal/execution is shifted
        self.returns[rep.name].loc[tp.buyPrice.index] = tp.buyPrice
        self.returns[rep.name].loc[tp.sellPrice.index] = tp.sellPrice
        self.returns = self.returns.dropna().pct_change()
        # works for now
        for i in self.returns.index:
            if tp.inTrade.loc[i][0] == "Buy":
                self.returns.loc[i] = -self.returns.loc[i]
        # self.returns.ffill(inplace=True)


class Stats:
    """
    Calculats various trade statistics based on returns
    """

    def __init__(self, rep):
        rep = rep
        r = Returns(rep)
        self.posReturns = r.returns[r.returns > 0].dropna()
        self.negReturns = r.returns[r.returns < 0].dropna()
        self.posTrades = len(self.posReturns)
        self.negTrades = len(self.negReturns)
        self.meanReturns = r.returns.mean()
        self.hitRatio = self.posTrades/(self.posTrades+self.negTrades)
        self.totalTrades = self.posTrades+self.negTrades


class Portfolio:
    def __init__(self):
        self.startAmount = 10000
        self.availAmount = self.startAmount
        self.value = pd.DataFrame()
        self.profit = pd.DataFrame()
        self.invested = pd.DataFrame()
        self.fees = pd.DataFrame()
        self.ror = pd.DataFrame()
#         self.weights = pd.DataFrame()
        self.capUsed = pd.DataFrame()


port = Portfolio()
ats = Agg_TradeSingal()
atp = Agg_TransPrice()
t = Trades()

#############################################
# Generate signals part
#############################################


class Repeater:
    """
    Common class to avoid repetition
    """

    def __init__(self, d, buyCond, sellCond, name):
        self.d = d
        self.buyCond = buyCond
        self.sellCond = sellCond
        self.name = name


def run():
    """
    Loop through files
    Generate signals
    Save them into common classes aggregate*
    """
    for name in data.data:
        d = data.data[name]
        sma5 = SMA(d, ["Close"], 5)
        sma25 = SMA(d, ["Close"], 25)

        buyCond = sma5() > sma25()
        sellCond = sma5() < sma25()

        rep = Repeater(d, buyCond, sellCond, name)

        ts = TradeSignal(rep)
        tp = TransPrice(rep, ts)
#         ret = Returns(rep)
        ats.buys = pd.concat([ats.buys, ts.buyCond], axis=1)
        ats.sells = pd.concat([ats.sells, ts.sellCond], axis=1)
        ats.all = pd.concat([ats.all, ts.all], axis=1)

#         atp.inTrade = pd.concat([atp.inTrade, tp.inTradePrice], axis=1)
        atp.buyPrice = pd.concat([atp.buyPrice, tp.buyPrice], axis=1)
        atp.sellPrice = pd.concat([atp.sellPrice, tp.sellPrice], axis=1)
#         atp.trades = pd.concat([atp.trades, tp.trades], axis=0)
        t.trades = pd.concat([t.trades, tp.trades], axis=0)
        t.inTradePrice = pd.concat([t.inTradePrice, tp.inTradePrice], axis=1)
#         t.inTradePrice = pd.concat([t.inTradePrice, tp.inTradePrice], axis=1)
#         port.tp = pd.concat([port.tp, tp.inTrade], axis=1)
#         port.ror = pd.concat([port.ror, ret.returns], axis=1)
#         port.inTrade = pd.concat([port.inTrade, tp.inTradePrice], axis=1)
#         port.transPrice = pd.concat([port.transPrice, tp.buyPrice], axis=1)
#         print(port.accRet)
        #stats = Stats(rep)


run()

#############################################
# Calculate portfolio part
#############################################


def runPortfolio():
    """
    Calculate profit and loss for the stretegy
    """
    t.weights = pd.DataFrame(index=t.inTradePrice.index,
                             columns=t.inTradePrice.columns)
    t.priceChange = t.inTradePrice - t.inTradePrice.shift()

    # calc portfolio change
    port.value = pd.DataFrame(index=t.inTradePrice.index,
                              columns=["Portfolio value"])
    port.value.iloc[0] = port.startAmount

    port.availAmount = pd.DataFrame(index=t.inTradePrice.index,
                                    columns=["Available amount"])
    port.availAmount.iloc[0] = port.startAmount
    # port.availAmount.ffill(inplace=True)

    port.invested = pd.DataFrame(index=t.inTradePrice.index,
                                 columns=t.weights.columns)
    port.invested.iloc[0] = 0
    # put trades in chronological order
    # t.trades.sort_values("Date/Time_entry", inplace=True)
    # t.trades.reset_index(drop=True, inplace=True)

    # set weights to 0 when exit
    t.weights.loc[atp.sellPrice.index] = 0

    # change change to avoid error
    atp.buyPrice.columns = t.weights.columns

    for ix, row in t.weights.iterrows():
        # weight = port value / entry
        prev_bar = port.availAmount.index.get_loc(ix) - 1

        if prev_bar != -1:
            port.availAmount.loc[ix] = port.availAmount.iloc[prev_bar]

        # if there was an entry on that date
        # allocate weight
        # update avail amount
        if ix in atp.buyPrice.index:
            toInvest = port.availAmount.loc[ix] * 0.1
    #         port.invested.loc[ix] =
    #         t.weights.loc[ix] = port.value.loc[ix].values/atp.buyPrice.loc[ix]
            t.weights.loc[ix] = toInvest.values/atp.buyPrice.loc[ix]
            port.availAmount.loc[ix] -= toInvest

        # if there was an exit on that date
        # set weight to 0
        # update avail amount
        elif ix in atp.sellPrice.index:
            # no need to set weight to 0 as it was already done
            # prob need to change this part for scaling implementation

            # find how to find amount invested
            pass

        # if no new trades/exits
        # update weight
        else:
            t.weights.loc[ix] = t.weights.iloc[prev_bar]
            pass
    #         prev_bar = port.availAmount.index.get_loc(ix) - 1
    #         if prev_bar != -1:
    #             port.availAmount.loc[ix] = port.availAmount.iloc[prev_bar]
        # update avail amount for gains/losses that day
        # done in the end to avoid factroing it in before buy
        # if != -1 to skip first row
        if prev_bar != -1:
            port.availAmount.loc[ix] += (t.priceChange.loc[ix]
                                         * t.weights.loc[ix]).sum()

        # profit = weight * chg
        # portfolio value += profit
