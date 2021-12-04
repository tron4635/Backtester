import numpy as np
import pandas as pd
import logging, sys
import datetime as dt
import time
from tqdm import tqdm #progress bar
import matplotlib.pyplot as plt
import matplotlib.dates as md

logging.basicConfig(stream=sys.stderr, format='%(asctime)s %(message)s', level=logging.INFO)

LATENCY =  200 # ms latency
SLIPPAGE = 0.0003
BROKERAGE_PCT = 0.0001
BROKERAGE_MIN = 1.0 # minimum brokerage in currency
currentstats = []

# non-class and helper functions
def round_down_to_multiple(num, divisor):
    return num - (num % divisor)

def estimate_parameters(df):
    return df.mean(), df.std()

# classes
class strat():
    def __init__(self, orderManager, cash):
        self.cash = cash # in currency
        self.position = 0 # position in SHARES
        self.param_mean = None
        self.param_stddev = None
        self.min_pos_increment = 10000 # increase/reduce position only at discrete sizes
        self.min_pos_type_pct = False # toggle whether min_pos_increment size is in % of cash or in currency
        self.desired_size = 0
        self.orderManager = orderManager
        self.currentBid = None
        self.currentAsk = None
        self.currentTimestamp = None

        # sizing table/buckets
        # linear interpolation between buckets
        # at what outperformance/stddev do you want to have what position (in % of total cash)
        # first number is minimum, i.e. at outperformance below this number the strategy wants to have zero position
        # has to be monotonically increasing
        self.sizing_buckets = {
            1: 0.1,
            2: 0.5,
            3: 1.0
            } #in this example, we want to have 10% of cash invested for a 1 stddev deviation, 50% at 2 stddev, and 100% at 3

    def check_desired_size(self, pct_outperformance):
        # check the desired position size at current price
        current_stddev = abs(pct_outperformance) / self.param_stddev
        #print('Outperformance ' +str(abs(pct_outperformance)) + ', vs stddev ' + str(self.param_stddev))
        lower_key = None
        lower_value = None
        upper_key = None
        upper_value = None
        for key, value in self.sizing_buckets.items(): # find the two buckets in which the current value lies
            if (current_stddev < key) & (lower_key is None):
                break
            elif (current_stddev > key) & (lower_key is None):
                lower_key = key
                lower_value = value
            elif upper_key is None:
                upper_key = key
                upper_value = value
            else:
                break

        if (lower_key is not None) & (upper_key is not None):
            a = (current_stddev - lower_key) / (upper_key - lower_key)
            size_unrounded = (a * (upper_value - lower_value) + lower_value) * self.cash

            if self.min_pos_type_pct:
                min_increment = self.min_pos_increment * self.cash
            else:
                min_increment = self.min_pos_increment

            if pct_outperformance > 0: # positive outperformance, need to sell
                self.desired_size = round_down_to_multiple(size_unrounded, min_increment) * -1
            else:
                self.desired_size = round_down_to_multiple(size_unrounded, min_increment)
        else:
            # current stddev is below minimum, so desired size is zero
            self.desired_size = 0

        #print('Desired position size: ' + str(self.desired_size))
        logging.debug('Desired position size: ' + str(self.desired_size))
    
    def new_priceUpdate(self, timestamp, newPrice, lastClose):
        #print("strat new price update")
        self.currentBid = newPrice
        self.currentAsk = newPrice
        self.currentTimestamp = timestamp
        pct_outperformance = newPrice / lastClose - 1.0
        self.check_trade_initiation(pct_outperformance)
        
        # update stats
        self.update_statistics()

    def check_trade_initiation(self, perf):
        # if desired size is different from current position, initiate increase/reduce position
        self.check_desired_size(perf)
        desired_size = self.desired_size
        trade = False
        if desired_size > self.position:
            # upsize position
            size2trade = desired_size - self.position # will be positive
            price2trade = self.currentAsk
            trade = True
        elif  desired_size < self.position:
            # downsize position
            size2trade = desired_size - self.position # will be negative
            price2trade = self.currentBid
            trade = True
        
        if trade:
            self.orderManager.place_order(size2trade, price2trade)
            # calculate average prices for P&L calculation
            if size2trade > 0:
                oldQuantity = self.orderManager.totalBuyQuantity
                self.orderManager.averageBuyPrice = (oldQuantity * self.orderManager.averageBuyPrice + size2trade * price2trade) / (oldQuantity + size2trade)
                self.orderManager.totalBuyQuantity = self.orderManager.totalBuyQuantity + size2trade
            else:
                oldQuantity = self.orderManager.totalSellQuantity
                self.orderManager.averageSellPrice = (oldQuantity * self.orderManager.averageSellPrice + size2trade * price2trade) / (oldQuantity + size2trade)
                self.orderManager.totalSellQuantity = self.orderManager.totalSellQuantity + size2trade

            logging.debug('Placed order for ' + str(size2trade) + ' at ' +  str(price2trade))

        else:
            logging.debug('No order placed')

    def update_params(self, mean, stddev):
        self.param_mean = mean
        self.param_stddev = stddev
        
    def update_statistics(self):
        # keep track of strategy statistics, e.g.
        # max DD, volatility, average holding time, etc
        # P&L
        PnL = 0
        Position = 0
        #for order in self.orderManager.orderqueue:
        #    PnL = PnL + order.calculate_PnL(self.currentAsk)
        #    Position = Position + order.size
        Position = self.currentAsk * (self.orderManager.totalSellQuantity + self.orderManager.totalBuyQuantity)
        PnL = self.orderManager.totalBuyQuantity * (self.currentAsk - self.orderManager.averageBuyPrice) + self.orderManager.totalSellQuantity * (self.currentAsk - self.orderManager.averageSellPrice)

        self.position = Position
        #print('Current P&L: ' + str(PnL) + ', position: ' + str(Position))

        global currentstats
        currentstats.append([self.currentTimestamp, PnL, Position, self.currentAsk])

class order():
    def __init__(self, timestamp, size, limit=0):
        self.timestamp = timestamp
        self.filled = None
        self.filledPrice = None
        self.size = size
        if limit == 0:
            self.limit = None
        else:
            self.limit = limit
        
        # for now, immediately fill the order until the functionality has been implemented
        self.filled = True
        self.filledPrice = limit

    def calculate_PnL(self, price):
        # calculate PnL for a single order
        pct_gain = (price - self.filledPrice) / self.filledPrice
        #print('P&L of order: ' + str(pct_gain * self.size))
        return pct_gain * self.size

class orderManager():
    def __init__(self):
        self.orderqueue = [] # array holding instances of order class
        #self.currentBid = None
        #self.currentAsk = None
        #self.currentTimestamp = None
        self.averageBuyPrice = 0.0
        self.averageSellPrice = 0.0
        self.totalBuyQuantity = 0.0
        self.totalSellQuantity = 0.0
        
    def place_order(self, timestamp, size, limit=0):
        # omitting optional argument 'limit' means market order
        myOrder = order(timestamp, size, limit)
        self.orderqueue.append(myOrder)
    
    def check_active_orders(self):
        # check if any active orders were executed (and mark them as such)
        for order in self.orderqueue:
            print("TEST")
    
    #def new_priceUpdate(self, df):
    #    print("handle new price update")

# main
def main():
    om = orderManager()
    myStrat = strat(om, 100000)

    logging.info('Reading HDF5 data')
    df = pd.read_hdf('./Data/HDF5/FxTickData.h5')
    logging.info('Sorting index')
    df = df.sort_index()
    # calculate mid prices
    logging.info('Calculating mid prices')
    df["mid"] = (df["Bid"] + df["Ask"])/2
    df = df.drop(columns='Bid')
    df = df.drop(columns='Ask')

    logging.info('Resampling to OHLC')
    ohlc = df.resample('D').ohlc()
    ohlc = ohlc.drop(('mid', 'open'),1)
    ohlc = ohlc.drop(('mid', 'high'),1)
    ohlc = ohlc.drop(('mid', 'low'),1)

    logging.info('Calculating percentage change')
    daily_return = ohlc.pct_change(1)

    # pick starting time
    i = 100
    currentday = df.index[i].date
    # run loop looping through every new timestamp
    # check if new parameters need to be estimated (e.g. due to new day)
    training_set = daily_return.loc['2020-1-9':'2020-1-22']

    logging.info('Estimating parameter')
    mean, std = estimate_parameters(training_set[('mid', 'close')])
    myStrat.update_params(mean, std)
    last_close = ohlc.loc['2020-1-21', ('mid', 'close')]

    new_day = df.loc['2020-1-22']
    #new_day = new_day.head(n=10)
    datapoints_length = len(new_day.index)
    p = 0

    logging.info('Starting iteration loop')
    price_now = None
    start_time = time.time()

    for index, row in tqdm(new_day.iterrows(), total=len(new_day)):
        #print(index)
        #print(row['mid'])
        timestamp = index
        price_now = row['mid']
        #om.new_priceUpdate(price_now)
        myStrat.new_priceUpdate(timestamp, price_now, last_close)

        #p = p + 1
        #print(p/datapoints_length)
    
    logging.info("Loop completed in --- %s seconds ---" % (time.time() - start_time))

    print('done')
    df2 = pd.DataFrame(currentstats, columns=['time', 'pnl', 'pos', 'price'])
    df2.set_index('time')

    df2.to_pickle('./Data/Pickles/result.pkl')

    #plt = df2.plot(y='pnl')
    #plt.ioff()
    #plt.gcf().show()
    df2.plot(kind='line',x='time',y='pnl',color='blue')
    plt.setp(plt.gca().xaxis.get_majorticklabels(),'rotation', 60)
    plt.xlabel("time")
    plt.ylabel("price")
    plt.gca().xaxis.set_major_locator(md.MinuteLocator(byminute = [0]))
    plt.gca().xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
    plt.show(block=True)

if __name__ == "__main__":
    main()
