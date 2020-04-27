from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import h5py
import os

def read_hdf(path, key):
    print(f"Reading stock: {key}, process_id: {os.getpid()}")
    df = pd.read_hdf(path, key)
    return df

def pass_path(key):
    a = A()
    a.test()

    path = r"D:\HDF5\stocks.h5"

    df =  read_hdf(path, key)
    a.cond.buy = df
    return a

class A:
    def __init__(self):
        self.cond = Cond()
        

    def test(self, a):
        with ProcessPoolExecutor() as executor:
            for result in executor.map(self.cond._combine, avail_stocks):
                print(result.cond.buy)
                


    # def test(self):
        

class Cond:
    def __init__(self):
        self.buy = pd.DataFrame()
        self.sell = pd.DataFrame()
        self.short = pd.DataFrame()
        self.cover = pd.DataFrame()
        self.all = pd.DataFrame()

    def _combine(self, a):
        for df in [self.buy, self.sell, self.short, self.cover]:
            self.all = self.all.append(df)
        self.all = self.all.T

        for df in [self.buy, self.sell, self.short, self.cover]:
            if df.name not in self.all.columns:
                self.all[df.name] = False

        # self.all = pd.concat([self.buy, self.sell, self.short, self.cover], axis=1)

if __name__ == "__main__":
    path = r"D:\HDF5\stocks.h5"
    f = h5py.File(path, "r")
    avail_stocks = list(f.keys())
    avail_stocks = avail_stocks[:10]
    # print(avail_stocks[:20])
    results = pd.DataFrame()

    a = A()
    a.test("A")
    # a.cond = Cond()

    # print("Number of cpu : ", mp.cpu_count())

    
        
        # results.append(pool.apply_async(read_hdf, args=(path, key)).get())
    # print(list(result))
    # print(len(results))
    # print(results.Symbol)
