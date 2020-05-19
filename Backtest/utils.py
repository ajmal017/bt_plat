import pandas as pd
import os

# own files
# from . import database_stuff as db
# from . import config

def csv_to_db(path):
    con, meta, session = db.connect(config.user, config.password, config.db)

    for ticker in os.listdir(path)[:2]:
        table_name = "data_" + ticker.split(".")[0]
        df = pd.read_csv(os.path.join(path, ticker))
        df["Date"] = pd.to_datetime(df["Date"])
        try:
            df.to_sql(
                table_name,
                con,
                index=False,
                index_label="Date",
                if_exists="replace")
            con.execute(
                'ALTER TABLE "{}" ADD PRIMARY KEY ("Date")'.format(table_name))
        except Exception as e:
            print(e)

def update_hdf5(hdf_path, csv_path):
    import h5py
    hdf_file = h5py.File(hdf_path, "r")
    existing_stocks = list(hdf_file.keys())
    for stock in os.listdir(csv_path)[:2]:
        print(stock)

        df = pd.read_csv(csv_path+"\\"+stock, index_col="DateTime")
        df.index = pd.to_datetime(df.index)
        df[["Open", "High", "Low", "Close"]] = df[["Open", "High", "Low", "Close"]].round(2)

        stock_name = stock.split(".csv")[0]
        if stock_name in existing_stocks:
            current_df = pd.read_hdf(hdf_path, stock_name)
            current_idx = current_df.index.max()
            new_idx = df.index.max()
            if current_idx != new_idx:
                idx_loc = df.index.get_loc(current_idx) + 1
                df = df.iloc[idx_loc:]
                df.to_hdf(hdf_path, stock_name, mode="a", format="table", append=True)
        else:
            df.to_hdf(hdf_file, stock_name, mode="a", format="table")

hdf_path = r"D:\HDF5\test.h5"
csv_path = r"E:\Windows\Data"

update_hdf5(hdf_path, csv_path)