import os
import pandas as pd
from functools import wraps

# own files
from . import database_stuff as db
from . import config
from . import Settings

class DataReader:
    def __init__(self, type_):
        self.data = {}
        self.type = type_
        
    def readCSV(self, path):
        # TODO: replace hardcoded index_col="Date" -> breaks if no Date col
        assert os.path.isfile(
            path), "You need to specify a file or the path doesnt exist."
        _fileName = os.path.basename(path).split(".")[0] # gets file name for a given path; splitting by . to get the name
        self.data[_fileName] = pd.read_csv(
            path,
            index_col="Date",
        )
        self.data[_fileName].index = pd.to_datetime(self.data[_fileName].index)

    def readCSVFiles(self, path):
        assert os.path.isdir(
            path), "You need to specify a folder or the path doesnt exist."
        for file in os.listdir(path):
            _fileName = file.split(".txt")[0]
            _temp = pd.read_csv(
                path + "\\" + file, index_col="Date")
            _temp.index.name = "Date"
            _temp.index = pd.to_datetime(_temp.index)
            self.data[_fileName] = _temp

    def readDB(self, con, meta, index_col):
        """
        Reads tables from database that start with data_.
        If index_col is not provided, default name "Date" is used for index.
        Index is converted to pd.to_datetime(), so it's important to provide one.
        """
        con, meta = db.connect(config.user, config.password, config.db)
        meta.reflect(bind=con)

        for table in meta.tables.keys():
            if table.startswith("data_"):
                _temp = pd.read_sql_table(table, con, index_col=index_col)
                _temp.index.name = "Date"
                _temp.index = pd.to_datetime(_temp.index)
                self.data[table] = _temp
    
    def readHDF(self, path):
        import h5py
        self.data = h5py.File(Settings.read_from_csv_path, "r")
        # self.data = list(self.data.keys())
        

    def _read_hdf(self, key):
        print(f"Reading stock: {key}, process_id: {os.getpid()}")
        df = pd.read_hdf(Settings.read_from_csv_path, key)
        return df

        
    # add conditional decorator
    def establish_con(func):
        if Settings.read_from.lower()=="db":
            con, meta = db.connect(config.user, config.password, config.db)
            meta.reflect(bind=con)

            # @wraps(func)
            def inner(self, *args, **kwargs):
                return func(self, con, *args, **kwargs)

            # con.close() # engine closes connection automatically?
            return inner
    
    
    @establish_con
    def execQuery(self, con, query):
        result = pd.read_sql(query, con)
        return result


if __name__ == "__main__":
    test = DataReader()
    df = test.execQuery("Select * from backtests limit 10")
    print(df)
