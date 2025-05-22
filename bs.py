import baostock as bs
import pandas as pd

lg = bs.login(user_id="anonymous", password="123456")


def query_stock_basic():
    rs = bs.query_stock_basic()
    # 打印结果集
    res = []
    while (rs.error_code == "0") & rs.next():
        # 获取一条记录，将记录合并在一起
        res.append(rs.get_row_data())
    result = pd.DataFrame(res, columns=rs.fields)
    return result


def query_sz50_stocks():
    rs = bs.query_sz50_stocks()
    # 打印结果集
    hs300_stocks = []
    while (rs.error_code == "0") & rs.next():
        # 获取一条记录，将记录合并在一起
        hs300_stocks.append(rs.get_row_data())
    result = pd.DataFrame(hs300_stocks, columns=rs.fields)
    return result


def query_hs300_stocks():
    rs = bs.query_hs300_stocks()
    # 打印结果集
    hs300_stocks = []
    while (rs.error_code == "0") & rs.next():
        # 获取一条记录，将记录合并在一起
        hs300_stocks.append(rs.get_row_data())
    result = pd.DataFrame(hs300_stocks, columns=rs.fields)
    return result


def query_history_k_data(code, start_date=None, end_date=None, *, fields="time,code,open,high,low,close,volume,amount,adjustflag", frequency="d", adjustflag=1, **kwargs):
    if start_date is not None:
        start_date = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    if str(frequency) in "dwm":
        fields = fields.replace("time", "date")
    rs = bs.query_history_k_data_plus(str(code), start_date=start_date, end_date=end_date, frequency=str(frequency), fields=fields, adjustflag=str(adjustflag), **kwargs)
    data_list = []
    while (rs.error_code == "0") & rs.next():
        d = rs.get_row_data()
        for i in range(len(d)):
            if d[i] == "":
                d[i] = None
        data_list.append(d)
    if len(data_list) == 0:
        return None
    df = pd.DataFrame(data_list, columns=rs.fields)
    if "date" in df:
        df.rename({"date": "time"}, axis=1, inplace=True)
        df["time"] = pd.to_datetime(df.time.astype(str).str.slice(0, 14))
    else:
        df["time"] = pd.to_datetime(df.time, format="%Y%m%d%H%M%S%f")
    for k in set(df.columns) - {"time"}:
        try:
            df[k] = pd.to_numeric(df[k])
        except:
            pass
    # df["time"] = pd.to_datetime(df.time.astype(str).str.slice(0, 14))
    return df


if __name__ == "__main__":
    df = query_history_k_data("sh.600000")
    print(df)
