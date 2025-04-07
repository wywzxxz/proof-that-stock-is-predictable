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


default_fields_d = set("code,date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST".split(","))
default_fields_60 = set("date,time,code,open,high,low,close,volume,amount,adjustflag".split(","))


def query_history_k_data(code, start_date=None, end_date=None, *, frequency="60", fields=None, **kwargs):
    if start_date is None:
        start_date = "1991-01-01"
    if "adjustflag" in kwargs:
        kwargs["adjustflag"] = str(kwargs["adjustflag"])
    if fields is None:
        if frequency == "d":
            fields = default_fields_d
        elif frequency == "60":
            fields = default_fields_60
        else:
            raise NotImplementedError("No default fields for frequency " + str(frequency))
    if fields is str:
        fields = set(fields.split(","))
    fields = set(fields)

    if "date" in fields:
        fields.add("time")
    if "time" in fields:
        fields.add("date")

    if frequency == "d":
        fields &= default_fields_d
    elif frequency == "60":
        fields &= default_fields_60
    else:
        raise NotImplementedError("Not supported frequency " + str(frequency))

    fields = ",".join(fields)
    rs = bs.query_history_k_data_plus(str(code), frequency=frequency, fields=fields, start_date=start_date, end_date=end_date, **kwargs)
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

    df["date"] = pd.to_datetime(df.date)
    if "time" not in df.columns and "date" in df.columns:
        df["time"] = df["date"]
    elif "time" in df.columns:
        df["time"] = pd.to_datetime(df.time.astype(str).str.slice(0, 14))
    # if "volume" in df.columns:
    #    df["volume"] = df.volume.astype(float, errors="ignore").fillna(None)
    return df


if __name__ == "__main__":
    print(query_hs300_stocks())
