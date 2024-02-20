import pandas as pd
import psycopg2, os
from vnstock import *
from datetime import date, datetime
from dotenv import load_dotenv
load_dotenv()

hostname = 'host.docker.internal'
port = os.getenv('DB_PORT')
username = os.getenv('DB_USER') # the username when you create the database
password = os.getenv('DB_PASS') #change to your password
database = os.getenv('DB_DBNAME')
 
def queryQuotes( conn ) :
    cur = conn.cursor()
    query = """ SELECT S.*
                FROM stock_daily AS S
                WHERE (S.stockcode, S.tradingdate, S.SH_ID) IN (
                    SELECT SH.stockcode, SH.tradingdate, MIN(SH.SH_ID)
                    FROM stock_daily AS SH
                    GROUP BY SH.stockcode, SH.tradingdate) 
                ORDER BY S.stockcode, S.tradingdate ASC"""
    cur.execute( query )
    df = pd.DataFrame(cur.fetchall(), columns=[x[0] for x in cur.description])
    return df


def ModelData():
    conn = psycopg2.connect( host=hostname, port=port, user=username, password=password, dbname=database )
    df = queryQuotes(conn)
    conn.close()
    # df = pd.read_csv('E:\Scrapy\DataQuery.csv')

    b = listing_companies()
    Stock = df.merge(b[['ticker', 'icbName']].rename(columns={'ticker':'stockcode', 'icbName':'industry'}), on='stockcode', how='left')
    # t = ['BasicPrice', 'OpenPrice', 'ClosePrice', 'HighestPrice', 'LowestPrice', 'AvrPrice', 'Change', 'PerChange','M_TotalVol', 'M_TotalVal', 'PT_TotalVol', 'PT_TotalVal', 'TotalVol', 'TotalVal', 'MarketCap']
    Stock.loc[:, 'basicprice':'marketcap'] = Stock.loc[:, 'basicprice':'marketcap'].astype(float)
    Stock.loc[:, 'basicprice':'change'] = Stock.loc[:, 'basicprice':'change',] / 1000
    Stock = Stock.assign(m_totalvol = Stock['m_totalvol'] / 1e6,)
    Stock['perchange'] = Stock['perchange'].astype(str) + '%'

    def MovingAverage(n, col):
        MA = Stock.groupby('stockcode')[f'{col}'].rolling(n, 1).mean().reset_index().set_index('level_1').rename(columns={f'{col}': f'{col}_ma{n}'})
        return MA[[f'{col}_ma{n}']]

    def HighestPeaks(n, col):
        MA = Stock.groupby('stockcode')[f'{col}'].rolling(n, 1).max().reset_index().set_index('level_1').rename(columns={f'{col}': f'{col}_highest_{int(n/20)}month'})
        return MA[[f'{col}_highest_{int(n/20)}month']]

    Stock = Stock.join(MovingAverage(5, 'm_totalvol'))\
                .join(MovingAverage(20, 'm_totalvol'))\
                .join(MovingAverage(50, 'm_totalvol'))\
                .join(MovingAverage(10, 'closeprice'))\
                .join(MovingAverage(20, 'closeprice'))\
                .join(MovingAverage(50, 'closeprice'))\
                .join(MovingAverage(200, 'closeprice'))\
                .join(HighestPeaks(20, 'closeprice'))\
                .join(HighestPeaks(60, 'closeprice'))\
                .join(HighestPeaks(240, 'closeprice'))

    def stock_model(d = None):
        if d == None:
            Stock_Newest = Stock[Stock['tradingdate'] == Stock['tradingdate'].max()]
        else:
            Stock_Newest = Stock[Stock['tradingdate'] == d]
        # Stock_Newest = Stock_Newest

        first_con = Stock_Newest['m_totalvol'] > 0.55 
        second_con = ((Stock_Newest['m_totalvol'] / Stock_Newest['m_totalvol_ma20']) > 1.4) | ((Stock_Newest['m_totalvol'] / Stock_Newest['m_totalvol_ma50']) > 1.9) | ((Stock_Newest['m_totalvol'] / Stock_Newest['m_totalvol_ma5']) > 2)
        third_con = Stock_Newest['change'] > 0
        fourth_con = Stock_Newest['closeprice'] > Stock_Newest['closeprice_ma200']
        fifth_con = Stock_Newest['closeprice_ma50'] > Stock_Newest['closeprice_ma200']
        # sixth_con = Stock
        z = Stock_Newest[ first_con & second_con & third_con & fourth_con & fifth_con]\
            .sort_values(['industry', 'perchange', 'marketcap'], ascending=[True, False, False]).reset_index(drop=True).set_index(['stockcode', 'exchange', 'industry'])\
            [['m_totalvol', 'm_totalvol_ma5', 'm_totalvol_ma20', 'm_totalvol_ma50', 'closeprice_highest_1month', 'closeprice_highest_3month', 'change', 'perchange', 'basicprice', 'openprice', 'closeprice', 'highestprice', 'lowestprice', 'avrprice', 'marketcap']]
        return z
    rs = stock_model()
    rs.to_csv(f'{os.getenv("AIRFLOW_HOME")}/Data/DataModel_{date.today():%Y%m%d}.xlsx')
    return rs

ModelData()