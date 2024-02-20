import scrapy, sys, logging, os
# sys.path.insert(0, "/workspaces/DockerAirflow")
# sys.path.insert(0, "/opt/airflow")
from Stock.items import StockItem
import time, json, holidays
import pandas as pd
from datetime import datetime, timedelta
# from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings

# sys.setrecursionlimit(3000)  # Set an appropriate limit


class StockDailySpider(scrapy.Spider):
    name = "stockdaily"
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.67',
    }

    data = {
        'page': '1',
        'pageSize': '30',
        'catID': '1',
        'date': '2023-08-21',
    }
    def get_cookies(self):
        with open('./Stock/spiders/cookies.json', 'r') as openfile:
            # Reading from json file
            json_object = json.load(openfile)
        return json_object

    def start_requests(self):
        print("Starting execution of start_requests")
        try:
            self.toDate = pd.to_datetime(self.toDate, format='%d%m%Y')
            self.fromDate = pd.to_datetime(self.fromDate, format='%d%m%Y')
            self.date_list = [(self.toDate - timedelta(days=x)).strftime('%Y-%m-%d') for x in range((self.toDate - self.fromDate).days + 1)\
                              if (self.toDate - timedelta(days=x)).weekday() not in [4,5] and (self.fromDate - timedelta(days=x)) not in holidays.VN()]
        except:
            self.date_list = [datetime.today().strftime('%Y-%m-%d')]
        cookie = {}
        for item in self.get_cookies():
            name = item.pop('name')
            cookie[name] = item.pop('value')
        start_urls = "https://finance.vietstock.vn/ket-qua-giao-dich?tab=thong-ke-gia&exchange=1"
        print("Ending execution of start_requests")
        yield scrapy.Request(url=start_urls, callback=self.parse, headers=self.headers, cookies=cookie)

    def parse(self, response):
        self.data['__RequestVerificationToken'] = response.css('input[name="__RequestVerificationToken"]::attr(value)').extract_first()
        for x in range(1,4):
            if x == 1:
                self.data['catID'] = str(x)
                yield scrapy.FormRequest('https://finance.vietstock.vn/data/KQGDThongKeGiaPaging', method='POST', formdata=self.data, headers=self.headers, meta={'count': x}, callback=self.parse_HSX)
            elif x == 2:
                self.data['catID'] = str(x)
                yield scrapy.FormRequest('https://finance.vietstock.vn/data/KQGDThongKeGiaPaging', method='POST', formdata=self.data, headers=self.headers, meta={'count': x}, callback=self.parse_HNX)
            else:
                self.data['catID'] = str(x)
                yield scrapy.FormRequest('https://finance.vietstock.vn/data/KQGDThongKeGiaPaging', method='POST', formdata=self.data, headers=self.headers, meta={'count': x}, callback=self.parse_UPCOM)

    def parse_HSX(self, response):
        for date in self.date_list:
            self.data['date'] = date
            self.data['catID'] = '1'
            for x in range(response.json()[3][0]):
                self.data['page'] = str(x+1)
                yield scrapy.FormRequest('https://finance.vietstock.vn/data/KQGDThongKeGiaPaging', method='POST', formdata=self.data, headers=self.headers, meta={'exchange': 'HOSE', 'page': x}, callback=self.parse_detail)

    def parse_HNX(self, response):
        for date in self.date_list:
            self.data['date'] = date
            self.data['catID'] = '2'
            for x in range(response.json()[3][0]):
                self.data['page'] = str(x+1)
                yield scrapy.FormRequest('https://finance.vietstock.vn/data/KQGDThongKeGiaPaging', method='POST', formdata=self.data, headers=self.headers, meta={'exchange': 'HNX', 'page': x}, callback=self.parse_detail)
    
    def parse_UPCOM(self, response):
        for date in self.date_list:
            self.data['date'] = date
            self.data['catID'] = '3'
            for x in range(response.json()[3][0]):
                self.data['page'] = str(x+1)
                yield scrapy.FormRequest('https://finance.vietstock.vn/data/KQGDThongKeGiaPaging', method='POST', formdata=self.data, headers=self.headers, meta={'exchange': 'UPCOM', 'page': x}, callback=self.parse_detail)
    def parse_detail(self, response):
        if response.meta['page'] == 1:
            if response.meta['exchange'] == 'HOSE':
                stockcode = "VNINDEX"
            else:
                stockcode = response.meta['exchange'] + 'INDEX'
            exchange = response.meta['exchange']
            tradingdate = (pd.to_datetime(int(response.json()[0][0]['TrDate'][6:-2]), unit='ms')  + pd.DateOffset(days=1)).date()
            basicprice = response.json()[0][0]['PriorIndex']
            closeprice = response.json()[0][0]['CloseIndex']
            change = response.json()[0][0]['Change']
            perchange = response.json()[0][0]['PerChange']
            m_totalvol = response.json()[1][0]['TotalMatchVolStock']
            m_totalval = response.json()[1][0]['TotalMatchValStock']
            pt_totalval = response.json()[1][0]['TotalPutValStock']
            pt_totalvol = response.json()[1][0]['TotalPutVolStock']
            totalvol = response.json()[1][0]['TotalVolStock']
            totalval = response.json()[1][0]['TotalValStock']
            Stock = StockItem(tradingdate=tradingdate, stockcode=stockcode, basicprice=basicprice, closeprice=closeprice, change=change, perchange=perchange, 
                                  m_totalvol=m_totalvol, m_totalval=m_totalval, pt_totalvol=pt_totalvol, pt_totalval=pt_totalval, totalvol=totalvol, totalval=totalval, exchange=exchange)
            yield Stock

        for row in response.json()[2]:
            exchange = response.meta['exchange']
            tradingdate= (pd.to_datetime(int(list(row.values())[0][6:-2]), unit='ms')  + pd.DateOffset(days=1)).date()
            stockcode= list(row.values())[1]
            financeurl= list(row.values())[2]
            stockname= list(row.values())[3]
            basicprice= list(row.values())[4]
            openprice= list(row.values())[5]
            closeprice= list(row.values())[6]
            highestprice= list(row.values())[7]
            lowestprice= list(row.values())[8]
            avrprice= list(row.values())[9]
            change= list(row.values())[10]
            perchange= list(row.values())[11]
            m_totalvol= list(row.values())[14]
            m_totalval= list(row.values())[15]
            pt_totalvol= list(row.values())[16]
            pt_totalval= list(row.values())[17]
            totalvol= list(row.values())[18]
            totalval= list(row.values())[19]
            marketcap= list(row.values())[20]
            stocknameen= list(row.values())[21]
            Stock = StockItem(tradingdate=tradingdate, stockcode=stockcode, financeurl=financeurl, stockname=stockname, basicprice=basicprice, openprice=openprice,
                                  closeprice=closeprice, highestprice=highestprice, lowestprice=lowestprice, avrprice=avrprice, change=change, perchange=perchange,
                                  m_totalvol=m_totalvol, m_totalval=m_totalval, pt_totalvol=pt_totalvol, pt_totalval=pt_totalval, totalvol=totalvol, totalval=totalval,
                                  marketcap=marketcap, stocknameen=stocknameen, exchange=exchange)
            yield Stock


# process = CrawlerProcess(settings = {
#     # Adjusting the scraping behavior to rotate appropriately through proxies and user agents
#     "CONCURRENT_REQUESTS": 3, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
#     "DOWNLOAD_TIMEOUT": 60, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
#     "RETRY_TIMES": 5, # Catch and retry failed requests up to 5 times
#     "ROBOTSTXT_OBEY": False, # Saves one API call
# })
# process.crawl(StockDailySpider)
# process.start()