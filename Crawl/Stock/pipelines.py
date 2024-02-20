# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
from scrapy.exceptions import DropItem
import os
from dotenv import load_dotenv
load_dotenv()


class DropDuplicates:
    itemlist = []

    def process_item(self, item, spider):
        if item in self.itemlist:
            raise DropItem
        self.itemlist.append(item)
        return item

class SavingToPostgresPipeline(object):

    def open_spider(self, spider):
        self.connection = psycopg2.connect(
            host="host.docker.internal",
            port = os.getenv('DB_PORT'),
            dbname=os.getenv('DB_DBNAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'))
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        for i in item.fields.keys():
            if i not in item.keys():
                item[i] = None
        
	    # Execute SQL command on database to insert data in table
        try:
            self.cur.execute(\
                """insert into stock_daily(\
                    exchange, tradingdate, stockcode, financeurl, stockname, basicprice, openprice, closeprice, highestprice, lowestprice,\
                    avrprice, change, perchange, m_totalvol, m_totalval, pt_totalvol, pt_totalval, totalvol, totalval, marketcap, stocknameen)\
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",\
                    (item['exchange'], item['tradingdate'], item['stockcode'], item['financeurl'], item['stockname'], item['basicprice'], item['openprice'],
                     item['closeprice'], item['highestprice'], item['lowestprice'], item['avrprice'], item['change'], item['perchange'], item['m_totalvol'],
                     item['m_totalval'], item['pt_totalvol'], item['pt_totalval'], item['totalvol'], item['totalval'], item['marketcap'], item['stocknameen'],))
            self.connection.commit()
        except:
            self.connection.rollback()
            raise
        return item