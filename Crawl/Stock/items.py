# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class StockItem(scrapy.Item):
    # sh_id = scrapy.Field()
    exchange = scrapy.Field()
    tradingdate = scrapy.Field()
    stockcode = scrapy.Field()
    financeurl = scrapy.Field()
    stockname = scrapy.Field()
    basicprice = scrapy.Field()
    openprice = scrapy.Field()
    closeprice = scrapy.Field()
    highestprice = scrapy.Field()
    lowestprice = scrapy.Field()
    avrprice = scrapy.Field()
    change = scrapy.Field()
    perchange = scrapy.Field()
    m_totalvol = scrapy.Field()
    m_totalval = scrapy.Field()
    pt_totalvol = scrapy.Field()
    pt_totalval = scrapy.Field()
    totalvol = scrapy.Field()
    totalval = scrapy.Field()
    marketcap = scrapy.Field()
    stocknameen = scrapy.Field()

    # def set_all(self, value):
    #     for keys, _ in self.fields.items():
    #         if self[keys]:
    #             self[keys] = value