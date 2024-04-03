import scrapy
import csv
import mysql.connector
import random
from lxml import etree
import re
import math
from proxy import rotating_proxy_list

class PropertySpider(scrapy.Spider):
    name = 'property'
    allowed_domains = ['rightmove.co.uk']
    start_urls = ['https://www.rightmove.com/']

    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188"
    ]

    property_data = {}
    property_Rentdata = {}

    def parse(self, response):
       
        for x in range(10, 11):#2963 outcodes
            postcode_url = f'https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E{x}&sortType=1'
            yield scrapy.Request(url=postcode_url, callback=self.parse_postcode, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})

        # for x in range(10, 11):
        #     postcode_url = f'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=OUTCODE%5E{x}&sortType=1'
        #     yield scrapy.Request(url=postcode_url, callback=self.parse_postcodeRent, headers={'User-Agent': random.choice(self.user_agents)})


    def parse_postcode(self, response):
        num = response.css('.searchHeader-resultCount::text').get()
        num = num.replace(',', '')  # Remove comma from the number string
        postcodeString = response.css('.searchTitle-heading::text').get()
        pattern = r'Properties For Sale in (\w+)'
        match = re.search(pattern, postcodeString)
        if match:
            postcode = match.group(1)
        pages = math.ceil(int(num) / 24)
        if pages <= 42:
            for x in range(pages):
                page_url = f"{response.url}&index={x * 24}"
                yield scrapy.Request(url=page_url, callback=self.parse_page, headers={'User-Agent': random.choice(self.user_agents)}, meta={'postcode' : postcode, 'proxy' : random.choice(ROTATING_PROXY_LIST)})
        else:
            page_url = f"{response.url}&maxPrice=100000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=100000&minPrice=150000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=150000&minPrice=200000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=200000&minPrice=250000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=250000&minPrice=300000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=300000&minPrice=350000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=350000&minPrice=400000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=400000&minPrice=450000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=450000&minPrice=500000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=500000&minPrice=550000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=550000&minPrice=600000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=650000&minPrice=700000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=700000&minPrice=800000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=800000&minPrice=900000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)},meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=900000&minPrice=1000000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)},meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&maxPrice=1000000&minPrice=1250000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)},meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})
            page_url = f"{response.url}&minPrice=1250000"
            yield scrapy.Request(url=page_url, callback=self.parse_page_over, headers={'User-Agent': random.choice(self.user_agents)}, meta={'proxy' : random.choice(ROTATING_PROXY_LIST)})

    def parse_postcodeRent(self, response):
        num = response.css('.searchHeader-resultCount::text').get()
        num = num.replace(',', '')  # Remove comma from the number string
        postcodeString = response.css('.searchTitle-heading::text').get()
        pattern = r'Properties To Rent in (\w+)'
        match = re.search(pattern, postcodeString)
        if match:
            postcode = match.group(1)
        pages = math.ceil(int(num) / 24)
        if pages <= 42:
            for x in range(pages):
                page_url = f"{response.url}&index={x * 24}"
                yield scrapy.Request(url=page_url, callback=self.parse_pageRent, headers={'User-Agent': random.choice(self.user_agents)}, meta={'postcode' : postcode, 'proxy' : random.choice(ROTATING_PROXY_LIST)})
     
    def parse_page(self, response):
        postocde = response.meta.get('postcode')
        property_links = response.css('a.propertyCard-link::attr(href)').extract()
        for link in property_links:
            self.property_data[link] = postocde

    def parse_pageRent(self, response):
        postocde = response.meta.get('postcode')
        property_links = response.css('a.propertyCard-link::attr(href)').extract()
        for link in property_links:
            self.property_Rentdata[link] = postocde
    
    def parse_page_over(self, response):
        num = response.css('.searchHeader-resultCount::text').get()
        num = num.replace(',', '')  # Remove comma from the number string
        pages = math.ceil(int(num) / 24)
        postcodeString = response.css('.searchTitle-heading::text').get()
        pattern = r'Properties For Sale in (\w+)'
        match = re.search(pattern, postcodeString)
        if match:
            postcode = match.group(1)
        for x in range(pages):
            page_url = f"{response.url}&index={x * 24}"
            yield scrapy.Request(url=page_url, callback=self.parse_page, headers={'User-Agent': random.choice(self.user_agents)}, meta={'postcode': postcode, 'proxy' : random.choice(ROTATING_PROXY_LIST)})
    
    def closed(self, reason):
        db = mysql.connector.connect(
            host="",  # e.g., "localhost"
            user="",
            password="",
            database=""
        )
        cursor = db.cursor()

        base_url = "https://www.rightmove.co.uk"

        cursor.execute('delete from property_links')

        for link, postcode in self.property_data.items():
            try:
                formatted_link = link if link.startswith("http") else base_url + link.split("#")[0]  # Remove fragment
                cursor.execute("INSERT INTO property_links (link, postcode, type) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE postcode = VALUES(postcode)", (formatted_link, postcode, 'sale'))
            except mysql.connector.Error as err:
                print(f"Failed inserting sale property data: {err}")
    
    # Insert rent property data
        for link, postcode in self.property_Rentdata.items():
            try:
                formatted_link = link if link.startswith("http") else base_url + link.split("#")[0]  # Remove fragment
                cursor.execute("INSERT INTO property_links (link, postcode, type) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE postcode = VALUES(postcode)", (formatted_link, postcode, 'rent'))
            except mysql.connector.Error as err:
                print(f"Failed inserting rent property data: {err}")

        cursor.execute("delete from images where property_link NOT IN (SELECT link FROM property_links where type='sale') ")
        cursor.execute("delete from properties where propertyLink NOT IN (SELECT link FROM property_links where type='sale') ")
        cursor.execute("delete from Rent where propertyLink NOT IN (SELECT link FROM property_links where type='rent') ")
        cursor.execute("delete from property_links where type='sale' and link in (select propertyLink from properties)")
        cursor.execute("delete from property_links where type='rent' and link in (select propertyLink from Rent)")

        
        db.commit()
        cursor.close()
        db.close()
 