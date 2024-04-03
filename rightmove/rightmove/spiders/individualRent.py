import scrapy
import csv
import mysql.connector
import random
from lxml import etree
import os
import math
from proxy import ROTATING_PROXY_LIST

class IndividualURL(scrapy.Spider):
    name = 'individualRent'

   
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

    def start_requests(self):
   
        db = mysql.connector.connect(
            host="",
            user="root",
            password="",
            database=""
        )
        cursor = db.cursor()

        # Query to select URL and postcode from property_links
        query = "SELECT link, postcode FROM property_links where type='rent'"
        cursor.execute(query)

        # Fetch all URLs and postcodes
        for link, postcode in cursor:
            # Now you can use both link and postcode here
            yield scrapy.Request(link, callback=self.parse, meta={'postcode': postcode, 'proxy' : random.choice(ROTATING_PROXY_LIST)}, headers={'User-Agent': random.choice(self.user_agents)})

        # Close database connection
        cursor.close()
        db.close()


    def parse(self, response):
        infoList = response.css('.ZBWaPR-rIda6ikyKpB_E2::text').extract()
        infoListValues = response.css('._1hV1kqpVceE9m-QrX_hWDN::text').extract()
        price = response.xpath('string(//*[@id="root"]/main/div/div[2]/div/article[1]/div/div/div[1]/span[1])').get()
        date = response.css('._2nk2x6QhNB1UrxdI5KpvaF::text').get()
        title = response.css('._2uQQ3SV0eMHL1P6t5ZDo2q::text').get()
        letType = response.xpath('//*[@id="root"]/main/div/div[2]/div/article[2]/div[1]/dl/div[4]/dd/text()').get()
        url = response.url


        property_data = {
             'title' : title,
             'updated' : date,
             'price' : price,
             'propertyLink' : url,
             'infoList' : infoList,
             'infoListValues' : infoListValues,
             'letType': letType
         }

        # Store property data in MySQL database
        self.store_property_in_database(response.url, property_data)
        
    def store_property_in_database(self, url, property_data):
        mysql_config = {
        'host': '',
        'user': '',
        'password': '',
        'database': '',
        }
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        propertyType = None
        bedrooms = 0
        bathrooms = 0
        letType = None
        size = None
        tenure = None
        price_integer = int(''.join(filter(str.isdigit, property_data['price'])))
        # Store property data in MySQL database
        for x, item in enumerate(property_data['infoList']):
            if item == 'PROPERTY TYPE':
                propertyType = property_data['infoListValues'][x]
            elif item == 'BEDROOMS':
                bedrooms = property_data['infoListValues'][x]
            elif item == 'BATHROOMS':
                bathrooms = property_data['infoListValues'][x]
            elif item == 'SIZE':
                size = property_data['infoListValues'][x]
            elif item == 'TENURE':
                tenure = property_data['infoListValues'][x]

        if propertyType != 'Garages' and propertyType != 'Parking' and propertyType != None and propertyType != 'Serviced Apartments' and propertyType != 'Retirement Property':
            query = "INSERT INTO Rent (propertyLink, title, price, updated, letType, propertyType, bedrooms, bathrooms, size) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s)"
            cursor.execute(query, (url, property_data['title'], price_integer, property_data['updated'], letType, propertyType, bedrooms, bathrooms, size))
            conn.commit()
            conn.close()
