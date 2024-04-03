import scrapy
import csv
import mysql.connector
import random
from lxml import etree
import os
import math
import re
import subprocess
from scrapy.http import Request
from proxy import ROTATING_PROXY_LIST



class IndividualURL(scrapy.Spider):
    name = 'individual'


    # to try to get images without selenium. https://media.rightmove.co.uk/110k/109751/144244259/109751_224218_IMG_02_0000.jpeg example link to url everything should be the same except img number. some links are longer and have for exaple 645x345 at end but can be cut out 0000 i have seen as 0002 just stay teh same

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
            user="",
            password="",
            database=""
        )
        cursor = db.cursor()

        # Query to select URL and postcode from property_links
        query = "SELECT link, postcode FROM property_links where type='sale'"
        cursor.execute(query)

        # Fetch all URLs and postcodes
        for (link, postcode) in cursor:
            yield scrapy.Request(link, callback=self.parse, meta={'postcode': postcode, 'proxy' : random.choice(ROTATING_PROXY_LIST)}, headers={'User-Agent': random.choice(self.user_agents)})
            
            

        # Close database connection
        cursor.close()
        db.close()

    def parse(self, response):
        postcode = response.meta['postcode']
        text_list = response.css('.STw8udCxUaBUMfOOZu0iL ::text').getall()
        text_list = [text.strip() for text in text_list if text.strip()]
        description = '\n'.join(text_list)
        picnum = response.css('.r62UN7T93Yr5BEGz48YBy').get()
        image = response.css('._2uGNfP4v5SSYyfx3rZngKM img::attr(src)').get()
        infoList = response.css('.ZBWaPR-rIda6ikyKpB_E2::text').extract()
        infoListValues = response.css('._1hV1kqpVceE9m-QrX_hWDN::text').extract()
        price = response.xpath('string(//*[@id="root"]/main/div/div[2]/div/article[1]/div/div/div[1]/span[1])').get()
        date = response.css('._2nk2x6QhNB1UrxdI5KpvaF::text').get()
        title = response.css('._2uQQ3SV0eMHL1P6t5ZDo2q::text').get()
        url = response.url
        shared = response.css('._3jcieslasohqOSzU9Na4x6::text').get()
        floorplan = response.css('._1EKvilxkEc0XS32Gwbn-iU img::attr(src)').get()
       
        try:
            if shared == 'SHARED OWNERSHIP':
                isShared = True
            else:
                isShared = False
        except:
            isShared = False
        
        if image:
            imageUrls = self.generateImageUrls(image)
            for url in imageUrls:
                yield Request(url, method="HEAD", callback=self.validateImageUrl, meta={'property_url': response.url, 'proxy' : random.choice(ROTATING_PROXY_LIST)}, headers={'User-Agent': random.choice(ROTATING_PROXY_LIST)})

        if floorplan:
            imageUrls = self.generateImageUrls(image)
            for url in imageUrls:
                yield Request(url, method="HEAD", callback=self.validateFloorplanUrl, meta={'property_url': response.url, 'proxy' : random.choice(ROTATING_PROXY_LIST)}, headers={'User-Agent': random.choice(ROTATING_PROXY_LIST)})


        property_data = {
             'title' : title,
             'description' : description,
             'updated' : date,
             'price' : price,
             'propertyLink' : url,
             'infoList' : infoList,
             'infoListValues' : infoListValues,
             'isShared' : isShared,
             'postcode' : postcode,
             'image' : image,
             'floorplan' : floorplan
         }

        # Store property data in MySQL database
        self.store_property_in_database(response.url, property_data)

    def validateImageUrl(self, response):
        if response.status == 200:
            self.storeImage(response.meta['property_url'], response.url)

    def validateFloorplanUrl(self, response):
        if response.status == 200:
            self.storeFloorplan(response.meta['property_url'], response.url)
    
    def storeImage(self, propertyUrl, imageUrl):
        db = mysql.connector.connect(
            host="",
            user="",
            password="",
            database=""
        )
        cursor = db.cursor()

        query = "INSERT INTO images (image_link, property_link, floorplan) VALUES (%s, %s, 0)" 
        cursor.execute(query, (imageUrl, propertyUrl))
        db.commit()

        cursor.close()
        db.close()
    
    def storeFloorplan(self, propertyUrl, imageUrl):
        db = mysql.connector.connect(
            host="",
            user="",
            password="",
            database=""
        )
        cursor = db.cursor()

        query = "INSERT INTO images (image_link, property_link, floorplan) VALUES (%s, %s, 1)" 
        cursor.execute(query, (imageUrl, propertyUrl))
        db.commit()

        cursor.close()
        db.close()


    def generateImageUrls(self, url):
        urls_with_numbers = []
        pattern = r'(IMG_\d{2})'
        match = re.search(pattern, url)
        if match:
            for i in range(20):
                num_str = str(i).zfill(2)  # Convert number to two-digit string
                modified_url = url[:match.start(1)] + "IMG_" + num_str + url[match.end(1):]
                modified_url = self.delete_max_and_extension(modified_url)  # Delete "_max_" and everything after "."
                urls_with_numbers.append(modified_url)
        else:
            urls_with_numbers.append(url)
        return urls_with_numbers
    
    def generate_floorplans_with_numbers(self, url):
        urls_with_numbers = []
        pattern = r'(IMG|FLP)_\d{2}'  # Updated pattern to match IMG or FLP followed by two digits
        matches = re.finditer(pattern, url)
        if matches:
            for match in matches:
                prefix = match.group(1)  # Get the matched prefix (IMG or FLP)
                for i in range(4):
                    num_str = str(i).zfill(2)  # Convert number to two-digit string
                    modified_url = url[:match.start()] + prefix + "_" + num_str + url[match.end():]
                    modified_url = self.delete_max_and_extension(modified_url)  # Delete "_max_" and everything after "."
                    urls_with_numbers.append(modified_url)
        else:
            urls_with_numbers.append(url)
        return urls_with_numbers
    
    def delete_max_and_extension(self, url):
        max_index = url.find("_max_")
        if max_index != -1:
            dot_index = url.rfind(".")
            if dot_index != -1:
                url = url[:max_index] + url[dot_index:]
        return url
        
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
        size = None
        tenure = None
        filtered_price = ''.join(filter(str.isdigit, property_data['price']))
        if filtered_price:
            price_integer = int(filtered_price)
        else:
            # Handle the case where price is not available
            # Set to 0, None, or some other default value
            price_integer = None  # or 0, depending on your needs
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

        query = "INSERT INTO properties (propertyLink, image, floorplan, title, price, description, isShared, updated, propertyType, bedrooms, bathrooms, size, tenure, postcode) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (url, property_data['image'], property_data['floorplan'], property_data['title'], price_integer, property_data['description'], property_data['isShared'], property_data['updated'], propertyType, bedrooms, bathrooms, size, tenure, property_data['postcode']))
        conn.commit()
        conn.close()
