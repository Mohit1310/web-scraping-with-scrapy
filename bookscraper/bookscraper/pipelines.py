# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):
        
        adapter = ItemAdapter(item)
        
        # strip all the whitespaces for strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()

        # Category & Product Type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()
            
        # Price --> convert to float
        price_keys = ['price_excl_tax', 'product_incl_tax', 'tax', 'price']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        # Availability --> extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if(len(split_string_array) > 2):
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        # Reviews --> convert string to number
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)
        
        # Stars --> convert text to number
        stars_string = adapter.get('stars')
        split_starts_array = stars_string.split(' ')
        star_text_value = split_starts_array[1].lower()
        if star_text_value == 'zero':
            adapter['stars'] = 0
        elif star_text_value == 'one':
            adapter['stars'] = 1
        elif star_text_value == 'two':
            adapter['stars'] = 2
        elif star_text_value == 'three':
            adapter['stars'] = 3
        elif star_text_value == 'four':
            adapter['stars'] = 4
        elif star_text_value == 'five':
            adapter['stars'] = 5

        return item

import mysql.connector

class SaveToMySQLPipeline:
    
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'Mohit@123',
            database = 'books'
        )
        
        # Create cursor, used to execute SQL commands
        self.cur = self.conn.cursor()
        
        # Create books table if it doesn't exist
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id int NOT NULL AUTO_INCREMENT,
                url VARCHAR(255),
                title text,
                upc VARCHAR(255),
                product_type VARCHAR(255),
                price_excl_tax DECIMAL,
                product_incl_tax DECIMAL,
                tax VARCHAR(255),
                price DECIMAL,
                availability INTEGER,
                num_reviews INTEGER,
                stars INTEGER,
                category VARCHAR(255),
                description TEXT,
                PRIMARY KEY (id)
            )
        """)
        
    def process_item(self, item, spider):
        
        # Insert data into the database
        self.cur.execute(""" insert into books (
            url,
            title,
            upc,
            product_type,
            price_excl_tax,
            product_incl_tax,
            tax,
            price,
            availability,
            num_reviews,
            stars,
            category,
            description 
            ) values (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
            )""", (
                item['url'],
                item['title'],
                item['upc'],
                item['product_type'],
                item['price_excl_tax'],
                item['product_incl_tax'],
                item['tax'],
                item['price'],
                item['availability'],
                item['num_reviews'],
                item['stars'],
                item['category'],
                str(item['description'][0])
            ))
        
        # Execute insert of data into database
        self.conn.commit()
        return item
    
    def close_spider(self, spider):
        
        # close cursor & connection to database
        self.cur.close()
        self.conn.close()


# import psycopg2

# class SaveToMySQLPipeline:
    
#     def __init__(self):
#         self.conn = psycopg2.connect(
#             host = 'localhost',
#             user = 'postgres',
#             password = 'Mohit@123',
#             dbname = 'books'
#         )
        
#         # Create cursor, used to execute SQL commands
#         self.cur = self.conn.cursor()
        
#         # Create books table if it doesn't exist
#         self.cur.execute("""
#             CREATE TABLE IF NOT EXISTS books (
#                 id SERIAL PRIMARY KEY,
#                 url VARCHAR(255),
#                 title TEXT,
#                 upc VARCHAR(255),
#                 product_type VARCHAR(255),
#                 price_excl_tax NUMERIC(10,2),
#                 product_incl_tax NUMERIC(10,2),
#                 tax VARCHAR(255),
#                 price NUMERIC(10,2),
#                 availability INTEGER,
#                 num_reviews INTEGER,
#                 stars INTEGER,
#                 category VARCHAR(255),
#                 description TEXT
#             )
#         """)
        
#     def process_item(self, item, spider):
        
#         # Insert data into the database
#         self.cur.execute(""" insert into books (
#             url,
#             title,
#             upc,
#             product_type,
#             price_excl_tax,
#             product_incl_tax,
#             tax,
#             price,
#             availability,
#             num_reviews,
#             stars,
#             category,
#             description 
#             ) values (
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s
#             )""", (
#                 item['url'],
#                 item['title'],
#                 item['upc'],
#                 item['product_type'],
#                 item['price_excl_tax'],
#                 item['product_incl_tax'],
#                 item['tax'],
#                 item['price'],
#                 item['availability'],
#                 item['num_reviews'],
#                 item['stars'],
#                 item['category'],
#                 str(item['description'][0])
#             ))
        
#         # Execute insert of data into database
#         self.conn.commit()
#         return item
    
#     def close_spider(self, spider):
        
#         # close cursor & connection to database
#         self.cur.close()
#         self.conn.close()
