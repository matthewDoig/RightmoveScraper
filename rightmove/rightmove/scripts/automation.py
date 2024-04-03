import subprocess
import pymysql
import images # This should be the module you've written for generating possible image links

# Function to run spiders or any external script
def run_spider(spider_command):
    try:
        subprocess.run(spider_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running spider: {e}")

# Connect to your MySQL database
def connect_to_db():
    connection = pymysql.connect(host='',
                                 user='root',
                                 password='',
                                 database='',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

# Main orchestration function
def main():
    db_conn = connect_to_db()
    with db_conn.cursor() as cursor:
        cursor.execute("DELETE FROM images_table",)
        db_conn.commit()
        cursor.execute("CREATE TABLE IF NOT EXISTS duplicate_properties LIKE properties;")
        db_conn.commit()
        # cursor.execute("CREATE TABLE IF NOT EXISTS duplicate_Rent LIKE Rent;")
        # db_conn.commit()
        # cursor.execute("DELETE FROM duplicate_Rent;")
        # db_conn.commit()
        cursor.execute("DELETE FROM duplicate_properties;")
        db_conn.commit()
        cursor.execute("INSERT INTO duplicate_properties SELECT * FROM properties;")
        db_conn.commit()
        # cursor.execute("INSERT INTO duplicate_Rent SELECT * FROM Rent;")
        # db_conn.commit()
        # cursor.execute("DELETE FROM Rent;")
        # db_conn.commit()
        # cursor.execute("DELETE FROM properties;")
        # db_conn.commit()
        cursor.execute("CREATE TABLE IF NOT EXISTS duplicate_images LIKE images;")
        db_conn.commit()
        cursor.execute("DELETE FROM duplicate_images;")
        db_conn.commit()
        cursor.execute("INSERT INTO duplicate_images SELECT * FROM images;")
        db_conn.commit()
        # cursor.execute("DELETE FROM images;")
        # db_conn.commit()
    db_conn.close()
    # Step 1: Run the spider to get all links
    run_spider(["scrapy", "crawl", "property"])

    # Step 2: Run the spider for individual links
    run_spider(["scrapy", "crawl", "individual"])

    
    # run_spider(["scrapy", "crawl", "individualRent"])

    db_conn = connect_to_db()
    with db_conn.cursor() as cursor:
        cursor.execute("""
            UPDATE images 
            SET propertyID = (
                SELECT id 
                FROM properties 
                WHERE properties.propertyLink = images.property_link
            )
            WHERE EXISTS (
                SELECT 1 
                FROM properties 
                WHERE properties.propertyLink = images.property_link
            );
                       """)
        db_conn.commit()
    db_conn.close()


if __name__ == "__main__":
    main()
