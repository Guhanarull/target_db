import mysql.connector
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import urllib.parse

# Configure logging
logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_mpn_upc(url):
    """Extract MPN and UPC from eBay URLs using web scraping."""
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract MPN
        mpn_element = soup.find('span', text='MPN')
        mpn = mpn_element.find_next('span').text.strip() if mpn_element else 'N/A'

        # Extract UPC
        upc_element = soup.find('span', text='UPC')
        upc = upc_element.find_next('span').text.strip() if upc_element else 'N/A'

        return mpn, upc
    except Exception as e:
        logging.error(f"Failed to extract MPN and UPC from {url}: {str(e)}")
        return 'N/A', 'N/A'

def decode_url(google_url):
    """Decode a Google redirect URL to extract the actual URL."""
    parsed_url = urllib.parse.urlparse(google_url)
    actual_url = urllib.parse.parse_qs(parsed_url.query)['q'][0]
    return urllib.parse.unquote(actual_url)

def fetch_and_insert_urls(domain, cursor_office, cursor_target, target_db):
    """Fetch URLs from the office database and insert them into the appropriate table in the target database."""
    fetch_query = f"SELECT MerchantURL1, MerchantURL2, MerchantURL3, MerchantURL4, MerchantURL5, MerchantURL6, MerchantURL7, MerchantURL8, MerchantURL9, MerchantURL10 FROM input WHERE Domain = '{domain}'"
    cursor_office.execute(fetch_query)
    urls = cursor_office.fetchall()

    for row in urls:
        for url in row:
            if url:
                decoded_url = decode_url(url)
                if "ebay.com" in decoded_url:
                    mpn, upc = extract_mpn_upc(decoded_url)
                    insert_query = "INSERT INTO ebay (URL, MPN, UPC) VALUES (%s, %s, %s)"
                    cursor_target.execute(insert_query, (decoded_url, mpn, upc))
                elif "officesupply.com" in decoded_url:
                    insert_query = "INSERT INTO officesupply (URL) VALUES (%s)"
                    cursor_target.execute(insert_query, (decoded_url,))
                elif "cleanitsupply.com" in decoded_url:
                    insert_query = "INSERT INTO cleanitsupply (URL) VALUES (%s)"
                    cursor_target.execute(insert_query, (decoded_url,))
                elif "walmart.com" in decoded_url:
                    insert_query = "INSERT INTO walmart (URL) VALUES (%s)"
                    cursor_target.execute(insert_query, (decoded_url,))
                elif "zerbee.com" in decoded_url:
                    insert_query = "INSERT INTO zerbee (URL) VALUES (%s)"
                    cursor_target.execute(insert_query, (decoded_url,))
                else:
                    # Insert into the not_match table if none of the conditions are met
                    insert_query = "INSERT INTO not_match (URL) VALUES (%s)"
                    cursor_target.execute(insert_query, (decoded_url,))
    target_db.commit()

def main():
    """Main function to manage database connections and process URLs."""
    try:
        office_db = mysql.connector.connect(host="localhost", user="root", password="Admin@123", database="office_supply")
        target_db = mysql.connector.connect(host="localhost", user="root", password="Admin@123", database="target_database")
        cursor_office = office_db.cursor()
        cursor_target = target_db.cursor()

        domains = ["ebay.com", "officesupply.com", "cleanitsupply.com", "walmart.com", "zerbee.com"]
        for domain in domains:
            fetch_and_insert_urls(domain, cursor_office, cursor_target, target_db)

    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
    finally:
        if office_db.is_connected():
            cursor_office.close()
            office_db.close()
        if target_db.is_connected():
            cursor_target.close()
            target_db.close()

    logging.info("Script execution completed.")

if __name__ == "__main__":
    main()
