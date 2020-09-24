import csv
import requests
import threading
import numpy as np
from urllib.parse import urlencode
from bs4 import BeautifulSoup
API = 'INPUT SCRAPER API HERE' ## --> Signup up for free API key here https://www.scraperapi.com/signup

def get_url(url):
    payload = {'api_key': API, 'url': url}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


with open('input_asins.csv', encoding='utf-8', errors='ignore') as f:
    # read csv file into list of dicts
    my_list = list(csv.DictReader(f, skipinitialspace=True))

output_file = open('output.csv', 'w', encoding='utf-8-sig', errors='ignore')
keys = ['asin', 'title', 'rating', 'num_of_rating', 'price', 'bullet_points', 'url']
dict_writer = csv.DictWriter(output_file, keys)
dict_writer.writeheader()
csv_writer_lock = threading.Lock()  # usefull when writing to same file from multiple threads


def check_amz(*asins):
    for asin in asins:
        url = "https://www.amazon.com/dp/" + asin
        scraperapi_url = get_url(url)
        for _ in range(2):
            # number of retries 2
            try:
                res = requests.get(scraperapi_url)
                if res.status_code in [200, 404]:
                    break
            except requests.exceptions.ConnectionError:
                res = ''
        if not res:
            continue
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'lxml')  # lxml parser
            title = soup.find(id="productTitle").text
            if soup.find(id="acrPopover"):
                rating = soup.find(id="acrPopover").get('title')
            else:
                rating = ''
            if soup.find(id="acrCustomerReviewText"):
                num_of_rating = soup.find(id="acrCustomerReviewText").text
            else:
                num_of_rating = ''
            price = soup.find(id="priceblock_ourprice") or soup.find(id="price_inside_buybox")
            if price:
                price = price.text
            elif soup.find(attrs={"data-asin-price": True}):
                price = soup.find(attrs={"data-asin-price": True}).get('data-asin-price')
            else:
                price = ''
            bullet_points = [i.text.strip() for i in soup.find(id="feature-bullets").find_all('span')]
            item = {'asin': asin, 'title': title, 'rating': rating, 'num_of_rating': num_of_rating,
                    'price': price, 'bullet_points': bullet_points, 'url': url}
            for k, v in item.items():
                if not v:
                    item[k] = ''
                elif isinstance(v, list):
                    item[k] = ", ".join(v)
                else:
                    item[k] = v.strip()
            with csv_writer_lock:
                dict_writer.writerow(item)


# Select column ASIN plus filter duplicates
asins_to_scrape = set(i['asin'] for i in my_list)
# Split the asins to 10 equal lengths chunks this is equal to amount of threads
chunks = [i.tolist() for i in np.array_split(list(asins_to_scrape), 2) if i.size > 0]
threads = []
for lst in chunks:
    # place each chunk in a thread
    threads.append(threading.Thread(target=check_amz, args=lst))
for x in threads:
    x.start()
for x in threads:
    x.join()
output_file.close()
print("Done")
