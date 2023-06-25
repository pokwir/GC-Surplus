#--------------------------------------Importing Libraries--------------------------------------#

import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
import re

import warnings
warnings.filterwarnings('ignore')

from tqdm import tqdm
from time import sleep

import sqlite3

#--------------------------------------Defining Functions--------------------------------------#
pages = [str(i) for i in range(0, 272, 1)] # page iterations



#----------------------------------------Creating URL List--------------------------------------#
page_urls = []
for page in pages:
    base_url = 'https://www.gcsurplus.ca/mn-eng.cfm?&snc=wfsav&vndsld=1&sc=ach-shop&lci=&sf=ferm-clos&so=DESC&saleType=&srchtype=&hpcs=2300&hpsr=&kws=&jstp=sly&str='+page+'1&sr=1&rpp=10'
    page_urls.append(base_url)

#-------------------------------------------Item links--------------------------------------------#

ad_links = []
pbar = tqdm(total=len(page_urls), dynamic_ncols=True, colour= 'green')
for i, page in enumerate(page_urls):
    time.sleep(0.5)
    pbar.update(1)
    pbar.set_description(f"Downloading Page {i+1}/{len(page_urls) }")
    page = requests.get(page_urls[i])
    soup = BeautifulSoup(page.content, 'html.parser')
    # fint all links ending with 'clos&saleType=A'
    for link in soup.find_all('a', href=re.compile('clos&saleType=')):
        item_link = 'https://www.gcsurplus.ca/' + link.get('href')
        ad_links.append(item_link)
pbar.close()


#-------------------------------------------Scrape data--------------------------------------------#

pbar = tqdm(total=len(ad_links), dynamic_ncols=True, colour= 'green')
for i, ad in enumerate(ad_links):
    time.sleep(3)
    pbar.update(1)
    pbar.set_description(f"Downloading Ad {i+1}/{len(ad_links) }", refresh=True)

    time.sleep(1)
    # create a dataframe
    df = pd.DataFrame(columns=["title", "sold_for", "location", "minimum_bid", "closing_date", "year", \
                            "make", "model", "trim", "body_style", "engine_type", \
                                "transmission", "drive_line", "brake_type", "vin", "odometer", 'url'])
    # loop through each ad
   
    response = requests.get(ad)
    soup = BeautifulSoup(response.text, 'lxml')

    schema1 = soup.find_all("dd", {"class": "short"})
    # specifications schema 
    schema = soup.find("div", {"class": "col-sp-12 tPad10 fontSize95 lPad10"})
    data = {}
    specifications = schema.find('dl', class_='table-display').find_all('dt')
    for spec in specifications:
        title = spec.text.strip()
        value = spec.find_next('dd').text.strip()
        data[title] = value

    # get the title of the car
    # get the location of the car
    try:
        location = BeautifulSoup(schema1[1].text, 'lxml').get_text().strip()
    except:
        location = None

    # get item 
    try:
        title = data.get('Item:')
    except:
        title = None

    # get sold for 
    try:
            sold_for = data.get('Sold For (CAD):')
    except:
            sold_for = None
    # get minimum bid
    try:
        minimum_bid = data.get('Minimum bid:')
    except:
        minimum_bid = None

    # get Closing date
    try:
        closing_date = data.get('Closing date:')
    except:
        closing_date = None

    # get year of the car
    try:
        year = data.get('Year:')
    except:
        year = None

    # get make of the car
    try:
        make = data.get('Make:')
    except:
        make = None

    # get model of the car
    try:
        model = data.get('Model:')
    except:
        model = None

    # get trim of the car
    try:
        trim = data.get('Trim:')
    except:
        trim = None

    # get Body style of the car
    try:
        body_style = data.get('Body style:')
    except:
        body_style = None

    # get engine type
    try:
        engine_type = data.get('Engine type:')
    except:
        engine_type = None

    # get transmission
    try:
        transmission = data.get('Transmission:')
    except:
        transmission = None

    # get drive line
    try:
        drive_line = data.get('Drive line:')
    except:
        drive_line = None

    #get brake type
    try:
        brake_type = data.get('Brake type:')
    except:
        brake_type = None

    # get VIN/Serial number
    try:
        vin = data.get('VIN/Serial number:')
    except:
        vin = None

    # get odometer
    try:
        odometer = data.get('Odometer:')
    except:
        odometer = None

    # append the results to the dataframe
    df = df.append({"title": title, "sold_for": sold_for, "location": location, "minimum_bid": minimum_bid, \
                "closing_date": closing_date, "year": year, "make": make, "model": model, "trim": trim, \
                    "body_style": body_style, "engine_type": engine_type, "transmission": transmission, \
                    "drive_line": drive_line, "brake_type": brake_type, "vin": vin, "odometer": odometer, "url": ad}, ignore_index=True)
    
    # ----------------------------------------Saving to Database--------------------------------------------#
    conn = sqlite3.connect("Gcsurplus.db")
    cur = conn.cursor()
    for i in range(len(df)):
        cur.execute("INSERT INTO kijiji (\
                title, \
                sold_for, \
                location, \
                minimum_bid, \
                closing_date, \
                year, \
                make, \
                model, \
                trim, \
                body_style, \
                engine_type, \
                transmission, \
                drive_line, \
                brake_type, \
                vin, \
                odometer,\
                url) \
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",\
                    (\
                    df["title"][i], \
                    df["sold_for"][i], \
                    df["location"][i], \
                    df["minimum_bid"][i], \
                    df["closing_date"][i], \
                    df["year"][i], \
                    df["make"][i], \
                    df["model"][i],\
                    df["trim"][i], \
                    df["body_style"][i], \
                    df["engine_type"][i], \
                    df["transmission"][i], \
                    df["drive_line"][i], \
                    df["brake_type"][i], \
                    df["vin"][i], \
                    df["odometer"][i], \
                    df["url"][i]))
    # make tqdm progress bar
    conn.commit()
    pbar.set_description(f"Adding {title} to database", refresh=True)
    time.sleep(1)
    # print len of data in database
    cur.execute("SELECT * FROM kijiji")
    rows = cur.fetchall()
    pbar.set_description(f"There are {len(rows)} records in the database", refresh=True)
    conn.close()
    time.sleep(1)
pbar.close()
