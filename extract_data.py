import re
import datetime
from datetime import date
import requests
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import os
from datetime import timedelta
import pandas as pd

def get_api_key(link):
    """Get the API key from wunderground
    Args:
        link (str): website link
    Returns:
        api_key: API key
    """
    response = requests.get(link)
    html_doc = response.text
    soup = BeautifulSoup(html_doc, 'html.parser')
    scrp = soup.find_all('script', id="app-root-state")
    str1 = 'SUN_API_KEY&q;:&q;'
    res1 = re.findall(str1+'(.*)', str(scrp))
    api_key = res1[0].split('&q;')[0]
    return api_key

d=get_api_key("https://www.wunderground.com/history/daily/us/ny/new-york-city/KJFK/")


def format_data(given_date, api_key):
    """Get relevant data from the wunderground request call
    Args:
        given_date (str): Date string
        api_key (str): API key from website
    Returns:
        transposed_object: Dictionary of lists with weather values every hour
    """
    given_date = datetime.datetime.strptime(given_date, '%Y-%m-%d').strftime('%Y%m%d')
    #end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d')
    url = f'https://api.weather.com/v1/location/KJFK:9:US/observations/historical.json?apiKey={api_key}&units=e&startDate={given_date}&endDate={given_date}'
    response = requests.get(url).json()
    mappings = {"Hour": 'hour', "Date": 'date', "Temp": 'temp',
                "Dew": 'dewPt', "humidity": 'rh', "Wind Cardinal": 'wdir_cardinal',
                "Wind Speed": 'wspd', "Wind Gust": 'gust', "Pressure List": 'pressure',
                "Precip Rate": 'precip_hrly', "Condition": 'wx_phrase'}
    formatted_object = []
    try:
        for tuple1 in response['observations']:
            timestamp = tuple1['valid_time_gmt']
            given_date = datetime.datetime.fromtimestamp(timestamp)
            tuple1["date"] = given_date.strftime("%d/%b/%Y")
            tuple1["hour"] = given_date.strftime("%H")
            formatted_tuple = {}
            for element in mappings.keys():
                formatted_tuple[element] = tuple1[mappings[element]] if tuple1[mappings[element]] else 0
            formatted_object.append(formatted_tuple)
        transposed_object = {}

        for element in mappings.keys():
            temp_list = []
            for tuple in formatted_object:
                temp_list.append(tuple[element])
            transposed_object[element] = temp_list
        return transposed_object
    except KeyError:
        return None

def get_list_of_dates_to_process(start_date):
    """
    Function to list of dates to process
    Args:
        start_date(date): Starting date
    Returns:
        (date): List of dates from start date to yesterday's date
    """
    en_date = date.today() - timedelta(days=2)
    date_df = pd.date_range(start_date, en_date, freq='d')
    return date_df.strftime('%Y/%m/%d').to_list()
    
#format_data("2022-02-05","2022-01-30", "e1f10a1e78da46f5b10a1e78da96f525")
k=[]
for i in get_list_of_dates_to_process("2022-3-27"):
    try:
        n=format_data(i,d)
        condition=n['Condition']
        datee=n['Date']
        dew=n['Dew']
        hour=n['Hour']
        humidity=n['humidity']
        precip_rate=n['Precip Rate']
        pressure_list=n['Pressure List']
        temp=n['Temp']
        wind_cardinal=n['Wind Cardinal']
        wind_gust=n['Wind Gust']
        wind_speed=n['Wind Speed']
        i=0
        while i<len(temp):
            k.append((condition[i],temp[i],
                  datee[i],dew[i],hour[i],
                  humidity[i],precip_rate[i],pressure_list[i],
                  wind_cardinal[i],wind_gust[i],
                  wind_speed[i]))
            i=i+1
    except:
        pass
    

        #print(k)
    #k.append(n)
    #print(n)
#wind_speed=   

# =============================================================================
# i = 0
# while i < len(k):
#     #m.update(k[i])
#     print(k[i])
#     i += 1
# =============================================================================


# df = pd.DataFrame(list(n.items()),columns = ["Hour", "Date", "Temp",
#                 "Dew", "humidity", "Wind Cardinal",
#                 "Wind Speed", "Wind Gust", "Pressure List",
#                 "Precip Rate", "Condition"]) 

kn=pd.DataFrame.from_dict(k)
kn.to_csv(r'C:\Users\RAJPUT\Desktop\CDAC\We.csv', index=False)
#df = pd.DataFrame(list(n.items()),columns = ["date", "hour"])

##for k in df.iloc[1]:
 #   print(k)
    

# cnt=0
# for q in quotes:
#     q
#     cnt+=1
#     cnt,q['Hour'],end='\n\n'   
    
# print(q)
    
    