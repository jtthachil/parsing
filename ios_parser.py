from re import sub  #regex==2022.6.2
from turtle import title
from urllib.request import urlopen, Request     #urllib for opening URLs    #urllib==1.26.9
from bs4 import BeautifulSoup   #BeautifulSoup for getting text from web pages(HTML+CSS files)  #beautifulsoup4==4.11.1
import requests     #requests for keep-alive, better and faster server connection   #requests==2.28.0 ; requests-aws4auth==1.1.2 ; requests-oauthlib==1.3.1 ; requests-toolbelt==0.10.1
import nltk     #nltk for extracting nouns from text    #nltk==3.7
import json       #json module for making json out of string    #built-in
#nltk.download('averaged_perceptron_tagger')
#nltk.download('punkt')
import datetime #built-in
from datetime import timedelta, date, datetime  #built-in
import pandas as pd #pandas==1.4.3
import numpy as np  #numpy==1.23.1
import os   #built-in
import threading    #built-in
import time #built-in
import urllib.request   #urllib3==1.26.9
import mysql.connector  #mysql-connector-python==8.0.30
import re   #regex==2022.6.2
import paramiko #paramiko==3.2.0
import socket   #built-in
from ipwhois import IPWhois #ipwhois==1.2.0
import ssl #built-in
from urllib.parse import urlparse

def extract_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc

startTime = datetime.now()

lock = threading.Lock()

def get_server_location(domain_eco):
    # Step 1: Get the IP address of the domain
    ip_address = socket.gethostbyname(domain_eco)
    
    # Step 2: Get the geolocation information for the IP address
    ipwhois = IPWhois(ip_address)
    result = ipwhois.lookup_rdap()

    # Extract relevant information
    country_eco = result['asn_country_code']
    #city = result['asn_description']

    #return f"The server of {domain} is located in {country}."
    return country_eco

def upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory):
    # Create an SSH client and connect to the SFTP server
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(sftp_url, username=username, password=password)

    # Create an SFTP client
    sftp = ssh_client.open_sftp()

    try:
        # Change to the remote directory where you want to store the file
        sftp.chdir(remote_directory)
         # Get the filename from the local file path
        local_filename = local_file_path.split('/')[-1]

        # Upload the file to the SFTP server
        sftp.put(local_file_path, local_filename)

        print(f"File {local_filename} uploaded successfully to {sftp_url}/{remote_directory}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Failed to upload the file: {str(e)}")
    finally:
        # Close the SFTP session and the SSH client
        sftp.close()
        ssh_client.close()

def get_page_size(url):
    url_1, url_2 = url, url
    if str(url).startswith('http') == False :
        url_1 = 'https://' + url
        url_2 = 'http://' + url
    try:
        response = requests.get(url_1)
        response.raise_for_status() 
        page_size = len(response.content)
        return page_size / 1000000000
    except :
        try :
            response = requests.get(url_1)
            response.raise_for_status() 
            page_size = len(response.content)
            return page_size / 1000000000
        except :
            return 0.003    #in gigabytes
        


brand_profile_ids_response = requests.get("https://certm8.com/platform/agency1/bprofileid/?k=786123").text

brand_profile_ids_response_jsonified = json.loads(brand_profile_ids_response)

for dictio_item in brand_profile_ids_response_jsonified :
    if type(dictio_item) == str :
        print("No brand profiles listed")
    elif type(dictio_item) != str :
        brand_profiles = brand_profile_ids_response_jsonified

def get_profile_id_settings(file_id_for_api_calls) :
    list_of_files_response = requests.get("https://certm8.com/platform/agency1/nx/?k=786123").text

    list_of_files_response_jsonified = json.loads(list_of_files_response)

    brand_profile_id = '-1'

    for dictionary_item in list_of_files_response_jsonified :
        if type(dictionary_item) == str :
            print("Bad response, no files listed in the API")
            return '0'
        if file_id_for_api_calls in dictionary_item["identifier"]:
            brand_profile_id = dictionary_item["brandprofileid"]
            break
    
    if brand_profile_id == '' :
        print("Brand profile ID is empty")
        return '0'
    if brand_profile_id == '-1' :
        print("File id could not be matched for brand profile id")
        return '0'
    if not brand_profiles :
        print("Empty brand profiles")
        return '0'
    for brand_profile in brand_profiles :
        if brand_profile["id"] == brand_profile_id :
            return brand_profile
    print("Brand profile ID could not be found in profiles list")

def get_brand_profile_id(file_id_for_api_calls) :
    list_of_files_response = requests.get("https://certm8.com/platform/agency1/nx/?k=786123").text

    list_of_files_response_jsonified = json.loads(list_of_files_response)

    brand_profile_id = '0'

    for dictionary_item in list_of_files_response_jsonified :
        if type(dictionary_item) == str :
            print("Bad response, no files listed in the API")
            return brand_profile_id
        if file_id_for_api_calls in dictionary_item["identifier"]:
            return dictionary_item["brandprofileid"]
            
    return brand_profile_id

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Iamgroot72",
  database="parsers"
)

mycursor = mydb.cursor()


eco_country_get_query = "SELECT country, renewablePercentage from greenrating"

mycursor.execute(eco_country_get_query)

country_and_renewable_dict = {}


for i in list(mycursor) :
    country_and_renewable_dict[i[0]] = i[1]


black_surnames_from_db_query = "SELECT surnames from black_surnames"
mycursor.execute(black_surnames_from_db_query)

black_surnames_list = []
for i in list(mycursor) :
    black_surnames_list.append(i[0])


black_first_names_from_db_query = "SELECT first_names from black_first_names"
mycursor.execute(black_first_names_from_db_query)

black_first_names_list = []
for i in list(mycursor) :
    black_first_names_list.append(i[0])

bipoc_surnames_from_db_query = "SELECT surnames from bipoc_surnames"
mycursor.execute(bipoc_surnames_from_db_query)

bipoc_surnames_list = []
for i in list(mycursor) :
    bipoc_surnames_list.append(i[0])


whitespaces = [" ", "\t", "\n", "\v", "\r", "\f"]

def trim_whitespaces_from_both_sides (keyword) :
    if keyword is None :
        return keyword
    while keyword[0] in whitespaces:
        keyword = keyword[1:]
    while keyword[-1] in whitespaces:
        keyword = keyword[:-1]
    return keyword

# 14.08 Brand safety    START
brand_safety_file = open('brand_safety_keywords.json')

brand_safety_dict = json.load(brand_safety_file)

brand_safety_file.close()

adult_list = brand_safety_dict["adult"]
alcohol_list = brand_safety_dict["alcohol"]
aviation_list = brand_safety_dict["aviation"]
politics_brand_safety_list = brand_safety_dict["politics"]
piracy_list = brand_safety_dict["piracy"]
tobacco_list = brand_safety_dict["tobacco"]
cannabis_list = brand_safety_dict["cannabis"]
car_accidents_list = brand_safety_dict["car_accidents"]
# 14.08 Brand safety    END

removed_keywords = ["https", "http", "utc", "us", "pm", "am", "follow", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                    "sunday", "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november",
                    "december"]



icategories_to_iab_file = open('icategories_to_iab.json')

icategories_to_iab_dict = json.load(icategories_to_iab_file)

icategories_to_iab_file.close()

def parse_the_ios_id(app_id_to_parse, deeper_green_calculation_flag, deeper_green_attributes_dictionary, error_ids_now, file_id_for_apis) :

    app_id_to_parse = str(app_id_to_parse)

    print("Starting to process iOS ID :", app_id_to_parse)
    if error_ids_now == False :
        try :
        #create soup
            url = Request("https://apps.apple.com/us/app/id" + app_id_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
            html = urlopen(url, timeout=10).read()
            soup = BeautifulSoup(html, features="html.parser")
        #if not found on app store
        except :
            if app_id_to_parse not in error_ids :
                error_ids.append(app_id_to_parse)
            return
    elif error_ids_now == True : 
        try :
            context = ssl._create_unverified_context()
            url = Request("https://apps.apple.com/us/app/id" + app_id_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
            html = urlopen(url, timeout=10, context=context).read()
            soup = BeautifulSoup(html, features="html.parser")
            print("soup created without ssl")
        except :
            try :
                url = Request("http://api.scraperapi.com?api_key=c23e0dd0e126b0904b80503545b26861&url=" + "https://apps.apple.com/us/app/id" + app_id_to_parse + "&render=true", headers={'User-Agent' : 'Mozilla/5.0'})
                html = urlopen(url, timeout=15, context=context).read()
                soup = BeautifulSoup(html, features="html.parser")
                print(app_id_to_parse + " was successful with api_url")
            except :
                print("soup cannot be created for ", app_id_to_parse)
                return
            
    try :
        
        lock.acquire()

        
        
        app_id_col.append(app_id_to_parse)
        app_type_col.append("iOS")
        downloads_col.append("")


        #removing trash(script, style tags) from the soup
        for script in soup(["script", "style"]) :
            script.extract()

        #getting the title tag
        if soup.title is not None :
            ios_app_name = str(soup.title.string)
            ios_app_name = trim_whitespaces_from_both_sides(ios_app_name)
            if len(ios_app_name) > 17 :
                ios_app_name = ios_app_name[:-17]
            app_name_col.append(ios_app_name)
        elif soup.title is None :
            app_name_col.append("")

        #getting the tablet optimized
        if soup.find("h3", class_="app-header__designed-for") is not None : 
            tablet_optimized_col.append("1")
        elif soup.find("h3", class_="app-header__designed-for") is None :
            tablet_optimized_col.append("0")


        #greenrating
        dev_web_div = soup.find("a", class_="link icon icon-after icon-external")

        is_privacy_found = False

        div_for_privacy = soup.find_all("a", class_="link icon icon-after icon-external")

        for subdiv in div_for_privacy :
            if subdiv.string.strip() == 'Privacy Policy' :
                privacy_link = subdiv['href']

                if privacy_link.startswith("http") and privacy_link.find('://') != -1 :
                    privacy_link = privacy_link[privacy_link.find('://') + 3:]
                if privacy_link.endswith('/') :
                    privacy_link = privacy_link[:-1]
                
                
                devurl_col.append(privacy_link)
                is_privacy_found = True
                break

        greenrating_value_for_appending = 35
        devurl_value_for_appending = 'Privacy URL Not Available'

        if dev_web_div is not None and dev_web_div.string.strip() == 'Developer Website': 
            dev_web = dev_web_div['href']

            if dev_web.startswith("http") and dev_web.find('://') != -1 :
                dev_web = dev_web[dev_web.find('://') + 3:]
            if dev_web.endswith('/') :
                dev_web = dev_web[:-1]
            domain_to_check = dev_web
            if is_privacy_found == False :
                devurl_col.append(domain_to_check)

            #greenrating
            try:
                location_info = get_server_location(domain_to_check)
                if location_info in country_and_renewable_dict :
                    greenrating_value_for_appending = country_and_renewable_dict[location_info]
                    greenrating_col.append(greenrating_value_for_appending)
                elif location_info not in country_and_renewable_dict :
                    greenrating_col.append(greenrating_value_for_appending)
            except Exception as e:
                greenrating_col.append(greenrating_value_for_appending)


        else:
            greenrating_col.append(greenrating_value_for_appending)
            if is_privacy_found == False :
                devurl_col.append(devurl_value_for_appending)

        
        
        #greenrating


        #getting the rating of the app
        rating = soup.find("figcaption", class_="we-rating-count star-rating__count")

        if rating :
            rating_full = trim_whitespaces_from_both_sides(rating.string)
            rating_col.append(rating_full[:rating_full.find("•") - 1])
        elif not rating :
            rating_col.append("")
            

        looking_for_coppa = '1'
        profile_for_checking_coppa = get_profile_id_settings(file_id_for_apis)
        if profile_for_checking_coppa != '0' :
            looking_for_coppa = profile_for_checking_coppa["coppa"]
        
        #getting the age rating of the app
        age_element = soup.find("span", class_="badge badge--product-title")

        if age_element :
            # 15.08 COPPA START
            age_rating_value = trim_whitespaces_from_both_sides(age_element.string)
            age_rating_value = age_rating_value.replace("+", "")
            unknown_format = False
            for digit in age_rating_value :
                if digit.isdigit() == False :
                    unknown_format = True
                    break
            if unknown_format == False :
                if int(age_rating_value) < 13 :
                    coppa_col.append('1')
                elif int(age_rating_value) >= 13 :
                    coppa_col.append('0')
            elif unknown_format == True :
                coppa_col.append("Incorrect format")
            age_rating_col.append(age_rating_value)
            # 15.08 COPPA END
        elif not age_element :
            age_rating_col.append("")
            # 15.08 COPPA
            coppa_col.append("Age rating not present")
            # 15.08 COPPA END

        #Because brand profile's coppa is set to 0, meaning no need to look for coppa, everything passes, so coppa is automatically 1
        if looking_for_coppa == '0' :
            coppa_col.pop()
            coppa_col.append("1")

        #getting the category of the app
        category_available = False
        parent_category_element = soup.find_all("dd", {"class" : "information-list__item__definition"})
        for i in parent_category_element :
            child_category_element = i.find("a", {"class" : "link"})
            if child_category_element :
                app_category = child_category_element.text.strip()
                app_category_col.append(app_category)
                iab_category_col.append(icategories_to_iab_dict[app_category])
                category_available = True
                break

        if category_available == False :
            app_category_col.append("")
            iab_category_col.append("IAB Category Not Available")

        #getting the developer of the app
        parent_dev_el = soup.find("h2", {"class" : "product-header__identity app-header__identity"})
        if parent_dev_el :
            child_dev_el = parent_dev_el.find("a")
        elif not parent_dev_el :
            child_dev_el = None

        if child_dev_el :
            app_dev_ios = child_dev_el.text.strip()
            app_developer_col.append(app_dev_ios)

            black_surname_found = False
            black_first_name_found = False
            bipoc_surname_found = False
            bipoc_value = 0
            bipoc_conf = 0

            for black_surname in black_surnames_list :
                if black_surname in app_dev_ios :
                    black_surname_found = True
                    break
            for black_first_name in black_first_names_list :
                if black_first_name in app_dev_ios :
                    black_first_name_found = True
                    break

            if black_surname_found == True and black_first_name_found == True :
                bipoc_value = 1
                bipoc_conf = 2
            elif black_surname_found == True or black_first_name_found == True :
                bipoc_value = 1
                bipoc_conf = 1
            elif black_surname_found == False and black_first_name_found == False :
                for bipoc_surname in bipoc_surnames_list :
                    if bipoc_surname in app_dev_ios :
                        bipoc_value = 1
                        bipoc_conf = 1
                        bipoc_surname_found = True
                        break

            bipoc_col.append(str(bipoc_value))
            bipoc_conf_col.append(str(bipoc_conf))

        elif not child_dev_el : 
            app_developer_col.append("")
            bipoc_col.append("")
            bipoc_conf_col.append("")



        #getting the top 10 keywords
        keywords_flag = False
        parent_desc_el = soup.find_all("div", {"class" : "l-row"})
        for i in parent_desc_el :
            finding = i.find("div", {"class" : "we-truncate we-truncate--multi-line we-truncate--interactive l-column small-12 medium-9 large-8"})
            if finding :
                the_p = finding.find("p")
                if the_p :
                    strips = list(the_p.stripped_strings)
                    keywords_flag = True
                    break

        if keywords_flag == False :
            top_10_keywords_col.append("")
            adult_col.append("")
            alcohol_col.append("")
            aviation_col.append("")
            crime_col.append("")
            drug_col.append("")
            hate_col.append("")
            human_made_disasters_col.append("")
            politics_brand_safe_col.append("")
            natural_disasters_col.append("")
            piracy_col.append("")
            profanity_col.append("")
            terrorism_col.append("")
            tobacco_col.append("")
            cannabis_col.append("")
            car_accidents_col.append("")
            brand_safe_col.append("")
            brand_profile_id_col.append("0")

        elif keywords_flag == True :
        
            nouns = []

            # 14.08 Brand safety    START
            verbs = []
            # 14.08 Brand safety END

            #filtering out nouns from all strings
            for strip in strips :
                for word, pos in nltk.pos_tag(nltk.word_tokenize(str(strip))) :
                    if(pos == 'NN' or pos == 'NNP' or pos == 'NNS' or pos == 'NNPS') : #if it is noun by nltk
                        if len(word) > 1 :  #if it has more than 1 character
                            if word.lower() not in removed_keywords :
                                nouns.append(word)
                    # 14.08 Brand safety    START
                    elif(pos == 'VBN' or pos == 'VBD' or pos == 'VB' or pos == 'VBG' or pos == 'VBP' or pos == 'VBZ') : #if it is verb by nltk
                        if len(word) > 1 :
                            verbs.append(word)

            nouns_and_verbs = nouns + verbs

            looking_for_adult = '1'
            looking_for_alcohol = '1'
            looking_for_aviation = '1'
            looking_for_politics = '1'
            looking_for_piracy = '1'
            looking_for_tobacco = '1'
            looking_for_cannabis = '1'
            looking_for_caraccidents = '1'

            brand_profile_id_for_output = '0'

            is_brand_safe = 1
            adult_value = 0
            alcohol_value = 0
            aviation_value = 0
            politics_brand_safety_value = 0
            piracy_value = 0
            tobacco_value = 0
            cannabis_value = 0
            car_accidents_value = 0

            profile_for_checking = get_profile_id_settings(file_id_for_apis)
            if profile_for_checking != '0' :
                looking_for_adult = profile_for_checking["adult"]
                looking_for_alcohol = profile_for_checking["alcohol"]
                looking_for_aviation = profile_for_checking["aviation"]
                looking_for_politics = profile_for_checking["politics"]
                looking_for_piracy = profile_for_checking["piracy"]
                looking_for_tobacco = profile_for_checking["tobacco"]
                looking_for_cannabis = profile_for_checking["cannabis"]
                looking_for_caraccidents = profile_for_checking["caraccidents"]
                brand_profile_id_for_output = profile_for_checking["id"]

            for noun_or_verb in nouns_and_verbs :
                if looking_for_adult == '1' and adult_value == 0 and noun_or_verb.lower() in adult_list :
                    adult_value = 1
                    is_brand_safe = 0
                if looking_for_alcohol == '1' and alcohol_value == 0 and noun_or_verb.lower() in alcohol_list :
                    alcohol_value = 1
                    is_brand_safe = 0
                if looking_for_aviation == '1' and aviation_value == 0 and noun_or_verb.lower() in aviation_list :
                    aviation_value = 1
                    #is_brand_safe = 0
                if looking_for_politics == '1' and politics_brand_safety_value == 0 and noun_or_verb.lower() in politics_brand_safety_list :
                    politics_brand_safety_value = 1
                    is_brand_safe = 0
                if looking_for_piracy == '1' and piracy_value == 0 and noun_or_verb.lower() in piracy_list :
                    piracy_value = 1
                    is_brand_safe = 0
                if looking_for_tobacco == '1' and tobacco_value == 0 and noun_or_verb.lower() in tobacco_list :
                    tobacco_value = 1
                    is_brand_safe = 0
                if looking_for_cannabis == '1' and cannabis_value == 0 and noun_or_verb.lower() in cannabis_list :
                    cannabis_value = 1
                    is_brand_safe = 0
                if looking_for_caraccidents == '1' and car_accidents_value == 0 and noun_or_verb.lower() in car_accidents_list :
                    car_accidents_value = 1
                    is_brand_safe = 0

            adult_col.append(str(adult_value))
            alcohol_col.append(str(alcohol_value))
            aviation_col.append(str(aviation_value))
            crime_col.append('0')
            drug_col.append('0')
            hate_col.append('0')
            human_made_disasters_col.append('0')
            politics_brand_safe_col.append(str(politics_brand_safety_value))
            natural_disasters_col.append('0')
            piracy_col.append(str(piracy_value))
            profanity_col.append('0')
            terrorism_col.append('0')
            tobacco_col.append(str(tobacco_value))
            cannabis_col.append(str(cannabis_value))
            car_accidents_col.append(str(car_accidents_value))
            brand_safe_col.append(str(is_brand_safe))
            brand_profile_id_col.append(brand_profile_id_for_output)
            # 14.08 Brand safety END

            result_nouns = {}

            #finding frequencies of nouns
            for noun in nouns :
                new_noun = noun[0].upper() + noun[1:].lower()   #common form for all strings, so that Example=eXAMPLE=ExAmPlE
                if new_noun in result_nouns :
                    result_nouns[new_noun] += 1
                else :
                    result_nouns[new_noun] = 1

            #descending sorting on frequencies
            sorted_result = sorted(result_nouns.items(), key=lambda x: x[1], reverse=True)

            top_10_keywords = []

            if len(sorted_result) > 9 :
                for i in range(10) :
                    top_10_keywords.append(sorted_result[i][0])
            elif len(sorted_result) <= 9 :
                for i in range(len(sorted_result)) :
                    top_10_keywords.append(sorted_result[i][0])


            keywords_string = ''
            for keywo in top_10_keywords :
                keywords_string += ', ' + keywo
            if len(keywords_string) > 3 :
                keywords_string = keywords_string[2:]

            top_10_keywords_col.append(str(keywords_string))

        processed_date_col.append(today)

        # App size
        app_size_from_app_store = 50
        app_metrics = 'MB'
        parent_size_el = soup.find_all("div", {"class" : "information-list__item l-column small-12 medium-6 large-4 small-valign-top"})

        for i in parent_size_el :
            intermezzo = i.find("dt", {"class" : "information-list__item__term medium-valign-top"})
            if intermezzo.text.strip() == 'Size' :
                size_text = i.find("dd").text.strip()
                if '.' in size_text :
                    app_size_from_app_store = int(size_text[:size_text.find('.')])
                    app_metrics = size_text[-2:]
                    break
                elif '.' not in size_text :
                    app_size_from_app_store = int(size_text[:size_text.find(' ')])
                    app_metrics = size_text[-2:]
                    break
                


        #gb conversion
        if app_metrics == 'GB' :
            app_size_in_gb = app_size_from_app_store
        elif app_metrics == 'MB' :
            app_size_in_gb = app_size_from_app_store / 1000
        elif app_metrics == 'KB' :
            app_size_in_gb = app_size_from_app_store / 1000000

        app_size_col.append(str(app_size_in_gb))

        page_size_col.append("")

        #Deeper green calculation
        if deeper_green_calculation_flag == False or greenrating_value_for_appending == 0 :
            totaldatatransfer_col.append("")
            emissionsnonrw_col.append("")
            emissionsrw_col.append("")
            totalcarbonemissions_col.append("")

        elif deeper_green_calculation_flag == True and greenrating_value_for_appending != 0:
            green_impressions = deeper_green_attributes_dictionary["impressions"]   #integer
            green_creatives = deeper_green_attributes_dictionary["creativesize"]    #float
            green_landingpage = deeper_green_attributes_dictionary["landingpage"]
            if green_landingpage != "" and green_landingpage is not None:
                landingpagesize = get_page_size(green_landingpage)
            elif green_landingpage == "" or green_landingpage is None :
                landingpagesize = 1

            if green_impressions == 0 or green_impressions is None:
                green_impressions = 1
            if green_creatives == 0 or green_creatives is None:
                green_creatives = 1
            if landingpagesize == 0 or landingpagesize is None:
                landingpagesize = 1

            totaldatatransfer = app_size_in_gb * int(green_impressions) * float(green_creatives) * landingpagesize

            energy_consumption = totaldatatransfer * 0.81

            greenrw = int(greenrating_value_for_appending) / 100
            greennonrw = 1 - greenrw

            emissionsnonrw = energy_consumption * 442 * greennonrw

            emissionrw = energy_consumption * 50 * greenrw

            totalcarbonemissions = emissionrw + emissionsnonrw

            totaldatatransfer_col.append(totaldatatransfer)
            emissionsnonrw_col.append(emissionsnonrw)
            emissionsrw_col.append(emissionrw)
            totalcarbonemissions_col.append(totalcarbonemissions)


        print("Process successful for iOS ID :", app_id_to_parse)
        print(len(app_id_col))

        if app_id_to_parse in error_ids :
            error_ids.remove(app_id_to_parse)

        #lock.release()

    except Exception as e:
        length_to_decrease = -1
        if len(app_id_col) != len(app_name_col) or len(app_id_col) != len(app_type_col) or len(app_id_col) != len(app_category_col) or len(app_id_col) != len(iab_category_col) or len(app_id_col) != len(top_10_keywords_col) or len(app_id_col) != len(downloads_col) or len(app_id_col) != len(rating_col) or len(app_id_col) != len(age_rating_col) or len(app_id_col) != len(tablet_optimized_col) or len(app_id_col) != len(app_developer_col) or len(app_id_col) != len(bipoc_col) or len(app_id_col) != len(bipoc_conf_col) or len(app_id_col) != len(processed_date_col) :
            length_to_decrease = len(app_id_col)
        if len(app_id_col) != len(adult_col) or len(app_id_col) != len(alcohol_col) or len(app_id_col) != len(aviation_col) or len(app_id_col) != len(crime_col) or len(app_id_col) != len(drug_col) or len(app_id_col) != len(hate_col) or len(app_id_col) != len(human_made_disasters_col) or len(app_id_col) != len(politics_brand_safe_col) or len(app_id_col) != len(natural_disasters_col) :
            length_to_decrease = len(app_id_col)
        if len(app_id_col) != len(piracy_col) or len(app_id_col) != len(profanity_col) or len(app_id_col) != len(terrorism_col) or len(app_id_col) != len(tobacco_col) or len(app_id_col) != len(cannabis_col) or len(app_id_col) != len(car_accidents_col) or len(app_id_col) != len(brand_safe_col) or len(app_id_col) != len(coppa_col) or len(app_id_col) != len(greenrating_col) :
            length_to_decrease = len(app_id_col)
        if len(app_id_col) != len(app_size_col) or len(app_id_col) != len(page_size_col) or len(app_id_col) != len(totaldatatransfer_col) or len(app_id_col) != len(emissionsnonrw_col) or len(app_id_col) != len(emissionsrw_col) or len(app_id_col) != len(totalcarbonemissions_col) or len(app_id_col) != len(brand_profile_id_col)  :
            length_to_decrease = len(app_id_col)

        if length_to_decrease != -1 :
            if length_to_decrease == len(app_id_col) :
                del app_id_col[-1]
            if length_to_decrease == len(app_name_col) :
                del app_name_col[-1]
            if length_to_decrease == len(app_type_col) :
                del app_type_col[-1]
            if length_to_decrease == len(app_category_col) :
                del app_category_col[-1]
            if length_to_decrease == len(iab_category_col) :
                del iab_category_col[-1]
            if length_to_decrease == len(top_10_keywords_col) :
                del top_10_keywords_col[-1]
            if length_to_decrease == len(downloads_col) :
                del downloads_col[-1]
            if length_to_decrease == len(rating_col) :
                del rating_col[-1]



            if length_to_decrease == len(age_rating_col) :
                del age_rating_col[-1]


            if length_to_decrease == len(tablet_optimized_col) :
                del tablet_optimized_col[-1]
            if length_to_decrease == len(app_developer_col) :
                del app_developer_col[-1]
            if length_to_decrease == len(bipoc_col) :
                del bipoc_col[-1]
            if length_to_decrease == len(bipoc_conf_col) :
                del bipoc_conf_col[-1]
            if length_to_decrease == len(processed_date_col) :
                del processed_date_col[-1]
            if length_to_decrease == len(adult_col) :
                del adult_col[-1]
            if length_to_decrease == len(alcohol_col) :
                del alcohol_col[-1]
            if length_to_decrease == len(aviation_col) :
                del aviation_col[-1]
            if length_to_decrease == len(crime_col) :
                del crime_col[-1]
            if length_to_decrease == len(drug_col) :
                del drug_col[-1]
            if length_to_decrease == len(hate_col) :
                del hate_col[-1]
            if length_to_decrease == len(human_made_disasters_col) :
                del human_made_disasters_col[-1]
            if length_to_decrease == len(politics_brand_safe_col) :
                del politics_brand_safe_col[-1]
            if length_to_decrease == len(natural_disasters_col) :
                del natural_disasters_col[-1]


            if length_to_decrease == len(piracy_col) :
                del piracy_col[-1]
            if length_to_decrease == len(profanity_col) :
                del profanity_col[-1]
            if length_to_decrease == len(terrorism_col) :
                del terrorism_col[-1]
            if length_to_decrease == len(tobacco_col) :
                del tobacco_col[-1]
            if length_to_decrease == len(cannabis_col) :
                del cannabis_col[-1]
            if length_to_decrease == len(car_accidents_col) :
                del car_accidents_col[-1]
            if length_to_decrease == len(brand_safe_col) :
                del brand_safe_col[-1]
            if length_to_decrease == len(coppa_col) :
                del coppa_col[-1]
            if length_to_decrease == len(greenrating_col) :
                del greenrating_col[-1]
            if length_to_decrease == len(app_size_col) :
                del app_size_col[-1]



            if length_to_decrease == len(page_size_col) :
                del page_size_col[-1]
            if length_to_decrease == len(totaldatatransfer_col) :
                del totaldatatransfer_col[-1]
            if length_to_decrease == len(emissionsnonrw_col) :
                del emissionsnonrw_col[-1]
            if length_to_decrease == len(emissionsrw_col) :
                del emissionsrw_col[-1]
            if length_to_decrease == len(totalcarbonemissions_col) :
                del totalcarbonemissions_col[-1]
            if length_to_decrease == len(brand_profile_id_col) :
                del brand_profile_id_col[-1]

        print(f"Error processing {app_id_to_parse}: {str(e)}")
        if app_id_to_parse not in error_ids :
            error_ids.append(app_id_to_parse)

    finally :
        lock.release()




ios_file_names = os.listdir("main/ios/")
ios_file_names_final = []

ios_status_query = "SELECT file_name from file_status where ios_started = 'No'"

mycursor.execute(ios_status_query)

file_names_from_db = []


for i in list(mycursor) :
    file_names_from_db.append(i[0])

for i in file_names_from_db :
    ios_started_query = "UPDATE file_status SET ios_started = 'Yes' WHERE file_name = '" + i + "'"
    mycursor.execute(ios_started_query)
    mydb.commit()

for i in ios_file_names :
    if i[:-19] + ".csv" in file_names_from_db :
        ios_file_names_final.append(i)


for file_name in ios_file_names_final :
    error_ids = []
    try :

        input_df = pd.read_csv('main/ios/' + file_name, header=None)
    except :
        merged_df = {"Empty_file" : []}
        merged_df = pd.DataFrame(merged_df)
        processed_ios_file = file_name[:-15] + ".csv"

        merged_df.to_csv(processed_ios_file, index=None, header=True)
        sftp_url = "sftp.dc2.gpaas.net"
        username = "7872195"
        password = "WonderBatmanSuperman12!"
        local_file_path = "C:/Users/Administrator/Desktop/parsers/" + processed_ios_file  # Corrected the file path
        remote_directory = "/vhosts/certm8.com/htdocs/platform/profcf"

        upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory)
        os.remove(processed_ios_file)
        os.remove("main/ios/" + file_name)

        ios_done_query = "UPDATE file_status SET ios_done = 'Yes' WHERE file_name = '" + file_name[:-19] + ".csv" + "'"
        mycursor.execute(ios_done_query)
        mydb.commit()

        others_query = "SELECT android_done, domain_done from file_status where file_name = '" + file_name[:-19] + ".csv" + "'"

        mycursor.execute(others_query)

        for i in list(mycursor) :
            if i[0] == "Yes" and i[1] == "Yes" :
                final_ios_call = requests.get('https://certm8.com/platform/u2/?k=786123&id=' + processed_ios_file[:-8])
                delete_query = "DELETE FROM file_status WHERE file_name = '" + file_name[:-19] + ".csv" + "'"
                mycursor.execute(delete_query)
                mydb.commit()

        continue
    #mycursor = mydb.cursor()


    #check if deeper green calculation is needed
    file_id = file_name[:-19]   #file_id-ios_tobeparsed.csv "-ios_tobeparsed.csv"  19 characters should be cut

    deeper_green_calculation_bool = False
    deeper_green_attributes = {}
    second_api_response = requests.get("https://certm8.com/platform/agency1/nx/?k=786123").text

    second_api_response_jsonified = json.loads(second_api_response)

    for dictio in second_api_response_jsonified :
        if type(dictio) == str :
            break
        if file_id in dictio["identifier"] and dictio["impressions"] is not None:
            deeper_green_calculation_bool = True
            deeper_green_attributes["impressions"] = dictio["impressions"]
            deeper_green_attributes["creativesize"] = dictio["creativesize"]
            deeper_green_attributes["landingpage"] = dictio["landingpage"]
            break

    profile_id_to_check = get_brand_profile_id(file_id)

    input_list = input_df.values.tolist()

    ios_ids_from_input_file = []

    for i in range(len(input_list)) :
        ios_ids_from_input_file.append(str(input_list[i][0]))

    ids_from_db = []
    dates_from_db = []
    greenrating_from_db = []
    totaldatatransfer_from_db = []
    brandprofileid_from_db = []

    ids_from_db_query = "SELECT app_id, processed_date, greenrating, totaldatatransfer, brandprofileid from ios_parser"

    mycursor.execute(ids_from_db_query)

    for i in list(mycursor) :
        ids_from_db.append(i[0])
        dates_from_db.append(i[1])
        greenrating_from_db.append(i[2])
        totaldatatransfer_from_db.append(i[3])
        brandprofileid_from_db.append(i[4])

    today = datetime.now()
    outdated_threshold = today - timedelta(days=92)
    outdated_threshold = outdated_threshold.strftime("%m/%d/%Y")
    outdated_year = int(outdated_threshold[-4:])
    outdated_month = int(outdated_threshold[:2])
    outdate_day = int(outdated_threshold[3:5])
    outdated_threshold_date = date(outdated_year, outdated_month, outdate_day)

    to_parse = []
    to_delete = []
    to_get_from_db = []
    for i in ios_ids_from_input_file :
        if i in ids_from_db :
            hit_index = ids_from_db.index(i)
            db_date_as_string = dates_from_db[hit_index]
            db_year = int(db_date_as_string[-4:])
            db_month = int(db_date_as_string[:2])
            db_day = int(db_date_as_string[3:5])
            db_date_as_date = date(db_year, db_month, db_day)
            if db_date_as_date <= outdated_threshold_date :
                to_parse.append(i)
                to_delete.append(i)
            elif db_date_as_date > outdated_threshold_date :
                if brandprofileid_from_db[hit_index] != profile_id_to_check :
                    to_parse.append(i)
                    to_delete.append(i)
                elif brandprofileid_from_db[hit_index] == profile_id_to_check :
                    if deeper_green_calculation_bool == True and greenrating_from_db[hit_index] != '0' and totaldatatransfer_from_db[hit_index] == '' :
                        to_parse.append(i)
                        to_delete.append(i)
                    elif deeper_green_calculation_bool != True or greenrating_from_db[hit_index] == '0' or totaldatatransfer_from_db[hit_index] != '' :
                        to_get_from_db.append(i)
        elif i not in ids_from_db :
            to_parse.append(i)


    app_id_col = []
    devurl_col = []
    app_name_col = []
    app_type_col = []
    app_category_col = []
    iab_category_col = []
    top_10_keywords_col = []
    downloads_col = []
    rating_col = []
    age_rating_col = []
    tablet_optimized_col = []
    app_developer_col = []
    bipoc_col = []
    bipoc_conf_col = []
    processed_date_col = []
    # 14.08 Brand safety START
    adult_col = []
    alcohol_col = []
    aviation_col = []
    crime_col = []
    drug_col = []
    hate_col = []
    human_made_disasters_col = []
    politics_brand_safe_col = []
    natural_disasters_col = []
    piracy_col = []
    profanity_col = []
    terrorism_col = []
    tobacco_col = []
    cannabis_col = []
    car_accidents_col = []
    brand_safe_col = []
    # 14.08 Brand safety END

    # 15.08 COPPA START
    coppa_col = []
    # 15.08 COPPA END
    greenrating_col = []
    app_size_col = []
    page_size_col = []
    totaldatatransfer_col = []
    emissionsnonrw_col = []
    emissionsrw_col = []
    totalcarbonemissions_col = []
    brand_profile_id_col = []

    today = today.strftime("%m/%d/%Y")

    to_parse = list(set(to_parse))

    print(f"Processing IDS : {to_parse} with length of {len(to_parse)}")

    are_these_error_ids = False
    threads = [threading.Thread(target=parse_the_ios_id, args=(url, deeper_green_calculation_bool, deeper_green_attributes, are_these_error_ids, file_id)) for url in to_parse]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    print(f"Processing error IDs : {error_ids} with length of {len(error_ids)}")
    print(f"Len of app_id column BEFORE ERROR IDS: {len(app_id_col)}")

    try_counter = 0
    are_these_error_ids = True
    while len(error_ids) > 0 and try_counter < 5 :
        print("Try errors with " + str(len(error_ids)))
        threads = [threading.Thread(target=parse_the_ios_id, args=(url, deeper_green_calculation_bool, deeper_green_attributes, are_these_error_ids, file_id)) for url in error_ids]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        try_counter = try_counter + 1
        print("Try number", try_counter, "finished")
    #for ios_url in to_parse :
    #    parse_the_ios_id(ios_url)

    print(f"Len of app_id column AFTER ERROR IDS : {len(app_id_col)}")

    data = {'App_ID' : app_id_col,
            'privacyurl' : devurl_col,
            'App_name' : app_name_col,
            'App_type' : app_type_col,
            'App_category' : app_category_col,
            'IAB_category' : iab_category_col,
            'Top_10_keywords' : top_10_keywords_col,
            'Downloads' : downloads_col,
            'Rating' : rating_col,
            'Age_rating' : age_rating_col,
            'Tablet_optimized' : tablet_optimized_col,
            'Developer' : app_developer_col,
            'BIPOC' : bipoc_col,
            'BIPOC_conf' : bipoc_conf_col,
            'Adult' : adult_col,
            'Alcohol' : alcohol_col,
            'Aviation' : aviation_col,
            'Crime' : crime_col,
            'Drug_abuse' : drug_col,
            'Hate_speech' : hate_col,
            'Human_made_disasters' : human_made_disasters_col,
            'Politics' : politics_brand_safe_col,
            'Natural_disasters' : natural_disasters_col,
            'Piracy' : piracy_col,
            'Profanity' : profanity_col,
            'Terrorism' : terrorism_col,
            'Tobacco' : tobacco_col,
            'Cannabis' : cannabis_col,
            'Car_accidents' : car_accidents_col,
            'Brand_safe' : brand_safe_col,
            'Coppa' : coppa_col,
            'Processed_date' : processed_date_col,
            'GreenRating' : greenrating_col,
            'appsize' : app_size_col,
            'pagesize' : page_size_col,
            'totaldatatransfer' : totaldatatransfer_col,
            'emissionsnonrw' : emissionsnonrw_col,
            'emissionsrw' : emissionsrw_col,
            'totalcarbonemissions' : totalcarbonemissions_col,
            'brandprofileid' : brand_profile_id_col}

    df = pd.DataFrame(data)

    values_to_sql = df.values.tolist()

    for i in to_delete :
        mycursor.execute("DELETE FROM ios_parser WHERE app_id = '" + i + "'")

    mydb.commit()

    sql_query = "INSERT INTO ios_parser (App_ID, privacyurl, App_name, App_type, App_category, IAB_category,Top_10_keywords, Downloads, Rating, Age_rating, Tablet_optimized, Developer, BIPOC, BIPOC_conf, Adult, Alcohol, Aviation, Crime, Drug_abuse, Hate_speech, Human_made_disasters, Politics, Natural_disasters, Piracy, Profanity, Terrorism, Tobacco, Cannabis, Car_accidents, Brand_safe, Coppa, Processed_date, GreenRating, appsize, pagesize, totaldatatransfer, emissionsnonrw, emissionsrw, totalcarbonemissions, brandprofileid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    mycursor.executemany(sql_query, values_to_sql)


    mydb.commit()

    for i in to_get_from_db :
        mycursor.execute("SELECT * FROM ios_parser WHERE app_id = '" + i + "'")
        db_row_as_list = list(mycursor)
        app_id_col.append(db_row_as_list[0][0])
        devurl_col.append(db_row_as_list[0][1])
        app_name_col.append(db_row_as_list[0][2])
        app_type_col.append(db_row_as_list[0][3])
        app_category_col.append(db_row_as_list[0][4])
        iab_category_col.append(db_row_as_list[0][5])
        top_10_keywords_col.append(db_row_as_list[0][6])
        downloads_col.append(db_row_as_list[0][7])
        rating_col.append(db_row_as_list[0][8])
        age_rating_col.append(db_row_as_list[0][9])
        tablet_optimized_col.append(db_row_as_list[0][10])
        app_developer_col.append(db_row_as_list[0][11])
        bipoc_col.append(db_row_as_list[0][12])
        bipoc_conf_col.append(db_row_as_list[0][13])
        # 14.08 Brand safety START
        adult_col.append(db_row_as_list[0][14])
        alcohol_col.append(db_row_as_list[0][15])
        aviation_col.append(db_row_as_list[0][16])
        crime_col.append(db_row_as_list[0][17])
        drug_col.append(db_row_as_list[0][18])
        hate_col.append(db_row_as_list[0][19])
        human_made_disasters_col.append(db_row_as_list[0][20])
        politics_brand_safe_col.append(db_row_as_list[0][21])
        natural_disasters_col.append(db_row_as_list[0][22])
        piracy_col.append(db_row_as_list[0][23])
        profanity_col.append(db_row_as_list[0][24])
        terrorism_col.append(db_row_as_list[0][25])
        tobacco_col.append(db_row_as_list[0][26])
        cannabis_col.append(db_row_as_list[0][27])
        car_accidents_col.append(db_row_as_list[0][28])
        brand_safe_col.append(db_row_as_list[0][29])
        # 14.08 Brand safety END

        # 15.08 COPPA START
        coppa_col.append(db_row_as_list[0][30])
        # 15.08 COPPA END
        greenrating_col.append(db_row_as_list[0][32])
        app_size_col.append(db_row_as_list[0][33])
        page_size_col.append(db_row_as_list[0][34])
        totaldatatransfer_col.append(db_row_as_list[0][35])
        emissionsnonrw_col.append(db_row_as_list[0][36])
        emissionsrw_col.append(db_row_as_list[0][37])
        totalcarbonemissions_col.append(db_row_as_list[0][38])
        brand_profile_id_col.append(db_row_as_list[0][39])

    #mycursor.close()
    

    merged_data = {'App_ID' : app_id_col,
                   'privacyurl' : devurl_col,
            'App_name' : app_name_col,
            'App_type' : app_type_col,
            'App_category' : app_category_col,
            'IAB_category' : iab_category_col,
            'Top_10_keywords' : top_10_keywords_col,
            'Downloads' : downloads_col,
            'Rating' : rating_col,
            'Age_rating' : age_rating_col,
            'Tablet_optimized' : tablet_optimized_col,
            'Developer' : app_developer_col,
            'BIPOC' : bipoc_col,
            'BIPOC_conf' : bipoc_conf_col,
            # 14.08 Brand safety START
            'Adult' : adult_col,
            'Alcohol' : alcohol_col,
            'Aviation' : aviation_col,
            'Crime' : crime_col,
            'Drug_abuse' : drug_col,
            'Hate_speech' : hate_col,
            'Human_made_disasters' : human_made_disasters_col,
            'Politics' : politics_brand_safe_col,
            'Natural_disasters' : natural_disasters_col,
            'Piracy' : piracy_col,
            'Profanity' : profanity_col,
            'Terrorism' : terrorism_col,
            'Tobacco' : tobacco_col,
            'Cannabis' : cannabis_col,
            'Car_accidents' : car_accidents_col,
            'Brand_safe' : brand_safe_col,
            # 15.08 COPPA START
            'Coppa' : coppa_col,
            'GreenRating' : greenrating_col,
            'appsize' : app_size_col,
            'pagesize' : page_size_col,
            'totaldatatransfer' : totaldatatransfer_col,
            'emissionsnonrw' : emissionsnonrw_col,
            'emissionsrw' : emissionsrw_col,
            'totalcarbonemissions' : totalcarbonemissions_col,
            'brandprofileid' : brand_profile_id_col}
            # 15.08 COPPA END
            # 14.08 Brand safety END

    merged_df = pd.DataFrame(merged_data)

    processed_ios_file = file_name[:-15] + ".csv"

    merged_df.to_csv(processed_ios_file, index=None, header=True)

    sftp_url = "sftp.dc2.gpaas.net"
    username = "7872195"
    password = "WonderBatmanSuperman12!"
    local_file_path = "C:/Users/Administrator/Desktop/parsers/" + processed_ios_file  # Corrected the file path
    remote_directory = "/vhosts/certm8.com/htdocs/platform/profcf"

    upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory)
    os.remove(processed_ios_file)
    os.remove("main/ios/" + file_name)

    ios_done_query = "UPDATE file_status SET ios_done = 'Yes' WHERE file_name = '"  + file_name[:-19] + ".csv" + "'"
    mycursor.execute(ios_done_query)
    mydb.commit()

    others_query = "SELECT android_done, domain_done from file_status where file_name = '"  + file_name[:-19] + ".csv" + "'"

    mycursor.execute(others_query)

    for i in list(mycursor) :
        if i[0] == "Yes" and i[1] == "Yes" :
            final_ios_call = requests.get('https://certm8.com/platform/u2/?k=786123&id=' + processed_ios_file[:-8])
            delete_query = "DELETE FROM file_status WHERE file_name = '"  + file_name[:-19] + ".csv" + "'"
            mycursor.execute(delete_query)
            mydb.commit()

mycursor.close()
mydb.close()

print("It took this much time : ", datetime.now() - startTime)