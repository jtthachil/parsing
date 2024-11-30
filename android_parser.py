from re import sub  #regex==2022.6.2
from turtle import title
from urllib.request import urlopen, Request     #urllib for opening URLs    #urllib==1.26.9
from bs4 import BeautifulSoup   #BeautifulSoup for getting text from web pages(HTML+CSS files)  #beautifulsoup4==4.11.1
import requests     #requests for keep-alive, better and faster server connection   #requests==2.28.0 ; requests-aws4auth==1.1.2 ; requests-oauthlib==1.3.1 ; requests-toolbelt==0.10.1
import nltk     #nltk for extracting nouns from text    #nltk==3.7
import json       #json module for making json out of string    #built-in
#nltk.download('averaged_perceptron_tagger')
#nltk.download('punkt')
import datetime     #built-in
from datetime import timedelta, date, datetime  #built-in
import pandas as pd     #pandas==1.4.3
import numpy as np      #numpy==1.23.1
import os       #built-in
import threading    #built-in
import time     #built-in
import urllib.request   #urllib3==1.26.9
import mysql.connector  #mysql-connector-python==8.0.30
import re   #regex==2022.6.2
import paramiko     #paramiko==3.2.0
import socket   #built-in
from ipwhois import IPWhois     #ipwhois==1.2.0
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

def android_app_name_conversion(app_name_to_change) :
    changed_name = ''
    for original_character in app_name_to_change :
        if original_character.isalnum() :
            changed_name = changed_name + original_character
        elif original_character.isalnum() == False :
            if len(changed_name) != 0 and changed_name[-1] != '-' :
                changed_name = changed_name + '-'
    return changed_name


def strip_and_convert(size_to_convert) :
    if "MB" in size_to_convert :
        return int(float(size_to_convert[:size_to_convert.find("MB") - 1])) / 1000
    elif "GB" in size_to_convert :
        return int(float(size_to_convert[:size_to_convert.find("GB") - 1]))
    elif "KB" in size_to_convert :
        return int(float(size_to_convert[:size_to_convert.find("GB") - 1])) / 1000000
    return 0.05

def get_app_size(app_id, app_name) : # returns in GB

    try :
        apk_url = Request("https://apkpure.net/" + app_name + "/" + app_id, headers={'User-Agent' : 'Mozilla/5.0'})
        apk_html = urlopen(apk_url, timeout=10).read()
        apk_soup = BeautifulSoup(apk_html, features="html.parser")
    except :
        return 0.05

    size_parent = apk_soup.find_all("div", {"class" : "apk-info"})

    for x in size_parent :
        possible_size = x.find_all("div", {"class" : "additional-info"})

        for i in possible_size :
            if "MB" in i.text.strip() :
                return strip_and_convert(i.text.strip())
            elif "GB" in i.text.strip() :
                return strip_and_convert(i.text.strip())

    old_versions = apk_soup.find_all("span", {"class" : "size"})

    if len(old_versions) != 0 :
        for old_version in old_versions :
            if old_version != None :
                return strip_and_convert(old_version.text.strip())

    return 0.05


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

static_file = open('android_categories_and_hitwords.json')

static_dict = json.load(static_file)

static_file.close()

iab_categories = static_dict["iab_categories"]
arts_and_ent = static_dict["arts_and_ent"]
automotive = static_dict["automotive"]
business = static_dict["business"]
careers = static_dict["careers"]
education = static_dict["education"]
family_and_parenting = static_dict["family_and_parenting"]
health_and_fitness = static_dict["health_and_fitness"]
food_and_drink = static_dict["food_and_drink"]
hobbies_and_interests = static_dict["hobbies_and_interests"]
home_and_garden = static_dict["home_and_garden"]
law_and_politics = static_dict["law_and_politics"]
news = static_dict["news"]
personal_finance = static_dict["personal_finance"]
society = static_dict["society"]
science = static_dict["science"]
pets = static_dict["pets"]
sports = static_dict["sports"]
style_and_fashion = static_dict["style_and_fashion"]
tech_and_computing = static_dict["tech_and_computing"]
travel = static_dict["travel"]
real_estate = static_dict["real_estate"]
shopping = static_dict["shopping"]
religion_and_spirituality = static_dict["religion_and_spirituality"]
#uncategorized = []
non_standard_content = static_dict["non_standard_content"]
illegal_content = static_dict["illegal_content"]

android_categories_to_iab_file = open('android_categories_to_iab.json')

android_categories_to_iab_dict = json.load(android_categories_to_iab_file)

android_categories_to_iab_file.close()

android_game_tags = ["Action", "Adventure", "Arcade", "Board", "Card", "Casino", "Casual", "Educational", "Music", "Puzzle",
                    "Racing", "Role Playing", "Simulation", "Sports", "Strategy", "Trivia", "Word"]

android_app_rating_valid_characters = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "."]



def parse_the_android_id(app_id_to_parse, deeper_green_calculation_flag, deeper_green_attributes_dictionary, error_ids_now, file_id_for_apis) :

    app_id_to_parse = str(app_id_to_parse)

    print("Starting to process ANDROID ID :", app_id_to_parse)
    #create soup
    if error_ids_now == False :
        try :
            url = Request("https://play.google.com/store/apps/details?id=" + app_id_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
            html = urlopen(url, timeout=10).read()
            soup = BeautifulSoup(html, features="html.parser")

        except :
            if app_id_to_parse not in error_ids :
                error_ids.append(app_id_to_parse)
            return
    elif error_ids_now == True :
        try :
            context = ssl._create_unverified_context()
            url = Request("https://play.google.com/store/apps/details?id=" + app_id_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
            html = urlopen(url, timeout=10, context=context).read()
            soup = BeautifulSoup(html, features="html.parser")
            print("soup created without ssl")
        except :
            try :
                url = Request("http://api.scraperapi.com?api_key=c23e0dd0e126b0904b80503545b26861&url=" + "https://play.google.com/store/apps/details?id=" + app_id_to_parse + "&render=true", headers={'User-Agent' : 'Mozilla/5.0'})
                html = urlopen(url, timeout=15, context=context).read()
                soup = BeautifulSoup(html, features="html.parser")
                print(app_id_to_parse + " was successful with api_url")
            except :
                print("soup cannot be created for ", app_id_to_parse)
                return
    try :

        lock.acquire()

        

        #Change 26.04
        whole_output[app_id_to_parse] = {"app_id_col" : app_id_to_parse}
        whole_output[app_id_to_parse]["app_type_col"] = "Android"
        whole_output[app_id_to_parse]["tablet_optimized_col"] = ""
        ####

        #app_id_col.append(app_id_to_parse)
        #app_type_col.append("Android")
        #tablet_optimized_col.append("")

        #removing trash(script, style tags) from the soup
        for script in soup(["script", "style"]) :
            script.extract()


        # Extract the app name
        app_name_element = soup.find("h1", {"class": "Fd93Bb"})
        if app_name_element :
            app_name = app_name_element.text.strip()
        elif not app_name_element :
            app_name = ""
        #app_name_col.append(app_name)
        whole_output[app_id_to_parse]["app_name_col"] = app_name

        app_dev_element = soup.find("div", {"class": "tv4jIf"})
        app_dev = app_dev_element.text.strip()
        app_dev = app_dev.replace('Contains adsIn-app purchases', '')
        if len(app_dev) > 12 and app_dev[-12:] == 'Contains ads' :
            app_dev = app_dev[:-12]
        if len(app_dev) > 16 and app_dev[-16:] == 'In-app purchases' :
            app_dev = app_dev[:-16]

        #app_developer_col.append(app_dev)
        whole_output[app_id_to_parse]["app_developer_col"] = app_dev

        #greenrating
        is_green_soup_ready = False
        try :
            green_url = Request("https://play.google.com/store/apps/datasafety?id=" + app_id_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
            green_html = urlopen(green_url, timeout=10).read()
            green_soup = BeautifulSoup(green_html, features="html.parser")
            is_green_soup_ready = True
        except :
            is_green_soup_ready = False

        greenrating_value_for_appending = 35
        devurl_value_for_appending = 'Privacy URL Not Available'
        if is_green_soup_ready :
            web_dev_parent = green_soup.find("div", {"class" : "viuTPb"})
            if web_dev_parent is not None :
                web_dev_child = web_dev_parent.find("a", {"class": "GO2pB"})
                if web_dev_child is not None :
                    href = web_dev_child["href"]

                    if href.startswith('http') and href.find('://') != -1:
                        href = href[href.find('://') + 3:]
            
                    if href.find('/') != -1 :
                        href = href[:href.find('/')]

                    
                    
                    whole_output[app_id_to_parse]["devurl_col"] = href


                    try:
                        location_info = get_server_location(href)

                        if location_info in country_and_renewable_dict :
                            #greenrating_col.append(country_and_renewable_dict[location_info])
                            greenrating_value_for_appending = country_and_renewable_dict[location_info]
                            whole_output[app_id_to_parse]["greenrating_col"] = greenrating_value_for_appending
                        elif location_info not in country_and_renewable_dict :
                            #greenrating_col.append(0)
                            whole_output[app_id_to_parse]["greenrating_col"] = greenrating_value_for_appending
                    except Exception as e:
                        #greenrating_col.append(0)
                        whole_output[app_id_to_parse]["greenrating_col"] = greenrating_value_for_appending
                        #whole_output[app_id_to_parse]["devurl_col"] = devurl_value_for_appending
                elif web_dev_child is None :
                    #greenrating_col.append(0)
                    whole_output[app_id_to_parse]["greenrating_col"] = greenrating_value_for_appending
                    whole_output[app_id_to_parse]["devurl_col"] = devurl_value_for_appending
            elif web_dev_parent is None :
                #greenrating_col.append(0)
                whole_output[app_id_to_parse]["greenrating_col"] = greenrating_value_for_appending
                whole_output[app_id_to_parse]["devurl_col"] = devurl_value_for_appending
        elif is_green_soup_ready == False :
            #greenrating_col.append(0)
            whole_output[app_id_to_parse]["greenrating_col"] = greenrating_value_for_appending
            whole_output[app_id_to_parse]["devurl_col"] = devurl_value_for_appending


        # App size
        new_app_name = android_app_name_conversion(app_name)

        app_size_in_gb = get_app_size(app_id_to_parse, new_app_name)

        #appsize_col.append(str(app_size_in_gb))
        whole_output[app_id_to_parse]["appsize_col"] = str(app_size_in_gb)

        #pagesize_col.append("")
        whole_output[app_id_to_parse]["pagesize_col"] = ""



        #Deeper green calculation
        if deeper_green_calculation_flag == False or greenrating_value_for_appending == 0 :
            whole_output[app_id_to_parse]["totaldatatransfer_col"] = ""
            whole_output[app_id_to_parse]["emissionsnonrw_col"] = ""
            whole_output[app_id_to_parse]["emissionsrw_col"] = ""
            whole_output[app_id_to_parse]["totalcarbonemissions_col"] = ""
            #totaldatatransfer_col.append("")
            #emissionsnonrw_col.append("")
            #emissionsrw_col.append("")
            #totalcarbonemissions_col.append("")

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

            whole_output[app_id_to_parse]["totaldatatransfer_col"] = totaldatatransfer
            whole_output[app_id_to_parse]["emissionsnonrw_col"] = emissionsnonrw
            whole_output[app_id_to_parse]["emissionsrw_col"] = emissionrw
            whole_output[app_id_to_parse]["totalcarbonemissions_col"] = totalcarbonemissions
            

            #totaldatatransfer_col.append(totaldatatransfer)
            #emissionsnonrw_col.append(emissionsnonrw)
            #emissionsrw_col.append(emissionrw)
            #totalcarbonemissions_col.append(totalcarbonemissions)



        black_surname_found = False
        black_first_name_found = False
        bipoc_surname_found = False
        bipoc_value = 0
        bipoc_conf = 0

        for black_surname in black_surnames_list :
            if black_surname in app_dev :
                black_surname_found = True
                break
        for black_first_name in black_first_names_list :
            if black_first_name in app_dev :
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
                if bipoc_surname in app_dev :
                    bipoc_value = 1
                    bipoc_conf = 1
                    bipoc_surname_found = True
                    break

        #bipoc_col.append(str(bipoc_value))
        #bipoc_conf_col.append(str(bipoc_conf))
        whole_output[app_id_to_parse]["bipoc_col"] = bipoc_value
        whole_output[app_id_to_parse]["bipoc_conf_col"] = bipoc_conf
        


        app_description = soup.find("div", {"class": "bARER"})
        if app_description :
            strips = list(app_description.stripped_strings)
            

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
                looking_for_coppa = profile_for_checking["coppa"]
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


            """
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
            # 14.08 Brand safety END"""

            whole_output[app_id_to_parse]["adult_col"] = adult_value
            whole_output[app_id_to_parse]["alcohol_col"] = alcohol_value
            whole_output[app_id_to_parse]["aviation_col"] = aviation_value
            whole_output[app_id_to_parse]["crime_col"] = '0'
            whole_output[app_id_to_parse]["drug_col"] = '0'
            whole_output[app_id_to_parse]["hate_col"] = '0'
            whole_output[app_id_to_parse]["human_made_disasters_col"] = '0'
            whole_output[app_id_to_parse]["politics_brand_safe_col"] = str(politics_brand_safety_value)
            whole_output[app_id_to_parse]["natural_disasters_col"] = '0'
            whole_output[app_id_to_parse]["piracy_col"] = str(piracy_value)
            whole_output[app_id_to_parse]["profanity_col"] = '0'
            whole_output[app_id_to_parse]["terrorism_col"] = '0'
            whole_output[app_id_to_parse]["tobacco_col"] = str(tobacco_value)
            whole_output[app_id_to_parse]["cannabis_col"] = str(cannabis_value)
            whole_output[app_id_to_parse]["car_accidents_col"] = str(car_accidents_value)
            whole_output[app_id_to_parse]["brand_safe_col"] = str(is_brand_safe)
            whole_output[app_id_to_parse]["brand_profile_id_col"] = brand_profile_id_for_output

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

            #top_10_keywords_col.append(str(top_10_keywords))
            #whole_output[app_id_to_parse]["top_10_keywords_col"] = str(top_10_keywords)

            keywords_string = ''
            for keywo in top_10_keywords :
                keywords_string += ', ' + keywo
            if len(keywords_string) > 3 :
                keywords_string = keywords_string[2:]
            whole_output[app_id_to_parse]["top_10_keywords_col"] = str(keywords_string)

        if not app_description :
            """
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
            """
            whole_output[app_id_to_parse]["top_10_keywords_col"] = ""
            whole_output[app_id_to_parse]["adult_col"] = ""
            whole_output[app_id_to_parse]["alcohol_col"] = ""
            whole_output[app_id_to_parse]["aviation_col"] = ""
            whole_output[app_id_to_parse]["crime_col"] = ""
            whole_output[app_id_to_parse]["drug_col"] = ""
            whole_output[app_id_to_parse]["hate_col"] = ""
            whole_output[app_id_to_parse]["human_made_disasters_col"] = ""
            whole_output[app_id_to_parse]["politics_brand_safe_col"] = ""
            whole_output[app_id_to_parse]["natural_disasters_col"] = ""
            whole_output[app_id_to_parse]["piracy_col"] = ""
            whole_output[app_id_to_parse]["profanity_col"] = ""
            whole_output[app_id_to_parse]["terrorism_col"] = ""
            whole_output[app_id_to_parse]["tobacco_col"] = ""
            whole_output[app_id_to_parse]["cannabis_col"] = ""
            whole_output[app_id_to_parse]["car_accidents_col"] = ""
            whole_output[app_id_to_parse]["brand_safe_col"] = ""
            whole_output[app_id_to_parse]["brand_profile_id_col"] = "0"

        #app_stars = soup.find("span", {"class": "w2kbF"})  # older html element containing the app rating
        app_stars_parent = soup.find("div", {"itemprop": "starRating"})
        if app_stars_parent :
            app_stars = app_stars_parent.find("div", {"class": "TT9eCd"})
        elif not app_stars_parent :
            app_stars = None
        
        if app_stars :
            app_stars= app_stars.text.strip()

            if len(app_stars) > 4 : #app_stars = 4.5star
                app_stars = app_stars[:-4]  #remove 'star' and leave the number

            correct_android_rating = True
            for i in app_stars :
                if i not in android_app_rating_valid_characters :
                    correct_android_rating = False
                    break

            if correct_android_rating == True :
                #rating_col.append(app_stars)
                whole_output[app_id_to_parse]["rating_col"] = app_stars
            elif correct_android_rating == False :
                #rating_col.append("")
                whole_output[app_id_to_parse]["rating_col"] = ""
            
        elif not app_stars :
            #rating_col.append("")
            whole_output[app_id_to_parse]["rating_col"] = ""

        app_downloads = soup.find("div", {"class": "w7Iutd"})
        app_downloads= app_downloads.text.strip()
        pattern = r'(\d+[MK]\+)'

        # Find the matching pattern in the text
        match = re.search(pattern, app_downloads)

        if match:
            desired_info = match.group()
            app_downloads= desired_info
        else:
            app_downloads= "No Value Available"

        #downloads_col.append(app_downloads)
        whole_output[app_id_to_parse]["downloads_col"] = app_downloads

        android_genre_availability = False
        app_genre = []
        app_genre_parent_element = soup.find("div", {"class": "Uc6QCc"})
        if app_genre_parent_element :
            app_genre_element = app_genre_parent_element.find_all("span", {"class" : "VfPpkd-vQzf8d"})
            if app_genre_element :
                android_genre_availability = True
                for i in app_genre_element :
                    app_genre.append(i.text.strip())


        if android_genre_availability == True :
            
            #app_category_col.append(str(app_genre))
            #whole_output[app_id_to_parse]["app_category_col"] = str(app_genre)

            app_genre_string = ''
            for android_app_genre in app_genre :
                app_genre_string += ', ' + android_app_genre
            if len(app_genre_string) > 3 :
                app_genre_string = app_genre_string[2:]
            whole_output[app_id_to_parse]["app_category_col"] = str(app_genre_string)

            if len(app_genre) == 1 :
                if app_genre[0] == "Sports" :
                    #iab_category_col.append(iab_categories[16])
                    whole_output[app_id_to_parse]["iab_category_col"] = iab_categories[16]
                elif app_genre[0] != "Sports" :
                    if app_genre[0] in android_game_tags :
                        #iab_category_col.append(iab_categories[8])
                        whole_output[app_id_to_parse]["iab_category_col"] = iab_categories[8]
                    elif app_genre[0] in android_categories_to_iab_dict :
                        #iab_category_col.append(android_categories_to_iab_dict[app_genre[0]])
                        whole_output[app_id_to_parse]["iab_category_col"] = android_categories_to_iab_dict[app_genre[0]]
                    elif app_genre[0] not in android_game_tags and app_genre[0] not in android_categories_to_iab_dict :
                        #iab_category_col.append("Genre cannot be matched") #Uncategorized
                        whole_output[app_id_to_parse]["iab_category_col"] = "IAB Category Not Available"

            elif len(app_genre) > 1 :
                iab_category_found = False
                for i in app_genre :
                    if i in android_game_tags and i != "Sports" :
                        #iab_category_col.append(iab_categories[8])  #If it is a game app, then IAB category is Hobbies and interests
                        whole_output[app_id_to_parse]["iab_category_col"] = iab_categories[8]
                        iab_category_found = True
                        break

                if iab_category_found == False :
                    for i in app_genre :
                        if i in android_categories_to_iab_dict :
                            #iab_category_col.append(android_categories_to_iab_dict[i])
                            whole_output[app_id_to_parse]["iab_category_col"] = android_categories_to_iab_dict[i]
                            iab_category_found = True
                            break

                if iab_category_found == False :
                    #iab_category_col.append("Genres cannot be matched")
                    whole_output[app_id_to_parse]["iab_category_col"] = "IAB Category Not Available"
            
            
        elif android_genre_availability == False :
            #app_category_col.append("")
            whole_output[app_id_to_parse]["app_category_col"] = ""

            if len(top_10_keywords) > 0 :
                nouns_used_for_categorization = top_10_keywords
                arts_counter = 0
                auto_counter = 0
                business_counter = 0
                careers_counter = 0
                education_counter = 0
                family_counter = 0
                health_counter = 0
                food_counter = 0
                hobbies_counter = 0
                home_counter = 0
                law_counter = 0
                news_counter = 0
                finance_counter = 0
                society_counter = 0
                science_counter = 0
                pets_counter = 0
                sports_counter = 0
                fashion_counter = 0
                tech_counter = 0
                travel_counter = 0
                real_estate_counter = 0
                shopping_counter = 0
                religion_counter = 0
                non_standard_counter = 0
                illegal_counter = 0

                max_count = 0
                max_index = 23


                for i in nouns_used_for_categorization :
                    if i.lower() in arts_and_ent or i in arts_and_ent :
                        arts_counter += 1
                        if arts_counter > max_count : 
                            max_count = arts_counter
                            max_index = 0
                    if i.lower() in automotive or i in automotive :
                        auto_counter += 1
                        if auto_counter > max_count : 
                            max_count = auto_counter
                            max_index = 1
                    if i.lower() in business or i in business :
                        business_counter += 1
                        if business_counter > max_count : 
                            max_count = business_counter
                            max_index = 2
                    if i.lower() in careers or i in careers :
                        careers_counter += 1
                        if careers_counter > max_count : 
                            max_count = careers_counter
                            max_index = 3
                    if i.lower() in education or i in education :
                        education_counter += 1
                        if education_counter > max_count : 
                            max_count = education_counter
                            max_index = 4
                    if i.lower() in family_and_parenting or i in family_and_parenting :
                        family_counter += 1
                        if family_counter > max_count : 
                            max_count = family_counter
                            max_index = 5
                    if i.lower() in health_and_fitness or i in health_and_fitness :
                        health_counter += 1
                        if health_counter > max_count : 
                            max_count = health_counter
                            max_index = 6
                    if i.lower() in food_and_drink or i in food_and_drink :
                        food_counter += 1
                        if food_counter > max_count : 
                            max_count = food_counter
                            max_index = 7
                    if i.lower() in hobbies_and_interests or i in hobbies_and_interests :
                        hobbies_counter += 1
                        if hobbies_counter > max_count : 
                            max_count = hobbies_counter
                            max_index = 8
                    if i.lower() in home_and_garden or i in home_and_garden :
                        home_counter += 1
                        if home_counter > max_count : 
                            max_count = home_counter
                            max_index = 9
                    if i.lower() in law_and_politics or i in law_and_politics :
                        law_counter += 1
                        if law_counter > max_count : 
                            max_count = law_counter
                            max_index = 10
                    if i.lower() in news or i in news :
                        news_counter += 1
                        if news_counter > max_count : 
                            max_count = news_counter
                            max_index = 11
                    if i.lower() in personal_finance or i in personal_finance :
                        finance_counter += 1
                        if finance_counter > max_count : 
                            max_count = finance_counter
                            max_index = 12
                    if i.lower() in society or i in society :
                        society_counter += 1
                        if society_counter > max_count : 
                            max_count = society_counter
                            max_index = 13
                    if i.lower() in science or i in science :
                        science_counter += 1
                        if science_counter > max_count : 
                            max_count = science_counter
                            max_index = 14
                    if i.lower() in pets or i in pets :
                        pets_counter += 1
                        if pets_counter > max_count : 
                            max_count = pets_counter
                            max_index = 15
                    if i.lower() in sports or i in sports :
                        sports_counter += 1
                        if sports_counter > max_count : 
                            max_count = sports_counter
                            max_index = 16
                    if i.lower() in style_and_fashion or i in style_and_fashion :
                        fashion_counter += 1
                        if fashion_counter > max_count : 
                            max_count = fashion_counter
                            max_index = 17
                    if i.lower() in tech_and_computing or i in tech_and_computing :
                        tech_counter += 1
                        if tech_counter > max_count : 
                            max_count = tech_counter
                            max_index = 18
                    if i.lower() in travel or i in travel :
                        travel_counter += 1
                        if travel_counter > max_count : 
                            max_count = travel_counter
                            max_index = 19
                    if i.lower() in real_estate or i in real_estate :
                        real_estate_counter += 1
                        if real_estate_counter > max_count : 
                            max_count = real_estate_counter
                            max_index = 20
                    if i.lower() in shopping or i in shopping :
                        shopping_counter += 1
                        if shopping_counter > max_count : 
                            max_count = shopping_counter
                            max_index = 21
                    if i.lower() in religion_and_spirituality or i in religion_and_spirituality :
                        religion_counter += 1
                        if religion_counter > max_count : 
                            max_count = religion_counter
                            max_index = 22
                    if i.lower() in non_standard_content or i in non_standard_content :
                        non_standard_counter += 1
                        if non_standard_counter > max_count : 
                            max_count = non_standard_counter
                            max_index = 24
                    if i.lower() in illegal_content or i in illegal_content :
                        illegal_counter += 1
                        if illegal_counter > max_count : 
                            max_count = illegal_counter
                            max_index = 25
                #iab_category_col.append(iab_categories[max_index])
                whole_output[app_id_to_parse]["iab_category_col"] = iab_categories[max_index]


            elif len(top_10_keywords) == 0 :
                #iab_category_col.append("")
                whole_output[app_id_to_parse]["iab_category_col"] = ""


        #app_age = soup.find("div", {"class": "w7Iutd"})
        app_age = soup.find("span", {"itemprop": "contentRating"})
        app_age= app_age.text.strip()
        app_age = app_age.split("Downloads", 1)[-1].strip()
        app_age = app_age.replace("info", "").strip()
        app_age = app_age.replace("Downloads", "").strip()

        looking_for_coppa = '1'
        profile_for_checking_coppa = get_profile_id_settings(file_id_for_apis)
        if profile_for_checking_coppa != '0' :
            looking_for_coppa = profile_for_checking_coppa["coppa"]

        if app_age == 'Teen'  :
            whole_output[app_id_to_parse]["coppa_col"] = "0"
        elif app_age == 'Everyone' :
            whole_output[app_id_to_parse]["coppa_col"] = "1"

        elif app_age.startswith("Rated for") and len(app_age) > 11 and app_age.endswith("+") :
            app_age = int(app_age[10:-1])
            if app_age < 13 :
                #coppa_col.append("1")
                whole_output[app_id_to_parse]["coppa_col"] = "1"
            elif app_age >= 13 :
                #coppa_col.append("0")
                whole_output[app_id_to_parse]["coppa_col"] = "0"

        elif app_age.startswith("Rated for") == False or len(app_age) <= 11 or app_age.endswith("+") == False :

            # 15.08 COPPA START
            if app_age.lower() == 'parental guidance' :
                app_age = '15'
                #coppa_col.append("0")
                whole_output[app_id_to_parse]["coppa_col"] = "0"
            elif "PEGI" in app_age :
                unknown_format = False
                if app_age[-4:] == 'PEGI' :
                    unknown_format = True
                if app_age.index("PEGI") + 5 < len(app_age) :
                    digits = app_age[app_age.index("PEGI") + 5:]
                    for digit in digits :
                        if digit.isdigit() == False :
                            unknown_format = True
                    if unknown_format == False :
                        app_age = digits
                        if int(digits) < 13 :
                            #coppa_col.append("1")
                            whole_output[app_id_to_parse]["coppa_col"] = "1"
                        elif int(digits) >= 13 :
                            #coppa_col.append("0")
                            whole_output[app_id_to_parse]["coppa_col"] = "0"
                elif app_age.index("PEGI") + 5 >= len(app_age) and unknown_format == False :
                    #coppa_col.append("Incorrect format")
                    whole_output[app_id_to_parse]["coppa_col"] = "Incorrect format"
                if unknown_format == True :
                    #coppa_col.append("Incorrect format")
                    whole_output[app_id_to_parse]["coppa_col"] = "Incorrect format"
            elif "PEGI" not in app_age :
                #coppa_col.append("Unknown format")
                whole_output[app_id_to_parse]["coppa_col"] = "Unknown format"
            # 15.08 COPPA END

        #Because brand profile's coppa is set to 0, meaning no need to look for coppa, everything passes, so coppa is automatically 1
        if looking_for_coppa == '0' :
            whole_output[app_id_to_parse]["coppa_col"] = '1'

        #age_rating_col.append(app_age)
        whole_output[app_id_to_parse]["age_rating_col"] = app_age

        #processed_date_col.append(today)
        whole_output[app_id_to_parse]["processed_date_col"] = today



                
        print("Process successful for ANDROID ID :", app_id_to_parse)
        print(len(whole_output))

        if app_id_to_parse in error_ids :
            error_ids.remove(app_id_to_parse)

        #lock.release()

    except Exception as e:
        print(f"Error processing {app_id_to_parse}: {str(e)}")
        if app_id_to_parse in whole_output :
            whole_output.pop(app_id_to_parse)
        if app_id_to_parse not in error_ids :
            error_ids.append(app_id_to_parse)

    finally :
        lock.release()


android_file_names = os.listdir("main/android/")
android_file_names_final = []

android_status_query = "SELECT file_name from file_status where android_started = 'No'"

mycursor.execute(android_status_query)

file_names_from_db = []


for i in list(mycursor) :
    file_names_from_db.append(i[0])

for i in file_names_from_db :
    android_started_query = "UPDATE file_status SET android_started = 'Yes' WHERE file_name = '" + i + "'"
    mycursor.execute(android_started_query)
    mydb.commit()

for i in android_file_names : # i = file_id-android_tobeparsed.csv
    if i[:-23] + ".csv" in file_names_from_db : #this cuts out the FILE_ID and adds ".csv" to it
        android_file_names_final.append(i)


for file_name in android_file_names_final :
    error_ids = []
    # Change 26.03.24
    whole_output = {}
    ####
    try :

        input_df = pd.read_csv('main/android/' + file_name, header=None)
    except :
        merged_df = {"Empty_file" : []}
        merged_df = pd.DataFrame(merged_df)
        processed_android_file = file_name[:-15] + ".csv"

        merged_df.to_csv(processed_android_file, index=None, header=True)
        sftp_url = "sftp.dc2.gpaas.net"
        username = "7872195"
        password = "WonderBatmanSuperman12!"
        local_file_path = "C:/Users/Administrator/Desktop/parsers/" + processed_android_file  # Corrected the file path
        remote_directory = "/vhosts/certm8.com/htdocs/platform/profcf"

        upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory)

        
        os.remove(processed_android_file)
        os.remove("main/android/" + file_name)
        
        android_done_query = "UPDATE file_status SET android_done = 'Yes' WHERE file_name = '" + file_name[:-23] + ".csv" + "'"
        mycursor.execute(android_done_query)
        mydb.commit()

        others_query = "SELECT ios_done, domain_done from file_status where file_name = '" + file_name[:-23] + ".csv" + "'"

        mycursor.execute(others_query)

        for i in list(mycursor) :
            if i[0] == "Yes" and i[1] == "Yes" :
                final_android_call = requests.get('https://certm8.com/platform/u2/?k=786123&id=' + processed_android_file[:-12])
                delete_query = "DELETE FROM file_status WHERE file_name = '" + file_name[:-23] + ".csv" + "'"
                mycursor.execute(delete_query)
                mydb.commit()
        continue

    #mycursor = mydb.cursor()

    #check if deeper green calculation is needed
    file_id = file_name[:-23]   #file_id-android_tobeparsed.csv "-android_tobeparsed.csv"  23 characters should be cut

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

    android_ids_from_input_file = []

    for i in range(len(input_list)) :
        android_ids_from_input_file.append(input_list[i][0])


    ids_from_db = []
    dates_from_db = []
    greenrating_from_db = []
    totaldatatransfer_from_db = []
    brandprofileid_from_db = []

    ids_from_db_query = "SELECT app_id, processed_date, greenrating, totaldatatransfer, brandprofileid from android_parser"
    
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


    # 26.03.24 Change
    android_ids_from_input_file = list(set(android_ids_from_input_file))
    #####

    to_parse = []
    to_delete = []
    to_get_from_db = []
    for i in android_ids_from_input_file :
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
    appsize_col = []
    pagesize_col = []
    totaldatatransfer_col = []
    emissionsnonrw_col = []
    emissionsrw_col = []
    totalcarbonemissions_col = []
    brand_profile_id_col = []

    today = today.strftime("%m/%d/%Y")

    to_parse = list(set(to_parse))

        
    are_these_error_ids = False
    threads = [threading.Thread(target=parse_the_android_id, args=(url, deeper_green_calculation_bool, deeper_green_attributes, are_these_error_ids, file_id)) for url in to_parse]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    try_counter = 0
    are_these_error_ids = True
    while len(error_ids) > 0 and try_counter < 5 :
        print("Try number errors with" + str(len(error_ids)))
        threads = [threading.Thread(target=parse_the_android_id, args=(url, deeper_green_calculation_bool, deeper_green_attributes, are_these_error_ids, file_id)) for url in error_ids]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        try_counter = try_counter + 1
        print("Try number", try_counter, "done")


    for processed_id in whole_output :
        app_id_col.append(whole_output[processed_id]["app_id_col"])
        devurl_col.append(whole_output[processed_id]["devurl_col"])
        app_name_col.append(whole_output[processed_id]["app_name_col"])
        app_type_col.append(whole_output[processed_id]["app_type_col"])
        app_category_col.append(whole_output[processed_id]["app_category_col"])
        iab_category_col.append(whole_output[processed_id]["iab_category_col"])
        top_10_keywords_col.append(whole_output[processed_id]["top_10_keywords_col"])
        downloads_col.append(whole_output[processed_id]["downloads_col"])
        rating_col.append(whole_output[processed_id]["rating_col"])
        age_rating_col.append(whole_output[processed_id]["age_rating_col"])
        tablet_optimized_col.append(whole_output[processed_id]["tablet_optimized_col"])
        app_developer_col.append(whole_output[processed_id]["app_developer_col"])
        bipoc_col.append(whole_output[processed_id]["bipoc_col"])
        bipoc_conf_col.append(whole_output[processed_id]["bipoc_conf_col"])
        processed_date_col.append(whole_output[processed_id]["processed_date_col"])
        # 14.08 Brand safety START
        adult_col.append(whole_output[processed_id]["adult_col"])
        alcohol_col.append(whole_output[processed_id]["alcohol_col"])
        aviation_col.append(whole_output[processed_id]["aviation_col"])
        crime_col.append(whole_output[processed_id]["crime_col"])
        drug_col.append(whole_output[processed_id]["drug_col"])
        hate_col.append(whole_output[processed_id]["hate_col"])
        human_made_disasters_col.append(whole_output[processed_id]["human_made_disasters_col"])
        politics_brand_safe_col.append(whole_output[processed_id]["politics_brand_safe_col"])
        natural_disasters_col.append(whole_output[processed_id]["natural_disasters_col"])
        piracy_col.append(whole_output[processed_id]["piracy_col"])
        profanity_col.append(whole_output[processed_id]["profanity_col"])
        terrorism_col.append(whole_output[processed_id]["terrorism_col"])
        tobacco_col.append(whole_output[processed_id]["tobacco_col"])
        cannabis_col.append(whole_output[processed_id]["cannabis_col"])
        car_accidents_col.append(whole_output[processed_id]["car_accidents_col"])
        brand_safe_col.append(whole_output[processed_id]["brand_safe_col"])
        # 14.08 Brand safety END
        # 15.08 COPPA START
        coppa_col.append(whole_output[processed_id]["coppa_col"])
        # 15.08 COPPA END
        greenrating_col.append(whole_output[processed_id]["greenrating_col"])
        appsize_col.append(whole_output[processed_id]["appsize_col"])
        pagesize_col.append(whole_output[processed_id]["pagesize_col"])
        totaldatatransfer_col.append(whole_output[processed_id]["totaldatatransfer_col"])
        emissionsnonrw_col.append(whole_output[processed_id]["emissionsnonrw_col"])
        emissionsrw_col.append(whole_output[processed_id]["emissionsrw_col"])
        totalcarbonemissions_col.append(whole_output[processed_id]["totalcarbonemissions_col"])
        brand_profile_id_col.append(whole_output[processed_id]["brand_profile_id_col"])

        
    #for android_url in to_parse :
    #    parse_the_android_id(android_url)

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
            'appsize' : appsize_col,
            'pagesize' : pagesize_col,
            'totaldatatransfer' : totaldatatransfer_col,
            'emissionsnonrw' : emissionsnonrw_col,
            'emissionsrw' : emissionsrw_col,
            'totalcarbonemissions' : totalcarbonemissions_col,
            'brandprofileid' : brand_profile_id_col}

    df = pd.DataFrame(data)

    values_to_sql = df.values.tolist()

    for i in to_delete :
        mycursor.execute("DELETE FROM android_parser WHERE app_id = '" + i + "'")

    mydb.commit()

    sql_query = "INSERT INTO android_parser (App_ID, privacyurl, App_name, App_type, App_category, IAB_category,Top_10_keywords, Downloads, Rating, Age_rating, Tablet_optimized, Developer, BIPOC, BIPOC_conf, Adult, Alcohol, Aviation, Crime, Drug_abuse, Hate_speech, Human_made_disasters, Politics, Natural_disasters, Piracy, Profanity, Terrorism, Tobacco, Cannabis, Car_accidents, Brand_safe, Coppa, Processed_date, GreenRating, appsize, pagesize, totaldatatransfer, emissionsnonrw, emissionsrw, totalcarbonemissions, brandprofileid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    mycursor.executemany(sql_query, values_to_sql)

    mydb.commit()

    for i in to_get_from_db :
        mycursor.execute("SELECT * FROM android_parser WHERE app_id = '" + i + "'")
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
        coppa_col.append(db_row_as_list[0][30])
        greenrating_col.append(db_row_as_list[0][32])
        # 14.08 Brand safety END
        appsize_col.append(db_row_as_list[0][33])
        pagesize_col.append(db_row_as_list[0][34])
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
            'Coppa' : coppa_col,
            'GreenRating' : greenrating_col,
            'appsize' : appsize_col,
            'pagesize' : pagesize_col,
            'totaldatatransfer' : totaldatatransfer_col,
            'emissionsnonrw' : emissionsnonrw_col,
            'emissionsrw' : emissionsrw_col,
            'totalcarbonemissions' : totalcarbonemissions_col,
            'brandprofileid' : brand_profile_id_col}
            # 14.08 Brand safety END

    merged_df = pd.DataFrame(merged_data)

    processed_android_file = file_name[:-15] + ".csv"

    merged_df.to_csv(processed_android_file, index=None, header=True)

    sftp_url = "sftp.dc2.gpaas.net"
    username = "7872195"
    password = "WonderBatmanSuperman12!"
    local_file_path = "C:/Users/Administrator/Desktop/parsers/" + processed_android_file  # Corrected the file path
    remote_directory = "/vhosts/certm8.com/htdocs/platform/profcf"

    upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory)
    
    os.remove(processed_android_file)
    os.remove("main/android/" + file_name)

    android_done_query = "UPDATE file_status SET android_done = 'Yes' WHERE file_name = '" + file_name[:-23] + ".csv" + "'"
    mycursor.execute(android_done_query)
    mydb.commit()

    others_query = "SELECT ios_done, domain_done from file_status where file_name = '" + file_name[:-23] + ".csv" + "'"

    mycursor.execute(others_query)

    for i in list(mycursor) :
        if i[0] == "Yes" and i[1] == "Yes" :
            final_android_call = requests.get('https://certm8.com/platform/u2/?k=786123&id=' + processed_android_file[:-12])
            delete_query = "DELETE FROM file_status WHERE file_name = '" + file_name[:-23] + ".csv" + "'"
            mycursor.execute(delete_query)
            mydb.commit()
    #upload file to server

mycursor.close()
mydb.close()

print("It took this much time : ", datetime.now() - startTime)