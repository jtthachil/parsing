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
from datetime import date, timedelta, datetime  #built-in
import pandas as pd #pandas==1.4.3
import numpy as np  #numpy==1.23.1
import os   #built-in
import threading    #built-in
import time #built-in
import urllib.request   #urllib3==1.26.9
import mysql.connector  #mysql-connector-python==8.0.30
import paramiko #paramiko==3.2.0
import socket   #built-in
from ipwhois import IPWhois #ipwhois==1.2.0
import ssl  #built-in

startTime = datetime.now()

lock = threading.Lock()


def does_title_have_sports_words(title_to_check_for_sports) :
    sports_related_words = ["sports", "sport", "football", "soccer", "basketball", "american football", "rugby", "cricket",
                            "baseball", "hockey", "volleyball", "handball", "water polo", "tennis", "badminton", "boxing",
                            "golf", "martial arts", "karate", "taekwondo", "judo", "wrestling", "swimming", "track and field",
                            "sprinting", "long distance", "cycling", "archery", "rowing", "canoeing", "kayaking", "surfing",
                            "diving", "sailing", "water skiing", "formula 1", "formula one", "nascar", "motogp", "rally racing",
                            "skateboarding", "bmx", "snowboarding", "rock climbing", "skydiving", "table tennis", "pickleball",
                            "skiing", "ice skating", "bobsledding", "curling", "mma", "mixed martial arts",
                            "kickboxing", "fencing"]
    
    for title_word in title_to_check_for_sports :
        if title_word.lower() in sports_related_words :
            return True
        
    return False
    

def string_to_list_of_words(string_to_convert) :
    list_of_words = []
    if string_to_convert is None or len(string_to_convert) == 0 :
        return list_of_words
    beginning_index = 0
    ending_index = 0
    indices_of_non_alpha = []

    while ending_index < len(string_to_convert) :
        if string_to_convert[ending_index].isalnum() == False :
            indices_of_non_alpha.append(ending_index)
        ending_index += 1

    if len(indices_of_non_alpha) == 0 :
        return [string_to_convert]
    
    for non_alpha in indices_of_non_alpha :
        if beginning_index + 1 == non_alpha :
            beginning_index = non_alpha
            continue
        if string_to_convert[beginning_index].isalnum() == False :
            list_of_words.append(string_to_convert[beginning_index + 1:non_alpha])
        elif string_to_convert[beginning_index].isalnum() == True :
            list_of_words.append(string_to_convert[beginning_index:non_alpha])
        beginning_index = non_alpha

    if string_to_convert[beginning_index].isalnum() == False :
            if beginning_index + 1!= len(string_to_convert) :
                list_of_words.append(string_to_convert[beginning_index + 1:])
    elif string_to_convert[beginning_index].isalnum() == True :
        list_of_words.append(string_to_convert[beginning_index:])
    #list_of_words.append(string_to_convert[beginning_index:])

    return list_of_words





def is_it_news(domain_to_check_for_news) :

    news_websites = ["nytimes.com", "washingtonpost.com", "usatoday.com", "latimes.com", "wsj.com", "chicagotribune.com", "bostonglobe.com","nypost.com",
                     "sfchronicle.com", "miamiherald.com","dallasnews.com", "houstonchronicle.com", "inquirer.com", "theguardian.com", "thetimes.co.uk",
                     "telegraph.co.uk", "independent.co.uk", "dailymail.co.uk", "thesun.co.uk", "mirror.co.uk", "ft.com", "scotsman.com", "standard.co.uk", "inews.co.uk",
                     "metro.co.uk", "express.co.uk"]
    
    news_websites_with_http = []

    for news_website in news_websites :
        news_websites_with_http.append("http://" + news_website)
        news_websites_with_http.append("http://www." + news_website)
        news_websites_with_http.append("https://" + news_website)
        news_websites_with_http.append("https://www." + news_website)

    if domain_to_check_for_news.startswith(tuple(news_websites)) or domain_to_check_for_news.startswith(tuple(news_websites_with_http)) : 
        return True
    
    return False

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
#start = time.time()

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

# 14.08 Brand safety    START
brand_safety_file = open('brand_safety_keywords.json')

brand_safety_dict = json.load(brand_safety_file)

brand_safety_file.close()

adult_list = brand_safety_dict["adult"]
alcohol_list = brand_safety_dict["alcohol"]
aviation_list = brand_safety_dict["aviation"]
crime_list = brand_safety_dict["crime"]
drug_list = brand_safety_dict["drug_abuse"]
hate_list = brand_safety_dict["hate_speech"]
human_made_disasters_list = brand_safety_dict["human_made_disasters"]
politics_brand_safety_list = brand_safety_dict["politics"]
natural_disasters_list = brand_safety_dict["natural_disasters"]
piracy_list = brand_safety_dict["piracy"]
profanity_list = brand_safety_dict["profanity"]
terrorism_list = brand_safety_dict["terrorism"]
tobacco_list = brand_safety_dict["tobacco"]
cannabis_list = brand_safety_dict["cannabis"]
car_accidents_list = brand_safety_dict["car_accidents"]
# 14.08 Brand safety    END

static_file = open('categories_and_hitwords.json')

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

# 15.08 COPPA START
coppa_file = open('coppa_keywords.json')

coppa_dict = json.load(coppa_file)

coppa_file.close()

coppa_list = coppa_dict["coppa_words"]
# 15.08 COPPA END

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




removed_keywords = ["https", "http", "utc", "us", "pm", "am", "follow", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                    "sunday", "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november",
                    "december"]


sql_query = "INSERT INTO domain_parser (Domain, Title, Keywords, Description, Deterministic_type, IAB, Adult, Alcohol, Aviation, Crime, Drug_abuse, Hate_speech, Human_made_disasters, Politics, Natural_disasters, Piracy, Profanity, Terrorism, Tobacco, Cannabis, Car_accidents, Brand_safe, Coppa, Processed_date, greenrating, appsize, pagesize, totaldatatransfer, emissionsnonrw, emissionsrw, totalcarbonemissions, brandprofileid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

whitespaces = [" ", "\t", "\n", "\v", "\r", "\f"]
def trim_whitespaces_from_both_sides (keyword) :
    if len(keyword) == 0 :
        return keyword
    while keyword[0] in whitespaces:
        keyword = keyword[1:]
    while keyword[-1] in whitespaces:
        keyword = keyword[:-1]
    searching_index = 0
    while searching_index < len(keyword) :
        if keyword[searching_index] in whitespaces and keyword[searching_index] != " ":
            index_of_whitespace = keyword.find(keyword[searching_index])
            keyword = keyword[:index_of_whitespace] + keyword[index_of_whitespace + 1 :]
            continue
        searching_index += 1
    return keyword


list_df = ["fandom.com", "imdb.com", "autozone.com", "carsales.com.au", "copart.com", "dodge.com", "squareup.com", "inc.com",
           "glassdoor.com", "ziprecruiter.com", "instructure.com", "quizlet.com", "udemy.com", "coursera.org", "khanacademy.org",
            "clever.com", "harvard.edu", "myparenttoolkit.com", "parents.com","todaysparent.com","babycenter.com",
              "myfitnesspal.com","menshealth.com","weightwatchers.com","vivino.com","liquor.com","allrecipes.com", 
              "foodnetwork.com", "freshhobby.com","craftsmanave.com","craftsy.com","gardeningknowhow.com","gardenersworld.com",
               "ikea.com", "ashleyfurniture.com","redstate.com","conservativejournalreview.com", "cnn.com",
            "ft.com", "nbcnews.com","msn.com","experian.com","nerdwallet.com","goodfinancialcents.com",
            "researchgate.net","livescience.com","science.org","petfinder.com","akc.org","espn.com","skysports.com", "nbcsports.com",
            "harpersbazaar.com","instyle.com", "zillow.com", "realtor.com"]

three_domains = ["fandom.com", "imdb.com", "autozone.com"]

could_not_process = []


#parsing new pixel domains
def parse_the_url(url_to_parse, deeper_green_calculation_flag, deeper_green_attributes_dictionary, file_id_for_apis) :

    url_to_parse = str(url_to_parse)
    #dictionary for the whole output
    result = {}

    #creating a soup(parse tree)  (for searching, iterating through parse tree)
    try :
        url = Request(url_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
        html = urlopen(url, timeout=15).read()
        soup = BeautifulSoup(html, features="html.parser")
    
    except :
    #if https is used
        try :
            url = Request("https://" + url_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
            html = urlopen(url, timeout=15).read()
            soup = BeautifulSoup(html, features="html.parser")
            
        
        #if http is used
        except :
            try : 
                url = Request("http://" + url_to_parse, headers={'User-Agent' : 'Mozilla/5.0'})
                html = urlopen(url, timeout=15).read()
                soup = BeautifulSoup(html, features="html.parser")
            
            except :
                try :
                    context = ssl._create_unverified_context()
                    if url_to_parse.startswith("http") == False :
                        corrected_url = "https://" + url_to_parse
                        shorter_corrected_url = 'http://' + url_to_parse
                    elif url_to_parse.startswith("http") == True :
                        corrected_url = url_to_parse
                        shorter_corrected_url = None
                    try :
                        url = Request(corrected_url, headers={'User-Agent' : 'Mozilla/5.0'})
                        html = urlopen(url, timeout=15, context=context).read()
                        soup = BeautifulSoup(html, features="html.parser")
                    except :
                        url = Request(shorter_corrected_url, headers={'User-Agent' : 'Mozilla/5.0'})
                        html = urlopen(url, timeout=15, context=context).read()
                        soup = BeautifulSoup(html, features="html.parser")
                except :
                    try :
                        if url_to_parse.startswith("http") == False :
                            api_url = "https://" + url_to_parse
                            shorter_api_url = 'http://' + url_to_parse
                        elif url_to_parse.startswith("http") == True :
                            api_url = url_to_parse
                            shorter_api_url = None
                        try :
                            url = Request("http://api.scraperapi.com?api_key=c23e0dd0e126b0904b80503545b26861&url=" + api_url + "&render=true", headers={'User-Agent' : 'Mozilla/5.0'})
                            html = urlopen(url, timeout=15, context=context).read()
                            soup = BeautifulSoup(html, features="html.parser")
                            print(url_to_parse + " was successful with api_url")
                        except :
                            url = Request("http://api.scraperapi.com?api_key=c23e0dd0e126b0904b80503545b26861&url=" + shorter_api_url + "&render=true", headers={'User-Agent' : 'Mozilla/5.0'})
                            html = urlopen(url, timeout=15, context=context).read()
                            soup = BeautifulSoup(html, features="html.parser")
                            print(url_to_parse + " was successful with shorter_api_url")

                    except :
                        print(f"Could not create the html soup for {url_to_parse}")
                        could_not_process.append(url_to_parse)
                        return  #returns None
                    
    try :

        lock.acquire()

        strips = list(soup.stripped_strings)
        for strip in strips : 
            if "�" in strip :
                print(f"{url_to_parse} could not be parsed and was skipped because of bad encoding")
                lock.release()
                return

        print("Processing the domain : ", url_to_parse)

        #removing trash(script, style tags)
        for script in soup(["script", "style"]) :
            script.extract()

        
        #getting the title tag
        if soup.title is not None :
            title_is = str(soup.title.string)
            title_is = trim_whitespaces_from_both_sides(title_is)
            result["Title tag"] = title_is.replace(";", ",")
        elif soup.title is None :
            result["Title tag"] = ''


        #declaring meta data variables
        meta_keywords = ''
        meta_description = ''
        #iterating through meta tags
        for tags in soup.find_all('meta') :
            if tags.get('name') == 'description' :   #getting meta data description
                meta_description = tags.get('content')
            elif tags.get('name') == 'keywords' :  #getting meta data keywords
                meta_keywords = tags.get('content')

        #Fixed on 14 Aug 2023
        if meta_description is not None :
            if meta_description != '' and "&#39;" in meta_description :
                meta_description = meta_description.replace("&#39;", "'")
            if ";" in meta_description :
                meta_description = meta_description.replace(";", " ")
        
            for letter in meta_description :
                if letter in whitespaces :
                    meta_description = meta_description.replace(whitespaces[whitespaces.index(letter)], " ")
        # Fixed

        if meta_description is not None and ";" in meta_description :
            meta_description = meta_description.replace(';', ',')


        result["Meta data description"] = meta_description



        #greenrating
        greenrating_value_for_appending = 35
        domain_to_check = url_to_parse
        if domain_to_check.startswith("http") and domain_to_check.find('://') != -1 :
            domain_to_check = domain_to_check[domain_to_check.find('://') + 3:]
        if domain_to_check.endswith('/') :
            domain_to_check = domain_to_check[:-1]
        
        try:
            location_info = get_server_location(domain_to_check)
            if location_info in country_and_renewable_dict :
                greenrating_value_for_appending = country_and_renewable_dict[location_info]
                greenrating_col.append(greenrating_value_for_appending)
            elif location_info not in country_and_renewable_dict :
                greenrating_col.append(greenrating_value_for_appending)
        except Exception as e:
            greenrating_col.append(greenrating_value_for_appending)


        # Page size
        
        page_size_in_gb = get_page_size(url_to_parse)

        app_size_col.append("")

        page_size_col.append(str(page_size_in_gb))


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

            totaldatatransfer = page_size_in_gb * int(green_impressions) * float(green_creatives) * landingpagesize

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





        #extracting all strings from the soup(html)
        strips = list(soup.stripped_strings)
        
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
        looking_for_crime = '1'
        looking_for_drug = '1'
        looking_for_hatespeech = '1'
        looking_for_human_disasters = '1'
        looking_for_politics = '1'
        looking_for_natural_disasters = '1'
        looking_for_piracy = '1'
        looking_for_profanity = '1'
        looking_for_terrorism = '1'
        looking_for_tobacco = '1'
        looking_for_cannabis = '1'
        looking_for_caraccidents = '1'
        looking_for_coppa = '1'

        brand_profile_id_for_output = '0'

        is_brand_safe = 1
        adult_value = 0
        alcohol_value = 0
        aviation_value = 0
        crime_value = 0
        drug_value = 0
        hate_value = 0
        human_made_disasters_value = 0
        politics_brand_safety_value = 0
        natural_disasters_value = 0
        piracy_value = 0
        profanity_value = 0
        terrorism_value = 0
        tobacco_value = 0
        cannabis_value = 0
        car_accidents_value = 0

        coppa_word_found = False

        profile_for_checking = get_profile_id_settings(file_id_for_apis)
        if profile_for_checking != '0' :
            looking_for_adult = profile_for_checking["adult"]
            looking_for_alcohol = profile_for_checking["alcohol"]
            looking_for_aviation = profile_for_checking["aviation"]
            looking_for_crime = profile_for_checking["crime"]
            looking_for_drug = profile_for_checking["drugabuse"]
            looking_for_hatespeech = profile_for_checking["hatespeech"]
            looking_for_human_disasters = profile_for_checking["humanmadedisasters"]
            looking_for_politics = profile_for_checking["politics"]
            looking_for_natural_disasters = profile_for_checking["naturaldisasters"]
            looking_for_piracy = profile_for_checking["piracy"]
            looking_for_profanity = profile_for_checking["profanity"]
            looking_for_terrorism = profile_for_checking["terrorism"]
            looking_for_tobacco = profile_for_checking["tobacco"]
            looking_for_cannabis = profile_for_checking["cannabis"]
            looking_for_caraccidents = profile_for_checking["caraccidents"]
            looking_for_coppa = profile_for_checking["coppa"]
            brand_profile_id_for_output = profile_for_checking["id"]

        for noun_or_verb in nouns_and_verbs :
            # 15.08 COPPA START
            if looking_for_coppa == '1' and noun_or_verb.lower() in coppa_list :
                coppa_word_found = True
            # 15.08 COPPA END
            if looking_for_adult == '1' and adult_value == 0 and noun_or_verb.lower() in adult_list :
                adult_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in adult :", noun_or_verb)
            if looking_for_alcohol == '1' and alcohol_value == 0 and noun_or_verb.lower() in alcohol_list :
                alcohol_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in alcohol :", noun_or_verb)
            if looking_for_aviation == '1' and aviation_value == 0 and noun_or_verb.lower() in aviation_list :
                aviation_value = 1
                #is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in aviation :", noun_or_verb)
            if looking_for_crime == '1' and crime_value == 0 and noun_or_verb.lower() in crime_list :
                crime_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in crime :", noun_or_verb)
            if looking_for_drug == '1' and drug_value == 0 and noun_or_verb.lower() in drug_list :
                drug_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in drug :", noun_or_verb)
            if looking_for_hatespeech == '1' and hate_value == 0 and noun_or_verb.lower() in hate_list :
                hate_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in hate :", noun_or_verb)
            if looking_for_human_disasters == '1' and human_made_disasters_value == 0 and noun_or_verb.lower() in human_made_disasters_list :
                human_made_disasters_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in human made :", noun_or_verb)
            if looking_for_politics == '1' and politics_brand_safety_value == 0 and noun_or_verb.lower() in politics_brand_safety_list :
                politics_brand_safety_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in politics :", noun_or_verb)
            if looking_for_natural_disasters == '1' and natural_disasters_value == 0 and noun_or_verb.lower() in natural_disasters_list :
                natural_disasters_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in natural :", noun_or_verb)
            if looking_for_piracy == '1' and piracy_value == 0 and noun_or_verb.lower() in piracy_list :
                piracy_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in piracy :", noun_or_verb)
            if looking_for_profanity == '1' and profanity_value == 0 and noun_or_verb.lower() in profanity_list :
                profanity_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in profanity :", noun_or_verb)
            if looking_for_terrorism == '1' and terrorism_value == 0 and noun_or_verb.lower() in terrorism_list :
                terrorism_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in terrorism :", noun_or_verb)
            if looking_for_tobacco == '1' and tobacco_value == 0 and noun_or_verb.lower() in tobacco_list :
                tobacco_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in tobacco :", noun_or_verb)
            if looking_for_cannabis == '1' and cannabis_value == 0 and noun_or_verb.lower() in cannabis_list :
                cannabis_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in cannabis :", noun_or_verb)
            if looking_for_caraccidents == '1' and car_accidents_value == 0 and noun_or_verb.lower() in car_accidents_list :
                car_accidents_value = 1
                is_brand_safe = 0
                #print("For domain ", url_to_parse, "this word in car accidents :", noun_or_verb)

        adult_col.append(str(adult_value))
        alcohol_col.append(str(alcohol_value))
        aviation_col.append(str(aviation_value))
        crime_col.append(str(crime_value))
        drug_col.append(str(drug_value))
        hate_col.append(str(hate_value))
        human_made_disasters_col.append(str(human_made_disasters_value))
        politics_brand_safe_col.append(str(politics_brand_safety_value))
        natural_disasters_col.append(str(natural_disasters_value))
        piracy_col.append(str(piracy_value))
        profanity_col.append(str(profanity_value))
        terrorism_col.append(str(terrorism_value))
        tobacco_col.append(str(tobacco_value))
        cannabis_col.append(str(cannabis_value))
        car_accidents_col.append(str(car_accidents_value))
        brand_safe_col.append(str(is_brand_safe))
        brand_profile_id_col.append(brand_profile_id_for_output)
        # 14.08 Brand safety END

        # 15.08 COPPA START
        if looking_for_coppa == '0' :
            coppa_col.append("1")
        elif looking_for_coppa == '1' :
            if coppa_word_found == True and is_brand_safe == 1 :
                coppa_col.append("1")
            elif coppa_word_found == False or is_brand_safe == 0 :
                coppa_col.append("0")
        # 15.08 COPPA END

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

        if len(sorted_result) >= 100 :
            nouns_used_for_categorization = sorted_result[:100]
        elif len(sorted_result) < 100 :
            nouns_used_for_categorization = sorted_result



        #if meta data keywords available
        if meta_keywords != '' and meta_keywords is not None :
            #iterating through all meta keywords and writing them into an array separately
            meta_keywords_list = []
            start = 0
            while meta_keywords.find(',', start) != -1:
                next_elem = meta_keywords[start:meta_keywords.find(',', start)]
                start = meta_keywords.find(',', start) + 1
                if len(next_elem) == 0 :
                    continue
                next_elem = trim_whitespaces_from_both_sides(next_elem)    #removing spaces
                meta_keywords_list.append(next_elem)

            keywords_string = ''
            for keywo in meta_keywords_list :
                keywords_string += ', ' + keywo
            if len(keywords_string) > 3 :
                keywords_string = keywords_string[2:]

            result["Meta data keywords"] = keywords_string.replace(";", ",")
            result["Deterministic"] = 1

        #elif meta data keywords not available
        elif meta_keywords == '' or meta_keywords is None :

            top_10_nouns = sorted_result[:10]

            inferred_meta_keywords = []

            for i in top_10_nouns :
                inferred_meta_keywords.append(i[0])

            keywords_string = ''
            for keywo in inferred_meta_keywords :
                keywords_string += ', ' + keywo
            if len(keywords_string) > 3 :
                keywords_string = keywords_string[2:]

            result["Meta data keywords"] = keywords_string.replace(";", ",")
            result["Deterministic"] = 0

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
        #sports_counter = 0
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
            if i[0].lower() in arts_and_ent or i in arts_and_ent :
                arts_counter += 1
                if arts_counter > max_count : 
                    max_count = arts_counter
                    max_index = 0
            if i[0].lower() in automotive or i in automotive :
                auto_counter += 1
                if auto_counter > max_count : 
                    max_count = auto_counter
                    max_index = 1
            if i[0].lower() in business or i in business :
                business_counter += 1
                if business_counter > max_count : 
                    max_count = business_counter
                    max_index = 2
            if i[0].lower() in careers or i in careers :
                careers_counter += 1
                if careers_counter > max_count : 
                    max_count = careers_counter
                    max_index = 3
            if i[0].lower() in education or i in education :
                education_counter += 1
                if education_counter > max_count : 
                    max_count = education_counter
                    max_index = 4
            if i[0].lower() in family_and_parenting or i in family_and_parenting :
                family_counter += 1
                if family_counter > max_count : 
                    max_count = family_counter
                    max_index = 5
            if i[0].lower() in health_and_fitness or i in health_and_fitness :
                health_counter += 1
                if health_counter > max_count : 
                    max_count = health_counter
                    max_index = 6
            if i[0].lower() in food_and_drink or i in food_and_drink :
                food_counter += 1
                if food_counter > max_count : 
                    max_count = food_counter
                    max_index = 7
            if i[0].lower() in hobbies_and_interests or i in hobbies_and_interests :
                hobbies_counter += 1
                if hobbies_counter > max_count : 
                    max_count = hobbies_counter
                    max_index = 8
            if i[0].lower() in home_and_garden or i in home_and_garden :
                home_counter += 1
                if home_counter > max_count : 
                    max_count = home_counter
                    max_index = 9
            if i[0].lower() in law_and_politics or i in law_and_politics :
                law_counter += 1
                if law_counter > max_count : 
                    max_count = law_counter
                    max_index = 10
            if i[0].lower() in news or i in news :
                news_counter += 1
                if news_counter > max_count : 
                    max_count = news_counter
                    max_index = 11
            if i[0].lower() in personal_finance or i in personal_finance :
                finance_counter += 1
                if finance_counter > max_count : 
                    max_count = finance_counter
                    max_index = 12
            if i[0].lower() in society or i in society :
                society_counter += 1
                if society_counter > max_count : 
                    max_count = society_counter
                    max_index = 13
            if i[0].lower() in science or i in science :
                science_counter += 1
                if science_counter > max_count : 
                    max_count = science_counter
                    max_index = 14
            if i[0].lower() in pets or i in pets :
                pets_counter += 1
                if pets_counter > max_count : 
                    max_count = pets_counter
                    max_index = 15
            #if i[0].lower() in sports or i in sports :
            #    sports_counter += 1
            #    if sports_counter > max_count : 
            #        max_count = sports_counter
            #        max_index = 16
            if i[0].lower() in style_and_fashion or i in style_and_fashion :
                fashion_counter += 1
                if fashion_counter > max_count : 
                    max_count = fashion_counter
                    max_index = 17
            if i[0].lower() in tech_and_computing or i in tech_and_computing :
                tech_counter += 1
                if tech_counter > max_count : 
                    max_count = tech_counter
                    max_index = 18
            if i[0].lower() in travel or i in travel :
                travel_counter += 1
                if travel_counter > max_count : 
                    max_count = travel_counter
                    max_index = 19
            if i[0].lower() in real_estate or i in real_estate :
                real_estate_counter += 1
                if real_estate_counter > max_count : 
                    max_count = real_estate_counter
                    max_index = 20
            if i[0].lower() in shopping or i in shopping :
                shopping_counter += 1
                if shopping_counter > max_count : 
                    max_count = shopping_counter
                    max_index = 21
            if i[0].lower() in religion_and_spirituality or i in religion_and_spirituality :
                religion_counter += 1
                if religion_counter > max_count : 
                    max_count = religion_counter
                    max_index = 22
            if i[0].lower() in non_standard_content or i in non_standard_content :
                non_standard_counter += 1
                if non_standard_counter > max_count : 
                    max_count = non_standard_counter
                    max_index = 24
            if i[0].lower() in illegal_content or i in illegal_content :
                illegal_counter += 1
                if illegal_counter > max_count : 
                    max_count = illegal_counter
                    max_index = 25

        
        Domain_col.append(url_to_parse)
        title_tag_col.append(result["Title tag"])
        meta_keywords_col.append(str(result["Meta data keywords"]))
        meta_description_col.append(result["Meta data description"])
        deterministic_col.append(str(result["Deterministic"]))
        iab_col.append(iab_categories[max_index])
        processed_date_col.append(today)

        print("Number of records appended : ", len(Domain_col))
        #print(nouns_used_for_categorization)

        #lock.release()

    except Exception as e:
        length_to_decrease = -1
        if len(greenrating_col) != len(Domain_col) or len(greenrating_col) != len(title_tag_col) or len(greenrating_col) != len(meta_keywords_col) or len(greenrating_col) != len(meta_description_col) or len(greenrating_col) != len(deterministic_col) or len(greenrating_col) != len(iab_col) or len(greenrating_col) != len(processed_date_col) or len(greenrating_col) != len(adult_col) or len(greenrating_col) != len(alcohol_col) or len(greenrating_col) != len(aviation_col) or len(greenrating_col) != len(crime_col) or len(greenrating_col) != len(drug_col) or len(greenrating_col) != len(hate_col) :
            length_to_decrease = len(greenrating_col)
        if len(greenrating_col) != len(human_made_disasters_col) or len(greenrating_col) != len(politics_brand_safe_col) or len(greenrating_col) != len(natural_disasters_col) or len(greenrating_col) != len(piracy_col) or len(greenrating_col) != len(profanity_col) or len(greenrating_col) != len(terrorism_col) or len(greenrating_col) != len(tobacco_col) or len(greenrating_col) != len(cannabis_col) or len(greenrating_col) != len(car_accidents_col) :
            length_to_decrease = len(greenrating_col)
        if len(greenrating_col) != len(brand_safe_col) or len(greenrating_col) != len(coppa_col) or len(greenrating_col) != len(app_size_col) or len(greenrating_col) != len(page_size_col) or len(greenrating_col) != len(totaldatatransfer_col) or len(greenrating_col) != len(emissionsnonrw_col) or len(greenrating_col) != len(emissionsrw_col) or len(greenrating_col) != len(totalcarbonemissions_col)  :
            length_to_decrease = len(greenrating_col)
        if  len(greenrating_col) != len(brand_profile_id_col)  :
            length_to_decrease = len(greenrating_col)

        if length_to_decrease != -1 :
            if length_to_decrease == len(greenrating_col) :
                del greenrating_col[-1]
            if length_to_decrease == len(Domain_col) :
                del Domain_col[-1]
            if length_to_decrease == len(title_tag_col) :
                del title_tag_col[-1]
            if length_to_decrease == len(meta_keywords_col) :
                del meta_keywords_col[-1]
            if length_to_decrease == len(meta_description_col) :
                del meta_description_col[-1]
            if length_to_decrease == len(deterministic_col) :
                del deterministic_col[-1]
            if length_to_decrease == len(iab_col) :
                del iab_col[-1]
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


        print(f"Error processing {url_to_parse}: {str(e)}")

    finally :
        lock.release()

domain_file_names = os.listdir("main/domain/")
domain_file_names_final = []

domain_status_query = "SELECT file_name from file_status where domain_started = 'No'"

mycursor.execute(domain_status_query)

file_names_from_db = []


for i in list(mycursor) :
    file_names_from_db.append(i[0])

for i in file_names_from_db :
    android_started_query = "UPDATE file_status SET domain_started = 'Yes' WHERE file_name = '" + i + "'"
    mycursor.execute(android_started_query)
    mydb.commit()

for i in domain_file_names :
    if i[:-21] + ".csv" in file_names_from_db :
        domain_file_names_final.append(i)


for file_name in domain_file_names_final :
    print("Starting to process file : ", file_name)
    try :

        input_df = pd.read_csv('main/domain/' + file_name, header=None)
    except :
        merged_df = {"Empty_file" : []}
        merged_df = pd.DataFrame(merged_df)
        processed_domain_file = file_name[:-15] + ".csv"


        merged_df.to_csv(processed_domain_file, index=None, header=True)
        sftp_url = "sftp.dc2.gpaas.net"
        username = "7872195"
        password = "WonderBatmanSuperman12!"
        local_file_path = "C:/Users/Administrator/Desktop/parsers/" + processed_domain_file  # Corrected the file path
        remote_directory = "/vhosts/certm8.com/htdocs/platform/profcf"

        upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory)
        os.remove(processed_domain_file)
        os.remove("main/domain/" + file_name)

        domain_done_query = "UPDATE file_status SET domain_done = 'Yes' WHERE file_name = '" + file_name[:-21] + ".csv" + "'"
        mycursor.execute(domain_done_query)
        mydb.commit()

        others_query = "SELECT ios_done, android_done from file_status where file_name = '" + file_name[:-21] + ".csv" + "'"

        mycursor.execute(others_query)

        for i in list(mycursor) :
            if i[0] == "Yes" and i[1] == "Yes" :
                final_domain_call = requests.get('https://certm8.com/platform/u2/?k=786123&id=' + processed_domain_file[:-10])
                delete_query = "DELETE FROM file_status WHERE file_name = '" + file_name[:-21] + ".csv" + "'"
                mycursor.execute(delete_query)
                mydb.commit()
        continue

    #mycursor = mydb.cursor()

    #check if deeper green calculation is needed
    file_id = file_name[:-21]   #file_id-sites_tobeparsed.csv "-sites_tobeparsed.csv"  22 characters should be cut to get the ID

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

    domains_from_input_file = []

    for i in range(len(input_list)) :
        domains_from_input_file.append(input_list[i][0])


    ids_from_db = []
    dates_from_db = []
    greenrating_from_db = []
    totaldatatransfer_from_db = []
    brandprofileid_from_db = []

    ids_from_db_query = "SELECT domain, processed_date, greenrating, totaldatatransfer, brandprofileid from domain_parser"

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
    for i in domains_from_input_file :
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



    Domain_col = []
    title_tag_col = []
    meta_keywords_col = []
    meta_description_col = []
    deterministic_col = []
    iab_col = []
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
    coppa_col = []
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


    threads = [threading.Thread(target=parse_the_url, args=(url, deeper_green_calculation_bool, deeper_green_attributes, file_id)) for url in to_parse]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    data = {'Domain' : Domain_col,
            'Title_tag' : title_tag_col,
            'Meta data keywords' : meta_keywords_col,
            'Description' : meta_description_col,
            'Determenistic' : deterministic_col,
            'IAB' : iab_col,
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
        mycursor.execute("DELETE FROM domain_parser WHERE domain = '" + i + "'")

    mydb.commit()


    mycursor.executemany(sql_query, values_to_sql)

    mydb.commit()

    for i in to_get_from_db :
        mycursor.execute("SELECT * FROM domain_parser WHERE domain = '" + i + "'")
        db_row_as_list = list(mycursor)
        Domain_col.append(db_row_as_list[0][0])
        title_tag_col.append(db_row_as_list[0][1])
        meta_keywords_col.append(db_row_as_list[0][2])
        meta_description_col.append(db_row_as_list[0][3])
        deterministic_col.append(db_row_as_list[0][4])
        iab_col.append(db_row_as_list[0][5])
        # 14.08 Brand safety START
        adult_col.append(db_row_as_list[0][6])
        alcohol_col.append(db_row_as_list[0][7])
        aviation_col.append(db_row_as_list[0][8])
        crime_col.append(db_row_as_list[0][9])
        drug_col.append(db_row_as_list[0][10])
        hate_col.append(db_row_as_list[0][11])
        human_made_disasters_col.append(db_row_as_list[0][12])
        politics_brand_safe_col.append(db_row_as_list[0][13])
        natural_disasters_col.append(db_row_as_list[0][14])
        piracy_col.append(db_row_as_list[0][15])
        profanity_col.append(db_row_as_list[0][16])
        terrorism_col.append(db_row_as_list[0][17])
        tobacco_col.append(db_row_as_list[0][18])
        cannabis_col.append(db_row_as_list[0][19])
        car_accidents_col.append(db_row_as_list[0][20])
        brand_safe_col.append(db_row_as_list[0][21])
        # 14.08 Brand safety END
        coppa_col.append(db_row_as_list[0][22])
        greenrating_col.append(db_row_as_list[0][24])
        app_size_col.append(db_row_as_list[0][25])
        page_size_col.append(db_row_as_list[0][26])
        totaldatatransfer_col.append(db_row_as_list[0][27])
        emissionsnonrw_col.append(db_row_as_list[0][28])
        emissionsrw_col.append(db_row_as_list[0][29])
        totalcarbonemissions_col.append(db_row_as_list[0][30])
        brand_profile_id_col.append(db_row_as_list[0][31])

    #mycursor.close()

    for iterating_var in range(len(Domain_col)) :
        if is_it_news(Domain_col[iterating_var]) :
            if str(brand_safe_col[iterating_var]) == '0' : #if domain is news and NOT brand safe
                iab_col[iterating_var] = "(IAB-12) News"
            elif str(brand_safe_col[iterating_var]) == '1' :  #if domain is news and IS brand safe
                if does_title_have_sports_words(string_to_list_of_words(title_tag_col[iterating_var])) :
                    iab_col[iterating_var] = "(IAB-17) Sports, (IAB-12) News"
                if does_title_have_sports_words(string_to_list_of_words(title_tag_col[iterating_var])) == False :
                    if iab_col[iterating_var] != "(IAB-12) News" :
                        iab_col[iterating_var] = "(IAB-12) News, " + iab_col[iterating_var]

        elif is_it_news(Domain_col[iterating_var]) == False :
            if does_title_have_sports_words(string_to_list_of_words(title_tag_col[iterating_var])) :
                if str(iab_col[iterating_var]) != '(IAB-17) Sports' : #if it is not categorised as Sports already
                    iab_col[iterating_var] = "(IAB-17) Sports, " + iab_col[iterating_var]


    merged_data = {'Domain' : Domain_col,
            'Title_tag' : title_tag_col,
            'Meta data keywords' : meta_keywords_col,
            'Description' : meta_description_col,
            'Determenistic' : deterministic_col,
            'IAB' : iab_col,
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
            'appsize' : app_size_col,
            'pagesize' : page_size_col,
            'totaldatatransfer' : totaldatatransfer_col,
            'emissionsnonrw' : emissionsnonrw_col,
            'emissionsrw' : emissionsrw_col,
            'totalcarbonemissions' : totalcarbonemissions_col,
            'brandprofileid' : brand_profile_id_col}

    merged_df = pd.DataFrame(merged_data)

    processed_domain_file = file_name[:-15] + ".csv"

    merged_df.to_csv(processed_domain_file, index=None, header=True)

    sftp_url = "sftp.dc2.gpaas.net"
    username = "7872195"
    password = "WonderBatmanSuperman12!"
    local_file_path = "C:/Users/Administrator/Desktop/parsers/" + processed_domain_file  # Corrected the file path
    remote_directory = "/vhosts/certm8.com/htdocs/platform/profcf"

    upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory)
    os.remove(processed_domain_file)
    os.remove("main/domain/" + file_name)

    domain_done_query = "UPDATE file_status SET domain_done = 'Yes' WHERE file_name = '" + file_name[:-21] + ".csv" + "'"
    mycursor.execute(domain_done_query)
    mydb.commit()

    others_query = "SELECT ios_done, android_done from file_status where file_name = '" + file_name[:-21] + ".csv" + "'"

    mycursor.execute(others_query)

    for i in list(mycursor) :
        if i[0] == "Yes" and i[1] == "Yes" :
            final_domain_call = requests.get('https://certm8.com/platform/u2/?k=786123&id=' + processed_domain_file[:-10])
            delete_query = "DELETE FROM file_status WHERE file_name = '" + file_name[:-21] + ".csv" + "'"
            mycursor.execute(delete_query)
            mydb.commit()
    #print("Elapsed Time: %s" % (time.time() - start))

mycursor.close()
mydb.close()

print("It took this much time : ", datetime.now() - startTime)

print("List of bad domains :")
for bad_domain in could_not_process :
    print(bad_domain)

print("Count of bad domains :", len(could_not_process))