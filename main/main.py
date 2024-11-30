from re import sub  #regex==2022.6.2
from turtle import title
from urllib.request import urlopen, Request     #urllib for opening URLs    #urllib==1.26.9
from bs4 import BeautifulSoup   #BeautifulSoup for getting text from web pages(HTML+CSS files)  #beautifulsoup4==4.11.1
import requests     #requests for keep-alive, better and faster server connection   #requests==2.28.0 ; requests-aws4auth==1.1.2 ; requests-oauthlib==1.3.1 ; requests-toolbelt==0.10.1
import nltk     #nltk for extracting nouns from text    #nltk==3.7
import json       #json module for making json out of string    #built-in
#nltk.download('averaged_perceptron_tagger')
#nltk.download('punkt')
from datetime import date, timedelta    #built-in
import datetime #built-in
import pandas as pd #pandas==1.4.3
import numpy as np  #numpy==1.23.1
import os   #built-in
import threading    #built-in
import time #built-in
import urllib.request   #urllib3==1.26.9
import mysql.connector  #mysql-connector-python==8.0.30
import httplib2     #httplib2==0.20.4; google-auth-httplib2==0.1.0
import re   #regex==2022.6.2
import paramiko #paramiko==3.2.0
import socket   #built-in
from ipwhois import IPWhois #ipwhois==1.2.0

lock = threading.Lock()

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

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Iamgroot72",
  database="parsers"
)

mycursor = mydb.cursor()

website_specific_patterns = tuple((".com", ".uk", ".org", ".ru", ".tv", ".to", ".net", ".cn", ".de", ".nl", ".br", ".xyz", ".blog",
                                  ".me", ".jp", ".mobi", ".es", ".it", ".fr", ".tr", ".com.br", ".com.uk", ".com.ru", ".com.cn",
                                  ".com.de", ".com.nl", ".com.jp", ".com.es", ".com.it", ".com.fr", ".cat", ".live.com", ".gg",
                                  ".com.tr", ".co.br", ".co.uk", ".co.ru", ".co.cn", ".co.tr",
                                  ".co.de", ".co.nl", ".co.jp", ".co.es", ".co.it", ".co.fr", ".com.au", ".co.au"))


def call_server(url_to_call) :
    lock.acquire()
    try :
        url = Request(url_to_call, headers={'User-Agent' : 'Mozilla/5.0'})
        html = urlopen(url, timeout=10).read()
        to_check[url_to_call] = 1
        lock.release()
    
    except :
    #if https is used
        try :
            url = Request("https://" + url_to_call, headers={'User-Agent' : 'Mozilla/5.0'})
            html = urlopen(url, timeout=10).read()
            to_check[url_to_call] = 1
            lock.release()
            
        
        #if http is used
        except :
            try : 
                url = Request("http://" + url_to_call, headers={'User-Agent' : 'Mozilla/5.0'})
                html = urlopen(url, timeout=10).read()
                to_check[url_to_call] = 1
                lock.release()
            
            except :
                to_check[url_to_call] = 0
                lock.release()


response = requests.get('https://certm8.com/platform/agency1/n/?k=786123')
#print(response.content)

response_string = str(response.content)

if response_string.find("https://certm8.com/") == -1 :
    print("No files are available for download")

elif response_string.find("https://certm8.com/") != -1 :
    substr_start = response_string.find("https://certm8.com/")
    substr_end = response_string.rfind(".csv") + 4

    substr = response_string[substr_start:substr_end]

    file_name_start = 0
    file_name_end = substr.find(".csv") + 4

    file_names = []

    while file_name_end < len(substr) :
        file_names.append(substr[file_name_start:file_name_end])
        file_name_start = substr.find("https://certm8.com/", file_name_end)
        file_name_end = substr.find(".csv", file_name_start) + 4
    else :
        file_names.append(substr[file_name_start:file_name_end])
        
    real_files = []
    for i in file_names :
        #if "serik" in i :
        real_files.append(i)

    for i in real_files :
        response = requests.get(i)
        #print("I downloaded :", i)
        with open(i[38:], "wb") as f :
            f.write(response.content)
            print(i[38:] + " successfully downloaded!")
        #open(i[38:], "wb").write(response.content)

        response = requests.get('https://certm8.com/platform/u/?k=786123&id=' + i[38:-4])
        print("I called : ", i[38:-4])

    for i in real_files :

        try :
            input_df = pd.read_csv(i[38:], header=None)
            print("Input file read successfully!")
        except :
            print("Empty files are going to be created!!!")
            ios_list = []
            android_list = []
            domain_list = []

            ios_dict = {"ios" : ios_list}
            android_dict = {"android" : android_list}
            domain_dict = {"domain" : domain_list}

            ios_df = pd.DataFrame(ios_dict)
            ios_df.to_csv('ios/' + i[38:-4] + '-ios_tobeparsed.csv', index=None, header=False)

            android_df = pd.DataFrame(android_dict)
            android_df.to_csv('android/' + i[38:-4] + '-android_tobeparsed.csv', index=None, header=False)

            domain_df = pd.DataFrame(domain_dict)
            domain_df.to_csv('domain/' + i[38:-4] + '-sites_tobeparsed.csv', index=None, header=False)
    
            os.remove(i[38:])
            files_for_processing_query = "INSERT INTO file_status(file_name, android_started, ios_started, domain_started, android_done, ios_done, domain_done) VALUES('" + i[38:] + "', 'No', 'No', 'No', 'No', 'No', 'No')"
            mycursor.execute(files_for_processing_query)
            mydb.commit()
            continue

        input_df = input_df.values.tolist()

        for index_row in range(len(input_df)) :
            if "?" in str(input_df[index_row][0]) :
                with_question_mark = input_df[index_row][0]
                question_mark_index = with_question_mark.index("?")
                input_df[index_row][0] = with_question_mark[:question_mark_index]

        input_df = pd.DataFrame(input_df)


        input_df = input_df.drop_duplicates()
        input_list = input_df.values.tolist()
        if "reloaded" in i :
            print("deleting from DB")
            for reloaded_id in input_list :
                mycursor.execute(f"DELETE FROM ios_parser WHERE app_id = '{reloaded_id[0]}';")
                mycursor.execute(f"DELETE FROM android_parser WHERE app_id = '{reloaded_id[0]}';")
                mycursor.execute(f"DELETE FROM domain_parser WHERE domain = '{reloaded_id[0]}';")
                mydb.commit()
            print("deleting completed")


        #### CTV ADDITION
        if i[52:56] == 'ctv-':
            print("reading ctv ids!")
            ctv_list = []
            for k in input_list :
                ctv_list.append(str(k[0]))

            #os.remove(i[38:]) #delete input file

            ctv_name_col = []
            ctv_app_id_col = []
            ctv_developer_col = []
            ctv_description_col = []
            ctv_keywords_col = []
            ctv_url_col = []
            ctv_iab_col = []
            ctv_iab_code_col = []
            ctv_coppa_col = []
            ctv_app_type_col = []
            ctv_rating_col = []

            for ctv_id in ctv_list :
                search_in_db_query = "SELECT * FROM ctv WHERE App_id = " + ctv_id

                mycursor.execute(search_in_db_query)

                db_ctv_row = list(mycursor)

                if len(db_ctv_row) == 0 :
                    continue
                    """
                    ctv_name_col.append("0")
                    ctv_app_id_col.append("0")
                    ctv_developer_col.append("0")
                    ctv_description_col.append("0")
                    ctv_keywords_col.append("0")
                    ctv_url_col.append("0")
                    ctv_iab_col.append("0")
                    ctv_iab_code_col.append("0")
                    ctv_coppa_col.append("0")
                    ctv_app_type_col.append("0")
                    ctv_rating_col.append("0")
                    """

                elif len(db_ctv_row) != 0 :
                    row_from_db = db_ctv_row[0]

                    ctv_name_col.append(row_from_db[0])
                    ctv_app_id_col.append(row_from_db[1])
                    ctv_developer_col.append(row_from_db[2])
                    ctv_description_col.append(row_from_db[3])
                    ctv_keywords_col.append(row_from_db[4])
                    ctv_url_col.append(row_from_db[5])
                    ctv_iab_col.append(row_from_db[6])
                    ctv_iab_code_col.append(row_from_db[7])
                    ctv_coppa_col.append(row_from_db[8])
                    ctv_app_type_col.append(row_from_db[9])
                    ctv_rating_col.append(row_from_db[10])


            merged_ctv_data = {"Name" : ctv_name_col,
                               "App_id" : ctv_app_id_col,
                               "Developer" : ctv_developer_col,
                               "Description" : ctv_description_col,
                               "Keywords" : ctv_keywords_col,
                               "URL" : ctv_url_col,
                               "IAB" : ctv_iab_col,
                               "IAB_code" : ctv_iab_code_col,
                               "Coppa" : ctv_coppa_col,
                               "App_type" : ctv_app_type_col,
                               "Rating" : ctv_rating_col}


            ctv_df = pd.DataFrame(merged_ctv_data)

            processed_ctv_file = i[38:-4] + '-ctv.csv'

            ctv_df.to_csv(processed_ctv_file, index=None, header=True)

            sftp_url = "sftp.dc2.gpaas.net"
            username = "7872195"
            password = "WonderBatmanSuperman12!"
            local_file_path = "C:/Users/Administrator/Desktop/parsers/main/" + processed_ctv_file  # Corrected the file path
            remote_directory = "/vhosts/certm8.com/htdocs/platform/profcf"

            upload_to_sftp(sftp_url, username, password, local_file_path, remote_directory)

            final_ctv_call = requests.get('https://certm8.com/platform/u2/?k=786123&id=' + i[38:-4])
            os.remove(i[38:])

            #done call is everything before the .csv

        ### CTV ADD END ###

        elif i[52:56] != 'ctv-' :

            print(input_list[0][0])

            ios_list = []
            android_list = []
            domain_list = []
            to_check = {}
            to_check_list = []
            for k in input_list :
                if str(k[0]).isnumeric() == True :
                    ios_list.append(str(k[0]))
                elif str(k[0]).isnumeric() == False :
                    if str(k[0]).startswith("com.") or str(k[0]).endswith(".free.games") :
                        android_list.append(str(k[0]))
                    elif str(k[0]).endswith(website_specific_patterns) or '/' in str(k[0]) :
                        domain_list.append(str(k[0]))
                    else :
                        to_check[str(k[0])] = 0
                        to_check_list.append(str(k[0]))
            
            threads = [threading.Thread(target=call_server, args=(url,)) for url in to_check_list]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
                        
            for url_in_check in to_check :
                if to_check[url_in_check] == 1 :
                    domain_list.append(url_in_check)
                elif to_check[url_in_check] == 0 :
                    android_list.append(url_in_check)

            ios_dict = {"ios" : ios_list}
            android_dict = {"android" : android_list}
            domain_dict = {"domain" : domain_list}

            ios_df = pd.DataFrame(ios_dict)
            ios_df.to_csv('ios/' + i[38:-4] + '-ios_tobeparsed.csv', index=None, header=False)

            android_df = pd.DataFrame(android_dict)
            android_df.to_csv('android/' + i[38:-4] + '-android_tobeparsed.csv', index=None, header=False)

            domain_df = pd.DataFrame(domain_dict)
            domain_df.to_csv('domain/' + i[38:-4] + '-sites_tobeparsed.csv', index=None, header=False)

            os.remove(i[38:])
            files_for_processing_query = "INSERT INTO file_status(file_name, android_started, ios_started, domain_started, android_done, ios_done, domain_done) VALUES('" + i[38:] + "', 'No', 'No', 'No', 'No', 'No', 'No')"
            mycursor.execute(files_for_processing_query)
            mydb.commit()