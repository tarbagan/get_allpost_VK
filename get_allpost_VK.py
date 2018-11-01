from urllib.request import urlopen
import json
import re
import csv
import pandas as pd
import requests
import urllib3
urllib3.disable_warnings()
import time

token = "YOUR TOKEN"
group_id = "ID GROUP OR ID USER"

def count():
"""Get the data counter for pagination"""
    try:
        url = "https://api.vk.com/method/wall.get.json?owner_id=%s&count=1&access_token=%s&v=5.87" % (group_id,token)
        response = urlopen(url)
        data = response.read()
        wall = json.loads(data)
        data_count = (wall["response"]["count"])
        return data_count
    except:
        print ("Ошибка в получении или обработки данных функции -count-") 

def get_data():
"""Parsing function"""
#--------------- Парсим данные ----------------------------    
    #try:
    url = "https://api.vk.com/method/execute?&v=5.87"
    post_all = []
    cnt = 100
    allpage = count() # allpage = count()
#--------------- Цикл получения данных execute ---------------------------- 
    for i in range(0,allpage,cnt): 
        code = """
        return API.wall.get({"owner_id": %s, "offset": %s, "count": %s});
        """ % (group_id,i,cnt)
        params = dict(code=code, access_token = token)
        execute = requests.post(url=url, data=params, verify=False, timeout=5)
        wall = execute.json()    
        print ("Грабим поток. Постов спарсено %s/%s." % (i, str(allpage)))
        print ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxххххxx")   
        
#--------------- Разбираем данные ----------------------------            
        if wall["response"]["items"] is not None:
            for i in wall["response"]["items"]:
                if 'id' in i: id_post = i['id']
                if 'from_id' in i: from_id = i['from_id'] 
                if 'owner_id' in i: owner_id = i['owner_id'] 
                if 'date' in i: 
                    date = str(pd.Timestamp(i["date"], unit="s"))
                    date = re.sub(r'\([^)]*\)', '', date)
                if 'post_type' in i: post_type = i['post_type'] 
                if 'text' in i: 
                    text = i['text'] 
                    text = (re.sub(r'\s', ' ', text))
                    text = (re.sub(r';', ',', text))
                #if 'attachments' in i: attachments = i['attachments'] 
                if 'post_source' in i: post_source = i['post_source']['type'] 
                if 'comments' in i: comments = i['comments']['count'] 
                if 'likes' in i: likes = i['likes']['count'] 
                if 'reposts' in i: reposts = i['reposts']['count'] 
                if 'views' in i: 
                    views = i['views']['count']
                else:
                    views = "0"
                post = {"id_post":id_post, "from_id":from_id, "owner_id":owner_id, "date":date, 
                       "post_type":post_type, "text":text, "post_source":post_source, "comments":comments, 
                       "likes":likes, "reposts":reposts,"views":views}
                post_all.append(post)
        else:
            break
    print ("******************")
    print ("Успешно!")
    print ("******************")
    return post_all
    
def save_cvs():
"""Save to CSV"""
    datagroup = get_data()
    with open( "d:/AnacodaProgect/Irgit_Post/date.csv", "w", encoding='utf-8' ) as file:
        fieldnames = ["id_post", "from_id", "owner_id","date", "post_type","text","post_source","comments", "likes","reposts","views"]
        writer = csv.DictWriter( file, fieldnames=fieldnames )
        writer.writeheader()
        for i in datagroup:
            
            id_post = i["id_post"]
            from_id = i["from_id"]
            owner_id = i["owner_id"]
            date = i["date"]
            post_type = i["post_type"]
            text = i["text"]
            post_source = i["post_source"]
            comments= i["comments"]
            likes = i["likes"]
            reposts= i["reposts"]
            views = i["views"]  
            
            writer.writerow({"id_post":id_post, "from_id":from_id, "owner_id":owner_id, "date":date, 
                            "post_type":post_type, "text":text, "post_source":post_source, "comments":comments, 
                            "likes":likes, "reposts":reposts,"views":views})
save_cvs()                            
