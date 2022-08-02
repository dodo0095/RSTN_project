import sys
import requests
from zipfile import ZipFile
import pandas as pd
import json
from pyspark.sql.functions import col
import findspark
findspark.init()
import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# 爬蟲function
def download_file(url, fileName):
    r = requests.get(url, stream=True)
    if r.status_code == 200: # HTTP 200 OK
        if r.headers['Content-Type'] == 'application/octet-stream': # zip檔串流
            size = r.headers['Content-Length'] # zip檔大小
            print('file size: {} bytes'.format(size))   #觀看檔案大小

            # 執行下載過程
            with open(fileName, 'wb') as f: # 在本地路徑開檔
                count = 0
                for chunk in r.iter_content(chunk_size=1024):  # 緩衝下載
                    if chunk: # 過濾掉保持活躍的新塊
                        f.write(chunk) # 寫入
                        # 計算下載進度
                        count += len(chunk)
                        #print('{}: {:3d}%'.format(fileName, int(count / int(size) * 100)))    #可以觀看下載進度
                    else:
                        print('no chunk')
            r.close() # 關閉 Response
            return fileName
        else:
            print('content type is not zip file.')
            r.close() # 關閉 Response
            return None
    else:
        print('request failed')
        r.close() # 關閉 Response
        return None
#中文 跟阿拉伯數字轉換    
number_map = {
"零": 0,
"一": 1,
"二": 2,
"三": 3,
"四": 4,
"五": 5,
"六": 6,
"七": 7,
"八": 8,
"九": 9
}

#爬蟲   輸入年份   (題目指定是108S2)
year=input("你要哪一個年份的資料 (民國)")
season=input("你要哪一季的資料  輸入數字 1~4")
url="http://plvr.land.moi.gov.tw/DownloadSeason?season="+year+"S"+season+"&type=zip&fileName=lvr_landcsv.zip"
download_file(url, year+"S"+season+".zip")

# 輸入想要看的數據資料    # 題目預設為   主要用途為 (住家用) 建物型態為  (住宅大樓)    樓層在13樓以上
how_to_use=input("你想查的房子 主要用途為?  (住家用,商業用,工業用,停車用....)")
house_type=input("你想查的房子 建物型態為?  (住宅大樓,套房,公寓....)")
house_level=input("你想查的房子 總樓層數希望大於等於幾層樓?  輸入阿拉伯數字 5,6,10....")


#開啟要跑的縣市    可寫轉換   題目指定為【臺北市/新北市/桃園市/臺中市/高雄市】的【不動產買賣】資料。
city_list=["臺北市","新北市","桃園市","臺中市","高雄市"]
city_list2=["a","f","h","b","e"]
all_city_json=[]





for city_var in range(len(city_list)):
    myzip=ZipFile(year+"S"+season+".zip")
    f=myzip.open(city_list2[city_var]+'_lvr_land_a.csv')
    df=pd.read_csv(f, encoding='utf-8')  
    #print(df)
    f.close()
    myzip.close()



    # 把中文的樓層  轉成數字
    level=df["總樓層數"].tolist()
    for i in range(len(level)):
        try:
            level[i]=level[i].replace("層","")
            if len(level[i])==1:
                level[i]=number_map[level[i]]
            elif len(level[i])==2:
                level[i]=10+number_map[level[i][1]]
            elif len(level[i])==3:
                level[i]=(number_map[level[i][0]])*10+number_map[level[i][2]]
        except:
            level[i]=0
    level[0]=0
    df["換算後的樓層數"]=level
    df2=df[['交易年月日',"鄉鎮市區",'主要用途','建物型態',"換算後的樓層數"] ].fillna(value="")
    
    df2['交易年月日']=df2[['交易年月日']].astype(str)
    
    #創出Spark得dataframe   
    sparkDF=spark.createDataFrame(df2) 
    #sparkDF.printSchema()
    #sparkDF.show()         #顯示他們德資訊

    df3=sparkDF.filter(sparkDF['換算後的樓層數'] > house_level)          #   篩選出題目需要的樓層
    df5=df3.filter(col("建物型態").contains(house_type) & col("主要用途").contains(how_to_use)   )  #   篩選出題目需要的用途

    
    #根據日期去整理 輸出所要的資訊
    date_list = df5.select('交易年月日').collect()

    #date_list=sorted(date_list)
    date_data2=list(set(date_list))    #刪除重複   
    date_data2=sorted(date_data2, reverse=True)

    pandas_df = df5.toPandas()
    final=[]
    time_slots=[]
    for i in range(len(date_data2)):
        mask = (pandas_df["交易年月日"]==str(date_data2[i][0]))  #篩選條件
        cols = ['交易年月日',"鄉鎮市區",'建物型態']  #提取名稱、價格、分類欄位
        df_temp=pandas_df.loc[mask,cols]
        events=[]
        date=date_data2[i][0]
        for j in range(len(df_temp)):

            district=df_temp["鄉鎮市區"].tolist()[j]
            building_state=df_temp["建物型態"].tolist()[j]

            events_dict={"district":district,"building_state":building_state}
            events.append(events_dict)
        dict_temp={"date":date,"events":events}
        time_slots.append(dict_temp)

    final={"city":city_list[city_var],"time_slots":time_slots}
    all_city_json.append(final)



#print(all_city_json)
# 輸出 json
with open('result-part1.json', 'w', encoding='utf-8') as f:
    json.dump(all_city_json[0], f,ensure_ascii=False)
    json.dump(all_city_json[1], f,ensure_ascii=False)
    
with open('result-part2.json', 'w', encoding='utf-8') as f:
    json.dump(all_city_json[2], f,ensure_ascii=False)
    json.dump(all_city_json[3], f,ensure_ascii=False)
    json.dump(all_city_json[4], f,ensure_ascii=False)
    
print("輸出成功")