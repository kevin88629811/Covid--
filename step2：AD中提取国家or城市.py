#!/usr/bin/env python
# coding: utf-8


#导入mysql库存储数据
import pymysql 
# 导入csv库提取数据
import csv
# 正则表达式模块
import re
#连接pneumonia数据库
db=pymysql.connect(host='localhost',user='djx_pneumonia', password='123456',db = 'pneumonia')


# 函数001
# 功能：提取数据库中作者的AD信息
# 返回值：AD，字符串类型
def extract_writer():
    writer = []
    sql = "select * from writer"
    try:
        db.ping(reconnect=True) # 在每次执行sql语句前先执行 conn.ping(reconnect=True)，可以保证conn丢失时自动重连,避免出现（0，''）报错
        cursor=db.cursor()
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            writer.append(list(row))
        cursor.close()
        return writer
    except:
        print ("Error: unable to fetch AD --函数001失败")
        cursor.close()
        return writer


# 函数002
# 功能：all in xx 型提取
# 参数：AD字符串
# 返回值：国家名称/州名称等,如果没有提取到就返回FALSE
def extract_all_in_xx(AD):
    try:
        pattern = re.compile('all in \w+')
        location = re.search(pattern,AD).group()
        location = location.split(' ')[-1]
        return location
    except:
        return False

# 函数003
# 功能：,US. 型提取
# 参数：AD字符串
# 返回值：国家名称/州名称等
def extract_us(AD):
    pattern = re.compile(', \w+\.')
    location = re.search(pattern,AD).group()
    location = location[2:-1]
    return location



# 函数004
# 功能：对非格式化的AD根据国家字典进行提取
# 参数：AD,国家字典
# 返回值： 统一名称后的国家名称
# 备注：在使用国家字典前对AD进行国家提取约有80%的成功数据，有10000条数据并非上述两种格式，因此进行自定义提取
def extract_others(AD,Reference_of_Country):
    for Country in Reference_of_Country:
        pattern = re.compile(Country)
        try:
            location = re.search(pattern,AD).group()
        except:
            location = False
        if location:
            return Reference_of_Country[Country]
    return False
# extract_others(writer[104][3],Reference_of_Country)



# 函数005
# 功能： 集合若干种提取国家的方式,用国家字典2号
# 参数：AD
# 返回值：Country（成功），False（失败）
def extract_country(AD):
    try:
        if AD == "Istituto di Ricerche Farmacologiche Mario Negri IRCCS.":
            Country = "Italy"
            return Country
        Country = extract_others(AD,Reference_of_Country)
        if Country != False:
            return Country
        Country = extract_all_in_xx(AD)
        if Country != False:
            return Country
        Country = extract_us(AD)
        if Country != False:
            return Country
    except:
        return ""




"""
# 新增国家字典1号--已经废弃
import pandas as pd
df = pd.read_csv(project_path+"\Country_city.csv")
Country_Reference1 = list(set(df["pays"]))
for i in range(len(Country_Reference1)):
    Country_Reference1[i] = Country_Reference1[i].capitalize()
#     print(i.capitalize())
Country_Reference1 = dict.fromkeys(Country_Reference1)
for i in Country_Reference1.keys():
    Country_Reference1[i] = i
"""

"""
# 函数005
# 功能： 集合若干种提取国家的方式,用国家字典1号---已经废弃
# 参数：AD
# 返回值：Country（成功），False（失败）
def extract_country1(AD):
    try:
        if AD == "Istituto di Ricerche Farmacologiche Mario Negri IRCCS.":
            Country = "Italy"
            return Country
        Country = extract_others(AD,Country_Reference1)
        if Country != False:
            return Country
        Country = extract_all_in_xx(AD)
        if Country != False:
            return Country
        Country = extract_us(AD)
        if Country != False:
            return Country
    except:
        return ""

"""

# 函数006
# 功能：修改数据库中作者国家信息
# 参数：AD
# 返回值：成功无返回值，失败返回作者ID
def update_data(ID,Country):
    sql = "update writer set Country = '%s' where AUID = '%s'" % (Country,ID)
    try:
        db.ping(reconnect = True)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
    except:
        cursor.close()
        print(ID)


"""
# 新增国家字典2号-城市国家对应

csvFile = open(project_path+r"\Country_city.csv", "r",encoding = 'utf-8-sig')
dict_reader = csv.reader(csvFile)

result = {}
for item in dict_reader:
    result[item[2]] = item[0].capitalize()
    print(item[2]+":"+item[0].capitalize())
print(result)
"""


# 新建（国家别名、城市名）-国家字典2号
Reference_of_Country = {"United Kingdom":'UK',
                        "Hong ?Kong":'China',"china":'China',"China":'China',"USA":'USA',"US":'USA',
                        "France":'France',"Portugal":'Portugal',"United States":'USA',
                        "New York":'USA',"Korea":'Korea',"Netherlands":'Netherlands',
                        "Dutch":'Netherlands',"UK":'UK',"Canada":'Canada',"Washington":'USA',
                        "Japan":'Japan',"Taiwan":'China',"Italy":'Italy',"Greece":'Greece',
                        "Saudi Arabia":"Saudi Arabia","South Africa":'South Africa',
                        "Boston":'USA',"San Francisco":'USA',"Zhengzhou":'China',"Turkey":'Turkey'
                        ,"New Jersey":'USA',"United Arab Emirates":"United Arab Emirates",
                        "New Zealand":'New Zealand', "Kentucky":'US',"Toronto":'Canada',
                        "North Carolina":'USA',"Iceland":'Iceland',"India":'India',"Singapore":'Singapore',
                        "American":'USA',"San Diego":'USA',"Cambridge":'UK',"Gregorio Marañón":'Madrid',
                        "Egypt":'Egypt',"Spain":'Spain',"Milan":'Italy',"Brazil":'Brazil',
                        "Trinidad and Tobago":'Trinidad and Tobago',"Cleveland":'USA',"New Haven":'USA',
                        "Durham":'USA',"NJ":'USA',"Atlanta, GA":'USA',"University of Michigan":'USA',
                        "Jiangsu":'China',"Florida":'USA',"Oregon":'USA',"California":'USA',"NSW":'Australia',
                        "NY":'USA',"Philippines":'Philippines',"Philadelphia":'USA',
                        "Texas":'USA',"Georgia":'Georgia',"Mankato":'USA',"San Antonio":'USA',
                        ", CA.":'USA',"Ohio":'USA',"Rochester":'USA',"Chicago":'USA',
                        "St. Gallen":'Switzerland',"Melbourne":'Australia',"Nashville":'USA',
                        "Detroit":'USA',"Huoshenshan":'China',"Mount Sinai":'USA',"Xiangya":'China',
                        "Hopkins":'USA',"Landspítali University Hospital":'Iceland',"Switzerland":'Switzerland',
                        "Trento":'Canada',"Scotland":'UK',"Wales":'UK',"St":'USA',"afd":'USA',"Brasil":'Brazil',
                        "England":'UK'
                       }
import time
import os
import sys

class Logger(object):

    def __init__(self, stream=sys.stdout):
        project_path = r"D:\personfile\study\大三\论文软著\王菲菲老师\疫情中各国学者在科技攻关和科技合作方面的贡献情况\project"
        demo_path = project_path + "\\notebook的导出代码"
        output_dir = demo_path + "\\log"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        log_name = '{}+step2.log'.format(time.strftime('%Y-%m-%d-%H-%M'))
        filename = os.path.join(output_dir, log_name) 

        self.terminal = stream
        self.log = open(filename, 'a+',encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass

# 增加国家字典后的提取准确率计算---90.46%-90.89-90.92
# 7.11日更新数据，只有85.3%的提取率了
def main():
#    project_path = r"D:\personfile\study\大三\论文软著\王菲菲老师\疫情中各国学者在科技攻关和科技合作方面的贡献情况\project"
    sys.stdout = Logger(sys.stdout)  #  将输出记录到log
    j = 0
    k = 0
    writer = extract_writer()
    for i in range(len(writer)):
        Country = extract_country(writer[i][3])
        print(Country)
        if Country == "":
            j = j + 1
            if writer[i][3] != "":
                k = k + 1
                print(str(i) + " "+ writer[i][3])
        else:
            update_data(writer[i][0],Country) # 写入数据库的步骤
            print(writer[i][0])
    print("有" + str(j) + "数据不能提取,其中有"+str(k)+"条可以改进")
    print("提取率为："+str((56236-j)*100/56236) + "%")


main()


