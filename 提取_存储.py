# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 14:34:14 2020

@author: djx
"""

#!/usr/bin/env python
# coding: utf-8



#导入mysql库存储数据
import pymysql 
#引入re，将作者名称中的单引号转义 pymysql.escape_string()有同样效果
#from re import escape
#连接pneumonia数据库
db=pymysql.connect(host='localhost',user='djx_pneumonia', password='123456',db = 'pneumonia')
# cursor.close() 用完指针对象是否要关闭 要
import pdb


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
        log_name = '{}.log'.format(time.strftime('%Y-%m-%d-%H-%M'))
        filename = os.path.join(output_dir, log_name)

        self.terminal = stream
        self.log = open(filename, 'a+',encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass



# 编号：001
# 功能：查询指定作者的个人信息
# 参数：作者的全名
# 返回值：查找成功则返回作者全部个人信息,失败返回None，输出报错信息
def select_writer(FAU):
    sql = "select * from writer where FAU = '%s'" % pymysql.escape_string(FAU)
    try:
        db.ping(reconnect=True) # 在每次执行sql语句前先执行 conn.ping(reconnect=True)，可以保证conn丢失时自动重连,避免出现（0，''）报错
#         执行SQL语句
        cursor=db.cursor()
        cursor.execute(sql)
        # 获取所有记录列表
        row = cursor.fetchone()
        writer = row
        return writer
    
    except Exception as e:
        print ("Error: unable to fetch writer's data " + str(e))
        print(sql)
        cursor.close()
        return None
    

# 编号：002
# 功能：插入一个作者的个人信息
# 参数：作者全名、简名、机构、国籍
# 返回值：插入成功则返回作者AUID,失败无返回值，输出报错信息
def insert_writer(AUID,FAU,AU,AD,Country):
    
    writer = select_writer(FAU) # 检查表中是否已经有该作者，若已有则返回其整行记录
#     print(dir(writer))
    if(writer is not None):
        return writer[0]
    AD = AD.replace("\n","") # 需要放到转义语句之前，因为换行符被转义了？对就是这样
    AD = pymysql.escape_string(AD) # 避免数据库注入时的单引号 or 双引号未转义导致失败
    AU = pymysql.escape_string(AU)
    FAU = pymysql.escape_string(FAU)
    Country = pymysql.escape_string(Country)
    sql = "insert into writer(AUID,FAU,AU,AD,Country) values('%s','%s','%s','%s','%s')" % (AUID,FAU,AU,AD,Country)
    try:
        db.ping(reconnect=True)
        cursor=db.cursor()
        cursor.execute(sql)
        db.commit()
        return AUID
    except Exception as e:
        db.rollback()
        cursor.close()
        print("Error: unable to insert writer's data " + FAU + " " + AU + " " + AD + " " + Country + str(e))




# 编号：003
# 功能：向article表插入文献信息
# 参数：PMID,DEP,LA,TA
# 返回值：PMID,失败则弹出提示信息
def insert_article(PMID,DEP,LA,TA,topic):
    
    DEP = pymysql.escape_string(DEP)
    LA = pymysql.escape_string(LA)
    TA = pymysql.escape_string(TA)
    
    sql_1 = "insert into article(PMID,DEP,LA,TA)values('%s','%s','%s','%s')" % (PMID,DEP,LA,TA)
    sql_2 = "insert into topic(PMID,topic)values('%s','%s')"%(PMID,topic)
    try:
        db.ping(reconnect=True)
        cursor = db.cursor()
        cursor.execute(sql_1)
        cursor.execute(sql_2)
    except Exception as e:
        db.rollback()
        cursor.close()
        print("Error: unable to insert article's infomation,可能已经插入了" + str(PMID) +"because" + str(e))
    else:
        db.commit()
        return PMID



# 编号：004
# 功能：查询作者与文献编号的关系是否已经录入
# 参数：AUID,PMID
# 返回值：如果已经插入就返回RecordID，未插入就返回空
def select_Relationship(AUID,PMID):
    sql = "select * from relationship where AUID = '%s' and PMID = '%s'"%(AUID,PMID)
    try:
        db.ping(reconnect = True)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        result = cursor.fetchone()
        return result
    except Exception as e:
        db.rollback()
        cursor.close()
        print("Error: 查询relationship失败" + "AUID:" + AUID + "PMID:" + PMID + str(e))


# 编号：005
# 功能：将作者AUID与文献编号PMID结合，存入RelationShip 表
# 参数：AUID,PMID
# 返回值：如果已经插入过，返回该记录信息。
# 若未插入，插入成功返回RecordID,如果插入失败会提示报错信息
def insert_article_and_author(AUID,PMID):
    sql = "insert into  relationship(AUID,PMID)values('%s','%s')" % (AUID,PMID)
    try:
        result = select_Relationship(AUID,PMID)
        if result:
            print('已存在该记录'+ str(result))
            return result
        db.ping(reconnect=True)
        cursor=db.cursor()
        cursor.execute(sql)
        db.commit()
        cursor.close()
        return True
    except Exception as e:
        db.rollback()
        cursor.close()
        print("Error: unable to insert relationship" + "AUID:" + str(AUID) + "PMID:" + str(PMID) + str(e),end='-------------\n\n')


def main():
    sys.stdout = Logger(sys.stdout)  #  将输出记录到log
    project_path = r"D:\personfile\study\大三\论文软著\王菲菲老师\疫情中各国学者在科技攻关和科技合作方面的贡献情况\project"
    result_folder_path = project_path + r"\result\7.11"
#    result_filepath = result_folder_path + r"\Forecasting.txt"
    for filename in os.listdir(result_folder_path):
        result_filepath = result_folder_path + "\\" + filename
        print(result_filepath)
        AUID = 1000
        with open(result_filepath,'r',encoding = 'utf-8') as result_file:
            topic = filename[:-4]
            DEP = ''
            LA = ''
            TA = ''
            ADline = []
            PMID = ''
            for i in result_file.readlines():
                # 观察发现前面不管标识字段有几位，破折号前算上空格总是四位
                if(i[:4] == 'PMID'): # PMID字段提取
        #             try:
                    flag = 'PMID'
                    insert_article(PMID,DEP,LA,TA,topic)
                    PMID = i[6:] 
                    continue
        #             except Exception as e:
        #                 PMID = i[6:] 
        #                 print("stop_point01" + str(e))
        #                 continue
                if(i[:4] == 'DEP '): #DEP发表时间字段提取
                    DEP = i[6:].strip()
                    continue
                if(i[:4] == 'LA  '): #LA语言字段提取
                    flag = 'LA'
                    LA = i[6:].strip()
                    continue
                if(i[:4] == 'TA  '): #TA 期刊名字字段提取
                    TA = i[6:].strip()
                    continue
                if(i[:4] == 'FAU '): # FAU作者全名字段提取
                    flag = 'FAU'
                    if('FAU' in locals().keys()):
                        # 如果这个键还没有，也就是第一个作者还没有开始录入，就不插入
                        # 如果这个键已经有了，就说明之前已经有FAU,AU,AD信息了，就录入并更新下一个
                        if('AD' not in locals().keys()):
                            AD = ''
                        temp = AUID
                        AUID = insert_writer(AUID,FAU,AU,AD,'Country')
                        ADline = [] # 每插入一个作者，上一个作者的全部属性都应该清空，为保持ADline为一个空列表，需要重新赋值
                        insert_article_and_author(AUID,PMID)
                        if AUID == temp:
                            AUID = temp + 1
                        else:
                            AUID = temp
                    FAU = i[6:].strip("\n '" ) # 对存入数据库的模块要去除换行符、空白格、转义单引号
                    continue
                if(i[:4] == 'AU  '): #AU作者缩写名字字段提取
                    AU = i[6:].strip("\n '")
                    continue
                if(i[:4] == 'AD  'or (i[:1]==' 'and flag == 'AD')): # AD作者所属机构字段提取
                    flag = 'AD' 
                    ADline.append(i[6:])
                    AD = "".join(ADline)
                    continue
            
main()


# #### 关于字符串注入数据库转义特殊字符的实例

# print("University of Toronto, St. Michael\\\'s Hospital, and BlueDot, Toronto, Ontario, Canada (K.K.).")
# 
# output:University of Toronto, St. Michael\'s Hospital, and BlueDot, Toronto, Ontario, Canada (K.K.).



