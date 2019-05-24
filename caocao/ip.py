# encoding : utf-8
# @Time    : 5/20/19 5:35 PM
# @Author  : magic
# @Email   : 
# @File    : ip.py
# @Software: PyCharm
# huoqv IP dizhiduiyingdexinxi
# {'code': 0, 'data': {'ip': '202.102.193.68', 'country': '中国', 'area': '', 'region': '安徽', 'city': '合肥', 'county': 'XX', 'isp': '电信', 'country_id': 'CN', 'area_id': '', 'region_id': '340000', 'city_id': '340100', 'county_id': 'xx', 'isp_id': '100017'}}
import requests
import pymysql

db = pymysql.connect("hadoop001", "root", "Aa123456!", "caocao")
cursor = db.cursor()


def checkip(ip_data):
    URL = 'http://ip.taobao.com/service/getIpInfo.php'
    # URL = 'http://freeipapi.17mon.cn/'
    ip = {'ip': ip_data}
    try:
        r = requests.get(URL, params=ip, timeout=3)
    except requests.RequestException as e:
        print(e)
    else:
        json_data = r.json()
        if json_data['code'] == 0:
            COUNTRY = json_data['data']['country']
            AREA = json_data[u'data'][u'area']
            REGION = json_data[u'data'][u'region']
            CITY = json_data[u'data'][u'city']
            COUNTY = json_data[u'data'][u'county']
            ISP = json_data[u'data'][u'isp']
            COUNTRY_ID = json_data['data']['country_id']
            AREA_ID = json_data[u'data'][u'area_id']
            REGION_ID = json_data[u'data'][u'region_id']
            CITY_ID = json_data[u'data'][u'city_id']
            COUNTY_ID = json_data[u'data'][u'county_id']
            ISP_ID = json_data[u'data'][u'isp_id']
            # SQL 插入语句
            sql = "INSERT INTO M_IP_DATA(IP,COUNTRY,AREA,REGION,CITY,COUNTY,ISP, \
                   COUNTRY_ID,AREA_ID,REGION_ID,CITY_ID,COUNTY_ID,ISP_ID) \
                   VALUES ('%s', '%s',  '%s',  '%s','%s', '%s',  '%s',  '%s',  \
                   '%s', '%s',  '%s',  '%s','%s') " % \
                  (
                      ip_data, COUNTRY, AREA, REGION, CITY, COUNTY, ISP, COUNTRY_ID, AREA_ID, REGION_ID, CITY_ID,
                      COUNTY_ID,
                      ISP_ID)
            print(sql)
            try:
                cursor.execute(sql)

            except:
                db.rollback()
        else:
            print('查询失败,请稍后再试！')


print('1')
try:
    # SQL 查询ip语句
    sql = "SELECT DISTINCT ip FROM DP_IP_DATA d \
WHERE d.ip NOT IN(SELECT IP FROM M_IP_DATA) \
AND d.dt_ip >'2019-04-01' "

    cursor.execute(sql)
    # 获取所有记录列表
    results = cursor.fetchall()
    for row in results:
        ip = row[0]
        checkip(ip)
except:
    print("Error: unable to fetch data")

db.commit()
db.close()
