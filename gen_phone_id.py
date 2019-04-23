from phone import Phone
import pymysql
from tqdm import tqdm

P = Phone()

db = pymysql.connect("localhost", "root", "huajian", "new_db")
cursor = db.cursor()


def get_pc(ss):
    # ss : '13565656565'
    # get_pc : provice and city
    try:
        res = P.find(ss.strip())
        if res:
            prov = res.get('province')
            cit = res.get('city')
            return prov, cit
        return 0, 0
    except:
        return 0, 0


def get_id(who, level):
    try:
        # 用名字和level确定ID
        cursor.execute(
            f'select id from region where name like "%{who}%" and arealevel={level};')
        res_id = cursor.fetchone()[0]
        return res_id
    except:
        return 0


wf = open('result.txt', 'w', encoding='utf8')

with open('table_name.csv', 'r', encoding='utf8') as rf:
    for line in tqdm(rf):
        ll = line.split(',')
        name = ll[1]
        id_card = ll[2]
        sex = ll[3]
        birthday = ll[4]
        address = ll[5]
        mobile = ll[6]
        email = ll[7].strip()
        age = ll[8]
        province, city = get_pc(mobile)
        province_id = get_id(province, 1)
        city_id = get_id(city, 2)
        wf.write(
            f'{name},{id_card},{sex},{birthday},{address},{mobile},{email},{age},{province},{province_id},{city},{city_id}\n')

wf.close()

db.close()
