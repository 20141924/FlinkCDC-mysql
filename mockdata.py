from faker import Faker
import random
import pymysql
province_id = [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43, 44, 45, 46,
               50, 51, 52, 53, 54, 61, 62, 63, 65, 65, 81, 82, 83]
phone_number = [139, 138, 137, 136, 135, 134, 159, 158, 15, 150, 151, 152, 188,
                130, 131, 132, 156, 155, 133, 153, 189]

# 随机生成出生日期
def get_birthday():
    # 随机生成年月日
    year = random.randint(1960, 2000)
    month = random.randint(1, 12)
    # 判断每个月有多少天随机生成日
    if year % 4 == 0:
        if month in (1, 3, 5, 7, 8, 10, 12):
            day = random.randint(1, 31)
        elif month in (4, 6, 9, 11):
            day = random.randint(1, 30)
        else:
            day = random.randint(1, 29)
    else:
        if month in (1, 3, 5, 7, 8, 10, 12):
            day = random.randint(1, 31)
        elif month in (4, 6, 9, 11):
            day = random.randint(1, 30)
        else:
            day = random.randint(1, 28)
    # 小于10的月份前面加0
    if month < 10:
        month = '0' + str(month)
    if day < 10:
        day = '0' + str(day)
    birthday = str(year) + "-" + str(month) + "-" + str(day)
    return birthday


# 随机生成手机号
def get_tel():
    tel = ''
    tel += str(random.choice(phone_number))
    ran = ''
    for i in range(8):
        ran += str(random.randint(0, 9))
    tel += ran
    return tel


get_sex = lambda: random.choice(['男', '女'])

# 随机生成身份证号
def get_idnum():
    id_num = ''
    # 随机选择地址码
    id_num += str(random.choice(province_id))
    # 随机生成4-6位地址码
    for i in range(4):
        ran_num = str(random.randint(0, 9))
        id_num += ran_num
    b = get_birthday()
    id_num += b
    # 生成15、16位顺序号
    num = ''
    for i in range(2):
        num += str(random.randint(0, 9))
    id_num += num
    # 通过性别判断生成第十七位数字 男单 女双
    s = get_sex()
    # print("性别:", s)
    if s == '男':
        # 生成奇数
        seventeen_num = random.randrange(1, 9, 2)
    else:
        seventeen_num = random.randrange(2, 9, 2)
    id_num += str(seventeen_num)
    eighteen_num = str(random.randint(1, 10))
    if eighteen_num == '10':
        eighteen_num = 'X'
    id_num += eighteen_num
    return id_num, s

def get_DB_conn():
    mysql_conn = pymysql.connect(host='localhost', port=3306, user='root', password='123456',
                                 db='test', charset='utf8')
    return mysql_conn


def main():
    mysql_conn = get_DB_conn()
    fake = Faker("zh_CN")
    for i in range(0, 10000):

        s = get_idnum()
        # tel = get_tel()
        # print(fake.name(), tel, get_birthday(), s[0], s[1])
        sql = "INSERT INTO persons (sname, stel, ssex, sprovince) VALUES ('{0}','{1}', '{2}','{3}')".format(fake.name(),
                                                                                                            get_tel(),
                                                                                                            s[1], s[0])
        sql2 = "INSERT INTO persons2 (sname, stel, ssex, sprovince) VALUES ('{0}','{1}', '{2}','{3}')".format(fake.name(),
                                                                                                            get_tel(),
                                                                                                            s[1], s[0])
        sql3 = "INSERT INTO persons3 (sname, stel, ssex, sprovince) VALUES ('{0}','{1}', '{2}','{3}')".format(
            fake.name(),
            get_tel(),
            s[1], s[0])
        print(sql)
        try:
            with mysql_conn.cursor() as cursor:
                cursor.execute(sql)
                cursor.execute(sql2)
                cursor.execute(sql3)
            mysql_conn.commit()
        except Exception as e:
            mysql_conn.rollback()
    mysql_conn.close()


if __name__ == '__main__':
    main()
    print("=========================完成============================")
