import pymysql
import time
import sys


def get_mysql_connection(db_info):
    result = pymysql.connect(host=db_info.get('host'), port=3306, user='zapple',
                           passwd=db_info.get('pw'), db='ads', charset='utf8')
    print('connected database to {}'.format(db_info.get('host')))
    return result


def quite_close(conn):
    if conn is None:
        return
    try:
        conn.close()
        print('DB connection closed')
    except pymysql.err.Error as mysqlErr:
        print(mysqlErr)

f = open('schedule_list.txt', 'r', encoding='utf-8')

campaign_id = None
creative_id = None
so_code = None
campaign_name = None

db_conn = None
cursor = None

master1 = {'host': '211.115.112.13',
           'pw': 'zapple2012'}
master2 = {'host': '211.115.112.14',
           'pw': 'zapple2016'}
so_db_info_dict = {'1': master2,
                   '2': master1,
                   '6': master1,
                   '7': master1,
                   '19': master1}

line = None
try:
    while True:
        line_ = f.readline()
        if not line_:
            break

        line = line_.replace('\n', '')
        info = None
        if not line or line.startswith('#'):
            print('Line skip: {}'.format(line))
        elif line.startswith('='):
            info = line[1:].split('|')
            so_code = info[0]
            campaign_id = info[1]
            creative_id = info[2]

            quite_close(db_conn)
            db_conn = get_mysql_connection(so_db_info_dict.get(so_code))
            cursor = db_conn.cursor()

            campaign_query = 'select * from tbl_campaign where id = %s and main_operator_id = %s'
            cursor.execute(campaign_query, (campaign_id, so_code))
            campaign_name = cursor.fetchall()[0][3]

            print('캠페인 정보')
            print('SO CODE: {}, CAMPAIGN_NAME: {}, CREATIVE_ID: {}'.format(so_code, campaign_name, creative_id))
        else:
            info = line.split('|')
            group_id = info[0]
            start_date = info[1]
            end_date = info[2]
            start_time = info[3]
            end_time = info[4]

            group_query = 'select group_name from tbl_group where id = %s'
            cursor.execute(group_query, group_id)
            group_name = cursor.fetchall()[0][0]

            now = time.strftime("%Y-%m-%d %H:%M:%S")
            schedule_name = '{} {} {}~{} {}-{}' \
                .format(group_name, campaign_name, start_date, end_date, start_time, end_time)

            insert_schedule_query = '''
            insert into tbl_schedule
            (main_operator_id, created, schedule_name, content_id, default_flag,
            user_id, start_date, end_date, group_id, priority_flag,
            campaign_id)
            values (%s, %s, %s, %s, '0', '2', %s, %s, %s, 3, %s)
            '''
            cursor.execute(insert_schedule_query, (so_code, now, schedule_name, creative_id,
                                                   start_date, end_date, group_id, campaign_id))

            # TODO: 쿼리변경
            cursor.execute('SELECT LAST_INSERT_ID()')
            last_id = cursor.fetchall()[0][0]

            insert_time_query = '''
            INSERT INTO tbl_time (start_hour, end_hour, schedule_id, status)
            VALUES (%s, %s, %s, null)
            '''

            cursor.execute(insert_time_query, (start_time, end_time, last_id))
            db_conn.commit()

            print('{} - 입력 성공!'.format(schedule_name))
except IndexError as e:
    print('Error occur!!!!', file=sys.stderr)
    print('Wrong line: {}'.format(line), file=sys.stderr)
    print('Message: {}'.format(e), file=sys.stderr)
finally:
    f.close()
    quite_close(db_conn)
