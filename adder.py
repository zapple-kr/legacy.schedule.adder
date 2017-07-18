import pymysql
import time
import sys

master1 = {'host': '211.115.112.13',
           'db': 'ads',
           'pw': 'zapple2012'}
cjhv = {'host': '211.115.112.14',
           'db': 'ads_cjhv',
           'pw': 'zapple2016'}
master3 = {'host': '211.115.112.10',
           'db': 'ads',
           'pw': 'zapp1030'}

so_db_info_dict = {'1': cjhv,
                   '2': master3,
                   '6': master1,
                   '7': master1,
                   '19': master3}


def connect_to_database(db_info):
    result = pymysql.connect(host=db_info.get('host'), port=3306, user='zapple',
                           passwd=db_info.get('pw'), db=db_info.get('db'), charset='utf8')
    print('connected database to {}'.format(db_info.get('host')))
    return result


def quite_db_close(conn):
    if conn is None:
        return
    try:
        conn.close()
        print('DB connection closed.\n')
    except pymysql.err.Error as mysqlErr:
        print('{}\n'.format(mysqlErr))


def parse_line_to_campaign_info(campaign_info_line):
    tokens = campaign_info_line.split(',')
    _so_code = tokens[0]
    _campaign_id = tokens[1]
    _creative_id = tokens[2]
    return _so_code, _campaign_id, _creative_id


def parse_line_to_schedule_info(schedule_info_line):
    tokens = schedule_info_line.split(',')
    _group_id = tokens[0]
    _start_date = tokens[1]
    _end_date = tokens[2]
    _start_time = tokens[3]
    _end_time = tokens[4]
    return _group_id, _start_date, _end_date, _start_time, _end_time


def get_campaign_name(conn, inputs):
    query = 'select name from tbl_campaign where id = %s and main_operator_id = %s'
    cursor = conn.cursor()
    cursor.execute(query, inputs)
    result = cursor.fetchall()[0][0]
    cursor.close()
    return result


def get_group_name(conn, grp_id):
    query = 'select group_name from tbl_group where id = %s'
    cursor = conn.cursor()
    cursor.execute(query, grp_id)
    result = cursor.fetchall()[0][0]
    cursor.close()
    return result


# TODO: modulation
def insert_schedule(conn, so_code, schedule_name, creative_id, start_date, end_date, start_time,
                    end_time, group_id, campaign_id):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    insert_schedule_query = '''
    insert into tbl_schedule
    (main_operator_id, created, schedule_name, content_id, default_flag,
    user_id, start_date, end_date, group_id, priority_flag,
    campaign_id)
    values (%s, %s, %s, %s, '0', '2', %s, %s, %s, 1, %s)
    '''

    cursor = conn.cursor()
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
    cursor.close()

db_conn = None
line = None
f = open('schedule_list.txt', 'r', encoding='utf-8')
try:
    campaign_id = None
    creative_id = None
    so_code = None
    campaign_name = None

    while True:
        line_ = f.readline()
        if not line_:
            break

        line = line_.replace('\n', '')
        info = None
        if not line or line.startswith('#'):
            print('Line skip: {}'.format(line))
        elif line.startswith('='):
            so_code, campaign_id, creative_id = parse_line_to_campaign_info(line[1:])

            # TODO: 이미 접속되어있으면 재접속하지 않도록.
            quite_db_close(db_conn)
            # TODO: Be immutable??
            db_conn = connect_to_database(so_db_info_dict.get(so_code))
            campaign_name = get_campaign_name(db_conn, (campaign_id, so_code))

            print('캠페인 정보')
            print('SO CODE: {}, CAMPAIGN_NAME: {}, CREATIVE_ID: {}'.format(so_code, campaign_name, creative_id))
        else:
            group_id, start_date, end_date, start_time, end_time = parse_line_to_schedule_info(line)
            group_name = get_group_name(db_conn, group_id)
            schedule_name = '{} {} {}~{} {}-{}' \
                .format(group_name, campaign_name, start_date, end_date, start_time, end_time)

            # TODO: date validation
            insert_schedule(db_conn, so_code=so_code, schedule_name=schedule_name, creative_id=creative_id,
                            start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time,
                            group_id=group_id, campaign_id=campaign_id)

            print('{} - 입력 성공!'.format(schedule_name))
except IndexError as e:
    print('Error occur!!!!', file=sys.stderr)
    print('Wrong line: {}'.format(line), file=sys.stderr)
    print('Message: {}'.format(e), file=sys.stderr)
finally:
    f.close()
    quite_db_close(db_conn)
