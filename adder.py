import pymysql
import time

f = open('test.txt', 'r', encoding='utf-8')

campaign_id = None
creative_id = None
so_code = None
campaign_name = None


def get_mysql_connection():
    return pymysql.connect(host='211.115.112.13', port=3306, user='zapple',
                           passwd='zapple2012', db='ads', charset='utf8')

conn = get_mysql_connection()

try:
    with conn.cursor() as cursor:
        while True:
            line = f.readline()
            if not line:
                break

            info = None
            if line.startswith('='):
                info = line[1:].split('|')
                so_code = info[0]
                campaign_id = info[1]
                creative_id = info[2]

                campaign_query = 'select * from tbl_campaign where id = %s and main_operator_id = %s'
                cursor.execute(campaign_query, (campaign_id, so_code))
                campaign_info = cursor.fetchall()[0]

                campaign_name = campaign_info[3]
                print('캠페인 정보')
                print('SO CODE: {}, CAMPAIGN_NAME: {}, CREATIVE_ID: {}'.format(so_code, campaign_name, creative_id));
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
                values (%s, %s, %s, %s, '0', 
                '2', %s, %s, %s, 3, %s)
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
                conn.commit()

                print('{} - 입력 성공!'.format(schedule_name))
finally:
    f.close()
    conn.close()
