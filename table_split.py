# coding: utf-8
# MySQL快速分表工具 V1.0

import MySQLdb as _m

def get_dbs(cursor):
    cursor.execute('show databases;')
    _sys_dbs = ('information_schema', 'performance_schema', 'mysql', 'sys')
    return [i[0] for i in cursor.fetchall() if i[0] not in _sys_dbs]

def get_tables(dbname, cursor):
    cursor.connection.select_db(dbname)
    cursor.execute('show tables;')
    return [i[0] for i in cursor.fetchall()]

def clone_table(table, num, cursor):
    sql = 'create table `%s_part%s` like `%s`;'
    try:
        for i in range(num):
            cursor.execute(sql % (table, i + 1, table))
            del_indexs('%s_part%s' % (table, i + 1), cursor)
    except Exception as e:
        print(repr(e))
        exit()

def del_indexs(table, cursor):
    cursor.execute('show index from `%s`;' % table)
    indexs = cursor.fetchall()
    if not indexs: return
    sql = 'alter table `%s` drop index %s'
    for i in indexs:
        cursor.execute(sql % (table, i[2]))

def clone_data(table, num, each_length, last_length, cursor):
    sql = 'insert into `%s_part%s` select * from `%s` limit %s,%s;'
    start = 0
    for i in range(1, num + 1):
        try:
            yield 'Dumping data to `%s_part%s`...' % (table, i)
            curr_sql = sql % (table, i, table, start, each_length)
            cursor.execute(curr_sql)
        except Exception as e:
            print(repr(e))
            exit()
        start += each_length
        if i == num - 1:
            if last_length:
                each_length = last_length

def count_lines(table, cursor):
    cursor.execute('select count(*) from `%s`;' % table)
    return int(cursor.fetchone()[0])

def get_input_num(msg, maxNum):
    while (True):
        num = input(msg)
        if not num.isdigit():
            print('\n请输入一个整数！')
            continue
        num = int(num)
        if not (num > 0 and num <= maxNum):
            print('\n超出范围！')
            continue
        break
    return num

if __name__ == "__main__":
    print('''\nMySQL快速分表工具 V1.0\n
本工具可以通过简单的设置进行分表并分割主表数据导入到分表，
分表结构与主表一致，但考虑到导入速度，分表不包含建与索引，
可在分表并导入数据成功后自行设置建与索引。\n
    ''')
    host = input('输入服务器名（留空默认为localhost）：') or 'localhost'
    port = input('输入端口号（留空默认为3306）：') or 3306
    port = int(port)
    user = input('输入用户名（留空默认为root）：') or 'root'
    pwd = input('输入密码：')
    try:
        conn = _m.connect(host = host, user = user, passwd = pwd, port=port, charset='gbk')
    except Exception as e:
        print(repr(e))
        exit()
    cursor = conn.cursor()
    dbs = get_dbs(cursor)
    if not dbs: exit('没有可用的数据库！')
    print('\n登录成功！\n')
    for i in range(len(dbs)):
        print('%s. %s' % (i + 1, dbs[i]))
    choose_db = get_input_num('\n请选择要操作的数据库，输入编号：', len(dbs))
    choose_db = dbs[choose_db - 1]
    print('\n当前库：' + choose_db + '\n')
    tables = get_tables(choose_db, cursor)
    for i in range(len(tables)):
        print('%s. %s' % (i + 1, tables[i]))
    choose_table = get_input_num('\n请选择要操作的表，输入编号：', len(tables))
    choose_table = tables[choose_table - 1]
    length = count_lines(choose_table, cursor)
    print('\n当前选择表：%s，共 %s 行' % (choose_table, length))
    print('\n请选择分割方式：\n1. 按行数分割，2. 分为指定份数\n')
    mode = get_input_num('>>> ', 2)
    if mode == 1:
        each_length = get_input_num('\n请输入每个分表行数：', length)
        last_length = length % each_length
        num = length // each_length
        if last_length: num += 1
    else:
        num = get_input_num('\n请指定分表数量：', length)
        if length % num:
            each_length = (length - 1) // (num - 1)
            last_length = length % each_length
        else:
            each_length = length // num
            last_length = 0
    print('\n当前选择表：%s，共 %s 行' % (choose_table, length))
    print('分为 %s 份，每份 %s 行（最后一份可能略少）' % (num, each_length))
    print('\n正在建表...', end='')
    clone_table(choose_table, num, cursor)
    print('完成！')
    print('\n正在导入数据...')
    pre_print = 0
    for status in clone_data(choose_table, num, each_length, last_length, cursor):
        print('\r' + ' ' * pre_print, end = '')
        print('\r' + status, end = '')
        pre_print = len(status)
    print('\r' + ' ' * pre_print)
    print('成功将表 %s 的数据分到 %s_part* 中，现在可以手工设置键和索引了！' % (choose_table, choose_table))
    cursor.close()
    conn.commit()
    conn.close()
    input('\n按任意键退出...')