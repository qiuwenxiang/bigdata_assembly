# coding=utf-8

import MySQLdb
import os

db_args = ('172.17.202.36', 'leopaard', 'qwe!@#', 'information_schema',)

def getCursorAndDb():
    db = MySQLdb.connect(*db_args)
    db.set_character_set('utf8')
    cursor = db.cursor()
    return cursor, db


def getOrderTableBySize(database):
    cursor, db = getCursorAndDb()
    data = []
    try:
        cursor.execute("select table_name,(DATA_LENGTH+INDEX_LENGTH) from tables where table_schema='%s' order by (DATA_LENGTH+INDEX_LENGTH) desc" % database)
        r = cursor.fetchone()
        while r is not None:
            data.append([r[0], r[1]])
            r = cursor.fetchone()
        return data
    finally:
        cursor.close()
        db.close()


def fun(amountsum, left_arr, result_arr, arr):
    """ 递归调用
    :param amountsum: 以第一个参数作为比较值
    :param left_arr: 剩余数组
    :param result_arr: 累计比较值
    :param arr: 累计数组
    :return:
    """
    if not left_arr:
        return
    if len(left_arr) <= 2:
        for i in left_arr:
            result.append([i])
        return
    if left_arr[-1][1] + result_arr[1] < amountsum[1]:
        arr.append(left_arr[-1])
        fun(amountsum, left_arr[0:-1], left_arr[-1] + result_arr , arr)
    else:
        result.append(arr)
        fun(amountsum, left_arr[1:], left_arr[1], [left_arr[1]])


def create_export_script(database, result):
    """
    生成的文件  set ff =unix ; chmod u+x export.sh
    :param database:
    :param result:
    :return:
    """
    fo = open("export.sh", "w")
    for i in range(len(result)):
        pre = 'mysqldump -u %s -p%s %s ' % (db_args[1], db_args[2], database)
        for _i in result[i]:
            pre += _i[0] +' '
        pre += '> part_%s.sql\n' % i
        fo.write(pre)

    # 关闭打开的文件
    fo.close()

def create_import_script(result, *args):
    """
    生成的文件  set ff =unix ; chmod u+x export.sh
    :param database:
    :param result:
    :return:
    """
    fo = open("import.sh", "w")
    pre = 'mysql -uroot -p123456 << EOF\n use autoHome;\n set names utf8;\n' % ()
    for i in range(len(result)):
        pre += 'source /opt/part_%s.sql;\n' % i
    pre += 'EOF\n'
    pre += 'echo "import end"'
    fo.write(pre)

    # 关闭打开的文件
    fo.close()


if __name__ == '__main__':
    database = 'autoHome'
    # 查库获取表名和大小排序
    data = getOrderTableBySize(database)
    # 大小分组
    result = []
    result.append([data[0]])
    fun(data[0], data[1:], data[1], [data[1]])
    create_export_script(database, result)
    target_args = ('root', '123456', database)
    create_import_script(result, *target_args)
