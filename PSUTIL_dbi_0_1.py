#!/usr/bin/python
import MySQLdb
import PSUTIL_settings_0_1 as dbi


cur = None
db = None


def initmysql():

    global cur, db

# your host, usually localhost

    db = MySQLdb.connect(
        host="localhost",
        user=dbi.database['USER'],
        passwd=dbi.database['PASS'],
        db=dbi.database['DATABASE']
    )
    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()


def showtables(table_name):
    global cur
    stmt = "SHOW TABLES"
    cur.execute(stmt)
    result = cur.fetchall()
    # print result
    if (table_name in str(result)):
        print "table '" + table_name + "' exists'"
        return 1
    else:
        print "table '" + table_name + "' does_NOT_exists'"
        # there are no tables named "tableName"
        return 0


def setup():
    global cur
    print 'Sanity check'
    for k in dbi.tables:
        r = showtables(k)
        if( r == 0):
            print "trying to add table '" + k + "''"
            try:
                cur.execute(dbi.tables[k])
            except cur.Error as err:
                print(("Something went wrong: {}".format(err)))


def get_all_metrics(dct):

    global cur, db
    mstr = ''
    try:
        cur.execute('''SELECT pdf_text_str FROM pdf_text;''')
        for row in cur:
            """ print row """
            mstr += str(row[0])
            """ if row[0]:
               return row[0]"""
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))
    return mstr


def add_name(name):
    global cur
    e_name = MySQLdb.escape_string(name)
    print "ename = " + "'" +e_name+"'"
    try:
        sql = "SELECT psutil_name_name, psutil_name_id FROM psutil_name WHERE psutil_name_name  = %s;"
        cur.execute(sql,(e_name,))
        for row in cur.fetchall():
            print "name row %s"% row[0]
            if row[0]:
                return  row[1]
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))
    print 'hmm ' + e_name
    # print (("LINK IS " + link))
    try:
        cur.execute('''INSERT into psutil_name (psutil_name_name) values (%s);''', (e_name,))
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))

    db.commit()
    print 'hmm_end ' + e_name
    return cur.lastrowid

def add_iter(epoch):
    global cur
    print epoch
    try:
        cur.execute('''SELECT psutil_iter_time,psutil_iter_time_id FROM psutil_iter_time WHERE psutil_iter_time  = %s;''', (epoch,))
        for row in cur.fetchall():
            print "row %s " % row[0]
            if row[0]:
                return  row[1]
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))
    try:
        print '''INSERT into psutil_iter_time (psutil_iter_time) values (%s);''', (epoch,)
        cur.execute('''INSERT into psutil_iter_time (psutil_iter_time) values ('%s');''', (epoch,))
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))
    db.commit()
    return cur.lastrowid



def add_user(user):
    global cur
    print 'add_user'
    try:
        cur.execute('''SELECT psutil_user_name,psutil_user_id FROM psutil_user WHERE psutil_user_name  = %s;''', (user,))
        for row in cur.fetchall():
            print "row %s " % row[0]
            if row[0]:
                return  row[1]
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))

   # print (("LINK IS " + link))
    try:
        cur.execute('''INSERT into psutil_user (psutil_user_name) values (%s);''', (user,))
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))
    print 'end add user'
    db.commit()
   # cursor.execute('SELECT last__id()')
    return cur.lastrowid

def convert_unix_time(id):
    global cur
    sql = "SELECT psutil_iter_time_id, psutil_iter_time, FROM_UNIXTIME(psutil_iter_time) FROM psutil_iter_time"

def add_metrics(ps_top):

    global cur, db
    #print ps_top['status']
    #print "mem_per %s", ps_top['memory_percent']
    iter_id = add_iter(ps_top['epoch'])
    name_id = add_name(ps_top['name'])
    user_id = add_user(ps_top['username'])
    print " name_id %s user_id %s" %(name_id,user_id)
    try:
        cur.execute(
            '''INSERT into psutil_top(
            psutil_top_user_id,
            psutil_top_iter_id,
            psutil_top_nice,
            psutil_top_memory_percent,
            psutil_top_cpu_percent,
            psutil_top_name_id,
            psutil_top_status)
            values (%s,%s,%s,%s,%s,%s,%s)''',
            (
                user_id,
                iter_id,
                ps_top['nice'],
                ps_top['memory_percent'],
                ps_top['cpu_percent'],
                name_id,
                ps_top['status']
            )
        )
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))

    db.commit()
    return cur.lastrowid


def add_cpu_times(ps_cpu):

    global cur, db
    print "cil sys %s", ps_cpu['children_system']
    try:
        cur.execute(
            '''INSERT into psutil_cpu_times(
            psutil_cpu_times_top_id,
            psutil_cpu_times_user,
            psutil_cpu_times_system,
            psutil_cpu_times_children_user,
            psutil_cpu_times_children_system)
            values (%s,%s,%s,%s,%s)''',
            (
                ps_cpu['top_id'],
                ps_cpu['user'],
                ps_cpu['system'],
                ps_cpu['children_user'],
                ps_cpu['children_system']
            )
        )
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))

    db.commit()
    return cur.lastrowid


def add_memory_info(ps_mem):

    global cur, db

    try:
        cur.execute(
            '''INSERT into psutil_memory_info(
            psutil_memory_info_top_id,
            psutil_memory_info_rss,
            psutil_memory_info_vms,
            psutil_memory_info_shared,
            psutil_memory_info_text,
            psutil_memory_info_lib,
            psutil_memory_info_data,
            psutil_memory_info_dirty)
            values (%s,%s,%s,%s,%s,%s,%s,%s)''',
            (
                ps_mem['top_id'],
                ps_mem['rss'],
                ps_mem['vms'],
                ps_mem['shared'],
                ps_mem['text'],
                ps_mem['lib'],
                ps_mem['data'],
                ps_mem['dirty']
            )
        )
    except cur.Error as err:
        print(("Something went wrong: {}".format(err)))

    db.commit()
    return cur.lastrowid

"""cpu  3357 0 4313 1362393
     The  amount of time, measured in units of USER_HZ (1/100ths of a second on most
     architectures, use sysconf(_SC_CLK_TCK) to obtain the right  value),  that  the
     system spent in various states:

     user   (1) Time spent in user mode.

     nice   (2) Time spent in user mode with low priority (nice).

     system (3) Time spent in system mode.

     idle   (4) Time spent in the idle task.  This value should be USER_HZ times the
            second entry in the /proc/uptime pseudo-file.

     iowait (since Linux 2.5.41)
            (5) Time waiting for I/O to complete.

     irq (since Linux 2.6.0-test4)
            (6) Time servicing interrupts.

     softirq (since Linux 2.6.0-test4)
            (7) Time servicing softirqs.

     steal (since Linux 2.6.11)
            (8) Stolen time, which is the time spent in other operating systems when
            running in a virtualized environment

     guest (since Linux 2.6.24)
            (9)  Time  spent running a virtual CPU for guest operating systems under
            the control of the Linux kernel.

     guest_nice (since Linux 2.6.33)
            (10) Time spent running a niced guest (virtual CPU for  guest  operating
            systems under the control of the Linux kernel).
"""
