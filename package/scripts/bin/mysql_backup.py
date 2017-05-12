
#-*-encoding: UTF-8-*-
import time, sched
import threading
import os
import datetime
import logging

def get_next_interval(conf):
    if conf.has_key("init"):
        return 3600 * 24
    else:
        current_time = datetime.datetime.now()
        current_hour = current_time.hour
        current_minute = current_time.minute
        next_hour = int(conf['mysql.backup.hour'])
        next_minute = int(conf['mysql.backup.minute'])
        now_sec = current_hour * 3600 + current_minute * 60
        next_sec = next_hour * 3600 + next_minute * 60
        if now_sec < next_sec:
            return next_sec - now_sec
        else:
            return 3600 * 24 - now_sec + next_sec

s = sched.scheduler(time.time, time.sleep)

kvs = {}

for line in open('/etc/mysql_backup/conf/mysql_backup.properties'):
	if len(line) >= 3:
		w = line.strip().split("=")
		kvs[w[0]] = w[1]

backup_databases = kvs['mysql.backup.databases'].split(',')
backup_dir_local = kvs['mysql.backup.local.directory']
backup_dir_hdfs = kvs['mysql.backup.hdfs.directory']
backup_log_file = kvs['mysql.backup.log']

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datetime='%a, %d %b %Y %H:%M:%S', filename=backup_log_file, filemode='a')

now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
logging.info(now + "  MySQL Backup daemon start...\n")
logging.info("configuration parameters:\n")
# print configuration parameters
for kv in kvs:
    logging.info(kv + " : " + kvs[kv] + "\n")


def mysql_backup(databases):
    logging.info("\n----------------- backup -----------------------\n")
    for database in databases:
        logging.info("begin time:\t" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + "\n")
        logging.info("thread id:\t" + threading.current_thread().getName() + "\n")
        logging.info("backup mysql database :  " + database + "\n")
        # cmd = 'mysqldump -uroot -pManWei1234_  ' + database + " > " + database + ".bk." + time.strftime("%Y%m%d%H%M%S")
        cmd = "mysqldump -uroot -pManWei1234_  %s > %s/%s.bk.%s" % (database, backup_dir_local, database, time.strftime("%Y%m%d%H%M%S"))
        logging.info(cmd + "\n")
        lines = os.popen(cmd).readlines()
        logging.info(str(lines))
        time.sleep(5)
        logging.info('\nend time:\t' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "\n")

def start_mysql_backup_daemon():
    try:
        while True:
            # start a daemon to backup databases in sequence
            t = threading.Thread(target=mysql_backup, args=(backup_databases,))
            seconds = get_next_interval(kvs)
            s.enter(seconds, 0, t.start,())
            s.run()
            kvs['init'] = 'True'
    except Exception, e:
        logging.info("%s:  ERROR:  %s" %(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), str(e)))
    finally:
        log.close()
start_mysql_backup_daemon()

