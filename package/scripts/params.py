#-*-encoding:UTF-8-*-
'''
解析mysql_backup 服务参数，该服务一定要安装在mysql服务器上，主要参数有下面几个：
  mysql.backup.daemon.pid.dir: mysql_backup 进程daemon id 的保存路径，默认为 /var/run
  mysql.backup.daemon.pid: mysq_backup 进程 daemon id 的保存文件名，默认 mysql_backup.pid
  mysql.backup.databases: 备份的数据库
  mysql.backup.hour: 备份的小时，默认凌晨3点
  mysql.backup.minute：备份的分钟，默认0
  mysql.backup.local.directory: 备份的数据库保存目录
  mysql.backup.hdfs.directory: 备份的数据库保存目录
  mysql.backup.log
'''

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.default import default

config = Script.get_config()
hostname = config['hostname']

conf_dir = default("/configurations/mysql_backup/mysql.backup.conf.dir", "/etc/mysql_backup/conf")
daemon_dir = default("/configurations/mysql_backup/mysql.backup.daemon.pid.dir", "/var/run")
daemon_pid_file = default("/configurations/mysql_backup/mysql.backup.daemon.pid", "mysql_backup.pid")
backup_hour = default("/configurations/mysql_backup/mysql.backup.hour", 3)
backup_minute = default("/configurations/mysql_backup/mysql.backup.minute", 0)
backup_local_dir = default("/configurations/mysql_backup/mysql.backup.local.directory", "/var/lib/mysql_backup")
backup_hdfs_dir = default("/configurations/mysql_backup/mysql.backup.hdfs.directory", "mysql_backup")
backup_log = default("/configurations/mysql_backup/mysql.backup/mysql.backup.log", "/var/log/mysql_backup.log")

mysql_backup_properties_map = config['configurations']['mysql_backup']
