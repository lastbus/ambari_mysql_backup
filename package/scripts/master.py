#!/usr/bin/python

#-*-encoding: utf-8 -*-

import time, sched
import threading
from resource_management import *
from backup import backup

class MySQLBackup(Script):

    def install(self, env):
        print "======  install MySQLBackup service ======"
	#print self.get_config()
	try:
		Execute("yum install -y mysql_backup", user = 'root')
	except:
		show_logs(params.backup_log, user = 'root')
		raise
	print "====== installed succeed!  ======="

    def configure(self, env):
	import params
	env.set_params(params)
	backup(None, None)
        print 'Configure the MySQL backup service'

    def stop(self, env):
        print 'Stopped the Sample Srv Master'
        daemon_cmd = format("mysql_backup stop ")
	try:
		Execute(daemon_cmd, user = "root")
	except:
		show_logs(params.backup_log, user = 'root')
		raise
	print "stop mysql backup service succeed!"


    def start(self, env):
        print '====== start the mysql_backup daemon  ========='
        import params
	print 'params.conf_dir'
	print params.conf_dir
        env.set_params(params)
	self.configure(env)
	print 'env'
	print dir(env)
        daemon_cmd = format("mysql_backup start")
        try:
            Execute(daemon_cmd, user = "root")
        except:
            show_logs(params.backup_log, user='root')
            raise
        print "===== start mysql_backup daemon succeed  ========"

    def restart(self, env):
        print 'Restart the mysql_backup'
	import params
	env.set_params(params)
	self.configure(env)
        daemon_cmd = format("mysql_backup restart")
	try:
		Execute(daemon_cmd, user = 'root')
	except:
		show_log(params.backup_log, user = 'root')
		raise
	print "restart mysql backup service succeed!"

    def status(self, env):
        print 'Status of the mysql_back'
        daemon_cmd = format("mysql_backup status")
	try:
		Execute(daemon_cmd, user = 'root')
	except:
		show_log(params.backup_log, user = 'root')
		raise	

if __name__ == '__main__':
	MySQLBackup().execute()
