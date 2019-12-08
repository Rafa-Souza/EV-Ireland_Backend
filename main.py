import database
import server
import etl_controller
from datetime import datetime
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-u", "--user", dest="dbUser", default="postgres")
parser.add_argument("-p", "--pwd", dest="dbPwd", default="admin")
args = parser.parse_args()
print(args)

start = datetime.now()
print('Process Started: ', start)
print("-------------Database-----------------")
database.startDb(args.dbUser, args.dbPwd)
etl_controller.initETL()
print("-------------------------------------------------")
finish = datetime.now()
print('Process Finished: ', datetime.now())
print('Time took: ', finish-start)
print("-------------Starting API Server-----------------")
server.startServer()
