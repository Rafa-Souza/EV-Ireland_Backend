import database
import server
import etl_controller
from datetime import datetime


start = datetime.now()
print('Process Started: ', start)
print("-------------Database-----------------")
database.startDb()
etl_controller.initETL()
print("-------------------------------------------------")
finish = datetime.now()
print('Process Finished: ', datetime.now())
print('Time took: ', finish-start)
print("-------------Starting API Server-----------------")
server.startServer()
