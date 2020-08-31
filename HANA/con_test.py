#Import your dependencies
import platform
from hdbcli import dbapi

#verify that this is a 64 bit version of Python
print ("Platform architecture: " + platform.architecture()[0])

#Initialize your connection
# 
conn = dbapi.connect(
    address='10.0.0.23',
    port='30315',
    user='USU',
    password='senha',
    #key='USER1UserKey', # address, port, user and password are retreived from the hdbuserstore
    encrypt=True, # must be set to True when connecting to HANA Cloud
    sslValidateCertificate=False # True HC, False for HANA Express.
)
#If no errors, print connected
print('connected')

cursor = conn.cursor()
sql_command = """select * from "_SYS_BIC"."VOT.BW.VIEWS.COMERCIAL/ZVC_CVC_SD_0004_CM_PLANT";"""
cursor.execute(sql_command)
rows = cursor.fetchall()
for row in rows:
    for col in row:
        print ("%s" % col, end=" ")
    print (" ")
cursor.close()
conn.close()

