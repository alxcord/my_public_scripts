# baixa arquivos e descomprime
# https://stackoverflow.com/questions/5230966/python-ftp-download-all-files-in-directory
# https://stackoverflow.com/questions/3451111/unzipping-files-in-python

from ftplib import FTP
import os, sys, os.path
import zipfile

def handleDownload(block):
    file.write(block)
    print ".",


ftp.login('user1\\anon', 'pswrd20')
ftp_directory = '\\data\\zip\\'
local_directory_zip = 'c:\\temp\\files'
local_directory_extract_to = 'c:\\temp\\files'

ddir='C:\\Data\\test\\'
os.chdir(ddir)
ftp = FTP('test1/server/')

print ('Logging in.')

ftp.cwd(ftp_directory)

filenames = ftp.nlst() # get filenames within the directory
print (filenames)

for filename in filenames:
    local_filename = os.path.join('C:\\test\\', filename)
    file = open(local_filename, 'wb')
    ftp.retrbinary('RETR '+ filename, file.write)
	# descompacta os arquivos
	if local_filename.lower.endswith('.zip'):
		with zipfile.ZipFile(local_filename, 'r') as zip_ref:
			zip_ref.extractall(directory_to_extract_to)
    file.close()

ftp.quit() # This is the “polite” way to close a connection