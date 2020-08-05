import urllib.request
...
url = 'http://example.com/'
response = urllib.request.urlopen(url)
data = response.read()      # a `bytes` object
text = data.decode('utf-8') # a `str`; this step can't be used if data is binary



import urllib.request
import gzip
...
# Read the first 64 bytes of the file inside the .gz archive located at `url`
url = 'http://example.com/something.gz'
with urllib.request.urlopen(url) as response:
    with gzip.GzipFile(fileobj=response) as uncompressed:
        file_header = uncompressed.read(64) # a `bytes` object
        # Or do anything shown above using `uncompressed` instead of `response`.
		


#I download files and save it locally using the below code:
import requests

url = 'https://www.python.org/static/img/python-logo.png'
fileName = 'D:\Python\dwnldPythonLogo.png'
req = requests.get(url)
file = open(fileName, 'wb')
for chunk in req.iter_content(100000):
    file.write(chunk)
file.close()



# Download 

import urllib2

url = "http://download.thinkbroadband.com/10MB.zip"

file_name = url.split('/')[-1]
u = urllib2.urlopen(url)
f = open(file_name, 'wb')
meta = u.info()
file_size = int(meta.getheaders("Content-Length")[0])
print "Downloading: %s Bytes: %s" % (file_name, file_size)

file_size_dl = 0
block_sz = 8192
while True:
    buffer = u.read(block_sz)
    if not buffer:
        break

    file_size_dl += len(buffer)
    f.write(buffer)
    status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
    status = status + chr(8)*(len(status)+1)
    print status,

f.close()


# Unzip
import requests, zipfile, io
r = requests.get(zip_file_url)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall()

# if you'd like to save the downloaded file in a different location, replace z.extractall() with z.extractall("/path/to/destination_directory")




# Outro exemplo

import zipfile, urllib.request, shutil

url = 'http://www....myzipfile.zip'
file_name = 'myzip.zip'

with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
    shutil.copyfileobj(response, out_file)
    with zipfile.ZipFile(file_name) as zf:
        zf.extractall()
		

