import ftplib
import pandas as pd
import io
filename = 'nasdaqlisted.txt'

def writeFunc(s):
  #print("Read: " + str(s).decode())
    file_bytes = io.BytesIO(s)
    return file_bytes

ftp = ftplib.FTP("ftp.nasdaqtrader.com")
ftp.login("Anonymous","guest")
ftp.cwd("Symboldirectory")
#ftp.dir()
#ftp.retrbinary("RETR /symboldirectory/" + filename, open(filename,'wb').write)
#file = ftp.retrbinary("RETR /symboldirectory/" + filename, writeFunc)
file = io.BytesIO()

ftp_retr_string = "RETR /Symboldirectory/" + filename

#print(ftp_retr_string)
ftp.retrbinary(ftp_retr_string, file.write)
file.seek(0)




file = io.StringIO(file.read().decode().replace('|','__'))

file.seek(0)

df = pd.read_csv(file,sep='__', encoding = 'utf-8')


df['Exchange'] = pd.Series(['NASDAQ' for x in range(len(df.index))],index = df.index)

df = df[['Exchange','Symbol']]

#Remove junk row "File Creation Time"

df.drop(df.tail(1).index, inplace = True)

print(df)
    #close(filename)

