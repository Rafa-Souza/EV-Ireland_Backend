import urllib.request
from os.path import basename
import web_scrapper
import zip_processor
import database


def validateFiles(links):
    processedFiles = database.getAllFiles()
    newLinks = [link for link in links if not link.endswith(tuple(processedFiles))]
    return sorted(newLinks)


def initETL():
    print("-------------WebScrapper-----------------")
    print('Checking new Data for EV points...')
    zipLinks = getNewData()
    print(len(zipLinks), ' new data files found.')
    print(zipLinks)
    if len(zipLinks) > 0:
        downloadNewData(zipLinks)
    print("-------------ETL Finished----------")


def getNewData():
    zipLinks = web_scrapper.scrapeData()
    zipLinks = [link['href'] for link in zipLinks]
    zipLinks = validateFiles(zipLinks)
    return zipLinks


def downloadNewData(links):
    print("-------------ETL-----------------")
    print('Processing new data files...')
    # link = links[0]
    for link in links:
        requestZip = urllib.request.Request(link)
        print('Downloading: ', link)
        with urllib.request.urlopen(requestZip) as zipRes:
            fileName = basename(zipRes.url)
            zipFile = zipRes.read()
            print('Processing: ', fileName)
            zip_processor.processZip(zipFile)
            database.insertFile(fileName)
            print(fileName, " Processed!")
