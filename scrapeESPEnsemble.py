#######################################################################################
#
# Author: Ian Bledsoe
# Date: 8/2/17
#
# Website: https://www.nwrfc.noaa.gov
# .CSV Location: itv00006\sqlupload$\ESPEnsembles.csv
#
# Running On:
# Windows 7 Serivce Pack 1
# Python 3.4.3
#
########################################################################################


"""
Hello World! (UCB Application Reviewers)
This is a Python script I wrote that downloads publicly available data
from the Northwest River Forecast Center. It scrapes their water supply forecasts for a collection of dams in the Columbia River watershed
in several flavors - The ESP0 does not include short-term weather forecasts, while the ESP10 includes a 10day weather (precipitation) forecast
It scrapes these in two flavors - natural is unregulated water supply (what the river would be like without diverting or impeding the water),
while the water supply forecast includes such regulation.

This script doesn't rely on proprietary data, and can be run with just a few library dependencies. fptest and filepath variables just need
to be set to a local path.

The script takes a long time, and generates a .csv at filepath that could be in excess of a gigabyte!

This manual process used to take hours of an analyst's time, and was therefore only updated monthly. Now it runs automatically every morning.

"""







#Library imports
import pypyodbc
import datetime
import time
from bs4 import BeautifulSoup #4.4.1
from robobrowser import RoboBrowser #0.5.3
import os
from tenacity import *



time1=time.time()

@retry(stop=stop_after_attempt(7),
       wait=wait_fixed(2))

def EnsembleScrape(waterID, \
                   forecastTypeCode, \
                   forecastType, \
                   flowType, \
                   flowTypeAbbr, \
                   flowTypeAlias): #Function for scraping all types 
    
    ##   http://www.nwrfc.noaa.gov/chpsesp/ensemble/watersupply/ALFW1IW_SQIN.ESPF0.csv
    browser = RoboBrowser(history=True, parser="html.parser") #Starts the browser
    url='https://www.nwrfc.noaa.gov/chpsesp/ensemble/'+ flowType + '/' + waterID + flowTypeAbbr +'_SQIN.'+ forecastTypeCode +'.csv' #Sets target url
    print("Scraping: ", url)
    request = browser.session.get(url, stream=True) #Go to url based on function parameters
    with open(fptest, "wb") as filex: #Open file and prepare for writing
        filex.truncate()
        filex.write(request.content)
    
    listy=[]
    with open(fptest, "r") as filex: #Open file and write
        for line in filex: #For every line
            listy.append(line.split(',')) #Split with a comma
    
  
        startDate = datetime.datetime.strptime(listy[7][0], "%Y-%m-%d %H:%M:%S") #Sets the start date in the correct format
        for item in listy[7:]: #For every item in the list
            
            for position, piece in enumerate(item[7:]):
                try:
                    histDate = listy[6][position+1] + str(startDate)[4:10] #Set histDate from the list
                    fDate=datetime.datetime.strptime(histDate, "%Y-%m-%d") #Format date
                    #print(str(histDate))
                except ValueError: #If the value doesn't work then do this procedure
                    modDate= startDate + datetime.timedelta(days=1) #Set modDate to the startDate plus one day
                    histDate= listy[6][position+1] + str(modDate)[4:10] #Sets histDate to the list date plus the newly created modDate
                    fDate=datetime.datetime.strptime(histDate, "%Y-%m-%d") #Formated date
                    
                file.write(\
                                forecastType + ',' + \
                                waterID.rstrip('I') + ',' + \
                                str(listy[1])[9:19] + ',' + \
                                str(histDate)  + ',' + \
                                str(startDate)[:10] + ',' + \
                                str(float(piece.strip('\n'))*1000) + ',' + \
                                flowTypeAlias + '\n') #Writes each row of the .csv file 
                    
            startDate+= datetime.timedelta(hours=6) #Move the start date forward 6 hours
    
       
#######################################################################
######################     P R O G R A M   ############################   
#######################################################################


#First, set the file and clear old data
fptest=r'\\itv00006\sqlupload$\TEMPEnsemble.csv'
filepath=r'\\itv00006\sqlupload$\ESPEnsembles.csv'

with open(fptest, 'w') as file: #Clear old data
    file.truncate()

with open(filepath, 'w') as file: #Write the headers/columns
    file.truncate()
    file.write('Forecast_Type,' + \
                'NWRFC_Water_ID,' + \
                'Publish_Date,' + \
                'Historic_Water_Date,' + \
                'Forecast_Date,' + \
                'Runoff_CFS,'+ \
                'Flow_Type' + '\n')


    ## ESP5 no longer exists ##
               
    #ALFW1I
    EnsembleScrape('ALFW1I', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('ALFW1I', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('ALFW1I', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #BONO3
    EnsembleScrape('BONO3', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('BONO3', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('BONO3', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('BONO3', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #CHJW1
    EnsembleScrape('CHJW1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('CHJW1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #DWRI1
    EnsembleScrape('DWRI1', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('DWRI1', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('DWRI1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('DWRI1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #GCDW1
    EnsembleScrape('GCDW1', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('GCDW1', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('GCDW1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('GCDW1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #HHWM8
    EnsembleScrape('HHWM8', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('HHWM8', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('HHWM8', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('HHWM8', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #IHDW1
    EnsembleScrape('IHDW1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('IHDW1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #JDAO3
    EnsembleScrape('JDAO3', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('JDAO3', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('JDAO3', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('JDAO3', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #LGDW1
    EnsembleScrape('LGDW1', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('LGDW1', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('LGDW1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('LGDW1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #LGSW1
    EnsembleScrape('LGSW1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('LGSW1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #LMNW1
    EnsembleScrape('LMNW1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('LMNW1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #LYDM8
    EnsembleScrape('LYDM8', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('LYDM8', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('LYDM8', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('LYDM8', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #MCDW1
    EnsembleScrape('MCDW1', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('MCDW1', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('MCDW1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('MCDW1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #TDAO3
    EnsembleScrape('TDAO3', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('TDAO3', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('TDAO3', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('TDAO3', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #ARKI1
    EnsembleScrape('ARKI1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('ARKI1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #LUCI1
    EnsembleScrape('LUCI1', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('LUCI1', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('LUCI1', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('LUCI1', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #CFMM8
    EnsembleScrape('CFMM8', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('CFMM8', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('CFMM8', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('CFMM8', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #ARDQ2I
    EnsembleScrape('ARDQ2I', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('ARDQ2I', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('ARDQ2I', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('ARDQ2I', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #DCDQ2
    EnsembleScrape('DCDQ2', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('DCDQ2', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('DCDQ2', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('DCDQ2', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #MCDQ2
    EnsembleScrape('MCDQ2', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('MCDQ2', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('MCDQ2', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('MCDQ2', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')
    #KERM8I
    EnsembleScrape('KERM8I', 'ESPF0', 'ESP0', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('KERM8I', 'ESPF10', 'ESP10', 'watersupply', 'W', 'Water Supply')
    EnsembleScrape('KERM8I', 'ESPF0', 'ESP0', 'natural', 'N', 'Natural')
    EnsembleScrape('KERM8I', 'ESPF10', 'ESP10', 'natural', 'N', 'Natural')

    os.remove(fptest)
