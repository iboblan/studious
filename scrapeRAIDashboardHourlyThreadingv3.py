#######################################################################################
#
# Author: Ian Bledsoe
# Date: 8/2/17
#
# Website: http://clatskanie.raiinc.com
# .CSV Location: itv00006\sqlupload$\RAI_Data.csv
#
# Running On:
# Windows 7 Service Pack 1
# Chrome 84
# Chromedriver 84.0.4147.30
# Python 3.4.3
#
########################################################################################

"""

Hello World! (UCB Application Reviewers)
This script is one I wrote that was based on an earlier script (also written by me) that was my first attempt at threading
in a python script. The previous script took too long to run to be useful for users.

The script has to log into an external, proprietary website, scrape several .csvs for each datapoint that are created by javascript on the site.

"""

#This version attempts to fix what I think might be a race condition. Something happens to
#the name of the file between setting oldfile equal to the newest file in the sql$ folder
#and opening that newest file to write into the file named newfile


#Library Import
from selenium import webdriver #3.8.0
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup
import time
import datetime
import os
import keyring #10.4.0
from sqlimport import SqlUpdate
from helpermodule import initializeChrome
from tenacity import *
import threading


class RAIThread(threading.Thread):
    """
    Thread checking URLs.
    """

    def __init__(self, Server, ReportID, Meter, lock):
        """
        Constructor.
        @param urls list of urls to check
        @param output file to write urls output
        """
        threading.Thread.__init__(self)
        self.Server = Server
        self.ReportID = ReportID
        self.Meter = Meter
        self.lock = lock
        

    def run(self):
        """
        Thread run method. Check URLs one by one.
        """

        Server=self.Server
        ReportID=self.ReportID
        Meter=self.Meter
        lock=self.lock

        begmonth=((datetime.datetime.today()).replace(day=1, hour=0, minute=0, second=0,microsecond=0))

        if int((datetime.datetime.today() - begmonth).total_seconds()/60/60) < 3: #Only do in first couple hrs of month
            print("Doing code for first couple hrs.")
            try:        
                if Server=='Local':
                    loginurl="http://192.168.105.69/DC3/login.aspx?ReturnUrl=%2fdc3%2fdefault.aspx"

                if Server=='Cloud':
                    loginurl='http://clatskanie.raiinc.com'

                filepath=r'\\itv00006\sqlupload$'
                driver = initializeChrome(filepath)

                driver.get(loginurl)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_Login1_UserName"))
                ).send_keys("ibledsoe")
                driver.find_element_by_id("ctl00_ContentPlaceHolder1_Login1_Password").send_keys(keyring.get_password('RAI', 'ibledsoe')) #Enters password with keyring
                driver.find_element_by_id("ctl00_ContentPlaceHolder1_Login1_LoginButton").click()
                driver.get("http://clatskanie.raiinc.com/overviews/customers/overview2.aspx?overviewID=" + ReportID)
     
                driver.switch_to.frame(driver.find_element_by_id("iFrameInclude"))

                today=(datetime.date.today()).strftime('%m/%d/%Y')  
                thismonth=datetime.date.today().replace(day=1)
                lastmonth=((((datetime.date.today()).replace(day=1))-datetime.timedelta(days=1)).replace(day=1)) #Sets the start of last month
                nextmonth=(((datetime.date.today()).replace(day=1))+datetime.timedelta(days=31)).replace(day=1)
                
                lastmonthfilestring=filepath + '//' + lastmonth.strftime('HourlyLoads_%Y_%m_%d_00_00.csv')
                thismonthfilestring=filepath + '//' + thismonth.strftime('HourlyLoads_%Y_%m_%d_00_00.csv')
                nextmonthfilestring=filepath + '//' + nextmonth.strftime('HourlyLoads_%Y_%m_%d_00_00.csv')
            
                #newfile=r'\\itv00006\sqlupload$\RAI_Data.csv' #Set new file name

                z=WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH,
                    '//*[@id="ctl00_ContentPlaceHolder1_LinkButton_DownloadCSV"]')) 
                )
                lock.acquire() #lock other threads from writing to file at the same time
                print("Locked for ", Meter, " part 1.", flush=True)
                if os.path.isfile(thismonthfilestring):
                    os.remove(thismonthfilestring)
                if os.path.isfile(nextmonthfilestring):
                    os.remove(nextmonthfilestring)
                z.click()
                print("Download clicked for", Meter, "part 1.", flush=True)
                timecounter=0
                while not os.path.isfile(thismonthfilestring) and timecounter<20:
                    timecounter+=1
                    time.sleep(timecounter)
                    if timecounter==20:
                        raise TimeoutException
                print("File found for", Meter, "part 1.", flush=True)
                cleanFile(thismonthfilestring, thismonth, Meter)
                os.remove(thismonthfilestring)
                print("File cleaned, old removed for", Meter, "part 1.", flush=True)
                SqlUpdate('Load_RAI_60min_Dashboard')
                print("Unlocking for ", Meter, " part 1.", flush=True)
                lock.release()
                
                
                WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located((By.LINK_TEXT,
                    'Previous')) 
                ).click()
            
                #EC.text_to_be_present_in_element((By.ID, "operations_monitoring_tab_current_ct_fields_no_data"), "No data to display")
                WebDriverWait(driver, 20).until(
                    EC.text_to_be_present_in_element((By.ID,
                                        "ctl00_ContentPlaceHolder1_Label_Month")
                                        ,lastmonth.strftime('%B %Y')))       
                lock.acquire()
                print("Locked for ", Meter, " part 2.", flush=True)
                if os.path.isfile(lastmonthfilestring): #had to move this after lock to prevent other threads' interfering
                    os.remove(lastmonthfilestring)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH,
                    '//*[@id="ctl00_ContentPlaceHolder1_LinkButton_DownloadCSV"]')) 
                ).click()
                timecounter=0
                while not os.path.isfile(lastmonthfilestring) and timecounter<20:
                    timecounter+=1
                    time.sleep(timecounter)
                cleanFile(lastmonthfilestring, lastmonth, Meter)
                os.remove(lastmonthfilestring)
                SqlUpdate('Load_RAI_60min_Dashboard')
                print("Unlocking for ", Meter, " part 2.", flush=True)
                lock.release()
                
            finally:
                driver.quit()
        else: #not the first couple hours of the month, so no need to scrape previous
            print("Doing normal code.")
            try:        
                if Server=='Local':
                    loginurl="http://192.168.105.69/DC3/login.aspx?ReturnUrl=%2fdc3%2fdefault.aspx"

                if Server=='Cloud':
                    loginurl='http://clatskanie.raiinc.com'

                filepath=r'\\itv00006\sqlupload$'
                driver = initializeChrome(filepath)

                driver.get(loginurl)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_Login1_UserName"))
                ).send_keys("ibledsoe")
                driver.find_element_by_id("ctl00_ContentPlaceHolder1_Login1_Password").send_keys(keyring.get_password('RAI', 'ibledsoe')) #Enters password with keyring
                driver.find_element_by_id("ctl00_ContentPlaceHolder1_Login1_LoginButton").click()
                driver.get("http://clatskanie.raiinc.com/overviews/customers/overview2.aspx?overviewID=" + ReportID)
     
                driver.switch_to.frame(driver.find_element_by_id("iFrameInclude"))

                today=(datetime.date.today()).strftime('%m/%d/%Y')  
                thismonth=datetime.date.today().replace(day=1)
                lastmonth=((((datetime.date.today()).replace(day=1))-datetime.timedelta(days=1)).replace(day=1)) #Sets the start of last month
                nextmonth=(((datetime.date.today()).replace(day=1))+datetime.timedelta(days=31)).replace(day=1)
                
                lastmonthfilestring=filepath + '//' + lastmonth.strftime('HourlyLoads_%Y_%m_%d_00_00.csv')
                thismonthfilestring=filepath + '//' + thismonth.strftime('HourlyLoads_%Y_%m_%d_00_00.csv')
                nextmonthfilestring=filepath + '//' + nextmonth.strftime('HourlyLoads_%Y_%m_%d_00_00.csv')
            
                #newfile=r'\\itv00006\sqlupload$\RAI_Data.csv' #Set new file name

                z=WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH,
                    '//*[@id="ctl00_ContentPlaceHolder1_LinkButton_DownloadCSV"]')) 
                )
                lock.acquire() #lock other threads from writing to file at the same time
                print("Locked for ", Meter, " part 1.", flush=True)
                if os.path.isfile(thismonthfilestring):
                    os.remove(thismonthfilestring)
                if os.path.isfile(nextmonthfilestring):
                    os.remove(nextmonthfilestring)
                z.click()
                print("Download clicked for", Meter, "part 1.", flush=True)
                timecounter=0
                while not os.path.isfile(thismonthfilestring) and timecounter<20:
                    timecounter+=1
                    time.sleep(timecounter)
                    if timecounter==20:
                        raise TimeoutException
                print("File found for", Meter, "part 1.", flush=True)
                cleanFile(thismonthfilestring, thismonth, Meter)
                os.remove(thismonthfilestring)
                print("File cleaned, old removed for", Meter, "part 1.", flush=True)
                SqlUpdate('Load_RAI_60min_Dashboard')
                print("Unlocking for ", Meter, " part 1.", flush=True)
                lock.release()
                
            finally:
                driver.quit()
            
def cleanFile(oldfile, monthobject, meter):
    newfile=r'\\itv00006\sqlupload$\RAI_Data_Dashboard.csv'
    
    with open(oldfile, 'r') as junkfile:
        #print(junkfile.read()[:2])
        with open(newfile, 'w') as file:
            file.truncate()
            for dayrow, tr in enumerate(junkfile.read().split('\n')[3:-3],1):                
                goodrow=(monthobject.replace(day=dayrow).strftime('%m/%d/%Y') + ',' +
                         str(tr.split(',')[1:-3]).replace("'", "").replace("[", "").replace("]", "") + ',' +
                         meter  + ',' +
                         "RAI\n")
                file.write(goodrow)    
            
def main():
    start_time = time.time()
    time.sleep(60)
    lock = threading.Lock()
    t1 = RAIThread('Cloud', '3', 'CLSKSystemLoad', lock)    
    t2 = RAIThread('Cloud', '13', 'WaunaCogenNet', lock)
    t3 = RAIThread('Cloud', '21', 'HalseyAMR', lock)
    t4 = RAIThread('Cloud', '14', 'CamasGross', lock)
    #t5 = RAIThread('Cloud', '16', 'CamasCogen', lock)
          
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    #t5.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    #t5.join()
    print(str((time.time() - start_time)/60), "minutes elapsed.")

main()
