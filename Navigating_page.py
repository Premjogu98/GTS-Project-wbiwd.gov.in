from typing import List , Any
from selenium import webdriver
import time
import re
import sys , os
from datetime import datetime
import Global_var
from Insert_On_databse import insert_in_Local
import string
import ctypes
import html


def Choromedriver():
    # File_Location = open("D:\\0 PYTHON EXE SQL CONNECTION & DRIVER PATH\\wbiwd.gov.in\\Location For Database & Driver.txt" , "r")
    # TXT_File_AllText = File_Location.read()
    # Chromedriver = str(TXT_File_AllText).partition("Driver=")[2].partition("\")")[0].strip()
    # browser = webdriver.Chrome(Chromedriver)
    browser = webdriver.Chrome(executable_path=str(f"D:\\Translation EXE\\chromedriver.exe"))
    browser.get('https://wbiwd.gov.in/index.php/applications/tenders/')
    browser.maximize_window()
    time.sleep(2)
    Scraping_data(browser)


def Scraping_data(browser):

    a = 0
    while a == 0:
        try:
            for next in range(2 , 10 , 2):
                elements = "https://wbiwd.gov.in/index.php/applications/tenders/"+str(next)+"0"
                # time.sleep(3)
                SagField = []
                for data in range(42):
                    SagField.append('')
                for add in range(2, 24, 1):
                    try:
                        for Opening_Date in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[5]"):
                            Opening_Date = Opening_Date.get_attribute("innerText")
                            Opening_Date = str(Opening_Date[0:10])
                            datetime_object = datetime.strptime(Opening_Date , '%Y-%m-%d')
                            Opening_Date = datetime_object.strftime("%Y-%m-%d")
                            SagField[3] = "NA" + "~" + "NA" + "~" + "NA" + "~" + "NA" + "~" + Opening_Date
                            break
                    except:
                        for Opening_Date in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[4]"):
                            Opening_Date = Opening_Date.get_attribute("innerText")
                            Opening_Date = str(Opening_Date[0:10])
                            datetime_object = datetime.strptime(Opening_Date , '%Y-%m-%d')
                            Opening_Date = datetime_object.strftime("%Y-%m-%d")
                            SagField[3] = "NA" + "~" + "NA" + "~" + "NA" + "~" + "NA" + "~" + Opening_Date
                            break
                    publish_date1 = ''
                    try:
                        for publish_date in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[3]"):
                            publish_date = publish_date.get_attribute("innerText")
                            publish_date = str(publish_date[0:10])
                            datetime_object = datetime.strptime(publish_date , '%Y-%m-%d')
                            publish_date1 = datetime_object.strftime("%Y-%m-%d")
                            break
                    except:
                        for publish_date in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[2]"):
                            publish_date = publish_date.get_attribute("innerText")
                            publish_date = str(publish_date[0:10])
                            datetime_object = datetime.strptime(publish_date , '%Y-%m-%d')
                            publish_date1 = datetime_object.strftime("%Y-%m-%d")
                            break
                    if publish_date1 >= Global_var.From_Date:
                        print("Publish Date Alive")
                    else:
                        print("Publish Date Dead")
                        ctypes.windll.user32.MessageBoxW(0, "Total: " + str(
                                Global_var.Total) + "\n""Duplicate: " + str(
                                Global_var.duplicate) + "\n""Expired: " + str(
                                Global_var.expired) + "\n""Inserted: " + str(
                                Global_var.inserted) + "\n""Skipped: " + str(
                                Global_var.skipped) + "\n""Deadline Not given: " + str(
                                Global_var.deadline_Not_given) + "\n""QC Tenders: "
                                                             + str(Global_var.QC_Tenders) + "", "wbiwd.gov.in",
                                                             1)
                        Global_var.Process_End()
                        browser.close()
                        sys.exit()
                    #  ====================================== THIS INFORMATION FOR HTML FILE PURPOSE ================================================================================

                    for tenderdate in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[3]"):
                        tenderdate = tenderdate.get_attribute("innerText")
                        SagField[4] = tenderdate
                        break
                    for Tender_Opening_Date in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[5]"):
                        Tender_Opening_Date = Tender_Opening_Date.get_attribute("innerText")
                        SagField[5] = Tender_Opening_Date
                        break
                    for Document in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[7]"):
                        Document = Document.get_attribute('outerHTML')
                        Document = re.search(r'(?<=<a href=").*?(?=" target=)' , Document).group(0)
                        SagField[6] = Document
                        break
                        #  ==========================================THIS INFORMATION FOR HTML FILE PURPOSE==================================================================================================

                    SagField[7] = "IN"
                    SagField[12] = "IRRIGATION & WATERWAYS DEPARTMENT OF WEST BENGAL"
                    try:
                        for NIT_No in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[2]"):
                            NIT_No = NIT_No.get_attribute("innerText")
                            SagField[13] = NIT_No.strip()
                            break
                    except:
                        for NIT_No in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[4]/td[2]"):
                            NIT_No = NIT_No.get_attribute("innerText")
                            SagField[13] = NIT_No.strip()
                            break
                        SagField[14] = "2".strip()  # notice_type
                    Details = ""
                    for Details in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[6]"):
                        Details = Details.get_attribute("innerText")
                        Details = string.capwords(str(Details)).strip()
                        break
                    for Tender_Opening_Date in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[5]"):
                        Tender_Opening_Date = Tender_Opening_Date.get_attribute("innerText")
                        SagField[18] = Details.strip() + "<br>\n""Tender Opening Date: " + Tender_Opening_Date.strip()
                        break
                    SagField[19] = Details.strip()
                    SagField[20] = "0.0"
                    SagField[22] = "0.0"

                    for Tender_Submission_Date in browser.find_elements_by_xpath("/html/body/div[1]/div[5]/div[1]/div/table/tbody/tr[" + str(add) + "]/td[4]"):
                        Tender_Submission_Date = Tender_Submission_Date.get_attribute("innerText")
                        Tender_Submission_Date = "" + str(Tender_Submission_Date)
                        Tender_Submission_Date = str(Tender_Submission_Date[0:10])
                        if Tender_Submission_Date != '':
                            datetime_object = datetime.strptime(Tender_Submission_Date , '%Y-%m-%d')
                            Tender_Submission_Date = datetime_object.strftime("%Y-%m-%d")
                            SagField[24] = Tender_Submission_Date.strip()
                            break
                        else:
                            SagField[24] = ""
                            break
                        break
                    SagField[26] = "0.0"
                    SagField[27] = "0"  # Financier
                    SagField[28] = "https://wbiwd.gov.in/index.php/applications/tenders/"

                    SagField[31] = "wbiwd.gov.in"
                    SagField[36] = "45200000"
                    for SegIndex in range(len(SagField)):
                        print(SegIndex, end=' ')
                        print(SagField[SegIndex])
                        SagField[SegIndex] = html.unescape(str(SagField[SegIndex]))
                        SagField[SegIndex] = str(SagField[SegIndex]).replace("'", "''")
                    Global_var.Total += 1
                    check_date(SagField)
                    print(" Total: " + str(
                                Global_var.Total) + " Duplicate: " + str(
                                Global_var.duplicate) + " Expired: " + str(
                                Global_var.expired) + " Inserted: " + str(
                                Global_var.inserted) + " Skipped: " + str(
                                Global_var.skipped) + " Deadline Not given: " + str(
                                Global_var.deadline_Not_given) + " QC Tenders: " + str(Global_var.QC_Tenders),'\n')
                    a = 1
                for next_button in browser.find_elements_by_xpath(elements):
                    next_button.click()
            # sys.exit()
        except Exception as e:
            a = 0
            exc_type , exc_obj , exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print("Error ON : " , sys._getframe().f_code.co_name + "--> " + str(e) , "\n" , exc_type , "\n" , fname ,"\n" , exc_tb.tb_lineno)


def check_date(SagField):
    tender_date = str(SagField[24])
    nowdate = datetime.now()
    date2 = nowdate.strftime("%Y-%m-%d")
    try:
        if tender_date != '':
            deadline = time.strptime(tender_date , "%Y-%m-%d")
            currentdate = time.strptime(date2 , "%Y-%m-%d")
            if deadline > currentdate:
                insert_in_Local(SagField)
            else:
                print("Tender Expired")
                Global_var.expired += 1
        else:
            print("l")
            Global_var.skipped += 1
            Global_var.deadline_Not_given += 1
    except Exception as e:
        # Global_var.Process_End()
        exc_type , exc_obj , exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("Error ON : " , sys._getframe().f_code.co_name + "--> " + str(e) , "\n" , exc_type , "\n" , fname , "\n" ,
              exc_tb.tb_lineno)

Choromedriver()
