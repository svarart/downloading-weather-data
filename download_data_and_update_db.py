from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import os
import time
from datetime import datetime, timedelta
import gzip
import csv
import psycopg2

#включение загрузки файлов в headless режиме
def enable_download_in_headless_chrome(driver, download_dir):
  driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
  params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
  driver.execute("send_command", params)

chrome_options = Options()
chrome_options.add_argument("--headless")
#путь где хранятся загруженные данные
download_dir = r'...\Downloads'

meteo_id = ["29430","UNTT","29642","29231","29638"]
table_name = ['"rp5_Tomsk"','"rp5_Bogashevo"','"rp5_Kemerovo"','"rp5_Kolpashevo"','"rp5_Ogurtsovo"']

Tomsk = "https://rp5.ru/%D0%90%D1%80%D1%85%D0%B8%D0%B2_%D0%BF%D0%BE%D0%B3%D0%BE%D0%B4%D1%8B_%D0%B2_%D0%A2%D0%BE%D0%BC%D1%81%D0%BA%D0%B5"
Bogashevo = "https://rp5.ru/%D0%90%D1%80%D1%85%D0%B8%D0%B2_%D0%BF%D0%BE%D0%B3%D0%BE%D0%B4%D1%8B_%D0%B2_%D0%91%D0%BE%D0%B3%D0%B0%D1%88%D1%91%D0%B2%D0%BE,_%D0%B8%D0%BC._%D0%9D._%D0%98._%D0%9A%D0%B0%D0%BC%D0%BE%D0%B2%D0%B0_(%D0%B0%D1%8D%D1%80%D0%BE%D0%BF%D0%BE%D1%80%D1%82),_METAR"
Kemerovo = "https://rp5.ru/%D0%90%D1%80%D1%85%D0%B8%D0%B2_%D0%BF%D0%BE%D0%B3%D0%BE%D0%B4%D1%8B_%D0%B2_%D0%9A%D0%B5%D0%BC%D0%B5%D1%80%D0%BE%D0%B2%D0%B5_(%D0%B0%D1%8D%D1%80%D0%BE%D0%BF%D0%BE%D1%80%D1%82)"
Kolpashevo = "https://rp5.ru/%D0%90%D1%80%D1%85%D0%B8%D0%B2_%D0%BF%D0%BE%D0%B3%D0%BE%D0%B4%D1%8B_%D0%B2_%D0%9A%D0%BE%D0%BB%D0%BF%D0%B0%D1%88%D0%B5%D0%B2%D0%BE"
Ogurtsovo = "https://rp5.ru/%D0%90%D1%80%D1%85%D0%B8%D0%B2_%D0%BF%D0%BE%D0%B3%D0%BE%D0%B4%D1%8B_%D0%B2_%D0%9E%D0%B3%D1%83%D1%80%D1%86%D0%BE%D0%B2%D0%BE"

ChoiceLink = [Tomsk,Bogashevo,Kemerovo,Kolpashevo,Ogurtsovo]

#загрузка данных с rp5
today = datetime.today()
yesterday = today - timedelta(days=1)
print(yesterday.strftime("%d.%m.%Y"))
#для заполнения ячеек на сайте
start_date = yesterday.strftime("%d.%m.%Y")
finish_date = yesterday.strftime("%d.%m.%Y")
for i in range(5):
  driver = webdriver.Chrome(executable_path=os.getcwd() +"\chromedriver.exe", options=chrome_options)
  enable_download_in_headless_chrome(driver, download_dir)
  driver.get(ChoiceLink[i])
  #вкладка скачать архив погоды
  if ChoiceLink[i] != Bogashevo:
    elem = driver.find_element_by_xpath('//*[@id="tabSynopDLoad"]')
  else:
    elem = driver.find_element_by_xpath('//*[@id="tabMetarDLoad"]')
  elem.click()
  #выбор формата csv
  elem = driver.find_element_by_xpath('//*[@id="toFileMenu"]/form/table[2]/tbody/tr[2]/td[3]/label/span')
  elem.click()
  #кодировка utf-8
  elem = driver.find_element_by_xpath('//*[@id="toFileMenu"]/form/table[2]/tbody/tr[3]/td[3]/label/span')
  elem.click()
  #заполение 1-2 ячейки для даты
  elem = driver.find_element_by_xpath('//*[@id="calender_dload"]')
  elem.clear()
  elem.send_keys(start_date)
  elem = driver.find_element_by_xpath('//*[@id="calender_dload2"]')
  elem.clear()
  elem.send_keys(finish_date)
  #кнопка выбрать в файл gz
  elem = driver.find_element_by_xpath('//*[@id="toFileMenu"]/form/table[2]/tbody/tr[3]/td[6]/table/tbody/tr/td[1]/div/div')
  elem.click()
  time.sleep(5)
  #скачать
  elem = driver.find_element_by_xpath('//*[@id="f_result"]/a').click()
  time.sleep(5)
  driver.close()

  #путь, где находится скачанный файл
  path = '.../Downloads/'+meteo_id[i]+'.'+start_date+'.'+finish_date+'.1.0.0.ru.utf8.00000000.csv.gz'
  if os.path.isfile(path):
    #распаковка архива с данными
    with gzip.open(path,'rt',encoding='utf-8', newline="") as fin, open("rewrite.csv", "w", encoding='utf-8', newline="") as fout:
        reader = csv.reader(fin, delimiter=';', quotechar='"')
        writer = csv.writer(fout, delimiter=';', quotechar='"')
        #пропуск первых строк в csv-файле (включая заголовки столбцов)
        for _ in range(6):
            next(reader)
        for row in reader:
            writer.writerow(row[:-1])
        fin.close()
        fout.close()
    #подключение к базе данных
    conn = psycopg2.connect(dbname='meteo_db', user='postgres', password='postgres',host='localhost')
    cur = conn.cursor()
    with open(r'rewrite.csv', 'r', encoding='utf-8', newline='') as f:
        # копирование в таблицу данных
        cur.copy_expert("COPY "+table_name[i]+" FROM STDIN DELIMITER ';' CSV header;",f)
        conn.commit()
        f.close()
    #вывод данных
    cur.execute('SELECT * FROM '+table_name[i]+'')
    for row in cur:
        print(row)
    cur.close()
    conn.close()
