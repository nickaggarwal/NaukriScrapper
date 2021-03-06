import csv
import re

import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlencode
from urllib.parse import urlparse

import warnings
warnings.simplefilter(action='ignore', category=Warning)

from scrapy import Selector
import xlsxwriter
import pandas as pd
import os


workbook = xlsxwriter.Workbook('DataExtracted.xlsx')
worksheet = workbook.add_worksheet()
worksheet.write(0, 0, "Name")
worksheet.write(0, 1, "Experience")
worksheet.write(0, 2, "Salary")
worksheet.write(0, 3, "Location")
worksheet.write(0, 4, "Designation")
worksheet.write(0, 5, "Company")
worksheet.write(0, 6, "College")
worksheet.write(0, 7, "Skills")

BASE_DIR = './Data'


def generate(pages):
    rowN = 1
    file_name = "Resdex - Search Result Page.html"

    for page in pages:

        try:
            base_file_name = os.path.join(page, file_name)
            print(base_file_name)
            with open(base_file_name, 'r', encoding='utf-8_sig', newline='') as f:
                page = f.read()
                response = Selector(text=str(page))

            rows = response.css("div.tuple")
            print(len(rows))
            for row in rows:
                try:
                    name = row.css("a.userName::text").extract_first()
                    exp = row.css("span.exp::text").extract_first()
                    exp = exp.split("yr")
                    exp = exp[0].strip() + "." + \
                        exp[-1].strip().replace("m", "")

                    sal = row.css("span.sal::text").extract_first()
                    sal = sal.split(" ")[0].strip()
                    loc = row.css("span.loc::text").extract_first()
                    desig2 = row.css(
                        "div.currInfo > a.designation::text").extract()
                    desig = row.css(
                        "div.currInfo > a.designation > em::text").extract()
                    desig = desig + desig2
                    desig = " ".join(desig)
                    company = row.css(
                        "div.desc.currInfo > a:nth-of-type(2)::text").extract_first()
                    skills = row.css(
                        "div.desc.kSklsInfo > a.skillkey::text").extract()
                    if not company:
                        company = ""
                    college = row.css("em.ugIns::text").extract_first()
                    year = row.css("div.desc.eduInfo::text").extract()[-1]
                    worksheet.write(rowN, 0, name.strip())
                    worksheet.write(rowN, 1, exp.strip())
                    worksheet.write(rowN, 2, sal.strip())
                    worksheet.write(rowN, 3, loc.strip())
                    worksheet.write(rowN, 4, desig.strip())
                    worksheet.write(rowN, 5, company.strip())
                    worksheet.write(rowN, 6, college.strip() +
                                    ", " + year.strip())
                    worksheet.write(rowN, 7, ';'.join(skills))
                    rowN += 1
                except Exception as ex:
                    print(name + " Extracted...")

                print(name + " Extracted...")
        except Exception as ex:
            print("Page not found")


def getpages():
    pages = []
    for path in os.listdir(BASE_DIR):
        print(os.path.join(BASE_DIR, path))
        pages.append(os.path.join(BASE_DIR, path))
    return pages


pages = getpages()
generate(pages)

workbook.close()

read_file = pd.read_excel("DataExtracted.xlsx", index_col=0)

# Write the dataframe object
# into csv file
read_file.to_csv("Test.csv",
                 index=None,
                 header=True)

# read csv file and convert
# into a dataframe object
df = pd.DataFrame(pd.read_csv("DataExtracted.csv"))


headers_csv = ["Name", "email", "phone", "linkedIn", "Experience", "Salary", "Location", "Designation", "Company",
               "College", "google_link", "skills"]

fileout = open('data.csv', 'w', newline='', encoding='utf-8')
writer = csv.DictWriter(fileout, fieldnames=headers_csv)
writer.writeheader()


def create_google_url(query, site=''):
    google_dict = {'q': query}
    if site:
        web = urlparse(site).netloc
        google_dict['as_sitesearch'] = web
        return 'https://www.google.com/search?' + urlencode(google_dict)
    return 'https://www.google.com/search?' + urlencode(google_dict)


class GoogleSpider(scrapy.Spider):
    name = 'google'
    allowed_domains = ['api.scraperapi.com']
    custom_settings = {'CLOSESPIDER_PAGECOUNT': 150000, 'ROBOTSTXT_OBEY': False, 'DOWNLOAD_TIMEOUT': 1000,
                       'CONCURRENT_REQUESTS': 16, 'AUTOTHROTTLE_ENABLED': False,
                       'CONCURRENT_REQUESTS_PER_DOMAIN': 16,
                       'RETRY_TIMES': 5, 'RETRY_HTTP_CODES': ['429']}
    proxy = 'http://c35231a6a3d54187bc679772c3ab19d9:@proxy.crawlera.com:8011/'

    def start_requests(self):
        in_file = open('DataExtracted.csv', 'r',
                       encoding="utf8", errors='ignore')
        file_rows = in_file.read().split('\n')
        for file_row in file_rows[1:]:
            data = file_row.split(',')
            try:
                query = 'site:http://linkedin.com/in/ intitle:"{}" AND "{}" intext:("gmail.com" OR "yahoo.com" OR "hotmail.com")'.format(
                    data[0], data[5].split('and')[0].strip())
                query2 = 'linkedin {} {} {}'.format(data[0], data[4], data[5])
                url = create_google_url(query)
                url2 = create_google_url(query2)
                #headers = {'X-Crawlera-Cookies': 'disable'}

                yield scrapy.Request(url, callback=self.parse, meta={'data': data, 'proxy': self.proxy, 'url2': url2,
                                                                     },
                                     )
            except Exception as ex:
                print("Could not Parse")

    def parse(self, response):
        data = response.meta['data']
        # for result in response.css('.rc')[:1]:
        for result in response.css('.g')[:1]:
            item = dict()
            item['Name'] = data[0]
            item['linkedIn'] = result.css(
                'a ::attr(href)').extract_first('').replace('"', '')
            item['Experience'] = data[1]
            item['Salary'] = data[2]
            item['Location'] = data[3]
            item['Designation'] = data[4]
            item['Company'] = data[5]
            item['College'] = data[6]
            item['google_link'] = response.url
            item['skills'] = data[8]

            text = ' '.join(result.css('::text').extract())
            emails = [v for v in text.replace(
                '@ ', '@').split(' ') if '@' in v and '.' in v]
            emails = list(set(emails))
            item['email'] = emails[0] if emails else ''
            item['email'] = item['email'].split(
                ':')[1] if ':' in item['email'] else item['email']
            item['email'] = item['email'][:item['email'].find(
                '.com') + 4] if '.com' in item['email'] else ''
            item['email'] = item['email'].replace('"', '')
            try:
                if (item['email'][0].isnumeric()) or (item['email'][0].isalpha()):

                    item['email'] = item['email']
                else:
                    item['email'] = ''
            except:
                pass
            phones = ', '.join(re.findall(
                r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]', text)).split(',')

            item['phone'] = phones[0].replace('"', '') if phones else ''
            try:
                if len(item['phone']) > 8:
                    item['phone'] = item['phone'].replace(" ", '')
                else:
                    item['phone'] = ''
            except:
                pass
            writer.writerow(item)
            fileout.flush()
        # if not response.css('.rc') and response.meta.get('url2'):
        if not response.css('.g') and response.meta.get('url2'):
            headers = {'X-Crawlera-Cookies': 'disable'}
            yield scrapy.Request(response.meta['url2'], callback=self.parse, meta={'data': response.meta['data'],
                                                                                   'proxy': self.proxy},
                                 headers=headers, dont_filter=True)


process = CrawlerProcess({
    'USER_AGENT': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36"
})

process.crawl(GoogleSpider)
process.start()
