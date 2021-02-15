import csv
import re

import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlencode
from urllib.parse import urlparse


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
    proxy = 'http://7c5dc75a899a4f6498d3f238929a3b33:@proxy.crawlera.com:8010/'

    def start_requests(self):
        in_file = open('DataExtracted.csv', 'r', encoding="utf8", errors='ignore')
        file_rows = in_file.read().split('\n')
        for file_row in file_rows[1:]:
            data = file_row.split(',')
            try:
                query = 'site:http://linkedin.com/in/ intitle:"{}" AND "{}" intext:("gmail.com" OR "yahoo.com" OR "hotmail.com")'.format(data[0], data[5].split('and')[0].strip())
                query2 = 'linkedin {} {} {}'.format(data[0], data[4], data[5] )
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
            item['linkedIn'] = result.css('a ::attr(href)').extract_first('').replace('"', '')
            item['Experience'] = data[1]
            item['Salary'] = data[2]
            item['Location'] = data[3]
            item['Designation'] = data[4]
            item['Company'] = data[5]
            item['College'] = data[6]
            item['google_link'] = response.url
            item['skills'] = data[8]

            text = ' '.join(result.css('::text').extract())
            emails = [v for v in text.replace('@ ', '@').split(' ') if '@' in v and '.' in v]
            emails = list(set(emails))
            item['email'] = emails[0] if emails else ''
            item['email'] = item['email'].split(':')[1] if ':' in item['email'] else item['email']
            item['email'] = item['email'][:item['email'].find('.com') + 4] if '.com' in item['email'] else ''
            item['email'] = item['email'].replace('"', '')
            try:
                if (item['email'][0].isnumeric()) or (item['email'][0].isalpha()):

                    item['email'] = item['email']
                else:
                    item['email'] = ''
            except:
                pass
            phones = ', '.join(re.findall(r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]', text)).split(',')

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
