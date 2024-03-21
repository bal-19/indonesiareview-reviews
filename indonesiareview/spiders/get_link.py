import scrapy


class GetLinkSpider(scrapy.Spider):
    name = "get_link"
    allowed_domains = ["indonesiareview.co.id"]
    start_urls = [
        'https://indonesiareview.co.id/makanan-minuman  '
    ]

    def start_request(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)
    
    def parse(self, response):
        all_link = response.css('#companies-reviews > div.bird-company-trending_list > figure > div > figcaption > h3 > strong > a::attr(href)').getall()
        for link in all_link:
            url = response.urljoin(link)
            with open('link.txt', 'a') as f:
                f.write('food & baverage : \t' + url + '\n')
                
        next_page = response.css('#companies-reviews > div.pagination > ul > li').getall()
        if next_page:
            next_page = next_page[-1]
            next_page_link = scrapy.Selector(text=next_page).css('a::attr(href)').get()
            aria_disabled_value = scrapy.Selector(text=next_page).css('li::attr(aria-disabled)').get()
            if aria_disabled_value == 'false':
                yield scrapy.Request(url=response.urljoin(next_page_link), callback=self.parse)
