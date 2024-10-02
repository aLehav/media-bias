import scrapy
from scrapy_splash import SplashRequest 

lua_script = """
function main(splash, args)
    assert(splash:go(args.url))

  while not splash:select('div.quote') do
    splash:wait(0.1)
    print('waiting...')
  end
  return {html=splash:html()}
end
"""

class QuoteItem(scrapy.Item):
    # define the fields for your item here like:
    text = scrapy.Field()
    author = scrapy.Field()
    tags = scrapy.Field()

class QuotesSpider(scrapy.Spider):
	name = 'quotes'

	def start_requests(self):
		url = 'https://quotes.toscrape.com/js/'
		yield SplashRequest(
			url, 
			callback=self.parse, 
			endpoint='execute',
			args={'wait':0.5, 'lua_source': lua_script, url :'https://quotes.toscrape.com/scroll'}
		)

	def parse(self, response):
		quote_item = QuoteItem()
		for quote in response.css('div.quote'):
			quote_item['text'] = quote.css('span.text::text').get()
			quote_item['author'] = quote.css('small.author::text').get()
			quote_item['tags'] = quote.css('div.tags a.tag::text').getall()
			yield quote_item

