BOT_NAME = 'linux_scraper'

SPIDER_MODULES = ['linux_scraper.spiders']
NEWSPIDER_MODULE = 'linux_scraper.spiders'

ROBOTSTXT_OBEY = False

ITEM_PIPELINES = {
    'linux_scraper.pipelines.LinuxScraperPipeline': 300,
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
FEED_EXPORT_ENCODING = 'utf-8'
