import ccxt.async as ccxt



_EXCHANGE_URLS = {
    ccxt.bittrex.__name__: '/Market/Index?MarketName={quote}-{base}',
    ccxt.binance.__name__: '/tradeDetail.html?symbol={base}_{quote}'
}


API_RETRY_COUNT = 4
def retrier(f):
    def wrapper(*args, **kwargs):
        count = kwargs.pop('count', API_RETRY_COUNT)
        try:
            return f(*args, **kwargs)
        except Exception as ex:
            if count > 0:
                count -= 1
                kwargs.update({'count': count})
                return wrapper(*args, **kwargs)
            else:
                raise ex
    return wrapper








