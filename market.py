import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Отправим запрос на Яндекс и получим информацию о товарах в каталоге.

    Args:
        page(str): Идентификатор страницы c результатами
        campaign_id(str): Идентификатор магазина продавца
        access_token(str): API токен продавца

    Results:
        dict: Словарь с информацией о товарах

    Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
        requests.exceptions.Error: Возможны исключения
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Позволяет изменить количество товара в наличии на сайте ЯндексМаркет.

    Args:
        stocks(list): Список с остатками продукции
        campaign_id(str): Идентификатор магазина продавца
        access_token(str): API токен продавца

    Returns:
        response_object(dict): Словарь со статусом подтверждения обновления

    Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
        requests.exceptions.Error: Возможны исключения
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Позволяет изменить цену одного или нескольких товаров продавца на сайте ЯндексМаркет.

    Args:
        prices(list): Список с новыми ценами продукции продавца
        campaign_id(str): Идентификатор магазина продавца
        access_token(str): API токен продавца

    Returns:
        response_object(dict): Словарь со статусом подтверждения обновления

    Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
        requests.exceptions.Error: Возможны исключения
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить из словаря артикулы товаров продавца.

    Args:
        campaign_id(str): Идентификатор магазина продавца
        market_token(str): API токен продавца

    Results:
        offer_ids(list): Список с артикулами товара продавца

    Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Скорректируем остатки продукции учитывая реальное наличие у продавца.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы,
            остатки и цены с сайта часов
        offer_ids(list): Список с артикулами товара продавца с ЯндексМаркет
        warehouse_id(int): Идентификатор хранения товара на складе маркетплейса
            или на складе поставщика

    Returns:
        stocks(list): Сформированный список, в котором учитываются
            реальные остатки продукции для обновления на маркетплейс ЯндексМаркет

    Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Скорректируем цену на продукцию, которая берется с сайта часов.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены
            с сайта часов
        offer_ids(list): Список с артикулами товара продавца с ЯндексМаркет

    Returns:
        prices(list): Сформированный список, в котором цена преображается в нужный формат
            для обновления на маркетплейс ЯндексМаркет

    Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загружаем на маркетплейс ЯндексМаркет обновленный ценник на товары продавца.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены
        campaign_id(str): Идентификатор магазина продавца
        market_token(str): API токен продавца

    Returns:
        prices(list): Список со статусами подтверждения обновления

     Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загружаем на маркетплейс ЯндексМаркет информацию о количестве товара в наличии у продавца.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены
        campaign_id(str): Идентификатор магазина продавца
        market_token(str): API токен продавца

    Returns:
        not_empty(list): Список в котором указана информация о товаре,
            который остался в запасе
        stocks(list): Список с остатками продукции
     Raises:
        SyntaxError: Если отсутствует или введен неправильный аргумент функции
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
