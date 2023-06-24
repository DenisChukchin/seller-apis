import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Отправить запрос на сайт озон и получить список товаров магазина.

    Args:
        last_id(str): Идентификатор последнего значения на странице
        client_id(str): Идентификатор клиента
        seller_token(str): API-ключ

    Returns:
        dict: Результат запроса в виде массива данных JSON

    Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции
        ReadTimeout: Превышено время ожидания
        ConnectionError: Ошибка соединения

    Example:
        >>> env = Env()
        >>> get_product_list("", env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        >>> {...}
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Из массива данных JSON получить артикулы товаров магазина озон.

    Args:
        client_id(str): Идентификатор клиента
        seller_token(str): API-ключ

    Returns:
        offer_ids(list): Список с артикулами товара продавца

    Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции

    Example:
        >>> env = Env()
        >>> get_offer_ids(, env.str("CLIENT_ID"), env.str("SELLER_TOKEN"))
        >>> ["143210608", "91132", "136748"...,"137208233"]
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Позволяет изменить цену одного или нескольких товаров продавца на сайте Озон.

    Args:
        prices(list): Список с новыми ценами продукции продавца
        client_id(str): Идентификатор клиента
        seller_token(str): API-ключ

    Returns:
        dict: Словарь с массивом данных JSON, в котором указаны данные, например,
            как идентификатор товара, артикул товара, подтверждение об обновлении и
            возможные ошибки

    Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции
        ReadTimeout: Превышено время ожидания
        ConnectionError: Ошибка соединения
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Позволяет изменить количество товара в наличии на сайте Озон.

    Args:
        stocks(list): Список с остатками продукции
        client_id(str): Идентификатор клиента
        seller_token(str): API-ключ

    Returns:
        dict: Словарь с массивом данных JSON, в котором указаны данные, например,
            как идентификатор товара, артикул товара, подтверждение об обновлении и
            возможные ошибки

    Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции
        ReadTimeout: Превышено время ожидания
        ConnectionError: Ошибка соединения
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Отправить запрос на сайт часов и сформировать актуальный список по остаткам товара.

    Returns:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены

    Raises:
        ReadTimeout: Превышено время ожидания
        ConnectionError: Ошибка соединения
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Скорректируем остатки продукции учитывая реальное наличие у продавца.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены
            с сайта часов
        offer_ids(lict): Список с артикулами товара маркетплейса Озон

    Returns:
        stocks(list): Сформированный список, в котором учитываются
            реальные остатки продукции для обновления на маркетплейс Озон

    Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Скорректируем цену на продукцию, которая берется с сайта часов.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены
            с сайта часов
        offer_ids(lict): Список с артикулами товара маркетплейса Озон

    Returns:
        prices(list): Сформированный список, в котором цена преображается в нужный формат
            для обновления на маркетплейс Озон

    Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует цену в упрощенный вид для загрузки на маркетплейс Озон.

    Args:
        price(str): Цена с дробной частью и с припиской руб
    Returns:
          str: Цена в виде целого числа
    Raises:
        ValueError: price must be str, not int
    Examples:
        >>> price_conversion("5'990.00 руб.")
        >>> "5990"
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на n частей.

    Args:
        lst(list): Список, который будем делить
        n(int): Число на которое будем делить/дробить список

    Returns:
        list: Список, который разделен на n-частей

    Raises:
        ZeroDivisionError: На ноль делить нельзя
        ValueError: n must be int, not str
        AttributeError: Если отсутствует или введен неправильный аргумент функции
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружаем на маркетплейс Озон обновленный ценник на товары продавца.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены
        client_id(str): Идентификатор клиента
        seller_token(str): API-ключ

    Returns:
        prices(list): Список из словарей, в котором указаны данные, такие как
            идентификатор товара, артикул товара, подтверждение об обновлении и
            возможные ошибки

     Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружаем на маркетплейс Озон информацию о количестве товара в наличии у продавца.

    Args:
        watch_remnants(dict): Словарь, который содержит актуальные артикулы, остатки и цены
        client_id(str): Идентификатор клиента
        seller_token(str): API-ключ

    Returns:
        not_empty(list): Список из словарей, в котором указана информация о товаре,
            который остался в запасе
        stocks(list): Список из словарей, в котором указаны данные, такие как
            идентификатор товара, артикул товара, подтверждение об обновлении и
            возможные ошибки

     Raises:
        AttributeError: Если отсутствует или введен неправильный аргумент функции
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
