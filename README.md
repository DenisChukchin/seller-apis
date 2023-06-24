# Автоматизация бизнеса
Эта программа служит для автоматизации обновления ассортимента на таких  маркетплейсах как Ozon и YandexMarket.
## seller.py
Программа скачивает текущий ассортимент продавца с маркетплейса [Ozon](https://www.ozon.ru). После чего происходит сравнение остатков и формирование актуальной информации о них (артикул, цена, остаток) с сайта [продавца](https://timeworld.ru). Затем на маркетплейс Ozon обновляется информация о товарах продавца.

Требования:
+ Client_id
+ [Seller Token](https://docs.ozon.ru/global/api/intro/?country=CN)
## market.py

Программа скачивает текущий ассортимент продавца с маркетплейса [Yandex Market](https://market.yandex.ru). После чего происходит сравнение остатков и формирование актуальной информации о них (артикул, цена, остаток) с сайта [продавца](https://timeworld.ru). Затем на маркетплейс Yandex Market обновляется информация о товарах продавца.

В данном маркетплейсе продавец работает на основе моделей [FBS И DBS](https://dzen.ru/a/Y8ZxDdMmXz8CJkRV).

Все основные токены и номера id можно получить используя эту [инструкцию](https://yandex.ru/support/marketplace/orders/fbs/settings/api.html).

Требования:
+ [market_token](https://yandex.ru/dev/market/partner-api/doc/ru/)
+ [campaign_fbs_id](https://yandex.ru/dev/market/partner-api/doc/ru/overview/fbs)
+ [campaign_fbs_id](https://yandex.ru/dev/market/partner-api/doc/ru/overview/dbs)
+ [warehouse_fbs_id](https://yandex.ru/dev/market/partner-api/doc/ru/reference/stocks/updateStocks)
+ [warehouse_dbs_id](https://yandex.ru/dev/market/partner-api/doc/ru/reference/stocks/updateStocks)
