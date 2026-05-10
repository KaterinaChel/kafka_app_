import faust
import asyncio
from typing import Dict, Any

APP_NAME = "product_filter_app"
KAFKA_BROKER = "kafka://localhost:9092"

app = faust.App(APP_NAME, broker=KAFKA_BROKER)

# Топики
products_topic = app.topic("shop_products", value_type=Dict[str, Any])
filtered_products_topic = app.topic("filtered_products", value_type=Dict[str, Any])

# Хранилище для стоп-листа (In-memory для простоты, можно заменить на Redis/BDRelational)
banned_products = set()

@app.agent(products_topic)
async def filter_products(stream):
    async for product in stream:
        product_name = product.get('name', '').lower()
        product_id = product.get('product_id')
        
        # Проверка по имени или ID
        if product_name in banned_products or product_id in banned_products:
            print(f"Товар {product_id} ЗАБЛОКИРОВАН. Пропускаем.")
            continue # Не отправляем дальше
            
        print(f"Товар {product_id} разрешен. Отправляем в {filtered_products_topic.name}")
        await filtered_products_topic.send(value=product)

@app.page('/ban/{product_name}')
@app.page('/ban')
async def manage_banlist(self, request, product_name: str = None):
    """Простой CLI-интерфейс через HTTP для управления стоп-листом"""
    if request.method == 'POST':
        if product_name:
            banned_products.add(product_name.lower())
            return self.json({'status': 'added', 'item': product_name})
    elif request.method == 'DELETE':
        if product_name and product_name in banned_products:
            banned_products.remove(product_name.lower())
            return self.json({'status': 'removed', 'item': product_name})
            
    return self.json({'banned_products': list(banned_products)})

if __name__ == '__main__':
    app.main()