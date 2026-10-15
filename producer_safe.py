import json
import re
from confluent_kafka import Producer

# Настройки подключения (localhost в Docker)
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"ERROR: Ошибка доставки: {err}")
    else:
        print(f"INFO: Сообщение успешно доставлено в топик: {msg.topic()}")

# Регулярное выражение для поиска скрытых номеров телефонов
phone_pattern = re.compile(r'\b\d{1}[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{2}[-.\s]?\d{2}\b|\b\d{11}\b')

# Тестовая выборка данных, расширенная критическими кейсами
messages = [
    {"id": 1, "text": "Привет, как дела?", "source": "user"},
    {"id": 2, "text": "купи слона", "source": "bot"},
    {"id": 3, "text": "", "source": "user"},  # Пустое сообщение для теста валидации
    {"id": 4, "text": "Мой тайный номер 89991234567", "source": "user"},  # Слив приватных данных
    {"id": 5, "text": "Здравствуйте! Чем могу помочь?", "source": "assistant"}
]

print("SYSTEM: Запуск локального шлюза безопасности данных...")

for m in messages:
    msg_id = m.get("id")
    text = m.get("text", "")
    
    # --- ЛОГИКА ВАЛИДАЦИИ И ФИЛЬТРАЦИИ АНОМАЛИЙ ---
    is_valid = True
    reason = ""
    
    # Валидация 1: Проверка теста на пустое сообщение
    if len(text.strip()) == 0:
        is_valid = False
        reason = "Пустой текст сообщения (ошибка полезной нагрузки)"
        
    # Валидация 2: Детектор спама и зацикливания ботов
    elif "купи слона" in text.lower():
        is_valid = False
        reason = "Обнаружена спам-манипуляция / галлюцинация бота"
        
    # Валидация 3: Контроль конфиденциальности и утечки перс. данных
    elif phone_pattern.search(text):
        is_valid = False
        reason = "Обнаружены персональные данные (номер телефона)"
        
    # --- ПРИНЯТИЕ РЕШЕНИЯ И ОТПРАВКА ---
    if is_valid:
        # Если сообщение чистое — кодируем и отправляем в Кафку
        print(f"ALLOW: ID {msg_id}: Данные безопасны. Отправка в брокер...")
        producer.produce('clean_data', json.dumps(m).encode('utf-8'), callback=delivery_report)
    else:
        # Аудит безопасности: блокируем на месте и пишем лог в консоль
        print(f"DENIED: ID {msg_id}: ЗАБЛОКИРОВАНО. Причина: {reason}")

# Дождаться отправки всех чистых сообщений
producer.flush()
print("SYSTEM: Анализ датасета и отправка завершены.")
