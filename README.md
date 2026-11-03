# Асинхронный сервис процессинга платежей

Тестовый микросервис для асинхронной обработки платежей. API принимает запрос на создание платежа, сохраняет платёж и событие Outbox в PostgreSQL. Отдельный Publisher публикует событие в RabbitMQ, Consumer эмулирует обработку платежа и отправляет результат клиенту через webhook.

## Возможности

- Создание платежей и получение их статуса через REST API.
- Статический API-ключ в заголовке `X-API-Key`.
- Идемпотентность создания платежей через обязательный `Idempotency-Key`.
- PostgreSQL и SQLAlchemy 2.x в асинхронном режиме.
- Alembic для миграций базы данных.
- Outbox pattern для согласованной записи платежа и события.
- Отдельный Publisher для публикации событий в RabbitMQ.
- Consumer на FastStream для асинхронной обработки платежей.
- Эмуляция платёжного шлюза: задержка 2–5 секунд, вероятность успеха 90%.
- Webhook с тремя HTTP-попытками и экспоненциальной задержкой.
- Dead Letter Exchange (DLX) и Dead Letter Queue (DLQ) для окончательно отклонённых сообщений.
- Docker Compose для запуска API, Publisher, Consumer, PostgreSQL и RabbitMQ.

## Технологии

- Python 3.12
- FastAPI, Pydantic v2
- SQLAlchemy 2.x (async), asyncpg
- PostgreSQL 16
- RabbitMQ 4.3.x, FastStream
- Alembic
- HTTPX
- Poetry
- Docker, Docker Compose

## Архитектура

```text
Клиент
  |
  | POST /api/v1/payments
  v
FastAPI
  |
  | одна транзакция PostgreSQL
  v
+-----------------------+
| Payment               |
| Outbox event          |
+-----------------------+
  |
  v
Outbox Publisher
  |
  | публикация события + publisher confirms
  v
RabbitMQ: payments.new (Quorum Queue)
  |
  v
Consumer (FastStream)
  |
  +---- эмуляция обработки платежа (2–5 сек.)
  |
  +---- POST webhook (если задан webhook_url)
  |
  v
PostgreSQL: обновлённый статус платежа

Ошибки, требующие помещения в DLQ
  |
  v
payments.dlx (Direct Exchange)
  |
  v
payments.dlq
```

### Создание платежа и Outbox pattern

При создании платежа API записывает `Payment` и событие Outbox в одной транзакции PostgreSQL. Поэтому платёж и событие фиксируются атомарно: нельзя успешно сохранить платёж, но потерять соответствующее событие из-за сбоя между двумя отдельными операциями записи.

Отдельный Publisher выбирает неопубликованные события Outbox и публикует их в RabbitMQ. Для публикации включены publisher confirms и обработка возврата не маршрутизированных сообщений. После подтверждённой публикации событие помечается как опубликованное. Если публикация не удалась, событие остаётся доступным для следующей попытки Publisher.

Чтобы не держать транзакцию открытой во время сетевых вызовов, Publisher выбирает события через `SELECT ... FOR UPDATE SKIP LOCKED`, устанавливает аренду (`claimed_until`) на 2 минуты и сразу фиксирует транзакцию. Другие экземпляры Publisher пропускают события с действующей арендой. Если Publisher завершился аварийно, аренда истекает, и событие может забрать другой экземпляр. Если процесс завершился после публикации в RabbitMQ, но до записи `published_at`, событие будет опубликовано повторно: используется гарантия at-least-once. Поэтому Consumer идемпотентен, см. раздел „Идемпотентность и доставка webhook“. Размер пачки (20 событий) и таймаут публикации (3 секунды на событие) рассчитаны так, чтобы обработка пачки укладывалась в срок аренды.

### Обработка платежа

Consumer получает сообщение из `payments.new`, извлекает `payment_id` и загружает платёж из PostgreSQL.

Если статус платежа `pending`, Consumer эмулирует обращение к платёжному шлюзу:

- задержка — случайное значение от 2 до 5 секунд;
- вероятность `succeeded` — 90%;
- вероятность `failed` — 10%.

Полученный статус и время обработки сохраняются в БД. Если задан `webhook_url`, Consumer отправляет клиенту HTTP POST с результатом обработки. Если webhook не задан, после обработки платежа сообщение подтверждается без HTTP-вызова.

## Повторные попытки и DLQ

В проекте используются два независимых механизма повторной обработки.

### Повторы отправки webhook

HTTP-клиент настроен с timeout 5 секунд. Для отправки webhook предусмотрено три попытки всего. Между неудачными попытками применяется экспоненциальная задержка:

| Попытка | Действие после ошибки |
|---|---|
| 1 | Подождать 1 секунду и повторить запрос |
| 2 | Подождать 2 секунды и повторить запрос |
| 3 | Повтор больше не выполнять; отклонить сообщение без requeue и направить его в DLQ |

Если webhook не доставлен за три HTTP-попытки, Consumer вызывает `reject(requeue=False)`. RabbitMQ направляет сообщение через `payments.dlx` в `payments.dlq`.

### Повторные доставки при ошибках Consumer

Основная очередь `payments.new` настроена как durable Quorum Queue со следующими аргументами:

- `x-delivery-limit = 2`;
- `x-dead-letter-exchange = payments.dlx`;
- `x-dead-letter-routing-key = payments.dlq`.

В данной конфигурации delivery limit задаёт количество повторных доставок после первоначальной. Поэтому при `x-delivery-limit = 2` Consumer может получить сообщение до трёх раз: первоначально и ещё два раза. Если ошибка повторяется и обработчик выполняет `reject(requeue=True)`, после превышения лимита RabbitMQ перенаправляет сообщение в DLQ.

Постоянные ошибки, например некорректный `payment_id` или отсутствие платежа, отправляются в DLQ сразу. Исчерпание трёх HTTP-попыток webhook также приводит к немедленному `reject(requeue=False)`. Непредвиденные ошибки обработки возвращаются в основную очередь через `reject(requeue=True)`, после чего применяется delivery limit Quorum Queue.

`payments.dlq` — отдельная durable Classic Queue. Автоматический повторный запуск сообщений из DLQ в проекте не реализован: такие сообщения предназначены для анализа и последующей ручной/операционной обработки.

## Идемпотентность и доставка webhook

### Idempotency-Key

Для `POST /api/v1/payments` обязателен заголовок `Idempotency-Key`. Перед созданием платежа сервис ищет запись с таким ключом. При повторном запросе с тем же ключом возвращается существующий платёж, новый платёж и новое событие Outbox не создаются.

Для создания другого платежа следует использовать новый ключ.

### Семантика webhook

Доставка webhook имеет семантику at-least-once. Например, получатель может успешно принять HTTP-запрос, а Consumer — завершиться аварийно до сохранения `webhook_sent_at`. При повторной доставке сообщение может привести к повторной отправке webhook. Поэтому получателю следует реализовать дедупликацию уведомлений по `payment_id`.

## Настройка окружения

В корне проекта находится шаблон `.env.example`. Создайте на его основе локальный `.env`.

PowerShell:

```powershell
Copy-Item .env.example .env
```

Linux/macOS:

```bash
cp .env.example .env
```

Пример содержимого `.env.example`:

```dotenv
API_KEY=change-me
RABBITMQ_USER=payment_rabbit_user
RABBITMQ_PASSWORD=change-this-password
DATABASE_URL=postgresql+asyncpg://payment_user:payment_password@localhost:5433/payment_db
RABBITMQ_URL=amqp://payment_rabbit_user:change-this-password@localhost:5672/
```

Это демонстрационные значения для локального запуска. Перед использованием замените `API_KEY` и `RABBITMQ_PASSWORD` на собственные значения. Не добавляйте `.env` с реальными секретами в репозиторий; в Git следует хранить `.env.example`.

`DATABASE_URL` и `RABBITMQ_URL` в `.env` предназначены для подключения с хост-машины (например, при выполнении Alembic-команд локально). В Docker Compose адреса для Publisher и Consumer переопределяются на имена сервисов внутри Docker-сети:

- PostgreSQL: `postgres:5432`;
- RabbitMQ: `rabbitmq:5672`.

В текущем `docker-compose.yml` параметры PostgreSQL заданы как `payment_db`, `payment_user`, `payment_password`, а порт PostgreSQL на хосте — `5433`. Если эти параметры меняются в Compose, синхронно обновите `DATABASE_URL`.

Логин и пароль в `RABBITMQ_URL` должны совпадать с `RABBITMQ_USER` и `RABBITMQ_PASSWORD`. При первом запуске RabbitMQ создаёт пользователя и пароль в своём data volume. Если изменить `RABBITMQ_USER` или `RABBITMQ_PASSWORD` после первого запуска, существующие учётные данные автоматически не обновятся: потребуется пересоздать volume RabbitMQ. Учтите, что `docker compose down -v` удаляет **все** volumes проекта, в том числе volume PostgreSQL со всеми данными; используйте эту команду только если полный сброс локального окружения допустим.

## Запуск проекта

Для запуска необходимы Docker Desktop или Docker Engine с Docker Compose plugin. Python 3.12 и Poetry нужны только для локальной разработки вне Docker.

Все команды ниже выполняются из корня проекта.

Перед запуском создайте и настройте `.env` по инструкции в разделе [«Настройка окружения»](#настройка-окружения). Не добавляйте `.env` с секретами в репозиторий.

### 1. Проверить конфигурацию Docker Compose

```powershell
docker compose config
```

Команда проверяет YAML-конфигурацию и подстановку переменных окружения, не запуская контейнеры.

### 2. Собрать и запустить проект

```powershell
docker compose up --build -d
```

Docker Compose выполнит запуск в следующем порядке:

1. Запустит PostgreSQL и RabbitMQ.
2. Дождётся, пока PostgreSQL пройдёт healthcheck.
3. Запустит одноразовый сервис `migrate`, который выполнит `alembic upgrade head`.
4. После успешного завершения миграций запустит API, Publisher и Consumer.

Отдельно запускать Alembic на хост-машине не требуется: миграции выполняются внутри контейнера `migrate`.

### 3. Проверить состояние сервисов и миграций

```powershell
docker compose ps -a
```

Пять долгоживущих сервисов — `postgres`, `rabbitmq`, `api`, `publisher` и `consumer` — должны работать. Сервис `migrate` после успешного выполнения завершается со статусом `Exited (0)`. Это ожидаемое поведение одноразового контейнера.

Проверить результат миграций:

```powershell
docker compose logs migrate
```

Проверить логи приложений:

```powershell
docker compose logs --tail=100 api publisher consumer
```

### Полезные адреса

- Swagger UI: <http://localhost:8000/docs>
- Health endpoint: <http://localhost:8000/health>
- RabbitMQ Management UI: <http://localhost:15672>

Для входа в RabbitMQ Management UI используйте `RABBITMQ_USER` и `RABBITMQ_PASSWORD` из `.env`.

### Остановка

```powershell
docker compose down
```

`docker compose down` удаляет контейнеры и сеть, но сохраняет именованные volumes с данными PostgreSQL и RabbitMQ. Команда `docker compose down -v` дополнительно удаляет volumes и все хранящиеся в них данные, включая данные PostgreSQL; используйте её только для полного сброса локального окружения.

## API

Swagger UI доступен по адресу <http://localhost:8000/docs>.

Платёжные endpoint'ы требуют заголовок `X-API-Key`. Для создания платежа дополнительно обязателен `Idempotency-Key`.

### Создать платёж

`POST /api/v1/payments`

Заголовки:

```http
X-API-Key: <API_KEY>
Idempotency-Key: order-unique-key-001
Content-Type: application/json
```

Тело запроса:

```json
{
  "amount": 100.00,
  "currency": "RUB",
  "description": "Test payment",
  "metadata": {},
  "webhook_url": null
}
```

Поля запроса:

- `amount` — положительная сумма типа Decimal, максимум 12 цифр и 2 знака после запятой;
- `currency` — `RUB`, `USD` или `EUR`;
- `description` — описание платежа;
- `metadata` — объект дополнительных данных, по умолчанию `{}`;
- `webhook_url` — необязательный URL для HTTP-уведомления.

При успешном принятии запроса API возвращает `202 Accepted` с данными платежа. Начальный статус — `pending`; обработка выполняется асинхронно.

Пример ответа:

```json
{
  "payment_id": "0197c7a0-1234-7abc-8def-0123456789ab",
  "amount": "100.00",
  "currency": "RUB",
  "description": "Test payment",
  "metadata": {},
  "status": "pending",
  "webhook_url": null,
  "created_at": "2026-11-02T10:00:00Z",
  "processed_at": null
}
```

В JSON-ответе идентификатор называется `payment_id`; это сериализационный alias поля `id` в Pydantic-схеме.

Пример для PowerShell:

```powershell
$headers = @{
    "X-API-Key" = "<API_KEY>"
    "Idempotency-Key" = "order-unique-key-001"
}

$body = @{
    amount = 100.00
    currency = "RUB"
    description = "Test payment"
    metadata = @{}
    webhook_url = $null
} | ConvertTo-Json

$payment = Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8000/api/v1/payments" `
    -Headers $headers `
    -ContentType "application/json" `
    -Body $body

$payment
```

Пример для Linux/macOS (curl):

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: <API_KEY>" \
  -H "Idempotency-Key: order-unique-key-001" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "Test payment", "metadata": {}}'
```

Повторите ту же команду с тем же `Idempotency-Key`: API вернёт тот же `payment_id`.

### Получить платёж

`GET /api/v1/payments/{payment_id}`

Пример для PowerShell:

```powershell
$headers = @{ "X-API-Key" = "<API_KEY>" }
$paymentId = "<PAYMENT_ID>"

Invoke-RestMethod -Method Get `
    -Uri "http://localhost:8000/api/v1/payments/$paymentId" `
    -Headers $headers
```

После обработки статус платежа меняется на `succeeded` или `failed`. Если платёж не найден, API возвращает `404 Not Found`.

## Проверка webhook локально

Для проверки webhook можно указать URL временного HTTP-получателя, например Webhook.site, в поле `webhook_url` при создании платежа.

Если тестовый HTTP-сервер запущен непосредственно на Windows-хосте, а Consumer работает в Docker, в `webhook_url` следует указывать адрес вида:

```text
http://host.docker.internal:<PORT>/webhook
```

Внутри контейнера `localhost` указывает на сам контейнер, а не на хостовую Windows-систему.

## Мониторинг и полезные команды

### Логи сервисов

```powershell
# Все основные приложения
docker compose logs --tail=100 api publisher consumer

# Следить за Consumer
docker compose logs -f consumer

# Следить за Publisher
docker compose logs -f publisher
```

### Состояние контейнеров

```powershell
docker compose ps
```

### Очереди RabbitMQ

```powershell
docker compose exec rabbitmq rabbitmqctl list_queues name type arguments messages_ready messages_unacknowledged
```

В проекте используются:

- `payments.new` — основная Quorum Queue;
- `payments.dlx` — Direct Dead Letter Exchange;
- `payments.dlq` — Dead Letter Queue.

RabbitMQ Management UI доступен по адресу <http://localhost:15672>. Для входа используйте `RABBITMQ_USER` и `RABBITMQ_PASSWORD` из `.env`.

## Проведённые ручные проверки

В локальном Docker-окружении были выполнены следующие проверки:

- создание платежа через API с ответом `202 Accepted`;
- получение финального статуса через GET endpoint;
- повторный POST с тем же `Idempotency-Key` возвращает тот же `payment_id` и не создаёт новый платёж;
- Consumer получает событие из `payments.new` и обновляет статус платежа;
- успешная доставка webhook тестовому HTTP-получателю;
- три неудачные HTTP-попытки webhook при ответе получателя `500`, после которых сообщение попало в `payments.dlq`;
- искусственная ошибка Consumer с `reject(requeue=True)` и перемещение сообщения в DLQ при первоначальном `x-delivery-limit = 3` (в логах наблюдались четыре доставки: первоначальная и три повторные);
- после этого `payments.new` была пересоздана с `x-delivery-limit = 2`, новое значение подтверждено через `rabbitmqctl list_queues`.

**Примечание:** после изменения `x-delivery-limit` с `3` на `2` значение конфигурации было проверено, но сам сценарий повторных доставок с новым значением отдельно не запускался.

## Ограничения

- Повторный запрос с тем же `Idempotency-Key`, но другим телом вернёт первый созданный платёж без предупреждения.
- Автоматические unit- и integration-тесты в проекте отсутствуют; поведение проверялось вручную (см. раздел «Проведённые ручные проверки»).
- Платёжный шлюз эмулируется случайным результатом; интеграции с реальным платёжным провайдером нет.
- Webhook использует at-least-once delivery: при аварийном завершении Consumer после успешного HTTP-ответа, но до сохранения `webhook_sent_at`, уведомление может быть отправлено повторно. Получателю рекомендуется реализовать дедупликацию по `payment_id`.
- Автоматическое восстановление и повторная публикация сообщений из DLQ не реализованы.
- Quorum Queue используется в локальном окружении с одним узлом RabbitMQ; это не обеспечивает отказоустойчивость при потере узла кластера.
