# Авторизация в Yandex CLI
yc init

FUNCTION_NAME="github-parser-from-cli"
# Указываем параметры подключения к PostgreSQL в переменные окружения клауд функции
ENV_VARS_FOR_DB="DB_HOST=value1,DB_PORT=value2,DB_SSLMODE=value3,DB_CERT_PATH=value4,DB_NAME=value5,DB_USER=value6,DB_PASS=value7,DB_TSA=value8"

# Создание функции
yc serverless function create --name=$FUNCTION_NAME

echo "Введите путь до zip-архива с github_parser.py и requirements.txt, например ./function.zip:"
read FUNCTION_ZIP_PATH

# Деплой функции
yc serverless function version create \
  --function-name $FUNCTION_NAME \
  --runtime python311 \
  --entrypoint github_parser.handler \
  --memory 128m \
  --execution-timeout 5s \
  --source-path $FUNCTION_ZIP_PATH \
  --environment $ENV_VARS_FOR_DB

echo "Деплой функции завершен успешно!"

echo "Создание триггера-таймера для вызова функции раз в 1 час"

# Получение идентификатора созданной функции по имени
echo "Введите FUNCTION_ID созданной функции:"
read FUNCTION_ID

# Получение идентификатора сервисного аккаунта для запуска триггера
echo "Введите SERVICE_ACCOUNT_ID для запуска триггера:"
read SERVICE_ACCOUNT_ID

# Создание триггера-таймера - раз в 1 час
yc serverless trigger create timer \
  --name timer-for-github-parser \
  --cron-expression '0 * ? * * *' \
  --invoke-function-id $FUNCTION_ID \
  --invoke-function-service-account-id $SERVICE_ACCOUNT_ID