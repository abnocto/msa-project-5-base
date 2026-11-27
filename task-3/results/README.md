# Задание 3: выгрузка перевозок через k8s CronJob

Каждый день в 20:00 по Москве CronJob запускает под, который выгружает перевозки за текущий день из таблицы `shipments` в CSV на PVC: `/exports/shipments_<дата>.csv`. В рамках POC этот PVC и есть хранилище для аналитиков.

- `export.js` - скрипт выгрузки. `COPY ... TO STDOUT` пишет поток из PostgreSQL сразу в файл, так что 200 тыс. строк за день не держим в памяти.
- `Dockerfile` - образ скрипта.
- `k8s/postgres.yaml` - PostgreSQL с тестовыми данными (300 перевозок за последние двое суток).
- `k8s/cronjob.yaml` - PVC и CronJob. `concurrencyPolicy: Forbid`, чтобы запуски не наложились друг на друга, `backoffLimit: 2` - два повтора, если под упал.

## Запуск

```
docker build -t shipments-export:1.0 .
minikube image load shipments-export:1.0
kubectl create namespace analytics
kubectl apply -f k8s/
```

Ручной запуск, лог и результат:

```
kubectl -n analytics create job --from=cronjob/shipments-export manual-1
kubectl -n analytics logs job/manual-1
minikube ssh -- head /tmp/hostpath-provisioner/analytics/exports/shipments_<дата>.csv
```

Расписание проверял через `*/2 * * * *`, потом вернул `0 20 * * *`:

```
kubectl -n analytics patch cronjob shipments-export -p '{"spec":{"schedule":"*/2 * * * *"}}'
kubectl -n analytics get cronjob,jobs
```

Скриншоты в [screenshots](screenshots): CronJob, ручной запуск и лог, CSV на PVC, запуск по расписанию.
