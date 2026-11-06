# medallion-lab

Учебный стек Databricks (Free Edition, serverless):
источник событий -> Bronze (Auto Loader) -> Silver (дедупликация, поздние данные) -> Gold (почасовые агрегаты),
плюс бэкфилл и замер времени работы слоёв.

## Структура
- `notebooks/medallion_lab.py` - ноутбук в формате Databricks source (ячейки `# COMMAND ----------`)

## Запуск через Git-папку Databricks
1. Workspace -> Create -> Git folder -> URL этого репозитория (для приватного нужен GitHub PAT/приложение).
2. Откройте `notebooks/medallion_lab`, подключите Serverless, запускайте ячейки сверху вниз.
3. Правки делайте в Git-папке и коммитьте оттуда (или локально + Pull).

## Проверка результата
Ячейка 7 должна показать: `silver_rows = silver_distinct_ids = gold_events_total`,
`removed_dups_or_bad > 0`, `late_rows > 0`.

## Замеры
Таблица `workspace.lab.ops_metrics`: секунды, строки и микробатчи по слоям и прогонам (live-1, live-2, backfill).
