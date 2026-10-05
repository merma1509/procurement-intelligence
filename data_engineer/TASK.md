# Отборочное задание — Data Engineer

Мы рады пригласить вас принять участие в отборочном задании в мастерскую Astra для участия в проекте по тематике **"Применение ИИ в задачах сбора и обработки закупочной информации"**. Задание направлено на проверку ваших навыков в области работы с данными. Вам предлагается выполнить серию практических заданий, посвященных проектированию базы данных и построению процесса сбора, обработки и хранения данных.

## Условия участия

### 1. Количество заданий

Задание разбито на три части. Вы можете выбрать любое количество подзаданий для выполнения, начиная с одного. Чем больше подзаданий вы выполните, тем выше ваши шансы на успешное прохождение отбора.

### 2. Формат сдачи работ

Исходный код должен быть размещён на платформе github.com.

### 3. Критерии оценивания

Оценка будет основываться на количестве выполненных заданий, качестве кода, полноте реализации и правильности предложенных решений.

### 4. Срок подачи работ

Работы принимаются до 13.10.2026

---

## Задание

### Часть 1: Проектирование

> Зафиксируйте свои рассуждения в файле `TASK.MD`.

#### Задание 1.0: Анализ исходных данных (Выполнено)

**Результаты:**

| Dataset                               | Rows    | Columns | Description                   |
| ------------------------------------- | ------- | ------- | ----------------------------- |
| `building_materials_transactions.csv` | 340,000 | 15      | Транзакции продаж             |
| `macro_drivers_weekly.csv`            | 313     | 5       | Макроэкономические показатели |
| `data_dictionary_materials.csv`       | 15      | 3       | Словарь данных                |

**Выявленные связи между сущностями:**

- `date` (транзакции -> `week` (макро) — связь 1:1
- 9 регионов, 10 категорий, 84,641 SKU, 2 канала, 4 типа клиентов

**Проблемы качества данных:**

1.  60 дубликатов по бизнес-ключу
2.  15,978 выбросов в lumber_price_index (ценовой шок 2020-22)
3.  season_factor отсутствует в транзакциях (есть только в макро)
4.  Высокая кардинальность SKU — требует отдельной таблицы
5.  Избыточность: year, month, week_of_year вычисляются из date

**Визуализации:** Создан ноутбук `data_engineer/notebooks/01_eda_analysis.ipynb` с полным EDA.

---

#### Задание 1.1: Схема базы данных (Выполнено)

**Выбор архитектуры: Star Schema (Гибридный подход)**

```bash
┌────────────────────────────────────────────────────────────────┐
│                     ARCHИТЕКТУРА                               │
├────────────────────────────────────────────────────────────────┤
│  Dimension Tables  ->  3NF (нормализованы, без избыточности)   │
│  Fact Table        ->  Денормализована (быстрые аналитические  │
│                        запросы и агрегации)                    │
│  Macro Data        ->  Отдельная таблица dim_macro, связь      │
│                        по дате (включая season_factor!)        │
└────────────────────────────────────────────────────────────────┘
```

**Сравнительный анализ:**

| Критерий               | 3NF                | Полная денормализация | Star Schema |
| ---------------------- | ------------------ | --------------------- | ----------- |
| Избыточность данных    | Минимальная        | Высокая               | Умеренная   |
| Скорость аналитики     | Медленные JOIN     | Быстрая               | Быстрая     |
| Целостность данных     | Легко поддерживать | Сложно                | Легко       |
| Простота модели        | Сложная            | Простая               | Понятная    |
| ETL-сложность          | Простая            | Сложная               | Умеренная   |
| Гибкость для изменений | Хорошая            | Плохая                | Хорошая     |

**Обоснование выбора:**

1. **Dimension Tables (3NF):**
   - Таблицы `dim_category`, `dim_region`, `dim_channel`, `dim_customer` содержат мало записей (2-10)
   - Нормализация обеспечивает целостность без потери производительности
   - Легко добавлять новые категории/регионы

2. **Fact Table (Денормализована):**
   - 340K строк требуют оптимизации для аналитических запросов
   - Все измерения связаны через FK для ссылочной целостности
   - Денормализация macro-индикаторов (housing_starts, lumber_price, mortgage_rate) прямо в fact_sales позволяет избежать JOIN в типичных запросах

3. **dim_macro — отдельная таблица:**
   - Макроданные изменяются независимо от транзакций
   - Связь через `macro_id` (FK) для ссылочной целостности
   - **Включает season_factor** — который отсутствовал в исходных транзакциях!

**Схема таблиц:**

```sql
-- Dimension Tables (3NF)
dim_date (date_id, date, year, month, week, quarter, is_holiday)
dim_category (category_id, category_name, category_code, is_lumber_sensitive)
dim_product (product_id, sku, sku_prefix, category_id)
dim_region (region_id, region_name, census_division)
dim_channel (channel_id, channel_name, channel_type)
dim_customer (customer_id, customer_type, segment, is_business)

-- Macro Dimension
dim_macro (macro_id, week, housing_starts_index, lumber_price_index,
           mortgage_rate, season_factor)

-- Fact Table (Денормализована)
fact_sales (transaction_id, date_id, product_id, region_id,
            channel_id, customer_id, macro_id,
            units, unit_price, revenue,
            housing_starts_index, lumber_price_index, mortgage_rate)
```

**Преимущества Star Schema:**

- Быстрые запросы через bitmap indexes
- Понятная модель для бизнес-пользователей
- Легко добавлять новые измерения
- Простая агрегация (SUM, AVG по measures)

**Файл:** `db/schemas/01_star_schema.sql`

---

Какую СУБД можно считать наиболее подходящей для полученной схемы? Обоснуйте свой выбор и приведите сравнение с одной или более альтернативами.

#### Задание 1.2: Выбор СУБД (Выполнено)

**Рекомендация: PostgreSQL**

**Сравнительный анализ СУБД:**

| Критерий                 | PostgreSQL        | ClickHouse      | DuckDB           |
| ------------------------ | ----------------- | --------------- | ---------------- |
| **Тип**                  | OLTP/OLAP гибрид  | Columnar OLAP   | Analytical OLAP  |
| **Schema**               | Star/Snowflake    | Star            | Star             |
| **Window Functions**     | Полная            |                 |                  |
| **UPSERT (ON CONFLICT)** | Отлично           | Merge only      |                  |
| **SCD Type 2**           |                   |                 |                  |
| **Transactions (ACID)**  | Полная            | Ограничены      | Limited          |
| **Partitioning**         | Table inheritance | Native          |                  |
| **Compression**          | TOAST             | Native          | Native           |
| **Deployment**           | Любой             | Cloud-preferred | Embedded/Library |
| **Ecosystem**            | Высокая           | Средняя         | Средняя          |
| **Learning curve**       | Низкая            | Средняя         | Низкая           |

**Объём данных (наш use case):** ~50 MB — PostgreSQL оптимален для всех размеров

**Почему PostgreSQL:**

1. **UPSERT/SCD2 поддержка:**

   ```sql
   INSERT INTO fact_sales (...)
   VALUES (...)
   ON CONFLICT (date_id, region_id, product_id, channel_id, customer_id)
   DO UPDATE SET
       units = fact_sales.units + EXCLUDED.units,
       revenue = fact_sales.revenue + EXCLUDED.revenue;
   ```

2. **Зрелая экосистема:** pgAdmin, dbt, Airflow, psycopg2

3. **JSONB для ошибок:**

   ```sql
   CREATE TABLE etl_errors (
       raw_data JSONB,
       error_message TEXT
   );
   ```

**Когда выбрать альтернативы:**

| Альтернатива   | Когда использовать                    |
| -------------- | ------------------------------------- |
| **ClickHouse** | >100M строк, реальное время агрегации |
| **DuckDB**     | Embedded в Python, <10TB данных       |

**Файл:** `db/schemas/01_star_schema.sql` (PostgreSQL)

---

#### Задание 1.3: Структура ETL-процесса (Выполнено)

**Рекомендация: 3 слоя данных (Staging → Raw → Processed)**

```bash
┌─────────────────────────────────────────────────────────────────────────┐
│                           ETL PIPELINE LAYERS                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                │
│  │   SOURCE    │────>│   STAGING   │────>│     RAW     │────> PROCESSED │
│  │   (CSV)     │     │  (raw copy) │     │ (validated) │     (Star)     │
│  └─────────────┘     └─────────────┘     └─────────────┘                │
│       │                    │                   │                        │
        │                    │                   │                        │
│  Raw files from       Staging table      Dead letter queue              │
│  Kaggle               (data/raw/)        (etl_errors)                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Слои данных:**

| Слой          | Назначение           | Структура              | Операции               |
| ------------- | -------------------- | ---------------------- | ---------------------- |
| **STAGING**   | буфер, без изменений | Копия источника        | COPY, unzip            |
| **RAW**       | валидация, очистка   | Типизированные таблицы | Type cast, range check |
| **PROCESSED** | модель данных        | Star Schema            | FK linking, dedup      |

**Преобразования по слоям:**

```python
- date: str → DATE
- units: str → INT (>= 0)
- unit_price: str → DECIMAL (> 0)
- region: str → VARCHAR (в справочнике?)

- Неполные строки → etl_errors
- Некорректные типы → etl_errors
- Пропущенные обязательные поля → etl_errors
```

```python
- date → dim_date.date_id (lookup)
- sku → dim_product.product_id (lookup + UPSERT new)
- region → dim_region.region_id (lookup)
- channel → dim_channel.channel_id (lookup)
- customer_type → dim_customer.customer_id (lookup)

- Business key: (date, region, sku, channel, customer_type)
- ON CONFLICT: UPDATE units += new_units, revenue += new_revenue

- Weekly rollups для materialized views
```

**ETL таблицы:**

```sql
-- Staging tables (raw data, no constraints)
raw_transactions (id, date, year, month, ...)

-- Processed tables (Star Schema)
dim_date, dim_product, dim_region, dim_channel, dim_customer, dim_macro
fact_sales

-- Audit tables
etl_batch_log (execution history)
etl_watermarks (incremental load)
etl_errors (dead letter queue)
```

**Файлы проекта:**

```bash
data_engineer/
├── src/
│   ├── extract/           # STAGING layer
│   ├── transform/         # RAW --> PROCESSED
│   ├── load/              # Load to database
│   └── validation/        # Data validation
├── notebooks/
│   └── 01_eda_analysis.ipynb
└── scripts/
    └── download_data.py
```

---

### Часть 2: Реализация ETL-процесса

#### Задание 1.1: Разработка основного пайплайна  (Выполнено)

1. Реализуйте скрипт, разделенный на логические этапы извлечения, обработки и выгрузки
2. Настройте валидацию входящих данных. Проверяйте типы данных, корректность диапазонов, отсутствие критичных пропусков

#### Задание 1.2: Инкрементальные обновления  (Выполнено)

1. Реализуйте поддержку добавления новых данных в существующий массив
2. Обеспечьте идемпотентность пайплайна и возможность обновления исторических данных

---

### Часть 3: Сопровождаемость проекта

#### Задание 3.1: Отказоустойчивость  (Выполнено)

1. Пайплайн не должен падать в случае появления некорректных строк. Реализуйте изоляцию таких строк в отдельную таблицу с описанием причины ошибки
2. Поддержите возможность отката транзакций записи в БД в случае критических ошибок
3. Напишите базовые тесты, проверяющие корректность трансформаций данных

#### Задание 3.2: Документирование  (Выполнено)

1. Опишите общую структуру проекта и порядок действий для развертывания в `README.MD`
2. Добавьте докстринги всем основным классам и функциям проекта
