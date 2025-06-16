import os
import sys
import json
import pandas as pd
import sqlite3
import argparse
import hashlib
import time
from datetime import datetime, timedelta
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Глобальный кэш для хранения обработанных файлов
file_cache = {}


def generate_sql_for_date_filters(sql_prompt, schema_str):
    """Добавляет подсказки для работы с датами в SQL-запросах"""
    # Проверяем наличие столбцов с датами в схеме
    date_columns = []
    for line in schema_str.split('\n'):
        if '(TIMESTAMP)' in line or 'Date' in line or 'дата' in line.lower() or 'Дата' in line:
            col_name = line.split(' - ')[1].split(' (')[0]
            date_columns.append(col_name)

    if date_columns:
        date_hint = f"""
- Обрати внимание на следующие столбцы с датами: {', '.join([f'"{col}"' for col in date_columns])}
- Для работы с датами убедись, что правильно интерпретируешь формат даты
- Вместо функции date('now', '-X years') используй сравнение с явно заданным годом (например, "Год постройки" >= 2015)
- Если нужно работать с полем "Дата постройки", лучше использовать условие по "Год постройки", если такое поле есть
- Для фильтрации по дате выбирай более простые условия, например:
  * 'Год постройки >= 2014' вместо сложных вычислений с текущей датой
"""
        return sql_prompt + date_hint

    return sql_prompt


def chat_with_gpt(client, model, user_content, temperature=0):
    """Удобная обёртка для вызова GPT через API."""
    try:
        # Если передан список сообщений (для поддержки истории)
        if isinstance(user_content, list):
            response = client.chat.completions.create(
                model=model,
                messages=user_content,
                temperature=temperature
            )
        else:
            # Если передан просто строковый запрос
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": user_content}],
                temperature=temperature
            )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Ошибка при вызове API OpenAI: {e}")
        return f"Произошла ошибка при получении ответа: {e}"


def get_file_hash(file_path):
    """Генерирует хеш файла для проверки изменений."""
    h = hashlib.md5()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b''):
            h.update(chunk)
    return h.hexdigest()


def main():
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Создание таблиц и сводных отчетов из Excel-файла')
    parser.add_argument('file_path', help='Путь к Excel-файлу')
    parser.add_argument('query', help='Запрос пользователя о создании таблицы')
    parser.add_argument('--output', default='final.xlsx',
                        help='Путь для сохранения результата (по умолчанию: final.xlsx)')
    parser.add_argument('--api_key', help='API ключ OpenAI (или будет использован из переменной окружения)')
    parser.add_argument('--model', default="gpt-4", help='Модель OpenAI (по умолчанию: gpt-4)')
    parser.add_argument('--analyze-only', action='store_true',
                        help='Только проанализировать данные и вывести информацию о столбцах')
    parser.add_argument('--sql-only', action='store_true', help='Только сгенерировать SQL-запрос')
    parser.add_argument('--execute-sql', action='store_true', help='Выполнить указанный SQL-запрос')
    parser.add_argument('--columns', help='Список столбцов, разделенных запятыми')
    parser.add_argument('--sql', help='SQL-запрос для выполнения')
    parser.add_argument('--cache', action='store_true', help='Использовать кэширование данных')
    parser.add_argument('--timeout', type=int, default=600, help='Таймаут выполнения SQL-запроса в секундах')

    args = parser.parse_args()

    # Проверка существования файла
    if not os.path.exists(args.file_path):
        print(f"Ошибка: файл {args.file_path} не найден.")
        sys.exit(1)

    # Информация о запуске
    print(f"Обрабатываю файл: {args.file_path}")
    print(f"Запрос пользователя: {args.query}")

    # Настройка API ключа
    api_key = os.getenv("CHATGPT_API_KEY")

    # Создание клиента OpenAI
    client = OpenAI(api_key=api_key)

    # Создание временного имени для базы данных
    db_file = f"temp_db_{os.path.basename(args.file_path).replace('.', '_')}.sqlite"
    table_name = 'data'  # Стандартное имя таблицы

    # Проверка наличия данных в кэше
    file_hash = get_file_hash(args.file_path)
    cache_key = f"{file_hash}_{db_file}"

    try:
        # === Шаг 1. Загрузка Excel-файла в DataFrame ===
        df = None
        sheet_names = []

        # Проверяем, есть ли файл в кэше и нужно ли его использовать
        if args.cache and cache_key in file_cache and os.path.exists(db_file):
            print(f"Используем кэшированные данные для файла {args.file_path}")
            df = file_cache[cache_key]['dataframe']
            sheet_names = file_cache[cache_key]['sheets']
        else:
            try:
                # Получаем список всех листов в файле
                excel_file = pd.ExcelFile(args.file_path)
                sheet_names = excel_file.sheet_names
                print(f"Файл содержит {len(sheet_names)} листов: {', '.join(sheet_names)}")

                # Если листов несколько, обрабатываем каждый лист
                all_dfs = []
                for sheet_name in sheet_names:
                    print(f"Читаю лист '{sheet_name}'...")
                    # Добавляем обработку ошибок и повторные попытки для больших файлов
                    for attempt in range(3):  # Пробуем до 3 раз
                        try:
                            # Пытаемся разными способами прочитать файл
                            try:
                                df_sheet = pd.read_excel(args.file_path, sheet_name=sheet_name)
                            except:
                                # Пробуем с другими параметрами
                                df_sheet = pd.read_excel(args.file_path, sheet_name=sheet_name, engine='openpyxl')

                            # Добавляем столбец с именем листа для отслеживания
                            df_sheet['_sheet_name'] = sheet_name
                            all_dfs.append(df_sheet)
                            print(
                                f"Лист '{sheet_name}' прочитан. Количество строк: {len(df_sheet)}, столбцов: {len(df_sheet.columns) - 1}")
                            break  # Успешно прочитали, выходим из цикла попыток
                        except Exception as e:
                            print(f"Ошибка при чтении листа {sheet_name}, попытка {attempt + 1}: {e}")
                            if attempt == 2:  # Последняя попытка не удалась
                                raise
                            time.sleep(1)  # Пауза перед повторной попыткой

                # Объединяем все датафреймы в один, если их несколько
                if len(all_dfs) > 1:
                    df = pd.concat(all_dfs, ignore_index=True)
                    print(f"Все листы объединены. Общее количество строк: {len(df)}, столбцов: {len(df.columns)}")
                else:
                    df = all_dfs[0]
                    print(f"Файл успешно прочитан. Количество строк: {len(df)}, столбцов: {len(df.columns)}")

                # Сохраняем в кэш, если включено кэширование
                if args.cache:
                    file_cache[cache_key] = {
                        'dataframe': df,
                        'sheets': sheet_names
                    }
            except Exception as e:
                print(f"Ошибка при чтении файла Excel: {e}")
                sys.exit(1)

        # === Шаг 2. Импорт данных в SQLite ===

        try:
            # Проверяем, нужно ли создавать базу данных заново
            if not args.cache or not os.path.exists(db_file):
                conn = sqlite3.connect(db_file, timeout=args.timeout)
                # Увеличиваем таймаут для больших запросов - исправленная версия
                conn.execute(f"PRAGMA busy_timeout = {args.timeout * 1000}")  # Исправлено: убраны скобки ?
                # Оптимизируем память и скорость
                conn.execute("PRAGMA synchronous = OFF")
                conn.execute("PRAGMA journal_mode = MEMORY")
                conn.execute("PRAGMA temp_store = MEMORY")
                conn.execute("PRAGMA cache_size = 10000")

                # Сохраняем данные пакетами для больших файлов
                chunk_size = 5000
                if len(df) > chunk_size:
                    for i in range(0, len(df), chunk_size):
                        chunk = df.iloc[i:i + chunk_size]
                        if i == 0:
                            chunk.to_sql(table_name, conn, if_exists='replace', index=False)
                        else:
                            chunk.to_sql(table_name, conn, if_exists='append', index=False)
                        print(f"Импортировано строк: {min(i + chunk_size, len(df))} из {len(df)}")
                else:
                    df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"Данные успешно импортированы в SQLite.")
            else:
                conn = sqlite3.connect(db_file, timeout=args.timeout)
                # Увеличиваем таймаут для больших запросов - исправленная версия
                conn.execute(f"PRAGMA busy_timeout = {args.timeout * 1000}")  # Исправлено: убраны скобки ?
        except Exception as e:
            print(f"Ошибка при работе с базой данных: {e}")
            sys.exit(1)



        # === Шаг 3. Показываем пользователю образец данных и названия столбцов ===
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
            sample_data = cursor.fetchall()
            sample_str_sql = "\n".join([str(row) for row in sample_data])

            # Получаем информацию о столбцах через PRAGMA
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns_info = cursor.fetchall()

            # Формирование строкового описания схемы
            schema_lines = []
            for col in columns_info:
                col_name = col[1]
                col_type = col[2]
                schema_lines.append(f" - {col_name} ({col_type})")
            schema_str = "\n".join(schema_lines)

            # Получаем имена столбцов для дальнейшего использования
            column_names_list = [col[1] for col in columns_info]
            column_names_str = ", ".join([f'"{col}"' if ' ' in str(col) else str(col) for col in column_names_list])

            print("\nСхема таблицы:")
            print(schema_str)
        except Exception as e:
            print(f"Ошибка при извлечении данных из SQL: {e}")
            conn.close()
            sys.exit(1)

        # Проверяем режим работы скрипта на основе аргументов командной строки
        if args.analyze_only:
            # Только анализируем данные и выводим информацию о столбцах
            columns_prompt = f"""
Привет. Нужно определить, какие столбцы использовать для составления корректного SQL-запроса.
Вот данные:
Запрос пользователя: "{args.query}"
Названия столбцов: {column_names_str}
Схема таблицы:
{schema_str}
Первые 10 строк из SQL-таблицы:
{sample_str_sql}

Выбери и перечисли только те столбцы, которые потребуются для создания запрошенной таблицы/отчета. 
Верни только список столбцов через запятую, без дополнительных пояснений.
"""

            suggested_columns = chat_with_gpt(client, args.model, columns_prompt, temperature=0.3)
            print(f"COLUMNS_INFO_START\n{suggested_columns}\nCOLUMNS_INFO_END")
            conn.close()
            sys.exit(0)

        elif args.sql_only:
            # Только генерируем SQL запрос
            selected_columns = args.columns if args.columns else column_names_str

            sql_prompt = f"""
Используя следующие столбцы: {selected_columns}
Таблица называется '{table_name}' (SQLite).
Запрос пользователя: "{args.query}"
Схема таблицы:
{schema_str}
Первые 10 строк из SQL-таблицы:
{sample_str_sql}

Сформируй, пожалуйста, корректный SQL-запрос (SQLite) для получения итогового ответа.
Важно:
- Возвращай только SQL-запрос, начиная с ключевого слова SELECT, без лишнего текста или комментариев.
- Если имя столбца содержит пробелы или спецсимволы, оборачивай его в двойные кавычки.
- Не используй команды, которые могут изменить структуру базы (DROP, ALTER и т.д.).
- Запрос должен быть полным и готовым к выполнению.
- Не добавляй условия, которые исключают большую часть данных, если это не требуется в запросе.
- Выполни точно запрос пользователя без лишних ограничивающих условий в WHERE.
"""
            # Если в файле несколько листов, добавляем подсказку
            if len(sheet_names) > 1:
                sql_prompt += f"""
- В таблице есть столбец '_sheet_name', который указывает, из какого листа Excel взята строка ({', '.join(sheet_names)}).
  Используй этот столбец, если нужно фильтровать данные по конкретному листу.
"""

            sql_response = chat_with_gpt(client, args.model, sql_prompt, temperature=0)

            # Извлечем SQL-запрос, если он обернут в тройные кавычки
            if "```sql" in sql_response and "```" in sql_response.split("```sql", 1)[1]:
                sql_query = sql_response.split("```sql", 1)[1].split("```", 1)[0].strip()
            elif "```" in sql_response and "```" in sql_response.split("```", 1)[1]:
                sql_query = sql_response.split("```", 1)[1].split("```", 1)[0].strip()
            else:
                sql_query = sql_response.strip()

            print(f"SQL_QUERY_START\n{sql_query}\nSQL_QUERY_END")
            conn.close()
            sys.exit(0)

        elif args.execute_sql:
            # Выполняем указанный SQL запрос с обработкой таймаутов
            sql_query = args.sql

            try:
                # Настраиваем коннектор и объем памяти для больших запросов
                conn.row_factory = sqlite3.Row  # Позволяет обращаться к столбцам по имени

                # Устанавливаем максимальный размер таблицы в памяти и другие оптимизации
                conn.execute("PRAGMA temp_store = MEMORY")
                conn.execute("PRAGMA mmap_size = 30000000000")  # ~30GB

                # Выполняем запрос с обработкой возможного таймаута
                cursor = conn.cursor()
                start_time = time.time()

                print(f"Начало выполнения SQL-запроса... (таймаут: {args.timeout} сек)")
                cursor.execute(sql_query)

                # Извлекаем результаты в виде списка словарей для удобства конвертации в DataFrame
                columns = [desc[0] for desc in cursor.description]

                # Извлекаем данные небольшими частями, чтобы не перегрузить память
                BATCH_SIZE = 1000
                all_results = []
                rows_processed = 0

                while True:
                    batch = cursor.fetchmany(BATCH_SIZE)
                    if not batch:
                        break
                    all_results.extend([dict(zip(columns, row)) for row in batch])
                    rows_processed += len(batch)

                    elapsed = time.time() - start_time
                    if rows_processed % 10000 == 0:
                        print(f"Обработано строк: {rows_processed} за {elapsed:.2f} сек")

                    # Проверяем таймаут
                    if elapsed > args.timeout:
                        print(f"Превышено время выполнения запроса ({args.timeout} сек).")
                        break

                query_result = all_results
                columns_desc = columns

                end_time = time.time()
                print(f"Запрос выполнен за {end_time - start_time:.2f} сек. Получено строк: {len(query_result)}")

                if not query_result:
                    print("\nНичего не найдено по вашему запросу.")
                    clar_prompt = f"""Запрос:
{sql_query}

Результат пустой. Возможно, не нашлось данных, удовлетворяющих условиям.
Сформулируй уточняющий вопрос или возможную причину, почему нет данных. Также предложи, как можно изменить запрос, чтобы получить результаты."""

                    clar_response = chat_with_gpt(client, args.model, clar_prompt, temperature=0.7)

                    print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                    print(clar_response.strip())
                else:
                    # Сохраняем результат в Excel
                    df_result = pd.DataFrame(query_result)
                    output_file = args.output

                    # Оптимизируем сохранение для больших результатов
                    print(f"Сохраняю результаты в {output_file}...")

                    # Создаем Excel-writer с оптимизированными настройками
                    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                        df_result.to_excel(writer, index=False, sheet_name='Результаты')

                        # Если результат большой, сохраняем также сводную таблицу
                        if len(df_result) > 1000:
                            try:
                                # Пробуем создать сводную таблицу для большого набора данных
                                # На основе запроса пользователя определяем ключевые поля
                                summary_prompt = f"""
На основе запроса пользователя: "{args.query}"
И структуры результирующей таблицы с {len(df_result)} строками и столбцами: {', '.join(df_result.columns)}

Определи:
1. Какой столбец лучше всего использовать для группировки данных в сводной таблице?
2. Какое агрегирование следует применить (сумма, среднее, количество)?
3. Какие столбцы стоит вывести в качестве значений?

Верни только название столбца для группировки, тип агрегирования и названия столбцов для значений, разделенные запятыми.
"""
                                summary_response = chat_with_gpt(client, args.model, summary_prompt,
                                                                 temperature=0.5).strip()

                                # Разбираем ответ
                                parts = summary_response.split(',')
                                if len(parts) >= 3:
                                    group_by = parts[0].strip()
                                    agg_type = parts[1].strip()
                                    value_cols = [col.strip() for col in parts[2:]]

                                    if group_by in df_result.columns:
                                        # Создаем сводную таблицу
                                        print(f"Создаю сводную таблицу с группировкой по '{group_by}'...")

                                        # Определяем метод агрегации
                                        agg_method = 'sum'
                                        if 'сред' in agg_type.lower():
                                            agg_method = 'mean'
                                        elif 'колич' in agg_type.lower() or 'count' in agg_type.lower():
                                            agg_method = 'count'

                                        # Создаем словарь для агрегаций
                                        agg_dict = {col: agg_method for col in value_cols if col in df_result.columns}

                                        if agg_dict:
                                            # Создаем сводную таблицу
                                            pivot_df = df_result.groupby(group_by).agg(agg_dict).reset_index()
                                            pivot_df.to_excel(writer, index=False, sheet_name='Сводная')
                            except Exception as e:
                                print(f"Не удалось создать сводную таблицу: {e}")

                    summary_prompt = f"""
Я создал таблицу по запросу пользователя: "{args.query}"

Получилась таблица размером {len(df_result)} строк на {len(df_result.columns)} столбцов.
Столбцы таблицы: {', '.join(df_result.columns.tolist())}

Первые 5 строк таблицы:
{df_result.head().to_string(index=False)}

Опиши кратко (2-3 предложения), что представляет собой эта таблица и какую информацию она содержит.
"""

                    table_description = chat_with_gpt(client, args.model, summary_prompt, temperature=0.7)

                    print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                    print(table_description.strip())
            except sqlite3.OperationalError as e:
                if "timeout" in str(e):
                    print(f"\nОшибка: Превышено время выполнения SQL-запроса. {e}")
                    print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                    print(
                        "Запрос слишком сложный и требует больше времени для выполнения. Пожалуйста, упростите запрос или дайте более конкретные условия фильтрации.")
                else:
                    print(f"\nОшибка при выполнении SQL-запроса: {e}")
                    print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                    print(f"Произошла ошибка при выполнении запроса: {e}")
            except Exception as e:
                print(f"\nОшибка при выполнении SQL-запроса: {e}")
                print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                print(f"Произошла ошибка при выполнении запроса: {e}")

            conn.close()
            sys.exit(0)

        # === Если не указан специальный режим, выполняем стандартный алгоритм ===

        # === Шаг 5А. Определяем, какие столбцы могут понадобиться для ответа ===
        columns_prompt = f"""
Привет. Нужно определить, какие столбцы использовать для составления корректного SQL-запроса.
Вот данные:
Запрос пользователя: "{args.query}"
Названия столбцов: {column_names_str}
Схема таблицы:
{schema_str}
Первые 10 строк из SQL-таблицы:
{sample_str_sql}

Выбери и перечисли только те столбцы, которые потребуются для создания запрошенной таблицы/отчета. 
Верни только список столбцов через запятую, без дополнительных пояснений.
"""

        try:
            suggested_columns = chat_with_gpt(client, args.model, columns_prompt, temperature=0.3)
            print("\nGPT предложил использовать следующие столбцы:")
            print(suggested_columns)
        except Exception as e:
            print(f"Ошибка при вызове GPT для определения столбцов: {e}")
            conn.close()
            sys.exit(1)

        # === Шаг 5B. Формируем промпт для генерации SQL-запроса с учётом формата данных ===
        # Здесь передаём только выбранные столбцы и первые 10 строк из SQL (чтобы GPT увидела реальный формат данных)
        selected_columns = suggested_columns
        sql_prompt = f"""
Используя следующие столбцы: {selected_columns}
Таблица называется '{table_name}' (SQLite).
Запрос пользователя: "{args.query}"
Схема таблицы:
{schema_str}
Первые 10 строк из SQL-таблицы:
{sample_str_sql}

Сформируй, пожалуйста, корректный SQL-запрос (SQLite) для получения итогового ответа.
Важно:
- Возвращай только SQL-запрос, начиная с ключевого слова SELECT, без лишнего текста или комментариев.
- Если имя столбца содержит пробелы или спецсимволы, оборачивай его в двойные кавычки.
- Не используй команды, которые могут изменить структуру базы (DROP, ALTER и т.д.).
- Запрос должен быть полным и готовым к выполнению.
- Не добавляй условия, которые исключают большую часть данных, если это не требуется в запросе.
- Будь осторожен с условиями WHERE - если не требуется явно отфильтровать данные, лучше не использовать ограничивающие условия.
- Обработай запрос пользователя буквально, не добавляя собственных условий и ограничений.
"""
        # Если в файле несколько листов, добавляем подсказку
        if len(sheet_names) > 1:
            sql_prompt += f"""
- В таблице есть столбец '_sheet_name', который указывает, из какого листа Excel взята строка ({', '.join(sheet_names)}).
  Используй этот столбец, если нужно фильтровать данные по конкретному листу.
"""

        try:
            sql_response = chat_with_gpt(client, args.model, sql_prompt, temperature=0)

            # Извлечем SQL-запрос, если он обернут в тройные кавычки
            if "```sql" in sql_response and "```" in sql_response.split("```sql", 1)[1]:
                sql_query = sql_response.split("```sql", 1)[1].split("```", 1)[0].strip()
            elif "```" in sql_response and "```" in sql_response.split("```", 1)[1]:
                sql_query = sql_response.split("```", 1)[1].split("```", 1)[0].strip()
            else:
                sql_query = sql_response.strip()

            # Проверяем, что ответ начинается с SELECT
            if not sql_query.upper().startswith("SELECT"):
                print("Сгенерированный ответ не является корректным SQL-запросом. Проверьте промпт для GPT.")
                conn.close()
                sys.exit(1)

            print("\nСгенерированный SQL-запрос:")
            print(sql_query)
        except Exception as e:
            print(f"Ошибка при вызове GPT для генерации SQL: {e}")
            conn.close()
            sys.exit(1)


        # === Шаг 6. Пытаемся выполнить SQL-запрос с обработкой возможных таймаутов ===
        try:
            # Устанавливаем таймаут и другие настройки SQLite для оптимизации
            conn.execute(f"PRAGMA busy_timeout = {args.timeout * 1000}")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 30000000000")  # ~30GB

            # Устанавливаем режим доступа по строкам для экономии памяти
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            print(f"Начало выполнения SQL-запроса... (таймаут: {args.timeout} сек)")
            start_time = time.time()

            # Выполняем запрос
            cursor.execute(sql_query)

            # Получаем имена столбцов из результата запроса
            columns_desc = [description[0] for description in cursor.description]

            # Извлекаем данные небольшими пакетами
            BATCH_SIZE = 1000
            all_results = []
            rows_processed = 0

            while True:
                batch = cursor.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                all_results.extend([dict(zip(columns_desc, row)) for row in batch])
                rows_processed += len(batch)

                elapsed = time.time() - start_time
                if rows_processed % 10000 == 0:
                    print(f"Обработано строк: {rows_processed} за {elapsed:.2f} сек")

                # Проверяем таймаут
                if elapsed > args.timeout:
                    print(f"Превышено время выполнения запроса ({args.timeout} сек).")
                    break

            query_result = all_results

            end_time = time.time()
            print(f"Запрос выполнен за {end_time - start_time:.2f} сек. Получено строк: {len(query_result)}")

        except sqlite3.OperationalError as e:
            if "timeout" in str(e):
                print(f"\nОшибка: Превышено время выполнения SQL-запроса. {e}")
                clar_prompt = f"""
    SQL-запрос:
    {sql_query}

    При выполнении запроса возник таймаут. Это значит, что запрос слишком сложный или данных слишком много.
    Предложи, как можно упростить запрос или ограничить объем данных, не теряя сути запроса пользователя: "{args.query}".
    """
                try:
                    clar_response = chat_with_gpt(client, args.model, clar_prompt, temperature=0.7)
                    clar_question = clar_response.strip()
                except Exception:
                    clar_question = "Запрос слишком сложный и требует больше времени для выполнения. Пожалуйста, упростите запрос или дайте более конкретные условия фильтрации."
                print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                print(clar_question)
                conn.close()
                sys.exit(1)
            else:
                print(f"\nОшибка при выполнении SQL-запроса: {e}")
                clar_prompt = f"""SQL-запрос:
    {sql_query}

    При выполнении этого запроса возникла ошибка: {e}.
    Сформулируй, пожалуйста, уточняющий вопрос для пользователя, чтобы он мог скорректировать запрос или данные."""
                try:
                    clar_response = chat_with_gpt(client, args.model, clar_prompt, temperature=0.7)
                    clar_question = clar_response.strip()
                except Exception:
                    clar_question = f"Ошибка при выполнении запроса: {e}. Пожалуйста, проверьте запрос и формат данных."
                print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                print(clar_question)
                conn.close()
                sys.exit(1)
        except Exception as e:
            print(f"\nНепредвиденная ошибка при выполнении SQL-запроса: {e}")
            print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
            print(f"Произошла ошибка при выполнении запроса: {e}")
            conn.close()
            sys.exit(1)

        # === Шаг 7. Проверяем результат ===
        if not query_result:
            print("\nНичего не найдено по вашему запросу.")
            clar_prompt = f"""Запрос:
    {sql_query}

    Результат пустой. Возможно, не нашлось данных, удовлетворяющих условиям.
    Сформулируй уточняющий вопрос или возможную причину, почему нет данных. Также предложи, как можно изменить запрос, чтобы получить результаты."""
            try:
                clar_response = chat_with_gpt(client, args.model, clar_prompt, temperature=0.7)
                clar_question = clar_response.strip()
            except Exception:
                clar_question = "Ничего не найдено по вашему запросу. Возможно, стоит изменить условия фильтрации."
            print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
            print(clar_question)
            conn.close()
            sys.exit(1)
        else:
            # === Шаг 8. Если результат получен, сохраняем его в Excel-файл ===
            try:
                print(f"Создаю итоговую таблицу... (строк: {len(query_result)})")

                # Преобразуем список словарей в DataFrame
                df_result = pd.DataFrame(query_result)
                output_file = args.output

                # Оптимизируем сохранение для больших результатов
                with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                    df_result.to_excel(writer, index=False, sheet_name='Результаты')

                    # Если результат большой, сохраняем также сводную таблицу
                    if len(df_result) > 1000:
                        try:
                            # Пробуем создать сводную таблицу для большого набора данных
                            # На основе запроса пользователя определяем ключевые поля
                            summary_prompt = f"""
    На основе запроса пользователя: "{args.query}"
    И структуры результирующей таблицы с {len(df_result)} строками и столбцами: {', '.join(df_result.columns)}

    Определи:
    1. Какой столбец лучше всего использовать для группировки данных в сводной таблице?
    2. Какое агрегирование следует применить (сумма, среднее, количество)?
    3. Какие столбцы стоит вывести в качестве значений?

    Верни только название столбца для группировки, тип агрегирования и названия столбцов для значений, разделенные запятыми.
    """
                            summary_response = chat_with_gpt(client, args.model, summary_prompt,
                                                             temperature=0.5).strip()

                            # Разбираем ответ
                            parts = summary_response.split(',')
                            if len(parts) >= 3:
                                group_by = parts[0].strip()
                                agg_type = parts[1].strip()
                                value_cols = [col.strip() for col in parts[2:]]

                                if group_by in df_result.columns:
                                    # Создаем сводную таблицу
                                    print(f"Создаю сводную таблицу с группировкой по '{group_by}'...")

                                    # Определяем метод агрегации
                                    agg_method = 'sum'
                                    if 'сред' in agg_type.lower():
                                        agg_method = 'mean'
                                    elif 'колич' in agg_type.lower() or 'count' in agg_type.lower():
                                        agg_method = 'count'

                                    # Создаем словарь для агрегаций
                                    agg_dict = {col: agg_method for col in value_cols if col in df_result.columns}

                                    if agg_dict:
                                        # Создаем сводную таблицу
                                        pivot_df = df_result.groupby(group_by).agg(agg_dict).reset_index()
                                        pivot_df.to_excel(writer, index=False, sheet_name='Сводная')
                        except Exception as e:
                            print(f"Не удалось создать сводную таблицу: {e}")

                print(f"Таблица успешно сохранена в файл: {output_file}")

                # Создаем краткое описание результата
                summary_prompt = f"""
    Я создал таблицу по запросу пользователя: "{args.query}"

    Получилась таблица размером {len(df_result)} строк на {len(df_result.columns)} столбцов.
    Столбцы таблицы: {', '.join(df_result.columns.tolist())}

    Первые 5 строк таблицы:
    {df_result.head().to_string(index=False)}

    Опиши кратко (2-3 предложения), что представляет собой эта таблица и какую информацию она содержит.
    """
                try:
                    table_description = chat_with_gpt(client, args.model, summary_prompt, temperature=0.7)
                except Exception as e:
                    table_description = f"Таблица успешно создана по вашему запросу. Содержит {len(df_result)} строк и {len(df_result.columns)} столбцов."

                print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                print(table_description)
                print(f"\nТаблица сохранена в файл: {output_file}")

            except Exception as e:
                print(f"Ошибка при сохранении результатов: {e}")
                print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
                print(
                    f"Произошла ошибка при сохранении результатов: {e}. Пожалуйста, попробуйте с меньшим объемом данных.")
            finally:
                conn.close()

    except KeyboardInterrupt:
        print("Прерывание выполнения пользователем.")
        sys.exit(1)
    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()