import os
import sys
import json
import pandas as pd
import sqlite3
import argparse
import hashlib
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Глобальный кэш для хранения обработанных файлов
file_cache = {}


def chat_with_gpt(client, model, messages, temperature=0):
    """Удобная обёртка для вызова GPT через API с поддержкой истории чата."""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
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
    parser = argparse.ArgumentParser(description='Ответ на вопросы по данным из Excel-файла')
    parser.add_argument('file_path', help='Путь к Excel-файлу')
    parser.add_argument('query', help='Вопрос пользователя')
    parser.add_argument('--api_key', help='API ключ OpenAI (или будет использован из переменной окружения)')
    parser.add_argument('--model', default="gpt-4", help='Модель OpenAI (по умолчанию: gpt-4)')
    parser.add_argument('--chat-history', help='Путь к JSON-файлу с историей чата')
    parser.add_argument('--cache', action='store_true', help='Использовать кэширование данных')

    args = parser.parse_args()

    # Проверка существования файла
    if not os.path.exists(args.file_path):
        print(f"Ошибка: файл {args.file_path} не найден.")
        sys.exit(1)

    # Загружаем историю чата, если она есть
    chat_history = []
    if args.chat_history and os.path.exists(args.chat_history):
        try:
            with open(args.chat_history, 'r', encoding='utf-8') as f:
                chat_history = json.load(f)
            print(f"История чата загружена из {args.chat_history}")
        except Exception as e:
            print(f"Ошибка при загрузке истории чата: {e}")
            # Продолжаем с пустой историей
            chat_history = []

    # Информация о запуске
    print(f"Анализирую файл: {args.file_path}")
    print(f"Вопрос пользователя: {args.query}")

    # Настройка API ключа
    api_key = os.getenv("CHATGPT_API_KEY")

    # Создание клиента OpenAI
    client = OpenAI(api_key=api_key)

    # Создание временного имени для базы данных
    db_file = f"temp_db_{os.path.basename(args.file_path).replace('.', '_')}.sqlite"
    table_name = 'data'  # Здесь всегда используем имя таблицы 'data'

    # Проверка наличия данных в кэше
    file_hash = get_file_hash(args.file_path)
    cache_key = f"{file_hash}_{db_file}"

    try:
        # Шаг 1: Загрузка Excel-файла в DataFrame
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
                    df_sheet = pd.read_excel(args.file_path, sheet_name=sheet_name)
                    # Добавляем столбец с именем листа для отслеживания
                    df_sheet['_sheet_name'] = sheet_name
                    all_dfs.append(df_sheet)
                    print(
                        f"Лист '{sheet_name}' прочитан. Количество строк: {len(df_sheet)}, столбцов: {len(df_sheet.columns) - 1}")

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

        # Шаг 2: Создание/перезапись таблицы в базе SQLite
        try:
            # Проверяем, нужно ли создавать базу данных заново
            if not args.cache or not os.path.exists(db_file):
                conn = sqlite3.connect(db_file)
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"Данные успешно импортированы в SQLite.")
            else:
                conn = sqlite3.connect(db_file)
        except Exception as e:
            print(f"Ошибка при работе с базой данных: {e}")
            sys.exit(1)

        # Шаг 3: Извлечение схемы таблицы и примеров строк
        try:
            cursor = conn.cursor()
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

            # Получаем первые 5 строк из таблицы
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
            rows = cursor.fetchall()
            examples_str = "\n".join([str(row) for row in rows])
        except Exception as e:
            print(f"Ошибка при извлечении схемы таблицы: {e}")
            conn.close()
            sys.exit(1)

        # Подготовка сообщений для модели
        messages = []

        # Добавляем системный промпт, если истории нет или начинаем новый чат
        if not chat_history or chat_history[0]["role"] != "system":
            system_prompt = f"""Ты ассистент для анализа данных Excel. У меня есть таблица '{table_name}' в базе данных SQLite.
Таблица '{table_name}' содержит столбцы:
{schema_str}

Примеры строк:
{examples_str}

Твоя задача - помогать анализировать данные из этой таблицы. 
Отвечай на вопросы, генерируя SQL-запросы к данным, а затем объясняя результаты.
Если нужно, используй подзапросы, группировку, объединения и другие SQL-конструкции для точного анализа.
"""
            # Добавляем информацию о структуре файла, если листов несколько
            if len(sheet_names) > 1:
                system_prompt += f"\nВажно: Excel-файл содержит {len(sheet_names)} листов: {', '.join(sheet_names)}.\n"
                system_prompt += "Все данные из разных листов объединены в одну таблицу с дополнительным столбцом '_sheet_name', который указывает, из какого листа взята строка.\n"

            messages.append({"role": "system", "content": system_prompt})

        # Добавляем историю чата, если она есть
        if chat_history:
            # Если первое сообщение системное, добавляем всю историю
            if chat_history[0]["role"] == "system":
                messages = chat_history.copy()
            # Иначе добавляем наш системный промпт, а затем историю чата
            else:
                messages.extend(chat_history)

        # Добавляем текущий вопрос пользователя, если его еще нет в истории
        if not chat_history or chat_history[-1]["role"] != "user" or chat_history[-1]["content"] != args.query:
            messages.append({"role": "user", "content": args.query})

        # Шаг 4: Формирование промпта для генерации SQL
        sql_prompt = f"""На основе предыдущей информации о таблице, напиши корректный SQL-запрос (SQLite) для ответа на вопрос: "{args.query}"
Важно: 
1. Таблица называется '{table_name}' (не используй другое имя таблицы).
2. Если имя столбца содержит пробелы или специальные символы, оборачивай его в двойные кавычки.
"""
        # Добавляем информацию о многолистовой структуре, если нужно
        if len(sheet_names) > 1:
            sql_prompt += f"3. В таблице есть столбец '_sheet_name', который указывает, из какого листа Excel взята строка ({', '.join(sheet_names)}).\n"
            sql_prompt += "   Используй этот столбец, если нужно фильтровать данные по конкретному листу.\n"

        sql_prompt += "Верни только SQL-запрос, ничего больше."

        # Создаем временные сообщения с запросом на SQL
        sql_messages = messages.copy()
        sql_messages.append({"role": "user", "content": sql_prompt})

        print("Формирую SQL-запрос на основе вопроса...")

        # Шаг 5: Вызов GPT для генерации SQL-запроса
        try:
            gpt_sql = chat_with_gpt(client, args.model, sql_messages, temperature=0)
            print("SQL-запрос сформирован.")
        except Exception as e:
            print(f"Ошибка при вызове GPT для генерации SQL: {e}")
            conn.close()
            sys.exit(1)

        # Извлечем SQL-запрос, если он обернут в тройные кавычки или код
        if "```sql" in gpt_sql and "```" in gpt_sql.split("```sql", 1)[1]:
            gpt_sql = gpt_sql.split("```sql", 1)[1].split("```", 1)[0].strip()
        elif "```" in gpt_sql and "```" in gpt_sql.split("```", 1)[1]:
            gpt_sql = gpt_sql.split("```", 1)[1].split("```", 1)[0].strip()

        print(f"Итоговый SQL-запрос: {gpt_sql}")

        # Шаг 6: Выполнение сгенерированного SQL-запроса в базе данных
        try:
            cursor.execute(gpt_sql)
            query_result = cursor.fetchall()

            # Получаем имена столбцов из результата запроса
            column_names = [description[0] for description in cursor.description]
            print(f"Запрос успешно выполнен. Получено строк: {len(query_result)}")
        except Exception as e:
            print(f"Ошибка при выполнении SQL-запроса: {e}")

            # Даже при ошибке, пытаемся дать содержательный ответ
            error_messages = messages.copy()
            error_messages.append({"role": "assistant",
                                   "content": f"Я попытался выполнить SQL-запрос, но возникла ошибка: {e}. Могу я помочь с другим подходом?"})

            error_response = chat_with_gpt(client, args.model, error_messages, temperature=0.7)
            print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
            print(error_response)

            # Не закрываем соединение, если используется кэширование
            if not args.cache:
                conn.close()
            sys.exit(1)

        # Шаг 7: Получение ответа от GPT на основе результата SQL
        try:
            # Подготовим результат в более читаемом виде
            result_str = "Результат запроса:\n"
            result_str += ", ".join(column_names) + "\n"
            for row in query_result[:10]:  # Ограничиваем до 10 строк для ответа
                result_str += str(row) + "\n"
            if len(query_result) > 10:
                result_str += f"... и еще {len(query_result) - 10} строк"

            # Вместо сложного промпта с историей чата, используем прямой запрос на интерпретацию
            summary_prompt = f"""
Я выполнил SQL-запрос для вопроса "{args.query}" и получил следующий результат:

SQL-запрос: {gpt_sql}

{result_str}

Пожалуйста, сформулируй информативный ответ на вопрос пользователя на основе этих данных.
Пиши так, как будто напрямую отвечаешь на вопрос: "{args.query}"
Не упоминай про SQL или запросы в своем ответе - просто дай фактический ответ на вопрос.
"""
            # Если результат пустой, добавляем информацию о многолистовой структуре
            if len(query_result) == 0 and len(sheet_names) > 1:
                summary_prompt += f"\nУчти, что Excel-файл содержит несколько листов: {', '.join(sheet_names)}. "
                summary_prompt += "Возможно, нужная информация находится на одном из этих листов."

            # Вызываем GPT для получения естественно-языкового ответа
            natural_language_answer = chat_with_gpt(client, args.model, [{"role": "user", "content": summary_prompt}],
                                                    temperature=0.7)

            # Выводим ответ (это будет возвращено боту)
            print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
            print(natural_language_answer)

            # Добавляем ответ в историю, если она есть
            if chat_history:
                messages.append({"role": "assistant", "content": natural_language_answer})

        except Exception as e:
            print(f"Ошибка при получении естественного ответа от GPT: {e}")
        finally:
            # Не закрываем соединение, если используется кэширование
            if not args.cache:
                conn.close()

        # Шаг 8: Удаление временной базы данных только если не используется кэширование
        if not args.cache:
            try:
                if os.path.exists(db_file):
                    os.remove(db_file)
            except Exception as e:
                print(f"Предупреждение: не удалось удалить временную базу данных: {e}")

    except KeyboardInterrupt:
        print("Прерывание выполнения пользователем.")
        sys.exit(1)
    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()