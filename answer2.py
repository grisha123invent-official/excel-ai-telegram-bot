import os
import sys
import pandas as pd
import sqlite3
import argparse
from openai import OpenAI


def chat_with_gpt(client, model, user_content, temperature=0):
    """Удобная обёртка для вызова GPT через API."""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": user_content}],
            temperature=temperature
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Ошибка при вызове API OpenAI: {e}")
        return f"Произошла ошибка при получении ответа: {e}"


def main():
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Ответ на вопросы по данным из Excel-файла')
    parser.add_argument('file_path', help='Путь к Excel-файлу')
    parser.add_argument('query', help='Вопрос пользователя')
    parser.add_argument('--api_key', help='API ключ OpenAI (или будет использован из переменной окружения)')
    parser.add_argument('--model', default="gpt-4", help='Модель OpenAI (по умолчанию: gpt-3.5-turbo)')

    args = parser.parse_args()

    # Проверка существования файла
    if not os.path.exists(args.file_path):
        print(f"Ошибка: файл {args.file_path} не найден.")
        sys.exit(1)

    # Информация о запуске
    print(f"Анализирую файл: {args.file_path}")
    print(f"Вопрос пользователя: {args.query}")

    # Настройка API ключа
    api_key = args.api_key or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        api_key = "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"

    # Создание клиента OpenAI
    client = OpenAI(api_key=api_key)

    # Создание временного имени для базы данных
    db_file = f"temp_db_{os.path.basename(args.file_path).replace('.', '_')}.sqlite"
    table_name = 'data'

    try:
        # Шаг 1: Загрузка Excel-файла в DataFrame
        try:
            df = pd.read_excel(args.file_path)
            print(f"Файл успешно прочитан. Количество строк: {len(df)}, столбцов: {len(df.columns)}")
        except Exception as e:
            print(f"Ошибка при чтении файла Excel: {e}")
            sys.exit(1)

        # Шаг 2: Создание/перезапись таблицы в базе SQLite
        try:
            conn = sqlite3.connect(db_file)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Данные успешно импортированы в SQLite.")
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

        # Шаг 4: Формирование промпта для генерации SQL
        prompt = f"""У меня есть таблица '{table_name}' в базе данных SQLite.
Таблица '{table_name}' содержит столбцы:
{schema_str}

Примеры строк:
{examples_str}

Вопрос пользователя: "{args.query}"

Напиши корректный SQL-запрос (SQLite) для ответа на вопрос.
Важно: если имя столбца содержит пробелы или специальные символы, оборачивайте его в двойные кавычки (например, "Род вагона").
"""
        print("Формирую SQL-запрос на основе вопроса...")

        # Шаг 5: Вызов GPT для генерации SQL-запроса
        try:
            gpt_sql = chat_with_gpt(client, args.model, prompt, temperature=0)
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

        # Шаг 6: Выполнение сгенерированного SQL-запроса в базе данных
        try:
            cursor.execute(gpt_sql)
            query_result = cursor.fetchall()

            # Получаем имена столбцов из результата запроса
            column_names = [description[0] for description in cursor.description]
        except Exception as e:
            print(f"Ошибка при выполнении SQL-запроса: {e}")
            conn.close()
            sys.exit(1)

        # Шаг 7: Получение "живого" (естественного) ответа от GPT на основе результата SQL
        try:
            # Подготовим результат в более читаемом виде
            result_str = "Результат запроса:\n"
            result_str += ", ".join(column_names) + "\n"
            for row in query_result[:10]:  # Ограничиваем до 10 строк для ответа
                result_str += str(row) + "\n"
            if len(query_result) > 10:
                result_str += f"... и еще {len(query_result) - 10} строк"

            summary_prompt = f"""У меня есть результат выполнения SQL-запроса по вопросу "{args.query}".
SQL-запрос: 
{gpt_sql}

{result_str}

Сформулируй, пожалуйста, ответ для пользователя в естественной форме, понятной и живой. Например, если запрос вернул количество вагонов, то ответ может выглядеть так: "Общее количество вагонов составляет ...". 

Включи в ответ ключевую статистику из результата запроса, если она есть.
"""
            natural_language_answer = chat_with_gpt(client, args.model, summary_prompt, temperature=0.7)

            # Выводим ответ (это будет возвращено боту)
            print("\n=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===\n")
            print(natural_language_answer)

        except Exception as e:
            print(f"Ошибка при получении естественного ответа от GPT: {e}")
        finally:
            conn.close()

        # Шаг 8: Удаление временной базы данных
        try:
            if os.path.exists(db_file):
                os.remove(db_file)
                print(f"Временная база данных удалена.")
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