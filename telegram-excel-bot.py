import os
import logging
import tempfile
import io
import asyncio
import subprocess
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ID папки Google Drive
FOLDER_ID = '1uMBbz1UOkb9XJEImYOdfbiDpREeuVG-q'

# Временная директория для скачанных файлов
temp_dir = tempfile.mkdtemp()
logger.info(f"Создана временная директория для файлов: {temp_dir}")

# Глобальная переменная для сервиса Drive API
drive_service = None
# Словарь для хранения состояний пользователей
user_states = {}
# Словарь для хранения данных пользователей
user_data = {}

# Состояния бота
STATE_IDLE = 'idle'
STATE_WAITING_QUERY = 'waiting_query'
STATE_CHAT_MODE = 'chat_mode'  # Новое состояние для режима чата

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Логирует ошибки, вызванные обновлениями."""
    logger.error(f"Произошла ошибка в обновлении {update}: {context.error}")
    # Отправляем сообщение пользователю о проблеме
    if update and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Извините, произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте позже."
        )
def create_drive_service():
    """Создаем сервис Google Drive API с сервисным аккаунтом."""
    global drive_service

    try:
        service_account_json = os.getenv('SERVICE_ACCOUNT')
        service_account_info = json.loads(service_account_json)

        # Создаем учетные данные используя from_service_account_info вместо from_service_account_file
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=['https://www.googleapis.com/auth/drive.readonly'])

        # Строим сервис
        drive_service = build('drive', 'v3', credentials=credentials)
        logger.info("Drive API сервис успешно создан")
        return True
    except Exception as e:
        logger.error(f"Ошибка при создании сервиса Drive API: {e}")
        return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user_id = update.effective_user.id
    logger.info(f"Получена команда /start от пользователя {user_id}")

    # Создаем сервис Google Drive API при первом старте
    if drive_service is None:
        success = create_drive_service()
        if not success:
            await update.message.reply_text(
                "Не удалось подключиться к Google Drive. Попробуйте позже."
            )
            return

    # Сбрасываем состояние пользователя
    user_states[user_id] = STATE_IDLE
    if user_id in user_data:
        user_data[user_id] = {}

    # Отправляем приветственное сообщение и запрашиваем список файлов
    await update.message.reply_text("Подключаюсь к Google Drive и получаю список файлов...")
    await list_excel_files(update, context)


async def list_excel_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает список Excel-файлов из корневой папки."""
    user_id = update.effective_user.id

    try:
        # Получаем список Excel-файлов в папке
        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and trashed = false and (mimeType='application/vnd.ms-excel' or mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')",
            pageSize=10,
            fields="files(id, name, mimeType)"
        ).execute()

        items = results.get('files', [])

        if not items:
            await update.message.reply_text(
                "В папке нет Excel-файлов. Пожалуйста, добавьте файлы в папку Google Drive."
            )
        else:
            # Формируем список кнопок для файлов
            keyboard = []
            for item in items:
                keyboard.append([
                    InlineKeyboardButton(f"📊 {item['name']}", callback_data=f"excel_{item['id']}")
                ])

            # Добавляем кнопку для обновления списка
            keyboard.append([InlineKeyboardButton("🔄 Обновить список", callback_data="refresh_files")])

            await update.message.reply_text(
                "Выберите Excel-файл для анализа:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    except Exception as e:
        logger.error(f"Ошибка при получении списка Excel-файлов: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при получении списка файлов: {e}"
        )


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на все кнопки."""
    query = update.callback_query
    user_id = update.effective_user.id

    logger.info(f"Получен callback от пользователя {user_id}: {query.data}")

    await query.answer()  # Обязательно отвечаем на callback

    if query.data == "refresh_files":
        # Обновляем список файлов
        await refresh_files(update, context)
    elif query.data.startswith("excel_"):
        # Пользователь выбрал Excel-файл
        # Формат: excel_[file_id]
        parts = query.data.split("_", 1)
        if len(parts) == 2:
            file_id = parts[1]  # Берем все, что после первого "_"

            # Обновляем сообщение, чтобы пользователь знал, что происходит обработка
            await query.edit_message_text("Получаю информацию о файле...")

            # Получаем имя файла по ID
            try:
                file = drive_service.files().get(fileId=file_id, fields='name').execute()
                file_name = file['name']

                # Обработка большого файла
                if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                    await query.edit_message_text(
                        f"Выбран файл: {file_name}\nПодготовка к обработке...")

                # Переходим к выбору действий
                await excel_file_selected(update, context, file_id, file_name)
            except Exception as e:
                logger.error(f"Ошибка при получении информации о файле: {e}")
                await query.edit_message_text(f"Ошибка при получении информации о файле: {e}")
    elif query.data == "action_question":
        # Пользователь выбрал "Задать вопрос"
        user_states[user_id] = STATE_WAITING_QUERY
        user_data[user_id]['action_type'] = 'question'
        await query.edit_message_text(
            f"Выбран файл: {user_data[user_id]['file_name']}\n\n"
            "Введите ваш вопрос к данным:"
        )
    elif query.data == "action_table":
        # Пользователь выбрал "Составить таблицу"
        user_states[user_id] = STATE_WAITING_QUERY
        user_data[user_id]['action_type'] = 'table'
        await query.edit_message_text(
            f"Выбран файл: {user_data[user_id]['file_name']}\n\n"
            "Опишите, какую таблицу нужно составить:"
        )
    elif query.data == "back_to_files":
        # Возвращаемся к списку файлов
        user_states[user_id] = STATE_IDLE
        if user_id in user_data:
            user_data[user_id] = {}
        await refresh_files(update, context)
    elif query.data == "new_query":
        # Новый запрос для того же файла
        action_type = user_data[user_id]['action_type']
        user_states[user_id] = STATE_WAITING_QUERY
        await query.edit_message_text(
            f"Файл: {user_data[user_id]['file_name']}\n\n"
            f"{'Введите новый вопрос к данным:' if action_type == 'question' else 'Опишите, какую таблицу нужно составить:'}"
        )
    elif query.data == "main_menu":
        # Возвращаемся в главное меню
        user_states[user_id] = STATE_IDLE
        if user_id in user_data:
            user_data[user_id] = {}
        await refresh_files(update, context)
    elif query.data == "end_chat":
        # Завершаем режим чата
        # Очищаем историю чата, но оставляем информацию о файле
        if 'chat_history' in user_data[user_id]:
            del user_data[user_id]['chat_history']

        user_states[user_id] = STATE_IDLE

        # Показываем меню действий
        keyboard = [
            [InlineKeyboardButton("🔄 Новый запрос", callback_data="new_query")],
            [InlineKeyboardButton("📁 Выбрать другой файл", callback_data="main_menu")]
        ]

        await query.edit_message_text(
            "Чат завершен. Что делаем дальше?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif query.data == "generate_report":
        # Генерируем отчет на основе чата
        await generate_report_from_chat(update, context)


async def refresh_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновляет список файлов."""
    query = update.callback_query

    try:
        # Получаем список Excel-файлов в папке
        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and trashed = false and (mimeType='application/vnd.ms-excel' or mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')",
            pageSize=10,
            fields="files(id, name, mimeType)"
        ).execute()

        items = results.get('files', [])

        if not items:
            await query.edit_message_text(
                "В папке нет Excel-файлов. Пожалуйста, добавьте файлы в папку Google Drive."
            )
        else:
            # Формируем список кнопок для файлов
            keyboard = []
            for item in items:
                keyboard.append([
                    InlineKeyboardButton(f"📊 {item['name']}", callback_data=f"excel_{item['id']}")
                ])

            # Добавляем кнопку для обновления списка с меткой времени для уникальности
            from datetime import datetime
            current_time = datetime.now().strftime("%H:%M:%S")
            keyboard.append(
                [InlineKeyboardButton(f"🔄 Обновить список ({current_time})", callback_data="refresh_files")])

            # Добавляем что-то к тексту сообщения чтобы сделать его уникальным
            message_text = f"Выберите Excel-файл для анализа (обновлено в {current_time}):"

            try:
                await query.edit_message_text(
                    message_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as e:
                logger.error(f"Ошибка при обновлении списка файлов: {e}")
                # Если не удалось обновить, отправляем новое сообщение
                await query.message.reply_text(
                    message_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
    except Exception as e:
        logger.error(f"Ошибка при обновлении списка файлов: {e}")
        try:
            await query.edit_message_text(
                f"Произошла ошибка при обновлении списка файлов: {e}"
            )
        except:
            # Если не удалось обновить сообщение, отправляем новое
            await query.message.reply_text(
                f"Произошла ошибка при обновлении списка файлов: {e}"
            )


async def excel_file_selected(update: Update, context: ContextTypes.DEFAULT_TYPE, file_id: str, file_name: str):
    """Обработка выбора Excel-файла."""
    query = update.callback_query
    user_id = update.effective_user.id

    # Сохраняем информацию о файле
    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]['file_id'] = file_id
    user_data[user_id]['file_name'] = file_name

    # Показываем меню действий
    keyboard = [
        [InlineKeyboardButton("❓ Задать вопрос", callback_data="action_question")],
        [InlineKeyboardButton("📊 Составить таблицу", callback_data="action_table")],
        [InlineKeyboardButton("🔙 Выбрать другой файл", callback_data="back_to_files")]
    ]

    await query.edit_message_text(
        f"Выбран файл: {file_name}\n\n"
        "Выберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстового ввода от пользователя."""
    user_id = update.effective_user.id
    user_query = update.message.text

    # Проверяем, есть ли пользователь в системе состояний
    if user_id not in user_states:
        return

    current_state = user_states[user_id]

    # Проверяем, в каком состоянии находится пользователь
    if current_state == STATE_WAITING_QUERY:
        # Начальный запрос пользователя
        # Проверяем, есть ли данные о файле
        if user_id not in user_data or 'file_id' not in user_data[user_id]:
            await update.message.reply_text("Сначала выберите файл. Используйте /start для начала.")
            return

        logger.info(f"Получен запрос от пользователя {user_id}: {user_query}")

        # Сохраняем запрос
        user_data[user_id]['query'] = user_query

        # Отправляем сообщение о начале обработки
        processing_message = await update.message.reply_text(
            "Обрабатываю ваш запрос...\n"
            "Скачиваю файл из Google Drive..."
        )

        # Скачиваем файл
        file_id = user_data[user_id]['file_id']
        file_name = user_data[user_id]['file_name']
        action_type = user_data[user_id]['action_type']

        try:
            # Скачиваем файл
            downloaded_file_path = await download_file(file_id, file_name)

            if not downloaded_file_path:
                await processing_message.edit_text(
                    "Ошибка при скачивании файла. Пожалуйста, попробуйте еще раз."
                )
                return

            # Обновляем сообщение о статусе
            await processing_message.edit_text(
                f"Файл успешно скачан. Обрабатываю запрос: '{user_query}'"
            )

            # Сохраняем путь к файлу
            user_data[user_id]['file_path'] = downloaded_file_path

            if action_type == 'question':
                # Инициализируем режим чата для запросов
                await start_chat_mode(update, context, processing_message, user_query, downloaded_file_path)
            else:
                # Запускаем скрипт table_file_answer.py
                result = await run_script('table_file_answer.py', downloaded_file_path, user_query)

                # Если результат - файл
                if os.path.exists('final.xlsx'):
                    # Отправляем файл пользователю
                    await processing_message.edit_text(
                        f"Таблица готова! Отправляю файл..."
                    )

                    with open('final.xlsx', 'rb') as file:
                        await update.message.reply_document(
                            document=file,
                            filename='result.xlsx',
                            caption="Результат обработки вашего запроса."
                        )

                    # Удаляем временный файл
                    os.remove('final.xlsx')
                else:
                    # Отправляем текстовый результат
                    await processing_message.edit_text(
                        f"Результат обработки вашего запроса:\n\n{result}"
                    )

                # Показываем кнопки для дальнейших действий
                keyboard = [
                    [InlineKeyboardButton("🔄 Новый запрос", callback_data="new_query")],
                    [InlineKeyboardButton("📁 Выбрать другой файл", callback_data="main_menu")]
                ]

                await update.message.reply_text(
                    "Что делаем дальше?",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

        except Exception as e:
            logger.error(f"Ошибка при обработке запроса: {e}")
            await processing_message.edit_text(
                f"Произошла ошибка при обработке запроса: {e}"
            )

    elif current_state == STATE_CHAT_MODE:
        # Обрабатываем вопрос пользователя в режиме чата
        await process_chat_query(update, context, user_query)


async def start_chat_mode(update: Update, context: ContextTypes.DEFAULT_TYPE, message_obj, user_query, file_path):
    """Начинает режим чата и обрабатывает первый вопрос."""
    user_id = update.effective_user.id

    # Переходим в режим чата
    user_states[user_id] = STATE_CHAT_MODE

    # Инициализируем историю чата, если её нет
    if 'chat_history' not in user_data[user_id]:
        user_data[user_id]['chat_history'] = []

    # Добавляем системный промпт для улучшения контекста
    system_prompt = f"Ты ассистент, анализирующий Excel-файл '{user_data[user_id]['file_name']}'. Отвечай на вопросы о данных в этом файле. Будь точным и информативным."

    # Добавляем первый вопрос пользователя в историю чата
    user_data[user_id]['chat_history'].append({"role": "system", "content": system_prompt})
    user_data[user_id]['chat_history'].append({"role": "user", "content": user_query})

    # Запускаем скрипт answer.py с использованием нового параметра для истории чата
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as chat_file:
        json.dump(user_data[user_id]['chat_history'], chat_file)
        chat_history_path = chat_file.name

    # Обрабатываем первый запрос
    result = await run_script('answer.py', file_path, user_query, chat_history=chat_history_path)

    # Удаляем временный файл с историей чата
    if os.path.exists(chat_history_path):
        os.remove(chat_history_path)

    # Ищем раздел с результатом для пользователя
    if "=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===" in result:
        parts = result.split("=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===")
        if len(parts) > 1:
            result = parts[1].strip()

    # Добавляем ответ в историю чата
    user_data[user_id]['chat_history'].append({"role": "assistant", "content": result})

    # Отправляем ответ и добавляем кнопки
    keyboard = [
        [InlineKeyboardButton("📊 Сгенерировать отчет", callback_data="generate_report")],
        [InlineKeyboardButton("🔚 Завершить чат", callback_data="end_chat")]
    ]

    await message_obj.edit_text(
        f"Ответ на ваш вопрос:\n\n{result}\n\nВы можете задать следующий вопрос или выбрать действие ниже:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def process_chat_query(update: Update, context: ContextTypes.DEFAULT_TYPE, user_query):
    """Обрабатывает вопрос пользователя в режиме чата."""
    user_id = update.effective_user.id
    file_path = user_data[user_id]['file_path']

    # Отправляем сообщение о начале обработки
    processing_message = await update.message.reply_text(
        f"Обрабатываю ваш вопрос: '{user_query}'"
    )

    # Добавляем вопрос пользователя в историю чата
    user_data[user_id]['chat_history'].append({"role": "user", "content": user_query})

    # Сохраняем историю чата во временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as chat_file:
        json.dump(user_data[user_id]['chat_history'], chat_file)
        chat_history_path = chat_file.name

    # Запускаем скрипт для получения ответа
    result = await run_script('answer.py', file_path, user_query, chat_history=chat_history_path)

    # Удаляем временный файл с историей чата
    if os.path.exists(chat_history_path):
        os.remove(chat_history_path)

    # Ищем раздел с результатом для пользователя
    if "=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===" in result:
        parts = result.split("=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===")
        if len(parts) > 1:
            result = parts[1].strip()

    # Добавляем ответ в историю чата
    user_data[user_id]['chat_history'].append({"role": "assistant", "content": result})

    # Показываем кнопки действий
    keyboard = [
        [InlineKeyboardButton("📊 Сгенерировать отчет", callback_data="generate_report")],
        [InlineKeyboardButton("🔚 Завершить чат", callback_data="end_chat")]
    ]

    await processing_message.edit_text(
        f"Ответ на ваш вопрос:\n\n{result}\n\nВы можете задать следующий вопрос или выбрать действие ниже:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def generate_report_from_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Генерирует отчет на основе истории чата."""
    query = update.callback_query
    user_id = update.effective_user.id

    await query.edit_message_text("Генерирую отчет на основе нашего диалога...")

    # Проверяем наличие истории чата
    if 'chat_history' not in user_data[user_id] or len(user_data[user_id]['chat_history']) < 3:
        await query.edit_message_text("Недостаточно данных для создания отчета. Задайте больше вопросов.")
        return

    file_path = user_data[user_id]['file_path']

    # Создаем промпт для генерации отчета
    chat_summary = "\n".join([
        f"{msg['role'].upper()}: {msg['content']}"
        for msg in user_data[user_id]['chat_history']
        if msg['role'] != 'system'
    ])

    report_prompt = f"На основе нашего диалога создай структурированный отчет:"

    # Сохраняем историю чата и промпт во временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as chat_file:
        # Добавляем специальный запрос для создания отчета
        report_history = user_data[user_id]['chat_history'].copy()
        report_history.append({"role": "user", "content": report_prompt})
        json.dump(report_history, chat_file)
        chat_history_path = chat_file.name

    # Запускаем скрипт для генерации отчета
    result = await run_script('answer.py', file_path, report_prompt, chat_history=chat_history_path)

    # Удаляем временный файл
    if os.path.exists(chat_history_path):
        os.remove(chat_history_path)

    # Ищем раздел с результатом
    if "=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===" in result:
        parts = result.split("=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===")
        if len(parts) > 1:
            result = parts[1].strip()

    # Отправляем результат
    await query.edit_message_text(f"Отчет на основе нашего диалога:\n\n{result}")

    # Показываем кнопки для дальнейших действий
    keyboard = [
        [InlineKeyboardButton("🔄 Продолжить чат", callback_data="new_query")],
        [InlineKeyboardButton("🔚 Завершить чат", callback_data="end_chat")]
    ]

    await context.bot.send_message(
        chat_id=user_id,
        text="Что делаем дальше?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def download_file(file_id, file_name):
    """Скачивает файл из Google Drive."""
    try:
        # Отправляем запрос на получение файла с прогрессом
        request = drive_service.files().get_media(fileId=file_id)

        # Создаем файловый объект
        file_path = os.path.join(temp_dir, file_name)

        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False

            # Скачиваем с отслеживанием прогресса
            while not done:
                status, done = downloader.next_chunk()
                progress = int(status.progress() * 100)
                logger.info(f"Скачивание: {progress}%")
                # Здесь можно было бы обновлять сообщение с прогрессом каждые 10%

        logger.info(f"Файл успешно скачан: {file_path}")
        return file_path

    except Exception as e:
        logger.error(f"Ошибка при скачивании файла: {e}")
        return None


async def run_script(script_name, file_path, query, **kwargs):
    """Запускает Python-скрипт и возвращает результат."""
    try:
        # Подготавливаем команду для запуска скрипта
        if script_name == 'answer.py':
            # Для скрипта вопросов
            cmd = ["python", script_name, file_path, query]

            # Добавляем путь к файлу истории чата, если есть
            if 'chat_history' in kwargs and os.path.exists(kwargs['chat_history']):
                # Проверяем, поддерживает ли скрипт chat-history
                # Создаем процесс для проверки аргументов
                check_process = await asyncio.create_subprocess_exec(
                    "python", script_name, "--help",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                check_stdout, check_stderr = await check_process.communicate()
                help_output = check_stdout.decode() + check_stderr.decode()

                # Если в выводе help есть упоминание chat-history, значит скрипт поддерживает этот аргумент
                if "--chat-history" in help_output:
                    cmd.extend(["--chat-history", kwargs['chat_history']])
                    logger.info(f"Используем историю чата: {kwargs['chat_history']}")
                else:
                    logger.warning("Скрипт не поддерживает параметр --chat-history. Игнорируем историю чата.")

            # Добавляем параметр кэширования для ускорения работы
            cmd.append("--cache")

        else:
            # Для скрипта создания таблиц
            output_file = 'final.xlsx'
            cmd = ["python", script_name, file_path, query, "--output", output_file]

        # Запускаем процесс асинхронно
        logger.info(f"Запускаем скрипт: {' '.join(cmd)}")

        # Создаем процесс с отслеживанием вывода в реальном времени
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # Функция для чтения вывода в реальном времени
        async def read_stream(stream):
            output = []
            while True:
                line = await stream.readline()
                if not line:
                    break
                decoded_line = line.decode('utf-8').rstrip()
                # Фильтруем строки о временной базе данных и другую техническую информацию
                if not (decoded_line.startswith("Временная база данных") or
                        "Все листы объединены" in decoded_line or
                        "Количество строк" in decoded_line or
                        "Получено строк" in decoded_line):
                    logger.info(f"Вывод скрипта: {decoded_line}")
                    output.append(decoded_line)
            return output

        # Запускаем параллельное чтение stdout и stderr
        stdout_lines, stderr_lines = await asyncio.gather(
            read_stream(process.stdout),
            read_stream(process.stderr)
        )

        # Ждем завершения процесса
        await process.wait()

        # Проверяем код завершения
        if process.returncode != 0:
            error_msg = "\n".join(stderr_lines)
            logger.error(f"Скрипт вернул ошибку: {error_msg}")
            return f"Ошибка при выполнении скрипта: {error_msg}"

        # Обрабатываем результат
        result = "\n".join(stdout_lines)

        # Ищем раздел с результатом для пользователя
        if "=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===" in result:
            parts = result.split("=== РЕЗУЛЬТАТ ДЛЯ ПОЛЬЗОВАТЕЛЯ ===")
            if len(parts) > 1:
                result = parts[1].strip()

        return result

    except asyncio.TimeoutError:
        logger.error(f"Таймаут при выполнении скрипта {script_name}")
        return f"Превышено время ожидания при обработке запроса. Файл может быть слишком большим или запрос слишком сложным."
    except Exception as e:
        logger.error(f"Ошибка при запуске скрипта {script_name}: {e}")
        return f"Произошла ошибка при запуске скрипта: {e}"

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет текущую операцию."""
    user_id = update.effective_user.id
    logger.info(f"Пользователь {user_id} отменил операцию")

    # Сбрасываем состояние пользователя
    user_states[user_id] = STATE_IDLE
    if user_id in user_data:
        user_data[user_id] = {}

    await update.message.reply_text(
        "Операция отменена. Используйте /start для начала работы."
    )


def main():
    """Основная функция."""
    try:
        logger.info("Запуск бота")

        # Создаем сервис Google Drive API
        success = create_drive_service()
        if not success:
            logger.error("Не удалось создать сервис Drive API. Завершение работы.")
            return

        # Создаем приложение с явными параметрами
        application = Application.builder().token('7820736396:AAFGm7Xy3o3kI-HqC7EXzudXHF-pHyCltDA').build()

        # Добавляем обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("cancel", cancel))
        application.add_handler(CallbackQueryHandler(button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

        # Добавляем обработчик ошибок
        application.add_error_handler(error_handler)
        # Запускаем с короткими интервалами
        logger.info("Запуск polling с короткими интервалами")
        application.run_polling(
            poll_interval=0.5,
            timeout=10,
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query"]
        )
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}")
    finally:
        logger.info("Бот остановлен")


if __name__ == '__main__':
    main()