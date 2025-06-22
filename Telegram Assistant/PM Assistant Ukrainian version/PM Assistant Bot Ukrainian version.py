import logging
import json
import os
import csv
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ContentType
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
import dateparser  # Додано для кращого парсингу дат

# Налаштування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константи
DATA_FILE = "pm_manager_data.json"
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE" # 🔐 ВАЖЛИВО: вставте свій токен, отриманий через @BotFather
EXPORT_FOLDER = "exports"
DEFAULT_CATEGORIES = ["Робота", "Особисте", "Навчанє"]

# Перевірка токена (додано нову перевірку)
if not BOT_TOKEN or len(BOT_TOKEN) < 30:
    logger.error("Помилка: Невірний формат токена бота!")
    exit(1)

# Ініціалізація
os.makedirs(EXPORT_FOLDER, exist_ok=True)

try:
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    scheduler = AsyncIOScheduler()
except Exception as e:
    logger.error(f"Помилка ініціалізації бота: {e}")
    exit(1)

# Для зберігання ID останніх повідомлень (max 3 на користувача)
user_messages = {}

# Обробник для всіх небажаних типів контенту
@dp.message(F.content_type.in_({
    ContentType.PHOTO,
    ContentType.VIDEO,
    ContentType.DOCUMENT,
    ContentType.AUDIO,
    ContentType.VOICE,
    ContentType.VIDEO_NOTE,
    ContentType.STICKER,
    ContentType.LOCATION,
    ContentType.CONTACT,
    ContentType.POLL
}))
async def handle_unwanted_content(message: types.Message):
    await message.answer("❌ Цей тип контенту не підтримується. Будь ласка, використовуйте текст.")

# Класи станів
class ReminderStates(StatesGroup):
    waiting_for_reminder_task = State()
    waiting_for_reminder_time = State()

class TaskStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_deadline = State()
    waiting_for_category = State()
    waiting_for_task_delete = State()
    waiting_for_task_complete = State()
    waiting_for_task_uncomplete = State()

class NoteStates(StatesGroup):
    waiting_for_task_selection = State()
    waiting_for_text = State()
    waiting_for_category = State()
    waiting_for_note_delete = State()

class SearchStates(StatesGroup):
    waiting_for_search_query = State()

class ExportStates(StatesGroup):
    waiting_for_export_format = State()

class SettingsStates(StatesGroup):
    waiting_for_new_category = State()

# Функції для роботи з даними
def load_data():
    default_data = {
        "tasks": [],
        "notes": [],
        "categories": DEFAULT_CATEGORIES.copy(),
        "statistics": {}
    }
    
    if not os.path.exists(DATA_FILE):
        return default_data
    
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # Міграція старих даних
            if isinstance(data.get("tasks", []), list) and len(data.get("tasks", [])) > 0 and isinstance(data["tasks"][0], str):
                data["tasks"] = [{
                    "text": task.split(" — ")[0], 
                    "deadline": task.split(" — ")[1] if " — " in task else "", 
                    "category": "", 
                    "created": str(datetime.now()),
                    "completed": False,
                    "completed_at": None
                } for task in data.get("tasks", [])]
            
            # Перевірка наявності всіх необхідних полів
            for task in data.get("tasks", []):
                task.setdefault("completed", False)
                task.setdefault("completed_at", None)
                task.setdefault("created", str(datetime.now()))
            
            return {
                "tasks": data.get("tasks", []),
                "notes": data.get("notes", []),
                "categories": data.get("categories", DEFAULT_CATEGORIES.copy()),
                "statistics": data.get("statistics", {})
            }
    except (json.JSONDecodeError, KeyError, AttributeError) as e:
        logger.error(f"Помилка завантаження даних: {e}, повертаються дані за замовчуванням")
        return default_data

def save_data(data):
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Помилка збереження даних: {e}")
        # Додаткова обробка помилок
        try:
            # Спробуємо зберегти резервну копію
            with open(DATA_FILE + ".backup", 'w', encoding='utf-8') as f_backup:
                json.dump(data, f_backup, ensure_ascii=False, indent=2)
            logger.info("Створено резервну копію даних")
        except Exception as backup_e:
            logger.error(f"Помилка створення резервної копії: {backup_e}")

# Завантаження даних
data = load_data()
tasks = data.get("tasks", [])
notes = data.get("notes", [])
categories = data.get("categories", DEFAULT_CATEGORIES.copy())
statistics = data.get("statistics", {})

# Клавіатури (кешовані версії)
_main_menu_kb = None
_back_kb = None

def get_main_menu_kb():
    global _main_menu_kb
    if _main_menu_kb is None:
        builder = ReplyKeyboardBuilder()
        builder.row(
            types.KeyboardButton(text="📋 Мої задачі"),
            types.KeyboardButton(text="🧠 Нотатки"),
        )
        builder.row(
            types.KeyboardButton(text="📄 Переглянути задачі"),
            types.KeyboardButton(text="🧾 Переглянути нотатки"),
        )
        builder.row(
            types.KeyboardButton(text="✅ Відмітити задачу"),
            types.KeyboardButton(text="🔄 Активувати задачу"),
        )
        builder.row(
            types.KeyboardButton(text="🗑️ Видалити задачу"),
            types.KeyboardButton(text="🗑️ Видалити нотатку"),
        )
        builder.row(
            types.KeyboardButton(text="🔍 Пошук"),
            types.KeyboardButton(text="📊 Статистика"),
        )
        builder.row(
            types.KeyboardButton(text="⏰ Нагадування"),
        )
        _main_menu_kb = builder.as_markup(resize_keyboard=True)
    return _main_menu_kb

def get_back_kb():
    global _back_kb
    if _back_kb is None:
        builder = ReplyKeyboardBuilder()
        builder.add(types.KeyboardButton(text="◀️ Назад"))
        _back_kb = builder.as_markup(resize_keyboard=True)
    return _back_kb

def get_tasks_kb(completed=False):
    builder = ReplyKeyboardBuilder()
    for i, task in enumerate(tasks):
        if task.get("completed", False) == completed:
            builder.add(types.KeyboardButton(text=f"{i+1}. {task['text']}"))
    builder.add(types.KeyboardButton(text="◀️ Назад"))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

def get_categories_kb():
    builder = ReplyKeyboardBuilder()
    for category in categories:
        builder.add(types.KeyboardButton(text=category))
    builder.add(types.KeyboardButton(text="◀️ Назад"))
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_export_kb():
    builder = InlineKeyboardBuilder()
    builder.add(types.InlineKeyboardButton(text="📝 TXT", callback_data="export_txt"))
    builder.add(types.InlineKeyboardButton(text="📊 CSV", callback_data="export_csv"))
    builder.add(types.InlineKeyboardButton(text="📑 JSON", callback_data="export_json"))
    builder.adjust(3)
    return builder.as_markup()

def get_tasks_for_notes_kb():
    builder = ReplyKeyboardBuilder()
    for i, task in enumerate(tasks):
        if not task.get("completed", False):
            builder.add(types.KeyboardButton(text=f"{i+1}. {task['text']}"))
    builder.add(types.KeyboardButton(text="◀️ Назад"))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

# Обробники команд
@dp.message(Command("start", "cancel"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    msg = await message.answer(
        "<b>🔴 Вітаю!</b> Я ваш особистий помічник.\n"
        "<b>Оберіть дію з меню:</b>", 
        reply_markup=get_main_menu_kb(),
        parse_mode="HTML"
    )
    await manage_messages(message.chat.id, msg.message_id, bot)

# Обробники для задач
@dp.message(F.text == "📋 Мої задачі")
async def add_task_start(message: types.Message, state: FSMContext):
    await state.set_state(TaskStates.waiting_for_text)
    msg = await message.answer(
        "📌 Напишіть назву задачі:",
        reply_markup=get_back_kb()
    )
    await manage_messages(message.chat.id, msg.message_id, bot)

@dp.message(TaskStates.waiting_for_text)
async def process_task_text(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
        
    await state.update_data(task_text=message.text)
    await state.set_state(TaskStates.waiting_for_deadline)
    await message.answer(
        "🗓 Введіть дедлайн для цієї задачі (наприклад, '15.12' або 'через 3 дні'):",
        reply_markup=get_back_kb()
    )

@dp.message(TaskStates.waiting_for_deadline)
async def process_task_deadline(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    await state.update_data(deadline=message.text)
    await state.set_state(TaskStates.waiting_for_category)
    await message.answer(
        "🏷 Оберіть категорію для задачі:",
        reply_markup=get_categories_kb()
    )

@dp.message(TaskStates.waiting_for_category)
async def process_task_category(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаєmosь до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text not in categories:
        await message.answer("❌ Оберіть категорію зі списку або натисніть «Назад»")
        return
    
    task_data = await state.get_data()
    task = {
        "text": task_data.get("task_text", ""),
        "deadline": task_data.get("deadline", ""),
        "category": message.text,
        "created": str(datetime.now()),
        "completed": False,
        "completed_at": None
    }
    tasks.append(task)
    
    # Оновлення статистики
    stats_key = f"tasks_{datetime.now().strftime('%Y-%m')}"
    statistics[stats_key] = statistics.get(stats_key, 0) + 1
    save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
    
    await state.clear()
    await message.answer(
        "✅ Задачу збережено.",
        reply_markup=get_main_menu_kb()
    )

# Відмітка задач як виконаних/невиконаних
@dp.message(F.text == "✅ Відмітити задачу")
async def complete_task_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ Немає задач для відмітки.", reply_markup=get_main_menu_kb())
        return
    
    active_tasks = [t for t in tasks if not t.get("completed", False)]
    if not active_tasks:
        await message.answer("❌ Всі задачі вже виконані.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(TaskStates.waiting_for_task_complete)
    await message.answer(
        "Оберіть задачу для відмітки як виконану:",
        reply_markup=get_tasks_kb(completed=False)
    )

@dp.message(TaskStates.waiting_for_task_complete)
async def process_task_complete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks) and not tasks[task_num].get("completed", False):
            tasks[task_num]["completed"] = True
            tasks[task_num]["completed_at"] = str(datetime.now())
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await state.clear()
            await message.answer(
                f"✅ Задачу '{tasks[task_num]['text']}' позначено як виконану.",
                reply_markup=get_main_menu_kb()
            )
            return
    
    await message.answer("❌ Оберіть задачу зі списку або натисніть «Назад»")

@dp.message(F.text == "🔄 Активувати задачу")
async def uncomplete_task_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ Немає задач для активації.", reply_markup=get_main_menu_kb())
        return
    
    completed_tasks = [t for t in tasks if t.get("completed", False)]
    if not completed_tasks:
        await message.answer("❌ Немає виконаних задач.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(TaskStates.waiting_for_task_uncomplete)
    await message.answer(
        "Оберіть задачу для активації:",
        reply_markup=get_tasks_kb(completed=True)
    )

@dp.message(TaskStates.waiting_for_task_uncomplete)
async def process_task_uncomplete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks) and tasks[task_num].get("completed", False):
            tasks[task_num]["completed"] = False
            tasks[task_num]["completed_at"] = None
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await state.clear()
            await message.answer(
                f"🔄 Задачу '{tasks[task_num]['text']}' активовано знову.",
                reply_markup=get_main_menu_kb()
            )
            return
    
    await message.answer("❌ Оберіть задачу зі списку або натисніть «Назад»")

# Статистика
@dp.message(F.text == "📊 Статистика")
async def show_statistics(message: types.Message):
    # Статистика по задачам
    completed_tasks = sum(1 for task in tasks if task.get("completed", False))
    active_tasks = len(tasks) - completed_tasks
    
    # Перевірка протермінованих задач
    overdue_tasks = 0
    for task in tasks:
        if not task.get("completed", False) and task.get("deadline"):
            deadline = parse_deadline(task["deadline"])
            if deadline and deadline < datetime.now():
                overdue_tasks += 1
    
    # Статистика по категоріям
    category_stats = {}
    for category in categories:
        category_tasks = sum(1 for task in tasks if task.get("category", "") == category)
        category_notes = sum(1 for note in notes if note.get("category", "") == category)
        if category_tasks or category_notes:
            category_stats[category] = (category_tasks, category_notes)
    
    response = [
        "📊 Статистика продуктивності:",
        f"✅ Виконано задач: {completed_tasks}",
        f"🔄 Активних задач: {active_tasks}",
        f"⏰ Протерміновано: {overdue_tasks}",
        f"📝 Всього нотаток: {len(notes)}",
        "\n📌 По категоріях:"
    ]
    
    for category, (task_count, note_count) in category_stats.items():
        response.append(f"  {category}: задач - {task_count}, нотаток - {note_count}")
    
    # Графік продуктивності (текстовий)
    months = sorted(statistics.keys())
    if months:
        response.append("\n📈 Продуктивність по місяцях:")
        for month in months[-6:]:  # Останні 6 місяців
            response.append(f"  {month}: {statistics[month]} задач")
    
    await message.answer("\n".join(response), reply_markup=get_main_menu_kb())

# Перегляд задач
@dp.message(F.text == "📄 Переглянути задачі")
async def show_tasks(message: types.Message):
    if not tasks:
        await message.answer("❌ У вас ще немає задач.", reply_markup=get_main_menu_kb())
        return
    
    tasks_list = []
    for i, task in enumerate(tasks):
        status = "✅" if task.get("completed", False) else "❌"
        deadline = f" — {task['deadline']}" if task.get("deadline") else ""
        completed_at = f" (завершено {task['completed_at']})" if task.get("completed_at") else ""
        tasks_list.append(f"{i+1}. {status} {task['text']}{deadline} ({task['category']}){completed_at}")
    
    await message.answer(
        f"📋 Ваші задачі:\n" + "\n".join(tasks_list),
        reply_markup=get_back_kb()  # Тепер тільки кнопка "Назад"
    )

@dp.callback_query(F.data == "complete_task")
async def complete_task(callback: types.CallbackQuery):
    if not tasks:
        await callback.answer("Немає задач для завершення")
        return
    
    # Знаходимо першу невиконану задачу
    for i, task in enumerate(tasks):
        if not task.get("completed", False):
            task["completed"] = True
            task["completed_at"] = str(datetime.now())
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await callback.message.edit_text(
                f"📋 Ваші задачі:\n" + "\n".join(
                    f"{j+1}. {'✅' if t.get('completed', False) else '❌'} {t['text']} — {t['deadline']} ({t['category']})"
                    + (f" (завершено {t['completed_at']})" if t.get("completed_at") else "")
                    for j, t in enumerate(tasks)
                ),
                reply_markup=get_tasks_kb()
            )
            await callback.answer(f"Задачу '{task['text']}' позначено як виконану")
            return
    
    await callback.answer("Всі задачі вже виконані")

@dp.message(F.text == "🔍 Пошук")
async def search_start(message: types.Message, state: FSMContext):
    await state.set_state(SearchStates.waiting_for_search_query)
    await message.answer(
        "🔍 Введіть пошуковий запит (можна шукати задачі, нотатки або категорії):",
        reply_markup=get_back_kb()
    )

@dp.message(SearchStates.waiting_for_search_query)
async def process_search(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":  # Виправлено умову
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    search_query = message.text.lower()
    results = []
    
    # Пошук у задачах
    task_results = []
    for i, task in enumerate(tasks):
        if (search_query in task['text'].lower() or 
            search_query in task['category'].lower() or 
            search_query in task.get('deadline', '').lower()):
            status = "✅" if task.get("completed", False) else "❌"
            deadline = f" — {task['deadline']}" if task.get("deadline") else ""
            task_results.append(f"{i+1}. {status} {task['text']}{deadline} ({task['category']})")
    
    if task_results:
        results.append("📋 Знайдені задачі:\n" + "\n".join(task_results))
    
    # Пошук у нотатках
    note_results = []
    for i, note in enumerate(notes):
        if (search_query in note['text'].lower() or 
            search_query in note['category'].lower()):
            task_text = tasks[note.get("task_id", 0)].get("text", "Невідома задача")  # Виправлено синтаксис
            note_results.append(f"{i+1}. {note['text']} (до задачі: '{task_text}', {note['category']})")
    
    if note_results:
        results.append("\n🧾 Знайдені нотатки:\n" + "\n".join(note_results))
    
    # Пошук у категоріях
    category_results = [cat for cat in categories if search_query in cat.lower()]
    if category_results:
        results.append("\n🏷 Знайдені категорії:\n" + ", ".join(category_results))
    
    if results:
        await message.answer("\n".join(results), 
                           reply_markup=ReplyKeyboardMarkup(
                               keyboard=[
                                   [KeyboardButton(text="🔍 Продовжити пошук")],
                                   [KeyboardButton(text="◀️ Назад")]
                               ],
                               resize_keyboard=True
                           ))
    else:
        await message.answer("🔍 Нічого не знайдено за вашим запитом.",
                           reply_markup=ReplyKeyboardMarkup(
                               keyboard=[
                                   [KeyboardButton(text="🔍 Продовжити пошук")],
                                   [KeyboardButton(text="◀️ Назад")]
                               ],
                               resize_keyboard=True
                           ))
    
    await state.set_state(SearchStates.waiting_for_search_query)  # Не очищуємо стан
    

# Обробник для нотаток
@dp.message(F.text == "🧠 Нотатки")
async def add_note_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ Спочатку додайте хоча б одну задачу, до якої можна прив'язати нотатку.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(NoteStates.waiting_for_task_selection)
    await message.answer(
        "📌 Оберіть задачу, до якої відноситься нотатка:",
        reply_markup=get_tasks_for_notes_kb()
    )

@dp.message(NoteStates.waiting_for_task_selection)
async def process_note_task_selection(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks):
            await state.update_data(task_id=task_num)
            await state.set_state(NoteStates.waiting_for_text)
            await message.answer(
                "📝 Введіть текст нотатки:",
                reply_markup=get_back_kb()
            )
            return
    
    await message.answer("❌ Оберіть задачу зі списку або натисніть «Назад»")

@dp.message(NoteStates.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    await state.update_data(note_text=message.text)
    await state.set_state(NoteStates.waiting_for_category)
    await message.answer(
        "🏷 Оберіть категорію для нотатки:",
        reply_markup=get_categories_kb()
    )

@dp.message(NoteStates.waiting_for_category)
async def process_note_category(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text not in categories:
        await message.answer("❌ Оберіть категорію зі списку або натисніть «Назад»")
        return
    
    note_data = await state.get_data()
    note = {
        "text": note_data.get("note_text", ""),
        "task_id": note_data.get("task_id", 0),
        "category": message.text,
        "created": str(datetime.now())
    }
    notes.append(note)
    save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
    
    await state.clear()
    await message.answer(
        "✅ Нотатку збережено.",
        reply_markup=get_main_menu_kb()
    )

# Перегляд нотаток
@dp.message(F.text == "🧾 Переглянути нотатки")
async def show_notes(message: types.Message):
    if not notes:
        await message.answer("❌ Нотаток поки немає.", reply_markup=get_main_menu_kb())
        return
    
    notes_list = []
    for i, note in enumerate(notes):
        task_text = tasks[note.get("task_id", 0)].get("text", "Невідома задача")
        notes_list.append(f"{i+1}. {note['text']} (до задачі: '{task_text}', {note['category']})")
    
    await message.answer(
        f"🧾 Ваші нотатки:\n" + "\n".join(notes_list),
        reply_markup=get_back_kb()
    )

# Видалення задач
@dp.message(F.text == "🗑️ Видалити задачу")
async def delete_task_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ Немає задач для видалення.", reply_markup=get_main_menu_kb())
        return
    
    tasks_list = "\n".join(f"{i+1}. {task['text']} — {task['deadline']}" for i, task in enumerate(tasks))
    await state.set_state(TaskStates.waiting_for_task_delete)
    await message.answer(
        f"Оберіть номер задачі для видалення:\n{tasks_list}\n\n"
        "Напишіть номер задачі або натисніть '◀️ Назад'",
        reply_markup=get_back_kb()
    )

@dp.message(TaskStates.waiting_for_task_delete)
async def process_task_delete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.isdigit():
        task_num = int(message.text) - 1
        if 0 <= task_num < len(tasks):
            deleted_task = tasks.pop(task_num)
            # Видаляємо нотатки, пов'язані з цією задачею
            global notes
            notes = [note for note in notes if note.get("task_id", -1) != task_num]
            # Видаляємо нагадування для цієї задачі
            try:
                scheduler.remove_job(f"reminder_{message.chat.id}_{task_num}")
            except Exception:
                pass
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await message.answer(
                f"✅ Задачу видалено: {deleted_task['text']} — {deleted_task['deadline']}",
                reply_markup=get_main_menu_kb()  # Додано повернення в меню
            )
            await state.clear()  # Перенесено після відправки повідомлення
        else:
            await message.answer("❌ Невірний номер задачі. Спробуйте ще раз.", reply_markup=get_back_kb())
    else:
        await message.answer("❌ Будь ласка, введіть номер задачі.", reply_markup=get_back_kb())

# Видалення нотаток
@dp.message(F.text == "🗑️ Видалити нотатку")
async def delete_note_start(message: types.Message, state: FSMContext):
    if not notes:
        await message.answer("❌ Немає нотаток для видалення.", reply_markup=get_main_menu_kb())
        return
    
    notes_list = "\n".join(f"{i+1}. {note['text']}" for i, note in enumerate(notes))
    await state.set_state(NoteStates.waiting_for_note_delete)
    await message.answer(
        f"Оберіть номер нотатки для видалення:\n{notes_list}\n\n"
        "Напишіть номер нотатки або натисніть '◀️ Назад'",
        reply_markup=get_back_kb()
    )

@dp.message(NoteStates.waiting_for_note_delete)
async def process_note_delete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.isdigit():
        note_num = int(message.text) - 1
        if 0 <= note_num < len(notes):
            deleted_note = notes.pop(note_num)
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await state.clear()
            await message.answer(
                f"✅ Нотатку видалено: {deleted_note['text']}",
                reply_markup=get_main_menu_kb()
            )
        else:
            await message.answer("❌ Невірний номер нотатки. Спробуйте ще раз.")
    else:
        await message.answer("❌ Будь ласка, введіть номер нотатки.")

@dp.message(F.text == "⏰ Нагадування")
async def set_reminder_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ Немає задач для нагадування.", reply_markup=get_main_menu_kb())
        return
    
    active_tasks = [t for t in tasks if not t.get("completed", False)]
    if not active_tasks:
        await message.answer("❌ Всі задачі вже виконані.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(ReminderStates.waiting_for_reminder_task)
    await message.answer(
        "📌 Оберіть задачу для нагадування:",
        reply_markup=get_tasks_kb(completed=False)
    )

@dp.message(ReminderStates.waiting_for_reminder_task)
async def process_reminder_task(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks) and not tasks[task_num].get("completed", False):
            await state.update_data(task_num=task_num)
            await state.set_state(ReminderStates.waiting_for_reminder_time)
            await message.answer(
                "⏰ Введіть час нагадування (наприклад: '15.12 14:30' або 'через 2 години'):\n"
                "Або напишіть 'скасувати'",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="Скасувати")],
                        [KeyboardButton(text="◀️ Назад")]
                    ],
                    resize_keyboard=True
                )
            )
            return
    
    await message.answer("❌ Оберіть задачу зі списку або натисніть «Назад»", reply_markup=get_back_kb())

@dp.message(ReminderStates.waiting_for_reminder_time)
async def process_reminder_time(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.lower() == "скасувати":
        await state.clear()
        await message.answer("Нагадування скасовано", reply_markup=get_main_menu_kb())
        return
    
    reminder_data = await state.get_data()
    task_num = reminder_data.get("task_num")
    task = tasks[task_num]
    
    try:
        now = datetime.now()
        if "через" in message.text.lower():
            parts = message.text.lower().split()
            try:
                num = int(parts[1])
                if "год" in parts[2]:
                    delta = timedelta(hours=num)
                elif "хв" in parts[2]:
                    delta = timedelta(minutes=num)
                else:
                    raise ValueError
                reminder_time = now + delta
            except (IndexError, ValueError):
                raise ValueError("Невірний формат часу")
        else:
            try:
                # Спроба парсингу "дд.мм гг:хх"
                reminder_time = datetime.strptime(message.text, "%d.%m %H:%M").replace(year=now.year)
                if reminder_time < now:
                    reminder_time = reminder_time.replace(year=now.year + 1)
            except ValueError:
                try:
                    # Спроба парсингу "гг:хх"
                    reminder_time = datetime.strptime(message.text, "%H:%M")
                    reminder_time = now.replace(hour=reminder_time.hour, minute=reminder_time.minute)
                    if reminder_time < now:
                        reminder_time += timedelta(days=1)
                except ValueError:
                    raise ValueError("Невірний формат часу")
        
        # Видаляємо старі нагадування
        try:
            scheduler.remove_job(f"reminder_{message.chat.id}_{task_num}")
        except Exception:
            pass
        
        # Додаємо нове нагадування
        scheduler.add_job(
            send_reminder,
            DateTrigger(run_date=reminder_time),
            args=(message.chat.id, f"⏰ Нагадування: {task['text']}\nДедлайн: {task.get('deadline', 'не вказано')}"),
            id=f"reminder_{message.chat.id}_{task_num}"
        )
        
        await message.answer(
            f"✅ Нагадування встановлено на {reminder_time.strftime('%d.%m %H:%M')}",
            reply_markup=get_main_menu_kb()
        )
        await state.clear()
    except Exception as e:
        logger.error(f"Помилка встановлення нагадування: {e}")
        await message.answer(
            "❌ Невірний формат часу. Приклади коректного формату:\n"
            "- '15.12 14:30' (дата і час)\n"
            "- '14:30' (час сьогодні)\n"
            "- 'через 2 години'\n"
            "- 'через 30 хвилин'\n\n"
            "Або напишіть 'скасувати' для відміни",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Скасувати")],
                    [KeyboardButton(text="◀️ Назад")]
                ],
                resize_keyboard=True
            )
        )

@dp.message(~F.text)
async def handle_non_text(message: types.Message):
    await message.answer("❌ Будь ласка, надсилайте лише текстові повідомлення.")

# Обробник для кнопки "Назад"
@dp.message(F.text.in_(["◀️ Назад", "↔ Назад", "Назад"]))
async def handle_back(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Повертаємось до головного меню",
        reply_markup=get_main_menu_kb()
    )

# Покращена функція парсингу дедлайнів
def parse_deadline(deadline_str):
    if not deadline_str:
        return None
    
    try:
        # Використовуємо dateparser для гнучкого парсингу
        parsed_date = dateparser.parse(
            deadline_str,
            languages=['uk', 'ru', 'en'],
            settings={'PREFER_DATES_FROM': 'future'}
        )
        
        if parsed_date:
            # Якщо дата в минулому (наприклад, для "15.12"), додаємо рік
            if parsed_date < datetime.now():
                if len(deadline_str) <= 5:  # Формати типу "15.12" або "14:30"
                    if ':' in deadline_str:  # Час без дати
                        parsed_date = parsed_date.replace(year=datetime.now().year + 1)
                    else:  # Дата без року
                        parsed_date = parsed_date.replace(year=datetime.now().year + 1)
            
            return parsed_date
        
        return None
    except Exception as e:
        logger.error(f"Помилка парсингу дедлайну '{deadline_str}': {e}")
        return None

# Покращена функція для нагадувань
async def process_reminder_time(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Повертаємось до головного меню", reply_markup=get_main_menu_kb())
        return
    
    if message.text.lower() == "скасувати":
        await state.clear()
        await message.answer("Нагадування скасовано", reply_markup=get_main_menu_kb())
        return
    
    reminder_data = await state.get_data()
    task_num = reminder_data.get("task_num")
    
    if not (0 <= task_num < len(tasks)):
        await message.answer("❌ Помилка: задача не знайдена")
        await state.clear()
        return
    
    task = tasks[task_num]
    
    try:
        reminder_time = parse_deadline(message.text)
        if not reminder_time:
            raise ValueError("Невірний формат часу")
        
        if reminder_time < datetime.now():
            await message.answer("❌ Час нагадування вже минув. Введіть майбутню дату/час.")
            return
        
        # Видаляємо старі нагадування
        try:
            scheduler.remove_job(f"reminder_{message.chat.id}_{task_num}")
        except Exception as e:
            logger.warning(f"Не вдалося видалити старе нагадування: {e}")
        
        # Додаємо нове нагадування
        scheduler.add_job(
            send_reminder,
            DateTrigger(run_date=reminder_time),
            args=(message.chat.id, f"⏰ Нагадування: {task['text']}\nДедлайн: {task.get('deadline', 'не вказано')}"),
            id=f"reminder_{message.chat.id}_{task_num}"
        )
        
        await message.answer(
            f"✅ Нагадування встановлено на {reminder_time.strftime('%d.%m.%Y %H:%M')}",
            reply_markup=get_main_menu_kb()
        )
        await state.clear()
    except Exception as e:
        logger.error(f"Помилка встановлення нагадування: {e}")
        await message.answer(
            "❌ Невірний формат часу. Приклади коректного формату:\n"
            "- '15.12.2023 14:30' (дата і час)\n"
            "- '15.12 14:30' (дата і час, поточний рік)\n"
            "- '14:30' (час сьогодні/завтра)\n"
            "- 'через 2 години'\n"
            "- 'через 30 хвилин'\n"
            "- 'завтра о 10:00'\n\n"
            "Або напишіть 'скасувати' для відміни",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Скасувати")],
                    [KeyboardButton(text="◀️ Назад")]
                ],
                resize_keyboard=True
            )
        )

async def manage_messages(chat_id: int, new_message_id: int, bot: Bot):
    """Функція для керування історією повідомлень (зберігає тільки 3 останні)"""
    if chat_id not in user_messages:
        user_messages[chat_id] = []
    
    user_messages[chat_id].append(new_message_id)
    
    # Видаляємо старі повідомлення, якщо їх більше 3
    while len(user_messages[chat_id]) > 3:
        try:
            oldest_msg = user_messages[chat_id].pop(0)
            await bot.delete_message(chat_id=chat_id, message_id=oldest_msg)
        except Exception as e:
            logger.error(f"Не вдалося видалити повідомлення: {e}")

async def send_reminder(chat_id, text):
    try:
        msg = await bot.send_message(
            chat_id,
            f"<b>{text}</b>",
            parse_mode="HTML"
        )
        await manage_messages(chat_id, msg.message_id, bot)  # Тепер manage_messages визначена
    except Exception as e:
        logger.error(f"Помилка при відправці нагадування: {e}")

async def export_to_csv(chat_id):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(EXPORT_FOLDER, f"tasks_export_{timestamp}.csv")

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['text', 'deadline', 'category', 'created', 'completed', 'completed_at']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for task in tasks:
                writer.writerow(task)

        await bot.send_document(
            chat_id=chat_id,
            document=types.FSInputFile(filename),
            caption='📊 Експорт задач у CSV'
        )
    except Exception as e:
        logger.error(f"Помилка експорту в CSV: {e}")
        await bot.send_message(chat_id, "❌ Помилка при експорті в CSV")

async def on_startup():
    try:
        scheduler.start()
        logger.info("Бот запущений")
        # Відновлення нагадувань при старті
        for i, task in enumerate(tasks):
            if not task.get("completed", False) and task.get("deadline"):
                deadline = parse_deadline(task["deadline"])
                if deadline and deadline > datetime.now():
                    scheduler.add_job(
                        send_reminder,
                        DateTrigger(run_date=deadline),
                        args=("global", f"⏰ Нагадування: {task['text']}\nДедлайн: {task.get('deadline', 'не вказано')}"),
                        id=f"reminder_global_{i}"
                    )
    except Exception as e:
        logger.error(f"Помилка при запуску: {e}")

async def on_shutdown():
    try:
        scheduler.shutdown()
        logger.info("Бот зупинений")
    except Exception as e:
        logger.error(f"Помилка при зупинці: {e}")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Помилка при роботі бота: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинений користувачем")
    except Exception as e:
        logger.error(f"Критична помилка: {e}")