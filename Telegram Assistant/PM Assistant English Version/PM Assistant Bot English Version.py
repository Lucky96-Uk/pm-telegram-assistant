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
import dateparser

# Settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DATA_FILE = "pm_manager_data.json"
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"  # 🔐 IMPORTANT: Insert your token from @BotFather
EXPORT_FOLDER = "exports"
DEFAULT_CATEGORIES = ["Work", "Personal", "Study"]

# Token validation
if not BOT_TOKEN or len(BOT_TOKEN) < 30:
    logger.error("Error: Invalid bot token format!")
    exit(1)

# Initialization
os.makedirs(EXPORT_FOLDER, exist_ok=True)

try:
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    scheduler = AsyncIOScheduler()
except Exception as e:
    logger.error(f"Bot initialization error: {e}")
    exit(1)

# For storing last message IDs (max 3 per user)
user_messages = {}

# Handler for unwanted content types
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
    await message.answer("❌ This content type is not supported. Please use text.")

# State classes
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

# Data handling functions
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
            
            # Data migration
            if isinstance(data.get("tasks", []), list) and len(data.get("tasks", [])) > 0 and isinstance(data["tasks"][0], str):
                data["tasks"] = [{
                    "text": task.split(" — ")[0], 
                    "deadline": task.split(" — ")[1] if " — " in task else "", 
                    "category": "", 
                    "created": str(datetime.now()),
                    "completed": False,
                    "completed_at": None
                } for task in data.get("tasks", [])]
            
            # Validate required fields
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
        logger.error(f"Data loading error: {e}, using default data")
        return default_data

def save_data(data):
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Data saving error: {e}")
        try:
            # Try backup
            with open(DATA_FILE + ".backup", 'w', encoding='utf-8') as f_backup:
                json.dump(data, f_backup, ensure_ascii=False, indent=2)
            logger.info("Created data backup")
        except Exception as backup_e:
            logger.error(f"Backup creation error: {backup_e}")

# Load data
data = load_data()
tasks = data.get("tasks", [])
notes = data.get("notes", [])
categories = data.get("categories", DEFAULT_CATEGORIES.copy())
statistics = data.get("statistics", {})

# Keyboards (cached versions)
_main_menu_kb = None
_back_kb = None

def get_main_menu_kb():
    global _main_menu_kb
    if _main_menu_kb is None:
        builder = ReplyKeyboardBuilder()
        builder.row(
            types.KeyboardButton(text="📋 My Tasks"),
            types.KeyboardButton(text="🧠 Notes"),
        )
        builder.row(
            types.KeyboardButton(text="📄 View Tasks"),
            types.KeyboardButton(text="🧾 View Notes"),
        )
        builder.row(
            types.KeyboardButton(text="✅ Complete Task"),
            types.KeyboardButton(text="🔄 Reactivate Task"),
        )
        builder.row(
            types.KeyboardButton(text="🗑️ Delete Task"),
            types.KeyboardButton(text="🗑️ Delete Note"),
        )
        builder.row(
            types.KeyboardButton(text="🔍 Search"),
            types.KeyboardButton(text="📊 Statistics"),
        )
        builder.row(
            types.KeyboardButton(text="⏰ Reminders"),
        )
        _main_menu_kb = builder.as_markup(resize_keyboard=True)
    return _main_menu_kb

def get_back_kb():
    global _back_kb
    if _back_kb is None:
        builder = ReplyKeyboardBuilder()
        builder.add(types.KeyboardButton(text="◀️ Back"))
        _back_kb = builder.as_markup(resize_keyboard=True)
    return _back_kb

def get_tasks_kb(completed=False):
    builder = ReplyKeyboardBuilder()
    for i, task in enumerate(tasks):
        if task.get("completed", False) == completed:
            builder.add(types.KeyboardButton(text=f"{i+1}. {task['text']}"))
    builder.add(types.KeyboardButton(text="◀️ Back"))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

def get_categories_kb():
    builder = ReplyKeyboardBuilder()
    for category in categories:
        builder.add(types.KeyboardButton(text=category))
    builder.add(types.KeyboardButton(text="◀️ Back"))
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
    builder.add(types.KeyboardButton(text="◀️ Back"))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

# Command handlers
@dp.message(Command("start", "cancel"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    msg = await message.answer(
        "<b>🔴 Welcome!</b> I'm your personal assistant.\n"
        "<b>Choose an action from the menu:</b>", 
        reply_markup=get_main_menu_kb(),
        parse_mode="HTML"
    )
    await manage_messages(message.chat.id, msg.message_id, bot)

# Task handlers
@dp.message(F.text == "📋 My Tasks")
async def add_task_start(message: types.Message, state: FSMContext):
    await state.set_state(TaskStates.waiting_for_text)
    msg = await message.answer(
        "📌 Enter task name:",
        reply_markup=get_back_kb()
    )
    await manage_messages(message.chat.id, msg.message_id, bot)

@dp.message(TaskStates.waiting_for_text)
async def process_task_text(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
        
    await state.update_data(task_text=message.text)
    await state.set_state(TaskStates.waiting_for_deadline)
    await message.answer(
        "🗓 Enter deadline for this task (e.g., '12.15' or 'in 3 days'):",
        reply_markup=get_back_kb()
    )

@dp.message(TaskStates.waiting_for_deadline)
async def process_task_deadline(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    await state.update_data(deadline=message.text)
    await state.set_state(TaskStates.waiting_for_category)
    await message.answer(
        "🏷 Select task category:",
        reply_markup=get_categories_kb()
    )

@dp.message(TaskStates.waiting_for_category)
async def process_task_category(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text not in categories:
        await message.answer("❌ Please select a category from the list or click 'Back'")
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
    
    # Update statistics
    stats_key = f"tasks_{datetime.now().strftime('%Y-%m')}"
    statistics[stats_key] = statistics.get(stats_key, 0) + 1
    save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
    
    await state.clear()
    await message.answer(
        "✅ Task saved.",
        reply_markup=get_main_menu_kb()
    )

# Task completion handlers
@dp.message(F.text == "✅ Complete Task")
async def complete_task_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ No tasks to complete.", reply_markup=get_main_menu_kb())
        return
    
    active_tasks = [t for t in tasks if not t.get("completed", False)]
    if not active_tasks:
        await message.answer("❌ All tasks are already completed.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(TaskStates.waiting_for_task_complete)
    await message.answer(
        "Select task to mark as completed:",
        reply_markup=get_tasks_kb(completed=False)
    )

@dp.message(TaskStates.waiting_for_task_complete)
async def process_task_complete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks) and not tasks[task_num].get("completed", False):
            tasks[task_num]["completed"] = True
            tasks[task_num]["completed_at"] = str(datetime.now())
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await state.clear()
            await message.answer(
                f"✅ Task '{tasks[task_num]['text']}' marked as completed.",
                reply_markup=get_main_menu_kb()
            )
            return
    
    await message.answer("❌ Please select a task from the list or click 'Back'")

@dp.message(F.text == "🔄 Reactivate Task")
async def uncomplete_task_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ No tasks to reactivate.", reply_markup=get_main_menu_kb())
        return
    
    completed_tasks = [t for t in tasks if t.get("completed", False)]
    if not completed_tasks:
        await message.answer("❌ No completed tasks found.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(TaskStates.waiting_for_task_uncomplete)
    await message.answer(
        "Select task to reactivate:",
        reply_markup=get_tasks_kb(completed=True)
    )

@dp.message(TaskStates.waiting_for_task_uncomplete)
async def process_task_uncomplete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks) and tasks[task_num].get("completed", False):
            tasks[task_num]["completed"] = False
            tasks[task_num]["completed_at"] = None
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await state.clear()
            await message.answer(
                f"🔄 Task '{tasks[task_num]['text']}' reactivated.",
                reply_markup=get_main_menu_kb()
            )
            return
    
    await message.answer("❌ Please select a task from the list or click 'Back'")

# Statistics
@dp.message(F.text == "📊 Statistics")
async def show_statistics(message: types.Message):
    # Task statistics
    completed_tasks = sum(1 for task in tasks if task.get("completed", False))
    active_tasks = len(tasks) - completed_tasks
    
    # Overdue tasks check
    overdue_tasks = 0
    for task in tasks:
        if not task.get("completed", False) and task.get("deadline"):
            deadline = parse_deadline(task["deadline"])
            if deadline and deadline < datetime.now():
                overdue_tasks += 1
    
    # Category statistics
    category_stats = {}
    for category in categories:
        category_tasks = sum(1 for task in tasks if task.get("category", "") == category)
        category_notes = sum(1 for note in notes if note.get("category", "") == category)
        if category_tasks or category_notes:
            category_stats[category] = (category_tasks, category_notes)
    
    response = [
        "📊 Productivity Statistics:",
        f"✅ Completed tasks: {completed_tasks}",
        f"🔄 Active tasks: {active_tasks}",
        f"⏰ Overdue: {overdue_tasks}",
        f"📝 Total notes: {len(notes)}",
        "\n📌 By categories:"
    ]
    
    for category, (task_count, note_count) in category_stats.items():
        response.append(f"  {category}: tasks - {task_count}, notes - {note_count}")
    
    # Productivity chart (text)
    months = sorted(statistics.keys())
    if months:
        response.append("\n📈 Monthly productivity:")
        for month in months[-6:]:  # Last 6 months
            response.append(f"  {month}: {statistics[month]} tasks")
    
    await message.answer("\n".join(response), reply_markup=get_main_menu_kb())

# View tasks
@dp.message(F.text == "📄 View Tasks")
async def show_tasks(message: types.Message):
    if not tasks:
        await message.answer("❌ You don't have any tasks yet.", reply_markup=get_main_menu_kb())
        return
    
    tasks_list = []
    for i, task in enumerate(tasks):
        status = "✅" if task.get("completed", False) else "❌"
        deadline = f" — {task['deadline']}" if task.get("deadline") else ""
        completed_at = f" (completed {task['completed_at']})" if task.get("completed_at") else ""
        tasks_list.append(f"{i+1}. {status} {task['text']}{deadline} ({task['category']}){completed_at}")
    
    await message.answer(
        f"📋 Your tasks:\n" + "\n".join(tasks_list),
        reply_markup=get_back_kb()
    )

@dp.callback_query(F.data == "complete_task")
async def complete_task(callback: types.CallbackQuery):
    if not tasks:
        await callback.answer("No tasks to complete")
        return
    
    # Find first incomplete task
    for i, task in enumerate(tasks):
        if not task.get("completed", False):
            task["completed"] = True
            task["completed_at"] = str(datetime.now())
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await callback.message.edit_text(
                f"📋 Your tasks:\n" + "\n".join(
                    f"{j+1}. {'✅' if t.get('completed', False) else '❌'} {t['text']} — {t['deadline']} ({t['category']})"
                    + (f" (completed {t['completed_at']})" if t.get("completed_at") else "")
                    for j, t in enumerate(tasks)
                ),
                reply_markup=get_tasks_kb()
            )
            await callback.answer(f"Task '{task['text']}' marked as completed")
            return
    
    await callback.answer("All tasks are already completed")

# Search
@dp.message(F.text == "🔍 Search")
async def search_start(message: types.Message, state: FSMContext):
    await state.set_state(SearchStates.waiting_for_search_query)
    await message.answer(
        "🔍 Enter search query (you can search tasks, notes or categories):",
        reply_markup=get_back_kb()
    )

@dp.message(SearchStates.waiting_for_search_query)
async def process_search(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    search_query = message.text.lower()
    results = []
    
    # Search in tasks
    task_results = []
    for i, task in enumerate(tasks):
        if (search_query in task['text'].lower() or 
            search_query in task['category'].lower() or 
            search_query in task.get('deadline', '').lower()):
            status = "✅" if task.get("completed", False) else "❌"
            deadline = f" — {task['deadline']}" if task.get("deadline") else ""
            task_results.append(f"{i+1}. {status} {task['text']}{deadline} ({task['category']})")
    
    if task_results:
        results.append("📋 Found tasks:\n" + "\n".join(task_results))
    
    # Search in notes
    note_results = []
    for i, note in enumerate(notes):
        if (search_query in note['text'].lower() or 
            search_query in note['category'].lower()):
            task_text = tasks[note.get("task_id", 0)].get("text", "Unknown task")
            note_results.append(f"{i+1}. {note['text']} (for task: '{task_text}', {note['category']})")
    
    if note_results:
        results.append("\n🧾 Found notes:\n" + "\n".join(note_results))
    
    # Search in categories
    category_results = [cat for cat in categories if search_query in cat.lower()]
    if category_results:
        results.append("\n🏷 Found categories:\n" + ", ".join(category_results))
    
    if results:
        await message.answer("\n".join(results), 
                           reply_markup=ReplyKeyboardMarkup(
                               keyboard=[
                                   [KeyboardButton(text="🔍 Continue search")],
                                   [KeyboardButton(text="◀️ Back")]
                               ],
                               resize_keyboard=True
                           ))
    else:
        await message.answer("🔍 Nothing found for your query.",
                           reply_markup=ReplyKeyboardMarkup(
                               keyboard=[
                                   [KeyboardButton(text="🔍 Continue search")],
                                   [KeyboardButton(text="◀️ Back")]
                               ],
                               resize_keyboard=True
                           ))
    
    await state.set_state(SearchStates.waiting_for_search_query)

# Note handlers
@dp.message(F.text == "🧠 Notes")
async def add_note_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ Please add at least one task first to attach notes to.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(NoteStates.waiting_for_task_selection)
    await message.answer(
        "📌 Select task for this note:",
        reply_markup=get_tasks_for_notes_kb()
    )

@dp.message(NoteStates.waiting_for_task_selection)
async def process_note_task_selection(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks):
            await state.update_data(task_id=task_num)
            await state.set_state(NoteStates.waiting_for_text)
            await message.answer(
                "📝 Enter note text:",
                reply_markup=get_back_kb()
            )
            return
    
    await message.answer("❌ Please select a task from the list or click 'Back'")

@dp.message(NoteStates.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    await state.update_data(note_text=message.text)
    await state.set_state(NoteStates.waiting_for_category)
    await message.answer(
        "🏷 Select note category:",
        reply_markup=get_categories_kb()
    )

@dp.message(NoteStates.waiting_for_category)
async def process_note_category(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text not in categories:
        await message.answer("❌ Please select a category from the list or click 'Back'")
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
        "✅ Note saved.",
        reply_markup=get_main_menu_kb()
    )

# View notes
@dp.message(F.text == "🧾 View Notes")
async def show_notes(message: types.Message):
    if not notes:
        await message.answer("❌ No notes yet.", reply_markup=get_main_menu_kb())
        return
    
    notes_list = []
    for i, note in enumerate(notes):
        task_text = tasks[note.get("task_id", 0)].get("text", "Unknown task")
        notes_list.append(f"{i+1}. {note['text']} (for task: '{task_text}', {note['category']})")
    
    await message.answer(
        f"🧾 Your notes:\n" + "\n".join(notes_list),
        reply_markup=get_back_kb()
    )

# Task deletion
@dp.message(F.text == "🗑️ Delete Task")
async def delete_task_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ No tasks to delete.", reply_markup=get_main_menu_kb())
        return
    
    tasks_list = "\n".join(f"{i+1}. {task['text']} — {task['deadline']}" for i, task in enumerate(tasks))
    await state.set_state(TaskStates.waiting_for_task_delete)
    await message.answer(
        f"Select task number to delete:\n{tasks_list}\n\n"
        "Enter task number or click '◀️ Back'",
        reply_markup=get_back_kb()
    )

@dp.message(TaskStates.waiting_for_task_delete)
async def process_task_delete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text.isdigit():
        task_num = int(message.text) - 1
        if 0 <= task_num < len(tasks):
            deleted_task = tasks.pop(task_num)
            # Delete related notes
            global notes
            notes = [note for note in notes if note.get("task_id", -1) != task_num]
            # Delete reminders
            try:
                scheduler.remove_job(f"reminder_{message.chat.id}_{task_num}")
            except Exception:
                pass
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await message.answer(
                f"✅ Task deleted: {deleted_task['text']} — {deleted_task['deadline']}",
                reply_markup=get_main_menu_kb()
            )
            await state.clear()
        else:
            await message.answer("❌ Invalid task number. Please try again.", reply_markup=get_back_kb())
    else:
        await message.answer("❌ Please enter task number.", reply_markup=get_back_kb())

# Note deletion
@dp.message(F.text == "🗑️ Delete Note")
async def delete_note_start(message: types.Message, state: FSMContext):
    if not notes:
        await message.answer("❌ No notes to delete.", reply_markup=get_main_menu_kb())
        return
    
    notes_list = "\n".join(f"{i+1}. {note['text']}" for i, note in enumerate(notes))
    await state.set_state(NoteStates.waiting_for_note_delete)
    await message.answer(
        f"Select note number to delete:\n{notes_list}\n\n"
        "Enter note number or click '◀️ Back'",
        reply_markup=get_back_kb()
    )

@dp.message(NoteStates.waiting_for_note_delete)
async def process_note_delete(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text.isdigit():
        note_num = int(message.text) - 1
        if 0 <= note_num < len(notes):
            deleted_note = notes.pop(note_num)
            save_data({"tasks": tasks, "notes": notes, "categories": categories, "statistics": statistics})
            await state.clear()
            await message.answer(
                f"✅ Note deleted: {deleted_note['text']}",
                reply_markup=get_main_menu_kb()
            )
        else:
            await message.answer("❌ Invalid note number. Please try again.")
    else:
        await message.answer("❌ Please enter note number.")

# Reminders
@dp.message(F.text == "⏰ Reminders")
async def set_reminder_start(message: types.Message, state: FSMContext):
    if not tasks:
        await message.answer("❌ No tasks for reminders.", reply_markup=get_main_menu_kb())
        return
    
    active_tasks = [t for t in tasks if not t.get("completed", False)]
    if not active_tasks:
        await message.answer("❌ All tasks are already completed.", reply_markup=get_main_menu_kb())
        return
    
    await state.set_state(ReminderStates.waiting_for_reminder_task)
    await message.answer(
        "📌 Select task for reminder:",
        reply_markup=get_tasks_kb(completed=False)
    )

@dp.message(ReminderStates.waiting_for_reminder_task)
async def process_reminder_task(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text.split(". ")[0].isdigit():
        task_num = int(message.text.split(". ")[0]) - 1
        if 0 <= task_num < len(tasks) and not tasks[task_num].get("completed", False):
            await state.update_data(task_num=task_num)
            await state.set_state(ReminderStates.waiting_for_reminder_time)
            await message.answer(
                "⏰ Enter reminder time (e.g., '12.15 14:30' or 'in 2 hours'):\n"
                "Or type 'cancel'",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="Cancel")],
                        [KeyboardButton(text="◀️ Back")]
                    ],
                    resize_keyboard=True
                )
            )
            return
    
    await message.answer("❌ Please select a task from the list or click 'Back'", reply_markup=get_back_kb())

@dp.message(ReminderStates.waiting_for_reminder_time)
async def process_reminder_time(message: types.Message, state: FSMContext):
    if message.text == "◀️ Back":
        await state.clear()
        await message.answer("Returning to main menu", reply_markup=get_main_menu_kb())
        return
    
    if message.text.lower() == "cancel":
        await state.clear()
        await message.answer("Reminder canceled", reply_markup=get_main_menu_kb())
        return
    
    reminder_data = await state.get_data()
    task_num = reminder_data.get("task_num")
    task = tasks[task_num]
    
    try:
        now = datetime.now()
        if "in" in message.text.lower():
            parts = message.text.lower().split()
            try:
                num = int(parts[1])
                if "hour" in parts[2]:
                    delta = timedelta(hours=num)
                elif "min" in parts[2]:
                    delta = timedelta(minutes=num)
                else:
                    raise ValueError
                reminder_time = now + delta
            except (IndexError, ValueError):
                raise ValueError("Invalid time format")
        else:
            try:
                # Try parsing "mm.dd HH:MM"
                reminder_time = datetime.strptime(message.text, "%m.%d %H:%M").replace(year=now.year)
                if reminder_time < now:
                    reminder_time = reminder_time.replace(year=now.year + 1)
            except ValueError:
                try:
                    # Try parsing "HH:MM"
                    reminder_time = datetime.strptime(message.text, "%H:%M")
                    reminder_time = now.replace(hour=reminder_time.hour, minute=reminder_time.minute)
                    if reminder_time < now:
                        reminder_time += timedelta(days=1)
                except ValueError:
                    raise ValueError("Invalid time format")
        
        # Remove old reminders
        try:
            scheduler.remove_job(f"reminder_{message.chat.id}_{task_num}")
        except Exception:
            pass
        
        # Add new reminder
        scheduler.add_job(
            send_reminder,
            DateTrigger(run_date=reminder_time),
            args=(message.chat.id, f"⏰ Reminder: {task['text']}\nDeadline: {task.get('deadline', 'not specified')}"),
            id=f"reminder_{message.chat.id}_{task_num}"
        )
        
        await message.answer(
            f"✅ Reminder set for {reminder_time.strftime('%m.%d %H:%M')}",
            reply_markup=get_main_menu_kb()
        )
        await state.clear()
    except Exception as e:
        logger.error(f"Reminder setting error: {e}")
        await message.answer(
            "❌ Invalid time format. Examples:\n"
            "- '12.15 14:30' (date and time)\n"
            "- '14:30' (time today/tomorrow)\n"
            "- 'in 2 hours'\n"
            "- 'in 30 minutes'\n\n"
            "Or type 'cancel'",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Cancel")],
                    [KeyboardButton(text="◀️ Back")]
                ],
                resize_keyboard=True
            )
        )

@dp.message(~F.text)
async def handle_non_text(message: types.Message):
    await message.answer("❌ Please send only text messages.")

# Back button handler
@dp.message(F.text.in_(["◀️ Back", "↔ Back", "Back"]))
async def handle_back(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Returning to main menu",
        reply_markup=get_main_menu_kb()
    )

# Improved deadline parsing
def parse_deadline(deadline_str):
    if not deadline_str:
        return None
    
    try:
        # Use dateparser for flexible parsing
        parsed_date = dateparser.parse(
            deadline_str,
            languages=['uk', 'ru', 'en'],
            settings={'PREFER_DATES_FROM': 'future'}
        )
        
        if parsed_date:
            # If date is in past (e.g. for "12.15"), add year
            if parsed_date < datetime.now():
                if len(deadline_str) <= 5:  # Formats like "12.15" or "14:30"
                    if ':' in deadline_str:  # Time without date
                        parsed_date = parsed_date.replace(year=datetime.now().year + 1)
                    else:  # Date without year
                        parsed_date = parsed_date.replace(year=datetime.now().year + 1)
            
            return parsed_date
        
        return None
    except Exception as e:
        logger.error(f"Deadline parsing error '{deadline_str}': {e}")
        return None

# Message management
async def manage_messages(chat_id: int, new_message_id: int, bot: Bot):
    """Manage message history (keep only last 3 messages)"""
    if chat_id not in user_messages:
        user_messages[chat_id] = []
    
    user_messages[chat_id].append(new_message_id)
    
    # Delete old messages if there are more than 3
    while len(user_messages[chat_id]) > 3:
        try:
            oldest_msg = user_messages[chat_id].pop(0)
            await bot.delete_message(chat_id=chat_id, message_id=oldest_msg)
        except Exception as e:
            logger.error(f"Failed to delete message: {e}")

async def send_reminder(chat_id, text):
    try:
        msg = await bot.send_message(
            chat_id,
            f"<b>{text}</b>",
            parse_mode="HTML"
        )
        await manage_messages(chat_id, msg.message_id, bot)  # Now manage_messages is defined
    except Exception as e:
        logger.error(f"Error sending reminder: {e}")

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
            caption='📊 Export tasks to CSV'
        )
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        await bot.send_message(chat_id, "❌ Error exporting to CSV")

async def on_startup():
    try:
        scheduler.start()
        logger.info("Bot is running")
        # Restore reminders at startup
        for i, task in enumerate(tasks):
            if not task.get("completed", False) and task.get("deadline"):
                deadline = parse_deadline(task["deadline"])
                if deadline and deadline > datetime.now():
                    scheduler.add_job(
                        send_reminder,
                        DateTrigger(run_date=deadline),
                        args=("global",f"⏰ Reminder: {task['text']}\nDeadline: {task.get('deadline', 'not specified')}"),
                        id=f"reminder_global_{i}"
                    )
    except Exception as e:
        logger.error(f"Startup error: {e}")

async def on_shutdown():
    try:
        scheduler.shutdown()
        logger.info("Bot stopped")
    except Exception as e:
        logger.error(f"Error while stopping: {e}")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error while running bot: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}")