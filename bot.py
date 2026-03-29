import os
import json
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import discord
from discord import app_commands
from discord.ext import commands, tasks

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
GUESS_CHANNEL_ID = int(os.getenv("GUESS_CHANNEL_ID"))

REQUIRED_ROLE_ID_RAW = os.getenv("REQUIRED_ROLE_ID", "").strip()
REQUIRED_ROLE_ID = int(REQUIRED_ROLE_ID_RAW) if REQUIRED_ROLE_ID_RAW else None

TIMEZONE = ZoneInfo("America/New_York")

# Automatic weekly schedule
# Monday = 0, Sunday = 6
AUTO_OPEN_DAY = 0
AUTO_OPEN_HOUR = 9
AUTO_OPEN_MINUTE = 0

AUTO_CLOSE_DAY = 6
AUTO_CLOSE_HOUR = 20
AUTO_CLOSE_MINUTE = 0

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
GUILD = discord.Object(id=GUILD_ID)

GUESSES_FILE = "guesses.json"
ANSWERS_FILE = "answers.json"
LEADERBOARD_FILE = "leaderboard.json"
STATUS_FILE = "status.json"
CLUES_FILE = "clues.json"
RESULTS_FILE = "results.json"


def get_now_local():
    return datetime.now(TIMEZONE)


def get_week_key_for_dt(dt: datetime) -> str:
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"


def get_week_key() -> str:
    return get_week_key_for_dt(get_now_local())


def load_json(filename):
    if not os.path.exists(filename):
        return {}
    try:
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return {}


def save_json(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def normalize_address(address: str) -> str:
    text = address.strip().lower()

    replacements = {
        "street": "st",
        "st.": "st",
        "avenue": "ave",
        "ave.": "ave",
        "road": "rd",
        "rd.": "rd",
        "drive": "dr",
        "dr.": "dr",
        "boulevard": "blvd",
        "blvd.": "blvd",
        "lane": "ln",
        "ln.": "ln",
        "court": "ct",
        "ct.": "ct",
        ",": "",
        ".": ""
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = " ".join(text.split())
    return text


def get_channel():
    return bot.get_channel(GUESS_CHANNEL_ID)


async def fetch_guess_channel():
    channel = get_channel()
    if channel is not None:
        return channel

    try:
        return await bot.fetch_channel(GUESS_CHANNEL_ID)
    except Exception as e:
        print(f"Could not fetch guess channel: {e}")
        return None


async def announce_to_guess_channel(message: str):
    channel = await fetch_guess_channel()
    if channel is None:
        return

    try:
        await channel.send(message)
    except Exception as e:
        print(f"Could not send message to guess channel: {e}")


def is_week_open(week_key: str) -> bool:
    status_data = load_json(STATUS_FILE)
    if week_key not in status_data:
        return True
    return status_data[week_key].get("is_open", True)


def set_week_open_status(week_key: str, is_open: bool, changed_by: str):
    status_data = load_json(STATUS_FILE)
    week_block = status_data.get(week_key, {})
    scheduler_runs = week_block.get("scheduler_runs", {})

    status_data[week_key] = {
        "is_open": is_open,
        "changed_by": changed_by,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scheduler_runs": scheduler_runs
    }

    save_json(STATUS_FILE, status_data)


def has_scheduler_run_key(week_key: str, action: str) -> bool:
    status_data = load_json(STATUS_FILE)
    week_block = status_data.get(week_key, {})
    scheduler_runs = week_block.get("scheduler_runs", {})
    return scheduler_runs.get(action, False)


def mark_scheduler_run_key(week_key: str, action: str):
    status_data = load_json(STATUS_FILE)

    if week_key not in status_data:
        status_data[week_key] = {
            "is_open": True,
            "changed_by": "system",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scheduler_runs": {}
        }

    if "scheduler_runs" not in status_data[week_key]:
        status_data[week_key]["scheduler_runs"] = {}

    status_data[week_key]["scheduler_runs"][action] = True
    status_data[week_key]["timestamp"] = datetime.now(timezone.utc).isoformat()

    save_json(STATUS_FILE, status_data)


def user_has_required_role(member: discord.Member) -> bool:
    if REQUIRED_ROLE_ID is None:
        return True
    return any(role.id == REQUIRED_ROLE_ID for role in member.roles)


def get_weekly_clue(week_key: str):
    clues = load_json(CLUES_FILE)
    week_clues = clues.get(week_key, [])

    for clue_entry in week_clues:
        if clue_entry.get("is_primary", False):
            return clue_entry

    if week_clues:
        return week_clues[0]

    return None


def add_clue(week_key: str, clue_text: str, posted_by: str, is_primary: bool = False):
    clues = load_json(CLUES_FILE)

    if week_key not in clues:
        clues[week_key] = []

    if is_primary:
        for clue_entry in clues[week_key]:
            clue_entry["is_primary"] = False

    clue_entry = {
        "clue": clue_text,
        "posted_by": posted_by,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_primary": is_primary
    }

    clues[week_key].append(clue_entry)
    save_json(CLUES_FILE, clues)


def finalize_week_if_possible(week_key: str):
    guesses = load_json(GUESSES_FILE)
    answers = load_json(ANSWERS_FILE)
    leaderboard = load_json(LEADERBOARD_FILE)
    results = load_json(RESULTS_FILE)

    if week_key in results and results[week_key].get("finalized", False):
        return results[week_key]

    if week_key not in answers:
        result = {
            "finalized": True,
            "winner_found": False,
            "reason": "no_answer_set",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        results[week_key] = result
        save_json(RESULTS_FILE, results)
        return result

    if week_key not in guesses or not guesses[week_key]:
        result = {
            "finalized": True,
            "winner_found": False,
            "reason": "no_guesses_submitted",
            "answer": answers[week_key]["answer"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        results[week_key] = result
        save_json(RESULTS_FILE, results)
        return result

    original_answer = answers[week_key]["answer"]
    normalized_answer = normalize_address(original_answer)

    matches = []
    for _, entry in guesses[week_key].items():
        raw_guess = entry.get("guess", "")
        normalized_guess = normalize_address(raw_guess)

        if normalized_guess == normalized_answer:
            matches.append(entry)

    if not matches:
        result = {
            "finalized": True,
            "winner_found": False,
            "reason": "no_correct_guess",
            "answer": original_answer,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        results[week_key] = result
        save_json(RESULTS_FILE, results)
        return result

    matches.sort(key=lambda x: x["timestamp"])
    winning_entry = matches[0]
    winner_name = winning_entry["username"]

    if winner_name not in leaderboard:
        leaderboard[winner_name] = 0

    leaderboard[winner_name] += 1
    save_json(LEADERBOARD_FILE, leaderboard)

    result = {
        "finalized": True,
        "winner_found": True,
        "winner_name": winner_name,
        "winning_guess": winning_entry["guess"],
        "winning_timestamp": winning_entry["timestamp"],
        "answer": original_answer,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    results[week_key] = result
    save_json(RESULTS_FILE, results)
    return result


async def post_open_announcement(week_key: str):
    primary_clue = get_weekly_clue(week_key)

    if primary_clue:
        await announce_to_guess_channel(
            f"🟢 **Guesses are now OPEN for {week_key}.**\n"
            f"🧩 **First clue:** {primary_clue['clue']}"
        )
    else:
        await announce_to_guess_channel(
            f"🟢 **Guesses are now OPEN for {week_key}.**\n"
            f"Use `/guess` to lock in your address."
        )


async def post_final_result_announcement(week_key: str):
    result = finalize_week_if_possible(week_key)

    if result.get("winner_found"):
        await announce_to_guess_channel(
            f"🔒 **Guesses are now CLOSED for {week_key}.**\n"
            f"🏆 **Winner: {result['winner_name']}**\n"
            f"Correct address: `{result['answer']}`\n"
            f"Winning guess submitted at: **{result['winning_timestamp']}**"
        )
    else:
        reason = result.get("reason")

        if reason == "no_answer_set":
            await announce_to_guess_channel(
                f"🔒 **Guesses are now CLOSED for {week_key}.**\n"
                f"No winner could be determined because no answer was set this week."
            )
        elif reason == "no_guesses_submitted":
            await announce_to_guess_channel(
                f"🔒 **Guesses are now CLOSED for {week_key}.**\n"
                f"No guesses were submitted this week.\n"
                f"Correct answer: `{result.get('answer', 'Not set')}`"
            )
        elif reason == "no_correct_guess":
            await announce_to_guess_channel(
                f"🔒 **Guesses are now CLOSED for {week_key}.**\n"
                f"No one guessed the correct address this week.\n"
                f"Correct answer: `{result.get('answer', 'Not set')}`"
            )
        else:
            await announce_to_guess_channel(
                f"🔒 **Guesses are now CLOSED for {week_key}.**\n"
                f"No winner this week."
            )


async def auto_open_current_week():
    week_key = get_week_key()
    if is_week_open(week_key):
        return

    set_week_open_status(week_key, True, "system_auto_open")
    await post_open_announcement(week_key)


async def auto_close_current_week():
    week_key = get_week_key()
    if not is_week_open(week_key):
        return

    set_week_open_status(week_key, False, "system_auto_close")
    await post_final_result_announcement(week_key)


@tasks.loop(minutes=1)
async def weekly_scheduler():
    now_local = get_now_local()
    weekday = now_local.weekday()
    hour = now_local.hour
    minute = now_local.minute

    if weekday == AUTO_OPEN_DAY and hour == AUTO_OPEN_HOUR and minute == AUTO_OPEN_MINUTE:
        week_key = get_week_key_for_dt(now_local)
        action_key = "auto_open"

        if not has_scheduler_run_key(week_key, action_key):
            await auto_open_current_week()
            mark_scheduler_run_key(week_key, action_key)
            print(f"Scheduler opened {week_key}")

    if weekday == AUTO_CLOSE_DAY and hour == AUTO_CLOSE_HOUR and minute == AUTO_CLOSE_MINUTE:
        week_key = get_week_key_for_dt(now_local)
        action_key = "auto_close"

        if not has_scheduler_run_key(week_key, action_key):
            await auto_close_current_week()
            mark_scheduler_run_key(week_key, action_key)
            print(f"Scheduler closed {week_key}")


@weekly_scheduler.before_loop
async def before_weekly_scheduler():
    await bot.wait_until_ready()


@bot.event
async def setup_hook():
    if not weekly_scheduler.is_running():
        weekly_scheduler.start()


@bot.event
async def on_ready():
    print("RUNNING LATEST CODE - V10 AUTO EVERYTHING")
    print(f"Logged in as {bot.user}")

    bot.tree.clear_commands(guild=GUILD)
    bot.tree.copy_global_to(guild=GUILD)
    synced = await bot.tree.sync(guild=GUILD)

    print(f"Synced {len(synced)} command(s) to guild {GUILD_ID}")
    for cmd in synced:
        print(f"- {cmd.name}")


@bot.tree.command(name="guess", description="Submit your one house guess for this week")
@app_commands.describe(address="Enter the house address you want to guess")
async def guess(interaction: discord.Interaction, address: str):
    try:
        week_key = get_week_key()

        if not is_week_open(week_key):
            await interaction.response.send_message(
                f"Guesses are closed for {week_key}.",
                ephemeral=True
            )
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Could not verify your server membership.",
                ephemeral=True
            )
            return

        if not user_has_required_role(interaction.user):
            if REQUIRED_ROLE_ID is None:
                message = "You do not have permission to guess."
            else:
                message = "You need the required role to submit a guess."

            await interaction.response.send_message(
                message,
                ephemeral=True
            )
            return

        user_id = str(interaction.user.id)
        username = str(interaction.user)

        guesses = load_json(GUESSES_FILE)

        if week_key not in guesses:
            guesses[week_key] = {}

        if user_id in guesses[week_key]:
            existing = guesses[week_key][user_id]["guess"]
            await interaction.response.send_message(
                f"You already used your guess this week. Your locked guess is: `{existing}`",
                ephemeral=True
            )
            return

        guesses[week_key][user_id] = {
            "username": username,
            "guess": address,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        save_json(GUESSES_FILE, guesses)

        await interaction.response.send_message(
            f"Your guess has been locked in for {week_key}: `{address}`",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /guess. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="myguess", description="See your current week's guess")
async def myguess(interaction: discord.Interaction):
    try:
        week_key = get_week_key()
        user_id = str(interaction.user.id)
        guesses = load_json(GUESSES_FILE)

        if week_key in guesses and user_id in guesses[week_key]:
            entry = guesses[week_key][user_id]
            await interaction.response.send_message(
                f"Your guess for {week_key} is: `{entry['guess']}`",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "You have not submitted a guess this week.",
                ephemeral=True
            )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /myguess. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="setanswer", description="Admin: set the winning address for this week")
@app_commands.describe(address="Enter the correct winning address")
async def setanswer(interaction: discord.Interaction, address: str):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You do not have permission to use this command.",
                ephemeral=True
            )
            return

        week_key = get_week_key()
        answers = load_json(ANSWERS_FILE)

        answers[week_key] = {
            "answer": address,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "set_by": str(interaction.user)
        }

        save_json(ANSWERS_FILE, answers)

        await interaction.response.send_message(
            f"Winning address for {week_key} has been set to: `{address}`",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /setanswer. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="setweeklyclue", description="Admin: set the main clue for this week")
@app_commands.describe(clue="The clue you want auto-posted when the week opens")
async def setweeklyclue(interaction: discord.Interaction, clue: str):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You do not have permission to use this command.",
                ephemeral=True
            )
            return

        week_key = get_week_key()
        add_clue(week_key, clue, str(interaction.user), is_primary=True)

        await interaction.response.send_message(
            f"Primary clue for {week_key} has been set.",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /setweeklyclue. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="postclue", description="Admin: post a clue for the current week")
@app_commands.describe(clue="The clue you want to post publicly")
async def postclue(interaction: discord.Interaction, clue: str):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You do not have permission to use this command.",
                ephemeral=True
            )
            return

        week_key = get_week_key()
        add_clue(week_key, clue, str(interaction.user), is_primary=False)

        await interaction.response.send_message(
            f"🧩 **Clue for {week_key}:** {clue}"
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /postclue. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="showclues", description="Show all clues for the current week")
async def showclues(interaction: discord.Interaction):
    try:
        week_key = get_week_key()
        clues = load_json(CLUES_FILE)
        week_clues = clues.get(week_key, [])

        if not week_clues:
            await interaction.response.send_message(
                f"No clues have been posted for {week_key} yet."
            )
            return

        message = f"🧩 **Clues for {week_key}**\n"
        for i, clue_entry in enumerate(week_clues, start=1):
            message += f"{i}. {clue_entry['clue']}\n"

        await interaction.response.send_message(message)
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /showclues. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="winner", description="Admin: determine this week's winner")
async def winner(interaction: discord.Interaction):
    try:
        await interaction.response.defer()

        if not interaction.user.guild_permissions.manage_guild:
            await interaction.followup.send(
                "You do not have permission to use this command.",
                ephemeral=True
            )
            return

        week_key = get_week_key()
        result = finalize_week_if_possible(week_key)

        if result.get("winner_found"):
            await interaction.followup.send(
                f"🏆 **Winner for {week_key}: {result['winner_name']}**\n"
                f"Correct address: `{result['answer']}`\n"
                f"Winning guess submitted at: **{result['winning_timestamp']}**"
            )
        else:
            reason = result.get("reason")

            if reason == "no_answer_set":
                await interaction.followup.send(
                    "No answer has been set for this week yet. Use `/setanswer` first.",
                    ephemeral=True
                )
            elif reason == "no_guesses_submitted":
                await interaction.followup.send(
                    f"No guesses were submitted this week.\nCorrect answer: `{result.get('answer', 'Not set')}`"
                )
            elif reason == "no_correct_guess":
                await interaction.followup.send(
                    f"No one guessed the correct address this week.\nCorrect answer: `{result.get('answer', 'Not set')}`"
                )
            else:
                await interaction.followup.send("No winner this week.")
    except Exception:
        print(traceback.format_exc())
        try:
            await interaction.followup.send(
                "Something broke in /winner. Check terminal.",
                ephemeral=True
            )
        except Exception:
            pass


@bot.tree.command(name="leaderboard", description="View the top winners")
async def leaderboard(interaction: discord.Interaction):
    try:
        leaderboard_data = load_json(LEADERBOARD_FILE)

        if not leaderboard_data:
            await interaction.response.send_message("No winners yet.")
            return

        sorted_lb = sorted(
            leaderboard_data.items(),
            key=lambda x: x[1],
            reverse=True
        )

        message = "🏆 **Leaderboard**\n"
        for i, (user, wins) in enumerate(sorted_lb[:10], start=1):
            message += f"{i}. {user} - {wins} win(s)\n"

        await interaction.response.send_message(message)
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /leaderboard. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="closeweek", description="Admin: close guesses for the current week")
async def closeweek(interaction: discord.Interaction):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You do not have permission to use this command.",
                ephemeral=True
            )
            return

        week_key = get_week_key()
        set_week_open_status(week_key, False, str(interaction.user))

        await interaction.response.send_message(
            f"🔒 Guesses are now CLOSED for {week_key}."
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /closeweek. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="openweek", description="Admin: open guesses for the current week")
async def openweek(interaction: discord.Interaction):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You do not have permission to use this command.",
                ephemeral=True
            )
            return

        week_key = get_week_key()
        set_week_open_status(week_key, True, str(interaction.user))
        await post_open_announcement(week_key)

        await interaction.response.send_message(
            f"🟢 Guesses are now OPEN for {week_key}.",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /openweek. Check terminal.",
            ephemeral=True
        )


@bot.tree.command(name="resetweek", description="Admin: manually clear guesses and answers for the current week")
async def resetweek(interaction: discord.Interaction):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You do not have permission to use this command.",
                ephemeral=True
            )
            return

        week_key = get_week_key()

        guesses = load_json(GUESSES_FILE)
        answers = load_json(ANSWERS_FILE)
        status_data = load_json(STATUS_FILE)
        clues = load_json(CLUES_FILE)
        results = load_json(RESULTS_FILE)

        if week_key in guesses:
            del guesses[week_key]

        if week_key in answers:
            del answers[week_key]

        if week_key in status_data:
            del status_data[week_key]

        if week_key in clues:
            del clues[week_key]

        if week_key in results:
            del results[week_key]

        save_json(GUESSES_FILE, guesses)
        save_json(ANSWERS_FILE, answers)
        save_json(STATUS_FILE, status_data)
        save_json(CLUES_FILE, clues)
        save_json(RESULTS_FILE, results)

        await interaction.response.send_message(
            f"Current week ({week_key}) has been reset.",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message(
            "Something broke in /resetweek. Check terminal.",
            ephemeral=True
        )


bot.run(TOKEN)