import os
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import discord
from discord import app_commands
from discord.ext import commands, tasks
from supabase import create_client, Client

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID_RAW = os.getenv("GUILD_ID")
GUESS_CHANNEL_ID_RAW = os.getenv("GUESS_CHANNEL_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
REQUIRED_ROLE_ID_RAW = os.getenv("REQUIRED_ROLE_ID", "").strip()

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN")

if not GUILD_ID_RAW:
    raise RuntimeError("Missing GUILD_ID")

if not GUESS_CHANNEL_ID_RAW:
    raise RuntimeError("Missing GUESS_CHANNEL_ID")

if not SUPABASE_URL:
    raise RuntimeError("Missing SUPABASE_URL")

if not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_KEY")

try:
    GUILD_ID = int(GUILD_ID_RAW)
except ValueError:
    raise RuntimeError(f"GUILD_ID must be numeric. Got: {GUILD_ID_RAW}")

try:
    GUESS_CHANNEL_ID = int(GUESS_CHANNEL_ID_RAW)
except ValueError:
    raise RuntimeError(f"GUESS_CHANNEL_ID must be numeric. Got: {GUESS_CHANNEL_ID_RAW}")

REQUIRED_ROLE_ID = None
if REQUIRED_ROLE_ID_RAW:
    try:
        REQUIRED_ROLE_ID = int(REQUIRED_ROLE_ID_RAW)
    except ValueError:
        raise RuntimeError(f"REQUIRED_ROLE_ID must be numeric if set. Got: {REQUIRED_ROLE_ID_RAW}")

TIMEZONE = ZoneInfo("America/New_York")

AUTO_OPEN_DAY = 0
AUTO_OPEN_HOUR = 9
AUTO_OPEN_MINUTE = 0

AUTO_CLOSE_DAY = 6
AUTO_CLOSE_HOUR = 20
AUTO_CLOSE_MINUTE = 0

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
GUILD = discord.Object(id=GUILD_ID)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_now_local():
    return datetime.now(TIMEZONE)


def get_week_key_for_dt(dt: datetime) -> str:
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"


def get_week_key() -> str:
    return get_week_key_for_dt(get_now_local())


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


# -----------------------------
# Supabase helpers
# -----------------------------

def get_status_row(week_key: str):
    res = supabase.table("status").select("*").eq("week_key", week_key).limit(1).execute()
    return res.data[0] if res.data else None


def is_week_open(week_key: str) -> bool:
    row = get_status_row(week_key)
    if not row:
        return True
    return row.get("is_open", True)


def set_week_open_status(week_key: str, is_open: bool, changed_by: str):
    existing = get_status_row(week_key)
    scheduler_runs = existing.get("scheduler_runs", {}) if existing else {}

    payload = {
        "week_key": week_key,
        "is_open": is_open,
        "changed_by": changed_by,
        "timestamp": utc_now_iso(),
        "scheduler_runs": scheduler_runs
    }

    if existing:
        supabase.table("status").update(payload).eq("week_key", week_key).execute()
    else:
        supabase.table("status").insert(payload).execute()


def has_scheduler_run_key(week_key: str, action: str) -> bool:
    row = get_status_row(week_key)
    if not row:
        return False
    scheduler_runs = row.get("scheduler_runs", {}) or {}
    return scheduler_runs.get(action, False)


def mark_scheduler_run_key(week_key: str, action: str):
    row = get_status_row(week_key)
    scheduler_runs = {}
    if row:
        scheduler_runs = row.get("scheduler_runs", {}) or {}

    scheduler_runs[action] = True

    payload = {
        "week_key": week_key,
        "is_open": row.get("is_open", True) if row else True,
        "changed_by": row.get("changed_by", "system") if row else "system",
        "timestamp": utc_now_iso(),
        "scheduler_runs": scheduler_runs
    }

    if row:
        supabase.table("status").update(payload).eq("week_key", week_key).execute()
    else:
        supabase.table("status").insert(payload).execute()


def get_guesses_for_week(week_key: str):
    res = (
        supabase.table("guesses")
        .select("*")
        .eq("week_key", week_key)
        .order("timestamp")
        .execute()
    )
    return res.data or []


def get_user_guess_for_week(week_key: str, user_id: str):
    res = (
        supabase.table("guesses")
        .select("*")
        .eq("week_key", week_key)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def insert_guess(week_key: str, user_id: str, username: str, guess: str):
    supabase.table("guesses").insert({
        "week_key": week_key,
        "user_id": user_id,
        "username": username,
        "guess": guess,
        "timestamp": utc_now_iso()
    }).execute()


def get_answer_for_week(week_key: str):
    res = (
        supabase.table("answers")
        .select("*")
        .eq("week_key", week_key)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def set_answer_for_week(week_key: str, answer: str, set_by: str):
    existing = get_answer_for_week(week_key)
    payload = {
        "week_key": week_key,
        "answer": answer,
        "timestamp": utc_now_iso(),
        "set_by": set_by
    }

    if existing:
        supabase.table("answers").update(payload).eq("week_key", week_key).execute()
    else:
        supabase.table("answers").insert(payload).execute()


def add_clue(week_key: str, clue_text: str, posted_by: str, is_primary: bool = False):
    if is_primary:
        supabase.table("clues").update({"is_primary": False}).eq("week_key", week_key).execute()

    supabase.table("clues").insert({
        "week_key": week_key,
        "clue": clue_text,
        "posted_by": posted_by,
        "timestamp": utc_now_iso(),
        "is_primary": is_primary
    }).execute()


def get_clues_for_week(week_key: str):
    res = (
        supabase.table("clues")
        .select("*")
        .eq("week_key", week_key)
        .order("timestamp")
        .execute()
    )
    return res.data or []


def get_weekly_clue(week_key: str):
    clues = get_clues_for_week(week_key)
    for clue_entry in clues:
        if clue_entry.get("is_primary", False):
            return clue_entry
    return clues[0] if clues else None


def get_result_for_week(week_key: str):
    res = (
        supabase.table("results")
        .select("*")
        .eq("week_key", week_key)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def upsert_result(week_key: str, payload: dict):
    existing = get_result_for_week(week_key)
    full_payload = {"week_key": week_key, **payload}

    if existing:
        supabase.table("results").update(full_payload).eq("week_key", week_key).execute()
    else:
        supabase.table("results").insert(full_payload).execute()


def increment_leaderboard(username: str):
    res = (
        supabase.table("leaderboard")
        .select("*")
        .eq("username", username)
        .limit(1)
        .execute()
    )

    if res.data:
        current = res.data[0]["wins"]
        supabase.table("leaderboard").update({"wins": current + 1}).eq("username", username).execute()
    else:
        supabase.table("leaderboard").insert({"username": username, "wins": 1}).execute()


def get_leaderboard():
    res = supabase.table("leaderboard").select("*").order("wins", desc=True).execute()
    return res.data or []


def finalize_week_if_possible(week_key: str):
    existing_result = get_result_for_week(week_key)
    if existing_result and existing_result.get("finalized", False):
        return existing_result

    answer_row = get_answer_for_week(week_key)
    guesses = get_guesses_for_week(week_key)

    if not answer_row:
        result = {
            "finalized": True,
            "winner_found": False,
            "reason": "no_answer_set",
            "timestamp": utc_now_iso()
        }
        upsert_result(week_key, result)
        return {"week_key": week_key, **result}

    if not guesses:
        result = {
            "finalized": True,
            "winner_found": False,
            "reason": "no_guesses_submitted",
            "answer": answer_row["answer"],
            "timestamp": utc_now_iso()
        }
        upsert_result(week_key, result)
        return {"week_key": week_key, **result}

    original_answer = answer_row["answer"]
    normalized_answer = normalize_address(original_answer)

    matches = []
    for entry in guesses:
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
            "timestamp": utc_now_iso()
        }
        upsert_result(week_key, result)
        return {"week_key": week_key, **result}

    matches.sort(key=lambda x: x["timestamp"])
    winning_entry = matches[0]
    winner_name = winning_entry["username"]

    increment_leaderboard(winner_name)

    result = {
        "finalized": True,
        "winner_found": True,
        "winner_name": winner_name,
        "winning_guess": winning_entry["guess"],
        "winning_timestamp": winning_entry["timestamp"],
        "answer": original_answer,
        "timestamp": utc_now_iso()
    }
    upsert_result(week_key, result)
    return {"week_key": week_key, **result}


def reset_current_week_data(week_key: str):
    supabase.table("guesses").delete().eq("week_key", week_key).execute()
    supabase.table("answers").delete().eq("week_key", week_key).execute()
    supabase.table("clues").delete().eq("week_key", week_key).execute()
    supabase.table("results").delete().eq("week_key", week_key).execute()
    supabase.table("status").delete().eq("week_key", week_key).execute()


# -----------------------------
# Discord helpers
# -----------------------------

async def fetch_guess_channel():
    channel = bot.get_channel(GUESS_CHANNEL_ID)
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


def user_has_required_role(member: discord.Member) -> bool:
    if REQUIRED_ROLE_ID is None:
        return True
    return any(role.id == REQUIRED_ROLE_ID for role in member.roles)


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


# -----------------------------
# Scheduler
# -----------------------------

@tasks.loop(minutes=1)
async def weekly_scheduler():
    now_local = get_now_local()
    weekday = now_local.weekday()
    hour = now_local.hour
    minute = now_local.minute

    if weekday == AUTO_OPEN_DAY and hour == AUTO_OPEN_HOUR and minute == AUTO_OPEN_MINUTE:
        week_key = get_week_key_for_dt(now_local)
        if not has_scheduler_run_key(week_key, "auto_open"):
            await auto_open_current_week()
            mark_scheduler_run_key(week_key, "auto_open")
            print(f"Scheduler opened {week_key}")

    if weekday == AUTO_CLOSE_DAY and hour == AUTO_CLOSE_HOUR and minute == AUTO_CLOSE_MINUTE:
        week_key = get_week_key_for_dt(now_local)
        if not has_scheduler_run_key(week_key, "auto_close"):
            await auto_close_current_week()
            mark_scheduler_run_key(week_key, "auto_close")
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
    print("RUNNING LATEST CODE - V11 SUPABASE")
    print(f"Logged in as {bot.user}")
    print(f"GUILD_ID={GUILD_ID}")
    print(f"GUESS_CHANNEL_ID={GUESS_CHANNEL_ID}")
    print(f"REQUIRED_ROLE_ID={REQUIRED_ROLE_ID}")

    bot.tree.clear_commands(guild=GUILD)
    bot.tree.copy_global_to(guild=GUILD)
    synced = await bot.tree.sync(guild=GUILD)

    print(f"Synced {len(synced)} command(s) to guild {GUILD_ID}")
    for cmd in synced:
        print(f"- {cmd.name}")


# -----------------------------
# Commands
# -----------------------------

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
            message = "You need the required role to submit a guess." if REQUIRED_ROLE_ID is not None else "You do not have permission to guess."
            await interaction.response.send_message(message, ephemeral=True)
            return

        user_id = str(interaction.user.id)
        username = str(interaction.user)

        existing = get_user_guess_for_week(week_key, user_id)
        if existing:
            await interaction.response.send_message(
                f"You already used your guess this week. Your locked guess is: `{existing['guess']}`",
                ephemeral=True
            )
            return

        insert_guess(week_key, user_id, username, address)

        await interaction.response.send_message(
            f"Your guess has been locked in for {week_key}: `{address}`",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /guess. Check terminal.", ephemeral=True)


@bot.tree.command(name="myguess", description="See your current week's guess")
async def myguess(interaction: discord.Interaction):
    try:
        week_key = get_week_key()
        user_id = str(interaction.user.id)
        entry = get_user_guess_for_week(week_key, user_id)

        if entry:
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
        await interaction.response.send_message("Something broke in /myguess. Check terminal.", ephemeral=True)


@bot.tree.command(name="setanswer", description="Admin: set the winning address for this week")
@app_commands.describe(address="Enter the correct winning address")
async def setanswer(interaction: discord.Interaction, address: str):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        week_key = get_week_key()
        set_answer_for_week(week_key, address, str(interaction.user))

        await interaction.response.send_message(
            f"Winning address for {week_key} has been set to: `{address}`",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /setanswer. Check terminal.", ephemeral=True)


@bot.tree.command(name="setweeklyclue", description="Admin: set the main clue for this week")
@app_commands.describe(clue="The clue you want auto-posted when the week opens")
async def setweeklyclue(interaction: discord.Interaction, clue: str):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        week_key = get_week_key()
        add_clue(week_key, clue, str(interaction.user), is_primary=True)

        await interaction.response.send_message(
            f"Primary clue for {week_key} has been set.",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /setweeklyclue. Check terminal.", ephemeral=True)


@bot.tree.command(name="postclue", description="Admin: post a clue for the current week")
@app_commands.describe(clue="The clue you want to post publicly")
async def postclue(interaction: discord.Interaction, clue: str):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        week_key = get_week_key()
        add_clue(week_key, clue, str(interaction.user), is_primary=False)

        await interaction.response.send_message(f"🧩 **Clue for {week_key}:** {clue}")
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /postclue. Check terminal.", ephemeral=True)


@bot.tree.command(name="showclues", description="Show all clues for the current week")
async def showclues(interaction: discord.Interaction):
    try:
        week_key = get_week_key()
        week_clues = get_clues_for_week(week_key)

        if not week_clues:
            await interaction.response.send_message(f"No clues have been posted for {week_key} yet.")
            return

        message = f"🧩 **Clues for {week_key}**\n"
        for i, clue_entry in enumerate(week_clues, start=1):
            message += f"{i}. {clue_entry['clue']}\n"

        await interaction.response.send_message(message)
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /showclues. Check terminal.", ephemeral=True)


@bot.tree.command(name="winner", description="Admin: determine this week's winner")
async def winner(interaction: discord.Interaction):
    try:
        await interaction.response.defer()

        if not interaction.user.guild_permissions.manage_guild:
            await interaction.followup.send("You do not have permission to use this command.", ephemeral=True)
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
                await interaction.followup.send("No answer has been set for this week yet. Use `/setanswer` first.", ephemeral=True)
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
            await interaction.followup.send("Something broke in /winner. Check terminal.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="leaderboard", description="View the top winners")
async def leaderboard(interaction: discord.Interaction):
    try:
        leaderboard_data = get_leaderboard()
        if not leaderboard_data:
            await interaction.response.send_message("No winners yet.")
            return

        message = "🏆 **Leaderboard**\n"
        for i, row in enumerate(leaderboard_data[:10], start=1):
            message += f"{i}. {row['username']} - {row['wins']} win(s)\n"

        await interaction.response.send_message(message)
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /leaderboard. Check terminal.", ephemeral=True)


@bot.tree.command(name="closeweek", description="Admin: close guesses for the current week")
async def closeweek(interaction: discord.Interaction):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        week_key = get_week_key()
        set_week_open_status(week_key, False, str(interaction.user))

        await interaction.response.send_message(f"🔒 Guesses are now CLOSED for {week_key}.")
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /closeweek. Check terminal.", ephemeral=True)


@bot.tree.command(name="openweek", description="Admin: open guesses for the current week")
async def openweek(interaction: discord.Interaction):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        week_key = get_week_key()
        set_week_open_status(week_key, True, str(interaction.user))
        await post_open_announcement(week_key)

        await interaction.response.send_message(f"🟢 Guesses are now OPEN for {week_key}.", ephemeral=True)
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /openweek. Check terminal.", ephemeral=True)


@bot.tree.command(name="resetweek", description="Admin: manually clear guesses and answers for the current week")
async def resetweek(interaction: discord.Interaction):
    try:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        week_key = get_week_key()
        reset_current_week_data(week_key)

        await interaction.response.send_message(
            f"Current week ({week_key}) has been reset.",
            ephemeral=True
        )
    except Exception:
        print(traceback.format_exc())
        await interaction.response.send_message("Something broke in /resetweek. Check terminal.", ephemeral=True)


bot.run(TOKEN)
