import discord
from discord.ext import commands
from discord import Embed
from discord.ui import View, Button
import csv
import io
import aiohttp
import asyncio
from datetime import datetime, timedelta
import time
import math

#Made By Nightshade / Pie123 for Celtic Heroes Discord Servers
intents = discord.Intents.default()
intents.message_content = True  # Enable reading message content
intents.guilds = True  # Enable access to guild (server) information
intents.reactions = True  # Enable handling of reactions
intents.members = True  # Enable fetching members

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

ACTIVE_TIMERS_FILENAME = "Active_Boss_Timers.csv"

DEFAULT_CONFIG = {
    "togglewindows": "true",
    "Active_timers": "false",
    "toggle_dl": "false",
    "toggle_edl": "false",
    "toggle_legacy": "false",
    "toggle_worldboss": "false",
    "toggle_ringboss": "false",
    "toggle_dl_role": "false",
    "toggle_edl_role": "false",
    "toggle_legacy_role": "false",
    "toggle_worldboss_role": "false",
    "toggle_ringboss_role": "false",
    "toggle_role_channel": "false",
    "dkp_vals_channel": "false",
    "auction_duration": "24",
    "decay_timeframe": "30",
    "decay_percent": "4",
    "toggle_decay": "false",
    "dl_emoji": "🐉",
    "edl_emoji": "🤖",
    "legacy_emoji": "🦵",
    "worldboss_emoji": "👹",
    "ringboss_emoji": "💍",
    "toggle_public_help_messages": "false",
    "nick_doublecheck": "false",
    "nickdelete_doublecheck": "true",
    "messagetoggle_a": "true",
    "messagetoggle_k": "false",
    "messagetoggle_dkpadd": "true",
    "messagetoggle_dkpsubtract": "true",
    "messagetoggle_dkpsubtractboth": "true",
    "messagetoggle_dkpaddcurrent": "true",
    "messagetoggle_dkpsubtractlifetime": "true",
    "messagetoggle_auction": "true",
    "whograntedtoggle_a": "true",
    "whograntedtoggle_dkpadd": "true",
    "whograntedtoggle_dkpsubtract": "true",
    "whograntedtoggle_dkpsubtractboth": "true",
    "whograntedtoggle_dkpaddcurrent": "true",
    "whograntedtoggle_dkpsubtractlifetime": "true",
    "whograntedtoggle_auction": "true",
    "role_req_toggledkpchannel": "true",
    "role_req_a": "true",
    "role_req_k": "false",
    "role_req_nick": "true",
    "role_req_dkpadd": "true",
    "role_req_dkpsubtract": "true",
    "role_req_dkpsubtractboth": "true",
    "role_req_setdkp": "true",
    "role_req_togglewindows": "true",
    "role_req_toggletimers": "true",
    "role_req_toggle": "true",
    "role_req_toggle_role": "true",
    "role_req_restorefromconfig": "true",
    "role_req_createbackup": "true",
    "role_req_restorefrombackup": "true",
    "role_req_togglerolechannel": "true",
    "role_req_toggledecay": "true",
    "role_req_setdecaypercent": "true",
    "role_req_setdecaytimeframe": "true",
    "role_req_bossadd": "true",
    "role_req_bossdelete": "true",
    "role_req_timeradd": "true",
    "role_req_timerdelete": "true",
    "role_req_timeredit": "true",
    "role_req_nickdelete": "true",
    "role_req_auction": "true",
    "role_req_auctionend": "true",
    "role_req_auctioncancel": "true",
    "role_req_setauctionduration": "true",
    "role_req_backup": "true",
    "role_req_assign": "true",
    "role_req_help": "false",
    "role_req_generatebalances": "false",
    "role_req_bal": "false",
    "role_req_cancel": "false",
    "role_req_bid": "false",
    "role_req_editcommandroles": "true",
    "role_req_dkpsubtractlifetime": "true",
    "role_req_dkpaddcurrent": "true",
    "role_req_setmain": "true",
    "role_req_removemain": "true",
    "role_req_toggles": "true",
}

async def setup_guild(guild):
    # Step 1: Ensure the required role exists
    role = discord.utils.get(guild.roles, name="DKP Keeper")
    if role is None:
        print(f'Creating role "DKP Keeper" in {guild.name}')
        role = await guild.create_role(name="DKP Keeper")

    # Step 2: Ensure the required channels exist
    log_channel = discord.utils.get(guild.text_channels, name="dkp-keeping-log")
    if log_channel is None:
        print(f'Creating channel "dkp-keeping-log" in {guild.name}')
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            role: discord.PermissionOverwrite(view_channel=True)
        }
        log_channel = await guild.create_text_channel('dkp-keeping-log', overwrites=overwrites)
    else:
        await log_channel.set_permissions(guild.default_role, view_channel=False)
        await log_channel.set_permissions(role, view_channel=True)

    db_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if db_channel is None:
        print(f'Creating channel "dkp-database" in {guild.name}')
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            role: discord.PermissionOverwrite(view_channel=True)
        }
        db_channel = await guild.create_text_channel('dkp-database', overwrites=overwrites)
    else:
        await db_channel.set_permissions(guild.default_role, view_channel=False)
        await db_channel.set_permissions(role, view_channel=True)

    # Step 3: Ensure CSVs exist (same list you already use)
    for file_name, create_func in [
        ("Boss_DKP_Values.csv", create_dkp_values_csv),
        ("Balances_Database.csv", create_balances_csv),
        ("Boss_Timers.csv", create_timers_csv),
        ("config.csv", create_config_csv),
        ("Active_Boss_Timers.csv", create_active_timers_csv),
    ]:
        message = await find_csv_message(db_channel, file_name)
        if message is None:
            print(f'Creating {file_name} in {guild.name}')
            await create_func(guild)
        else:
            # your existing config validation / decay restart
            if file_name == "config.csv":
                config_data = await download_csv(message.attachments[0])
                if config_data is not None:
                    updated_config = update_config_with_defaults(config_data)
                    if updated_config:
                        output = io.StringIO()
                        writer = csv.writer(output)
                        writer.writerows(config_data)
                        output.seek(0)
                        new_config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
                        await db_channel.send(file=new_config_file)
                        await message.delete()

                    # re-start decay if toggle_decay is true
                    for row in config_data:
                        if row[0] == "toggle_decay" and row[1].lower() == "true":
                            print(f"toggle_decay is set to true in {guild.name}. Restarting decay timer.")
                            # make sure decay_timer is defined elsewhere like in your code
                            await decay_timer(None, db_channel)
                            break

async def create_timers_csv(guild):
    # Data for the timers CSV based on the boss_timers dictionary
    timer_data = [["Boss Name", "Timer Duration (seconds)", "Window Duration (seconds)", "Type"]]

    # Append boss data to the CSV rows
    for boss_name, info in boss_timers.items():
        timer_data.append([boss_name, info["timer"], info["window"], info["type"]])

    # Create an in-memory CSV file
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(timer_data)
    output.seek(0)

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is not None:
        # Send the new CSV file to the "dkp-database" channel
        timers_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Boss_Timers.csv")
        await dkp_database_channel.send("Here are the timers for each boss:", file=timers_file)

async def create_active_timers_csv(guild):
    # make a CSV with just the header
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Boss Name", "Timer End (epoch)", "Window End (epoch)", "Channel ID"])
    output.seek(0)

    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print(f"[Timers] dkp-database not found in {guild.name}, cannot create Active_Boss_Timers.csv.")
        return

    file = discord.File(io.BytesIO(output.getvalue().encode()), filename=ACTIVE_TIMERS_FILENAME)
    await dkp_database_channel.send(file=file)
    print(f"[Timers] Created empty {ACTIVE_TIMERS_FILENAME} in {guild.name}.")

@bot.event
async def on_ready():
    global decay_active
    print(f'Bot is ready and logged in as {bot.user}')

    for guild in bot.guilds:
        # Step 1: Ensure the required roles and channels exist
        role = discord.utils.get(guild.roles, name="DKP Keeper")
        if role is None:
            print(f'Creating role "DKP Keeper" in {guild.name}')
            role = await guild.create_role(name="DKP Keeper")
        else:
            print(f'Role "DKP Keeper" already exists in {guild.name}')

        log_channel = discord.utils.get(guild.text_channels, name="dkp-keeping-log")
        if log_channel is None:
            print(f'Creating channel "dkp-keeping-log" in {guild.name}')
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                role: discord.PermissionOverwrite(view_channel=True)
            }
            log_channel = await guild.create_text_channel('dkp-keeping-log', overwrites=overwrites)
        else:
            await log_channel.set_permissions(guild.default_role, view_channel=False)
            await log_channel.set_permissions(role, view_channel=True)

        db_channel = discord.utils.get(guild.text_channels, name="dkp-database")
        if db_channel is None:
            print(f'Creating channel "dkp-database" in {guild.name}')
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                role: discord.PermissionOverwrite(view_channel=True)
            }
            db_channel = await guild.create_text_channel('dkp-database', overwrites=overwrites)
        else:
            await db_channel.set_permissions(guild.default_role, view_channel=False)
            await db_channel.set_permissions(role, view_channel=True)

        # Step 2: Ensure the required CSV files exist
        for file_name, create_func in [
            ("Boss_DKP_Values.csv", create_dkp_values_csv),
            ("Balances_Database.csv", create_balances_csv),
            ("Boss_Timers.csv", create_timers_csv),
            ("config.csv", create_config_csv),
            ("Active_Boss_Timers.csv", create_active_timers_csv),
        ]:
            message = await find_csv_message(db_channel, file_name)
            if message is None:
                print(f'Creating {file_name} in {guild.name}')
                await create_func(guild)
            else:
                print(f"{file_name} already exists in {guild.name}")

                # Step 3: Validate and update config.csv if it exists
                if file_name == "config.csv":
                    config_data = await download_csv(message.attachments[0])
                    if config_data is not None:
                        updated_config = update_config_with_defaults(config_data)
                        if updated_config:
                            output = io.StringIO()
                            writer = csv.writer(output)
                            writer.writerows(config_data)
                            output.seek(0)

                            new_config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
                            await db_channel.send(file=new_config_file)
                            await message.delete()

                        # Check if toggle_decay is enabled
                        for row in config_data:
                            if row[0] == "toggle_decay" and row[1].lower() == "true":
                                print(f"toggle_decay is set to true in {guild.name}. Restarting decay timer.")
                                decay_active = True
                                await decay_timer(None, db_channel)
                                break
        await load_active_boss_timers(guild)
        await update_timers_embed_if_active(guild)

@bot.event
async def on_guild_join(guild):
    # run the exact same setup for the new guild
    print(f"Joined new guild: {guild.name}, running setup...")
    await setup_guild(guild)

# Dictionary to store boss names, timer durations (when the boss is dead), and window durations (when it can spawn)
boss_timers = {
    "!155": {"timer": 3810, "window": 180, "type": "DL"},
    "!160": {"timer": 4110, "window": 180, "type": "DL"},
    "!165": {"timer": 4410, "window": 180, "type": "DL"},
    "!170": {"timer": 4710, "window": 180, "type": "DL"},
    "!180": {"timer": 5310, "window": 180, "type": "DL"},
    "!185": {"timer": 4966, "window": 202, "type": "EDL"},
    "!190": {"timer": 5479, "window": 224, "type": "EDL"},
    "!195": {"timer": 5993, "window": 244, "type": "EDL"},
    "!200": {"timer": 6506, "window": 266, "type": "EDL"},
    "!205": {"timer": 7021, "window": 286, "type": "EDL"},
    "!210": {"timer": 7534, "window": 308, "type": "EDL"},
    "!215": {"timer": 8048, "window": 328, "type": "EDL"},
    "!mrb": {"timer": 12960, "window": 2880, "type": "Ring Boss"},
    "!nrb": {"timer": 12960, "window": 2880, "type": "Ring Boss"},
    "!erb": {"timer": 12960, "window": 2880, "type": "Ring Boss"},
    "!srb": {"timer": 12960, "window": 2880, "type": "Ring Boss"},
    "!aggy": {"timer": 114048, "window": 114018, "type": "World Boss"},
    "!prot": {"timer": 64800, "window": 900, "type": "World Boss"},
    "!gele": {"timer": 115200, "window": 100800, "type": "World Boss"},
    "!dino": {"timer": 122400, "window": 100800, "type": "World Boss"},
    "!lich": {"timer": 7200, "window": 7200, "type": "World Boss"},
    "!reaver": {"timer": 7200, "window": 7200, "type": "World Boss"},
    "!bt": {"timer": 122400, "window": 100800, "type": "World Boss"},
    "!hrung": {"timer": 79200, "window": 57600, "type": "World Boss"},
    "!mord": {"timer": 72000, "window": 57600, "type": "World Boss"},
    "!necro": {"timer": 79200, "window": 57600, "type": "World Boss"}

}


def update_config_with_defaults(config_data):
    updated = False
    existing_settings = {row[0]: row[1] for row in config_data}

    for setting, default_value in DEFAULT_CONFIG.items():
        if setting not in existing_settings:
            config_data.append([setting, default_value])
            updated = True
    return updated


@bot.command(name="timeradd")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def timeradd(ctx, boss_name: str, timer: int, window: int, *, boss_type: str):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Step 1: Update Boss_Timers.csv
    # Find the "Boss_Timers.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
    if message is None:
        await ctx.send("Could not find the Boss_Timers.csv file.")
        return

    # Download and parse the Boss_Timers.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the Boss_Timers.csv file.")
        return

    # Check if a boss with the same name already exists
    boss_exists = any(row[0].lower() == f"!{boss_name.lower()}" for row in csv_data)
    if boss_exists:
        await ctx.send(f"A timer for the boss '{boss_name}' already exists. Please choose a different name or edit the existing timer.")
        return

    # Prepare the new row data for the timer
    new_row = [f"!{boss_name}", timer, window, boss_type]
    csv_data.append(new_row)  # Append the new row to the Boss_Timers.csv data

    # Create and upload the updated Boss_Timers.csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Boss_Timers.csv")
    await dkp_database_channel.send(file=new_csv_file)
    await message.delete()

    # Step 2: Update config.csv if the boss type is new
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    csv_file = message.attachments[0]
    config_data = await download_csv(csv_file)
    if config_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Check if the boss type exists in config.csv, and add it if not
    boss_type_lower = boss_type.lower().replace(" ", "_")
    toggle_channel_setting = f"toggle_{boss_type_lower}"
    toggle_role_setting = f"toggle_{boss_type_lower}_role"

    # Check if the settings already exist in the config file
    settings_exist = any(row[0] in [toggle_channel_setting, toggle_role_setting] for row in config_data)

    if not settings_exist:
        # Add new entries for the boss type to config.csv
        config_data.append([toggle_channel_setting, "false"])
        config_data.append([toggle_role_setting, "false"])

        # Create and upload the updated config.csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(config_data)
        output.seek(0)
        new_config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
        await dkp_database_channel.send(file=new_config_file)
        await message.delete()

        await ctx.send(f"New boss type '{boss_type}' has been added to the config file with default settings.")

    # Step 3: Update the timers embed with the new boss added
    await update_timers_embed_if_active(ctx.guild)

    await ctx.send(f"New boss {boss_name} has been added with a timer of {timer} and window of {window}.")

# Error handler for MissingRole
@timeradd.error
async def timeradd_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use the !timeradd command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Usage: `!timeradd boss_name timer_duration window_duration boss_type`.")
    else:
        await ctx.send("An unexpected error occurred while adding the timer.")

@bot.command(name="timerdelete")
@commands.has_role("DKP Keeper")
async def timerdelete(ctx, boss_name: str):
    try:
        # Fetch the DKP Database Channel
        dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
        if dkp_database_channel is None:
            await ctx.send("The DKP database channel does not exist.")
            return

        # Find the "Boss_Timers.csv" message in the "dkp-database" channel
        message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
        if message is None:
            await ctx.send("Could not find the Boss_Timers.csv file.")
            return

        # Download and parse the CSV file
        csv_file = message.attachments[0]
        csv_data = await download_csv(csv_file)
        if csv_data is None:
            await ctx.send("Could not download or parse the Boss_Timers.csv file.")
            return

        # Step 1: Find and delete the row matching the boss name
        boss_type = None
        row_deleted = False
        for row in csv_data:
            if row[0].lower() == f"!{boss_name.lower()}":
                boss_type = row[3].strip().lower()
                csv_data.remove(row)
                row_deleted = True
                break

        if not row_deleted:
            await ctx.send(f"No boss timer found for {boss_name}.")
            return

        # Step 2: Check if this is the last boss of the type in the Boss_Timers.csv
        remaining_bosses_of_type = any(row[3].strip().lower() == boss_type for row in csv_data)

        config_data = None  # Ensure it's initialized
        if not remaining_bosses_of_type:
            # Find the "config.csv" message
            config_message = await find_csv_message(dkp_database_channel, "config.csv")
            if config_message is None:
                await ctx.send("Could not find the config.csv file.")
                return

            # Download and parse the config.csv file
            config_file = config_message.attachments[0]
            config_data = await download_csv(config_file)
            if config_data is None:
                await ctx.send("Could not download or parse the config.csv file.")
                return

            # Remove the corresponding toggle entries and emoji
            boss_type_lower = boss_type.replace(" ", "_")
            toggle_channel_setting = f"toggle_{boss_type_lower}"
            toggle_role_setting = f"toggle_{boss_type_lower}_role"
            emoji_setting = f"{boss_type_lower}_emoji"

            config_data = [row for row in config_data if row[0] not in [toggle_channel_setting, toggle_role_setting, emoji_setting]]

            # Create new config.csv file with updated data
            output_config = io.StringIO()
            writer = csv.writer(output_config)
            writer.writerows(config_data)
            output_config.seek(0)

            # Send updated config.csv to "dkp-database"
            new_config_file = discord.File(io.BytesIO(output_config.getvalue().encode()), filename="config.csv")
            await dkp_database_channel.send(file=new_config_file)
            await config_message.delete()

            # Delete the role
            role = discord.utils.get(ctx.guild.roles, name=boss_type)
            if role:
                await role.delete()

        # Step 3: Create new Boss_Timers.csv with updated data
        output_timers = io.StringIO()
        writer = csv.writer(output_timers)
        writer.writerows(csv_data)
        output_timers.seek(0)

        # Send updated Boss_Timers.csv to "dkp-database"
        new_csv_file = discord.File(io.BytesIO(output_timers.getvalue().encode()), filename="Boss_Timers.csv")
        await dkp_database_channel.send(file=new_csv_file)
        await message.delete()

        # Step 4: Update the role embed (check config_data here)
        if config_data:  # Ensure config_data is available
            role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")
            if role_channel:
                await generate_role_embed(ctx.guild, role_channel, config_data)

        await ctx.send(f"The timer for {boss_name} has been deleted.")

        # Step 4: Update the timers embed
        await update_timers_embed_if_active(ctx.guild)

    except Exception as e:
        # Capture any exceptions and log them
        print(f"An error occurred: {e}")
        await ctx.send(f"An unexpected error occurred: {e}")

# Error handler for MissingRole and BadArgument
@timerdelete.error
async def timerdelete_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use the !timerdelete command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Usage: `!timerdelete boss_name`.")
    else:
        # Log the error for debugging purposes
        #print(f"An error occurred during the !timerdelete command: {error}")
        await ctx.send("An unexpected error occurred while deleting the timer.")


@bot.command(name="timeredit")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def timeredit(ctx, boss_name: str, new_timer: int, new_window: int, *, new_type: str):
    try:
        # Fetch the DKP Database Channel
        dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
        if dkp_database_channel is None:
            await ctx.send("The DKP database channel does not exist.")
            return

        # Find the "Boss_Timers.csv" message in the "dkp-database" channel
        message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
        if message is None:
            await ctx.send("Could not find the Boss_Timers.csv file.")
            return

        # Download and parse the CSV file
        csv_file = message.attachments[0]
        csv_data = await download_csv(csv_file)
        if csv_data is None:
            await ctx.send("Could not download or parse the Boss_Timers.csv file.")
            return

        # Step 1: Find the row matching the boss name and capture the old boss type
        old_type = None
        row_edited = False
        for row in csv_data:
            if row[0].lower() == f"!{boss_name.lower()}":
                old_type = row[3].strip().lower()  # Save the old boss type before editing
                row[1] = new_timer  # Update timer
                row[2] = new_window  # Update window
                row[3] = new_type.strip()  # Update type (supports multi-word types)
                row_edited = True
                break

        if not row_edited:
            await ctx.send(f"No boss timer found for {boss_name}.")
            return

        # Step 2: Create a new CSV file with the updated Boss_Timers.csv data
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(csv_data)
        output.seek(0)

        # Send the updated CSV to the "dkp-database" channel
        new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Boss_Timers.csv")
        await dkp_database_channel.send(file=new_csv_file)

        # Delete the original message containing the old CSV
        await message.delete()

        # Step 3: Handle the emoji and config changes
        config_message = await find_csv_message(dkp_database_channel, "config.csv")
        if config_message is None:
            await ctx.send("Could not find the config.csv file.")
            return

        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)
        if config_data is None:
            await ctx.send("Could not download or parse the config.csv file.")
            return

        # Check if the old type is the last one in the Boss_Timers.csv
        remaining_bosses_of_old_type = any(row[3].strip().lower() == old_type for row in csv_data)

        # If the old type is the last of its kind, remove its emoji and toggles
        if not remaining_bosses_of_old_type:
            old_type_lower = old_type.replace(" ", "_")
            emoji_setting = f"{old_type_lower}_emoji"
            toggle_channel_setting = f"toggle_{old_type_lower}"
            toggle_role_setting = f"toggle_{old_type_lower}_role"

            # Remove the emoji and toggles for the old type
            config_data = [row for row in config_data if
                           row[0] not in [emoji_setting, toggle_channel_setting, toggle_role_setting]]
            await ctx.send(f"Emoji and settings for {old_type} have been removed as it was the last of its kind.")

        # Step 4: Check if the new boss type already exists in config
        new_type_lower = new_type.strip().lower().replace(" ", "_")
        emoji_setting_new = f"{new_type_lower}_emoji"
        toggle_channel_setting_new = f"toggle_{new_type_lower}"
        toggle_role_setting_new = f"toggle_{new_type_lower}_role"

        # Add the new type to config if it doesn't exist already
        if not any(row[0] == toggle_channel_setting_new for row in config_data):
            config_data.append([toggle_channel_setting_new, "false"])
            config_data.append([toggle_role_setting_new, "false"])
            await ctx.send(f"New boss type '{new_type}' added to config with default settings.")

        # Check if the emoji exists for the new boss type, transfer it if needed
        emoji_exists = any(row[0] == emoji_setting_new for row in config_data)

        # If the new type doesn't exist in the config, transfer the old emoji if necessary
        if not emoji_exists and not remaining_bosses_of_old_type:
            old_type_lower = old_type.replace(" ", "_")
            emoji_setting_old = f"{old_type_lower}_emoji"
            for row in config_data:
                if row[0] == emoji_setting_old:
                    config_data.append([emoji_setting_new, row[1]])  # Transfer the old emoji to the new type
                    await ctx.send(f"Emoji for {old_type} has been reassigned to {new_type}.")
                    break

        # Step 5: Create a new config.csv file with the updated data
        output_config = io.StringIO()
        writer = csv.writer(output_config)
        writer.writerows(config_data)
        output_config.seek(0)

        # Send the updated config.csv to the "dkp-database" channel
        new_config_file = discord.File(io.BytesIO(output_config.getvalue().encode()), filename="config.csv")
        await dkp_database_channel.send(file=new_config_file)

        # Delete the original message containing the old config.csv
        await config_message.delete()

        # Step 6: Update the role embed
        role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")
        if role_channel:
            await generate_role_embed(ctx.guild, role_channel, config_data)

        # Step 7: Update the timers embed
        await update_timers_embed_if_active(ctx.guild)

        await ctx.send(f"The timer for {boss_name} has been updated to type '{new_type}'.")

    except Exception as e:
        # Capture any exceptions and log them
        print(f"An error occurred: {e}")
        await ctx.send(f"An unexpected error occurred: {e}")

# Error handler for MissingRole and BadArgument
@timeredit.error
async def timeredit_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use the !timeredit command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Usage: `!timeredit boss_name new_timer new_window new_type`.")
    else:
        # Log the error for debugging purposes
        #print(f"An error occurred during the !timeredit command: {error}")
        await ctx.send("An unexpected error occurred while editing the timer.")

async def create_dkp_values_csv(guild):
    # Data for the initial CSV
    dkp_data = [["Boss", "DKP"],
                ["155/4", 1],
                ["155/5", 2],
                ["155/6", 3],
                ["160/4", 1],
                ["160/5", 2],
                ["160/6", 4],
                ["165/4", 2],
                ["165/5", 3],
                ["165/6", 5],
                ["170/4", 2],
                ["170/5", 5],
                ["170/6", 15],
                ["180/4", 2],
                ["180/5", 10],
                ["180/6", 20],
                ["185/4", 2],
                ["185/5", 3],
                ["185/6", 6],
                ["190/4", 2],
                ["190/5", 4],
                ["190/6", 8],
                ["195/4", 2],
                ["195/5", 4],
                ["195/6", 9],
                ["200/4", 2],
                ["200/5", 4],
                ["200/6", 10],
                ["205/4", 2],
                ["205/5", 5],
                ["205/6", 15],
                ["210/4", 2],
                ["210/5", 10],
                ["210/6", 20],
                ["215/4", 5],
                ["215/5", 25],
                ["215/6", 50],
                ["aggy", 10],
                ["necro", 25],
                ["hrung", 15],
                ["prot", 100],
                ["gele", 120],
                ["lich", 30],
                ["reaver", 30],
                ["mord", 30],
                ["bt", 200],
                ["dino", 400],
                ["cook", 1],
                ["rb/5", 4],
                ["rb/6", 30]]

    # Create an in-memory CSV file
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(dkp_data)
    output.seek(0)

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is not None:
        # Send the new CSV file to the "dkp-database" channel
        dkp_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Boss_DKP_Values.csv")
        await dkp_database_channel.send("Here are the default DKP values for each boss:", file=dkp_file)

@bot.command(name="bossadd")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def bossadd(ctx, boss_name: str, dkp_value: int):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the Boss_DKP_Values.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Boss_DKP_Values.csv")
    if message is None:
        await ctx.send("Could not find the Boss_DKP_Values.csv file.")
        return

    # Download and parse the Boss_DKP_Values.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the Boss_DKP_Values.csv file.")
        return

    # Check if the boss already exists
    boss_exists = any(row[0].lower() == boss_name.lower() for row in csv_data)

    if boss_exists:
        await ctx.send(f"The boss '{boss_name}' already exists in the file.")
        return

    # Add the new boss and DKP value to the CSV
    csv_data.append([boss_name, str(dkp_value)])

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Boss_DKP_Values.csv")

    # Send the updated CSV to the "dkp-database" channel
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Notify the user
    await ctx.send(f"New boss '{boss_name}' with a DKP value of {dkp_value} has been added.")

    # Call the function to update the DKP values embed
    await send_dkp_values_embed(ctx.guild)

# Error handler for MissingRole and BadArgument
@bossadd.error
async def bossadd_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use the !bossadd command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Usage: `!bossadd boss_name dkp_value`.")
    else:
        await ctx.send("An unexpected error occurred while adding the boss.")


@bot.command(name="bossdelete")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def bossdelete(ctx, boss_name: str):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the Boss_DKP_Values.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Boss_DKP_Values.csv")
    if message is None:
        await ctx.send("Could not find the Boss_DKP_Values.csv file.")
        return

    # Download and parse the Boss_DKP_Values.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the Boss_DKP_Values.csv file.")
        return

    # Check if the boss exists
    initial_length = len(csv_data)
    csv_data = [row for row in csv_data if row[0].lower() != boss_name.lower()]

    # If the length is the same, it means the boss doesn't exist
    if len(csv_data) == initial_length:
        await ctx.send(f"The boss '{boss_name}' does not exist in the file.")
        return

    # Create a new CSV file with the updated data (boss deleted)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Boss_DKP_Values.csv")

    # Send the updated CSV to the "dkp-database" channel
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    await ctx.send(f"The boss '{boss_name}' has been deleted from the file.")

    # Call the function to update the DKP values embed
    await send_dkp_values_embed(ctx.guild)

# Error handler for MissingRole and BadArgument
@bossdelete.error
async def bossdelete_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use the !bossdelete command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Usage: `!bossdelete boss_name`.")
    else:
        await ctx.send("An unexpected error occurred while deleting the boss.")


async def create_balances_csv(guild):
    # Create an in-memory file for the CSV
    output = io.StringIO()
    writer = csv.writer(output)

    # Write the header for the CSV
    writer.writerow(["Username", "Current Balance", "Lifetime Balance"])

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        return  # Exit if the channel doesn't exist

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = []

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Collect all nicknames
            for row in nickname_csv_data:
                username = row[0]
                nickname_list = row[1].split(", ")
                nicknames.extend([[nickname, username] for nickname in nickname_list])

    # Add all members to the balances CSV
    for member in guild.members:
        writer.writerow([member.name, 0, 0])

    # Add all nicknames to the balances CSV
    for nickname, username in nicknames:
        writer.writerow([nickname, 0, 0])

    # Seek to the beginning of the StringIO buffer
    output.seek(0)

    # Create a discord.File object from the CSV data
    dkp_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")

    # Send the file to the dkp-database channel without a message
    await dkp_database_channel.send(file=dkp_file)

async def create_config_csv(guild):
    # Create the header and populate the settings from DEFAULT_CONFIG
    config_data = [["Setting", "Choice"]] + [[setting, value] for setting, value in DEFAULT_CONFIG.items()]

    # Create an in-memory file for the CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(config_data)
    output.seek(0)

    # Create a discord.File object from the CSV data
    config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")

    # Fetch the DKP Database Channel
    db_channel = discord.utils.get(guild.text_channels, name="dkp-database")

    if db_channel:
        # Send the config.csv file to the channel
        await db_channel.send(file=config_file)
        print(f"config.csv created and sent to the 'dkp-database' channel in {guild.name}.")
    else:
        print(f"Could not find the 'dkp-database' channel in {guild.name}.")

async def get_dkp_value_for_boss(boss_command, guild):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        return None

    # Find the "Boss_DKP_Values.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Boss_DKP_Values.csv")
    if message is None:
        return None

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        return None

    # Clean the boss command to extract the boss name correctly
    boss_command_clean = boss_command.replace("!", "").strip()

    # Handle specific cases for commands that start with 'k'
    if boss_command_clean.startswith('k'):
        boss_command_clean = boss_command_clean[1:]  # Remove the 'k' for lookup

    # Check if the cleaned command is numeric or a known boss name
    if boss_command_clean.isnumeric():
        # Directly check numeric values (e.g., boss levels)
        boss_level = boss_command_clean
        for row in csv_data:
            if row[0] == boss_level:  # Compare with level
                return int(row[1])  # Return DKP value
    else:
        # Check for boss names
        for row in csv_data:
            if row[0].lower() == boss_command_clean.lower():  # Case-insensitive comparison
                return int(row[1])  # Return DKP value

    return None  # If not found

# Function to read the "togglewindows" setting from config.csv

async def get_togglewindows_setting(guild):
    # Find the "dkp-database" channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")

    if dkp_database_channel is None:
        return None

    # Find the config.csv file
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        return None

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        return None

    # Search for the "togglewindows" setting
    for row in csv_data:
        if row[0] == "togglewindows":
            return row[1].lower() == "true"  # Return as boolean

    return None  # Default to None if not found

# Function to read the "Active_timers" setting from config.csv
async def get_active_timers_setting(guild):
    # Find the "dkp-database" channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")

    if dkp_database_channel is None:
        return None

    # Find the config.csv file
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        return None

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        return None

    # Search for the "Active_timers" setting
    for row in csv_data:
        if row[0] == "Active_timers":
            return row[1].lower() == "true"  # Return as boolean

    return None  # Default to None if not found


@bot.event
async def on_message(message):
    if message.author == bot.user:
        return  # Ignore messages sent by the bot itself

    # Check if the message is in a DM
    if message.guild is None:
        # Process DM-specific commands
        if message.content.lower().startswith("!help"):
            # Directly call the help_command in a DM context
            ctx = await bot.get_context(message)
            await bot.invoke(ctx)
        return  # Exit after handling DM

    # Load the valid commands from the CSV file dynamically
    valid_commands = await load_valid_commands()

    # Debugging: print the message to check if it's captured correctly
    #print(f"Received message: {message.content}")

    # Check if the message starts with either !k or !a followed by a valid boss command
    if message.content.lower().startswith("!k") or message.content.lower().startswith("!a"):
        # Extract the boss command part (e.g., "155/4" in "!k155/4")
        command_prefix = message.content[1].lower()  # 'k' or 'a'
        command_name = "k" if command_prefix == "k" else "a"

        # Confirm if the user has the required role for the command
        result = await role_confirm_command(message, command_name)
        if result is None:
            return  # Exit if the user doesn't have the required role

        boss_command = message.content[2:].split(" ")[0].lower()  # Strip the prefix and extract boss part
        #print(f"Extracted boss command: {boss_command}")

        if f"!k{boss_command}" in valid_commands or f"!a{boss_command}" in valid_commands:
            if message.content.startswith("!k"):
                await message.add_reaction('⚔️')
            elif message.content.startswith("!a"):
                await handle_attendance_command(message)
            return

    # Check if the message content starts with "!assign " for the emoji assignment function
    if message.content.startswith("!assign "):
        await bot.process_commands(message)  # Process command normally
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(message.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        return  # Exit if the database channel does not exist

    # Find the Boss_Timers.csv message in the "dkp-database" channel
    csv_message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
    if csv_message is None:
        return  # Exit if the Boss_Timers.csv file does not exist

    # Download and parse the CSV file
    csv_file = csv_message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        return  # Exit if the CSV file could not be downloaded or parsed

    # Check if the message content matches a valid boss timer command (e.g., !155, !200)
    boss_name = message.content.lower()  # Make the command case-insensitive
    valid_boss = False
    for row in csv_data:
        if row[0].strip().lower() == boss_name:  # Ensure both command and row are lowercased
            valid_boss = True
            break

    if valid_boss:
        # Call the unified function that handles both notifications and embed updates
        await handle_boss_timers(message)
        return

    # Allow the bot to process other commands after on_message event
    await bot.process_commands(message)

async def handle_attendance_command(message):
    # Load valid commands dynamically from the Boss_DKP_Values.csv
    valid_commands = await load_valid_commands()

    # Extract the boss command and users from message content
    parts = message.content.split(" ")
    boss_command = parts[0][2:]  # Extract the boss part (e.g., 155/4)
    names = parts[1:]  # Get the remaining parts as names

    if len(names) == 0:
        await message.channel.send("Please mention at least one user or nickname.")
        return

    # Check if the boss is a valid command
    if f"!a{boss_command}".lower() not in valid_commands:
        await message.channel.send(f"Could not find DKP value for {boss_command}.")
        return

    # Find the DKP value for the boss from the CSV
    dkp_value = await get_dkp_value_for_boss(boss_command, message.guild)
    if dkp_value is None:
        await message.channel.send(f"Could not find DKP value for {boss_command}.")
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(message.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await message.channel.send("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    balance_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if balance_message is None:
        await message.channel.send("Could not find the Balances_Database.csv file.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = {}
    mains = {}

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create dictionaries for nicknames and main entries
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ") if row[1] else []
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping
                if len(row) > 2 and row[2]:  # Check if a main is assigned
                    mains[row[0]] = row[2]  # Username -> Main Nickname mapping

    # Download and parse the Balances CSV file
    balance_csv_file = balance_message.attachments[0]
    balance_csv_data = await download_csv(balance_csv_file)

    if balance_csv_data is None:
        await message.channel.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name
    updated_members = []
    for name in names:
        target_name = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = message.guild.get_member(int(user_id))
            if member:
                # Use the main nickname if it exists, otherwise default to the user's name
                target_name = mains.get(member.name, member.name)
        else:
            # Check if it's a nickname
            if name.lower() in nicknames:
                target_name = name.lower()  # Use the nickname directly
            else:
                # Fallback: Check if it's a username
                member = discord.utils.get(message.guild.members, name=name)
                if member:
                    target_name = member.name  # Use the username directly

        if not target_name:
            await message.channel.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found target
        updated = False
        for row in balance_csv_data:
            if row[0].lower() == target_name:  # Match by name or nickname
                current_balance = int(row[1]) + dkp_value
                lifetime_balance = int(row[2]) + dkp_value
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        # If the target was not found, add them
        if not updated:
            balance_csv_data.append([target_name, str(dkp_value), str(dkp_value)])

        updated_members.append(target_name)

    if not updated_members:
        await message.channel.send("No users or nicknames were found or processed for attendance.")
        return  # Exit if no members were successfully processed

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(balance_csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await balance_message.delete()

    # Check if logging is enabled
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is not None:
        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is not None:
            # Find the "messagetoggle_a" and "whograntedtoggle_a" settings
            log_toggle = next((row[1] for row in config_data if row[0] == "messagetoggle_a"), "true").lower()
            who_granted_toggle = next((row[1] for row in config_data if row[0] == "whograntedtoggle_a"), "false").lower()

            if log_toggle == "true":
                # Log the attendance in the DKP log channel
                log_channel = discord.utils.get(message.guild.text_channels, name="dkp-keeping-log")
                if log_channel:
                    granted_by = f"{message.author.display_name} granted: " if who_granted_toggle == "true" else ""
                    await log_channel.send(
                        f"{granted_by}{', '.join(updated_members)} have attended {boss_command} and earned {dkp_value} DKP."
                    )

    # Send confirmation to the channel where the command was issued
    await message.channel.send(f"{dkp_value} DKP added for attending {boss_command} to:\n" + "\n".join(updated_members))

async def load_valid_commands():
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(bot.guilds[0].text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print("The DKP database channel does not exist.")
        return []

    # Find the Boss_DKP_Values.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Boss_DKP_Values.csv")
    if message is None:
        print("Could not find the Boss_DKP_Values.csv file.")
        return []

    # Download and parse the Boss_DKP_Values.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        print("Could not download or parse the Boss_DKP_Values.csv file.")
        return []

    # Create a list of valid commands for both !k and !a
    valid_commands = [f"!k{row[0].lower()}" for row in csv_data]
    valid_commands.extend([f"!a{row[0].lower()}" for row in csv_data])

    return valid_commands

# helper function for reaction addition to cope with new nickname tech
async def update_dkp_for_user(member, target_name, dkp_value, boss_name, guild, add=True):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    csv_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if csv_message is None:
        print("Could not find the Balances_Database.csv file.")
        return

    # Download and parse the CSV file
    csv_file = csv_message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        print("Could not download or parse the CSV file.")
        return

    # Modify the data
    updated = False
    for row in csv_data:
        if row[0] == target_name:  # Match by name
            current_balance = int(row[1]) + dkp_value if add else int(row[1]) - dkp_value
            lifetime_balance = int(row[2]) + dkp_value if add else int(row[2]) - dkp_value
            row[1] = str(current_balance)
            row[2] = str(lifetime_balance)
            updated = True
            break

    # If the user was not found, add them to the CSV (for addition only)
    if not updated and add:
        csv_data.append([target_name, str(dkp_value), str(dkp_value)])

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await csv_message.delete()

    # Check if logging is enabled
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is not None:
        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is not None:
            # Find the "messagetoggle_k" setting
            log_toggle = next((row[1] for row in config_data if row[0] == "messagetoggle_k"), "true").lower()
            if log_toggle == "true":
                # Log to the DKP log channel
                log_channel = discord.utils.get(guild.text_channels, name="dkp-keeping-log")
                if log_channel:
                    action = "earned" if add else "lost"
                    if target_name == member.name:
                        # Simple message if the target is the username
                        log_message = f"{member.display_name} has {action} {dkp_value} DKP for {boss_name}."
                    else:
                        # Include the linked text if the target is a nickname
                        log_message = f"{target_name} (linked to {member.display_name}) has {action} {dkp_value} DKP for {boss_name}."
                    await log_channel.send(log_message)

reaction_tracking = {}  # Dictionary to track reactions { (message_id, user_id): nickname }

@bot.event
async def on_raw_reaction_add(payload):
    guild = bot.get_guild(payload.guild_id)
    channel = bot.get_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    member = guild.get_member(payload.user_id)

    if member.bot:
        return  # Ignore bot reactions

    # Load valid commands dynamically from the Boss_DKP_Values.csv
    valid_commands = await load_valid_commands()

    if str(payload.emoji) == '⚔️' and message.content.lower() in valid_commands:
        command = message.content.lower()
        boss_name = command.replace("!k", "")  # Extract boss name
        dkp_value = await get_dkp_value_for_boss(command, guild)

        if dkp_value is None:
            print(f"Could not find DKP value for {command}.")
            return

        # Fetch the DKP Database Channel
        dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
        if dkp_database_channel is None:
            print("The DKP database channel does not exist.")
            return

        # Fetch user nicknames from Nicknames.csv
        nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
        nicknames = []
        main_nickname = None

        if nickname_message:
            nickname_csv_file = nickname_message.attachments[0]
            nickname_csv_data = await download_csv(nickname_csv_file)

            if nickname_csv_data:
                for row in nickname_csv_data:
                    if row[0] == member.name:  # Match username
                        nicknames = row[1].split(", ") if row[1] else []
                        main_nickname = row[2] if len(row) > 2 else None
                        break

        # Determine if a prompt is needed
        if len(nicknames) == 1 and nicknames[0] == main_nickname:
            # Only one nickname, and it's the "main" — credit directly
            await update_dkp_for_user(member, main_nickname, dkp_value, boss_name, guild, add=True)
            return

        # Prepare options for selection
        view = discord.ui.View()

        # Add the user's username as an option if no main is set
        if not main_nickname:
            button = discord.ui.Button(label=member.name, custom_id=f"add_dkp_{member.name}")
            view.add_item(button)

        # Add all nicknames as options
        for nickname in nicknames:
            button = discord.ui.Button(label=nickname, custom_id=f"add_dkp_{nickname}")
            view.add_item(button)

        # Handle button interaction
        async def button_callback(interaction):
            if interaction.user.id != member.id:
                await interaction.response.send_message("This selection is not for you.", ephemeral=True)
                return

            selected_nickname = interaction.data["custom_id"].split("_")[2]

            # Update DKP for the selected nickname or username
            await update_dkp_for_user(member, selected_nickname, dkp_value, boss_name, guild, add=True)

            # Update reaction tracking
            reaction_tracking[(message.id, member.id)] = selected_nickname

            await interaction.response.send_message(
                f"DKP for {boss_name} has been added to {selected_nickname}.", ephemeral=True
            )
            await temp_message.delete()

        # Add callback to buttons
        for button in view.children:
            button.callback = button_callback

        # Send the selection message (tag the user for clarity)
        temp_message = await message.channel.send(
            content=f"{member.mention}, which account should gain DKP for {boss_name}?",
            view=view
        )

@bot.event
async def on_raw_reaction_remove(payload):
    guild = bot.get_guild(payload.guild_id)
    channel = bot.get_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    member = guild.get_member(payload.user_id)

    if member is None or member.bot:
        return  # Ignore bot reactions or missing members

    # Load valid commands dynamically from the Boss_DKP_Values.csv
    valid_commands = await load_valid_commands()

    if str(payload.emoji) == '⚔️' and message.content.lower() in valid_commands:
        command = message.content.lower()
        boss_name = command.replace("!k", "")  # Extract boss name
        dkp_value = await get_dkp_value_for_boss(command, guild)

        if dkp_value is None:
            print(f"Could not find DKP value for {command}.")
            return

        # Check reaction tracking
        tracked_nickname = reaction_tracking.pop((message.id, member.id), None)

        if tracked_nickname:
            # Remove DKP using the tracked nickname
            await update_dkp_for_user(member, tracked_nickname, dkp_value, boss_name, guild, add=False)
            return  # Exit early if the tracked nickname exists

        # Fallback: Handle cases where the bot was restarted or no nickname was tracked
        # Fetch the DKP Database Channel
        dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
        if dkp_database_channel is None:
            print("The DKP database channel does not exist.")
            return

        # Fetch the user's nicknames from Nicknames.csv
        nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
        nicknames = []
        main_nickname = None

        if nickname_message:
            nickname_csv_file = nickname_message.attachments[0]
            nickname_csv_data = await download_csv(nickname_csv_file)

            if nickname_csv_data:
                for row in nickname_csv_data:
                    if row[0] == member.name:  # Match username
                        nicknames = row[1].split(", ") if row[1] else []
                        main_nickname = row[2] if len(row) > 2 else None
                        break

        # Determine if a prompt is needed
        if len(nicknames) == 1 and nicknames[0] == main_nickname:
            # Only one nickname, and it's the "main" — remove DKP directly
            await update_dkp_for_user(member, main_nickname, dkp_value, boss_name, guild, add=False)
            return

        # Prepare options for selection
        view = discord.ui.View()

        # Add the user's username as an option if no main is set
        if not main_nickname:
            button = discord.ui.Button(label=member.name, custom_id=f"remove_dkp_{member.name}")
            view.add_item(button)

        # Add all nicknames as options
        for nickname in nicknames:
            button = discord.ui.Button(label=nickname, custom_id=f"remove_dkp_{nickname}")
            view.add_item(button)

        # Handle button interaction
        async def button_callback(interaction):
            if interaction.user.id != member.id:
                await interaction.response.send_message("This selection is not for you.", ephemeral=True)
                return

            selected_nickname = interaction.data["custom_id"].split("_")[2]

            # Remove DKP for the selected nickname or username
            await update_dkp_for_user(member, selected_nickname, dkp_value, boss_name, guild, add=False)

            await interaction.response.send_message(
                f"DKP for {boss_name} has been removed from {selected_nickname}.", ephemeral=True
            )
            await temp_message.delete()

        # Add callback to buttons
        for button in view.children:
            button.callback = button_callback

        # Send the selection message (tag the user for clarity)
        temp_message = await message.channel.send(
            content=f"{member.mention}, which account should lose DKP for {boss_name}?",
            view=view
        )

@bot.command(name="createbackup")
async def create_backup(ctx):

    result = await role_confirm_command(ctx,"createbackup")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if message is None:
        await ctx.send("Could not find the Balances_Database.csv file to backup.")
        return

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Get the current date and time
    from datetime import datetime
    now = datetime.now()
    timestamp = now.strftime("%m.%d.%Y_%H%M")  # Month.Day.Year_HourMinute (in military time)

    # Create the backup filename with the correct format
    backup_filename = f"Balances_Database_{timestamp}.csv"

    # Create a new CSV file in memory with the existing data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)  # Write the data to the backup file
    output.seek(0)

    # Create a discord.File object from the CSV data
    backup_file = discord.File(io.BytesIO(output.getvalue().encode()), filename=backup_filename)

    # Send the backup file to the "dkp-database" channel
    await dkp_database_channel.send(file=backup_file)

    # Send a confirmation message in the channel where the command was issued
    await ctx.send(f"Backup created: {backup_filename}")

@bot.command(name="restorebackup")
async def restore_backup(ctx):

    result = await role_confirm_command(ctx,"restorebackup")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Check if there is an existing "Balances_Database.csv" file
    existing_file_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")

    if existing_file_message:
        # Prompt user for confirmation to overwrite
        confirmation_message = await ctx.send(
            "There is a current Balances_Database.csv file, are you sure you want to restore from backup?")

        # React with checkmark and X
        await confirmation_message.add_reaction('✅')  # Checkmark for confirmation
        await confirmation_message.add_reaction('❌')  # X for cancellation

        # Wait for the user who sent the command to react
        def check(reaction, user):
            return user == ctx.author and str(reaction.emoji) in ['✅',
                                                                  '❌'] and reaction.message.id == confirmation_message.id

        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)

            if str(reaction.emoji) == '✅':
                # Proceed with restoration if confirmed
                await ctx.send("Restoring from backup...")
                await proceed_with_restore(ctx, dkp_database_channel)
            else:
                # Cancel if X is clicked
                await ctx.send("Restore from backup canceled.")
        except asyncio.TimeoutError:
            await ctx.send("No reaction received, restore from backup canceled.")
    else:
        # If no existing file, proceed with restoration directly
        await proceed_with_restore(ctx, dkp_database_channel)

async def proceed_with_restore(ctx, dkp_database_channel):
    # Collect all backup files following the pattern "Balances_Database_MM.DD.YYYY_HHMM.csv"
    backup_files = []
    async for message in dkp_database_channel.history(limit=100):
        for attachment in message.attachments:
            if attachment.filename.startswith("Balances_Database_") and attachment.filename.endswith(".csv"):
                backup_files.append((attachment.filename, attachment.url))

    # If no backup files were found, notify the user
    if not backup_files:
        await ctx.send("No backup files found.")
        return

    # If only one backup file is found, automatically restore it
    if len(backup_files) == 1:
        filename, file_url = backup_files[0]
        await restore_specific_backup_file(ctx, filename, file_url)
        return

    # Display the list of backup files for the user to choose from
    backup_message = "Backup files found:\n"
    for i, (filename, _) in enumerate(backup_files, 1):
        backup_message += f"{i}: {filename}\n"
    backup_message += "Reply with !backup <number> to restore from the desired backup."

    # Send the list of backups to the user
    await ctx.send(backup_message)

    # Store the backup files in a dictionary to reference later
    global available_backups  # Store globally to access in reply handling
    available_backups = backup_files

async def restore_specific_backup_file(ctx, filename, file_url):
    # Download the selected backup CSV
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as response:
            if response.status == 200:
                file_content = await response.read()

                # Decode the content and read the CSV file
                decoded_content = file_content.decode('utf-8')
                csv_data = decoded_content

                # Upload the selected backup as the new Balances_Database.csv
                new_backup_file = discord.File(io.BytesIO(csv_data.encode()), filename="Balances_Database.csv")

                # Fetch the DKP Database Channel
                dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")

                if dkp_database_channel:
                    # Find the existing Balances_Database.csv message
                    message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
                    if message:
                        await message.delete()  # Delete the old CSV file

                    # Send the new backup file
                    await dkp_database_channel.send(file=new_backup_file)

                    # Confirm the restoration to the user
                    await ctx.send(f"Successfully restored from {filename}.")
                else:
                    await ctx.send("The DKP database channel does not exist.")
            else:
                await ctx.send(f"Failed to download {filename}.")

@bot.command(name="backup")
async def restore_specific_backup(ctx, backup_number: int):

    result = await role_confirm_command(ctx,"backup")
    if result is None:
        return

    # Fetch the selected backup file from the available backups
    backup_index = backup_number - 1

    if 0 <= backup_index < len(available_backups):
        filename, file_url = available_backups[backup_index]
        await restore_specific_backup_file(ctx, filename, file_url)
    else:
        await ctx.send("Invalid backup selection. Please choose a valid backup number.")

# Command to generate and send the Balances_Database CSV
@bot.command(name="generatebalances")
async def generate_balances(ctx):

    result = await role_confirm_command(ctx,"generatebalances")
    if result is None:
        return

    # Ensure the command is only allowed in the "dkp-database" channel
    if ctx.channel.name != "dkp-database":
        await ctx.send("This command can only be used in the #dkp-database channel.")
        return

    # Create an in-memory file
    output = io.StringIO()
    writer = csv.writer(output)

    # Write the header for the CSV
    writer.writerow(["Username", "Current Balance", "Lifetime Balance"])

    # Fetch all members in the server and write their data to the CSV
    async for member in ctx.guild.fetch_members(limit=None):
        # Replace the 0 placeholders with actual balance data if available
        writer.writerow([member.name, 0, 0])

    # Seek to the beginning of the StringIO buffer
    output.seek(0)

    # Create a discord.File object from the CSV data
    csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")

    # Send the file to the channel
    await ctx.send("Here is the current Balances Database:", file=csv_file)

async def save_active_boss_timers(guild):
    """Write the current in-memory active_boss_timers to #dkp-database as Active_Boss_Timers.csv."""
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print(f"[Timers] dkp-database not found in {guild.name}, cannot save active timers.")
        return

    # Build CSV rows
    rows = [["Boss Name", "Timer End (epoch)", "Window End (epoch)", "Channel ID"]]
    for boss_name, data in active_boss_timers.items():
        rows.append([
            boss_name,
            str(data["timer_end"]),
            str(data["window_end"]),
            str(data.get("channel_id", "")),
        ])

    # Make CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(rows)
    output.seek(0)

    # If there is an old Active_Boss_Timers.csv, delete it so we don’t pile up messages
    old_msg = await find_csv_message(dkp_database_channel, ACTIVE_TIMERS_FILENAME)
    if old_msg is not None:
        await old_msg.delete()

    new_file = discord.File(
        io.BytesIO(output.getvalue().encode()),
        filename=ACTIVE_TIMERS_FILENAME
    )
    await dkp_database_channel.send(file=new_file)
    print(f"[Timers] Saved {len(active_boss_timers)} active timers for {guild.name}.")

async def load_active_boss_timers(guild):
    """Read Active_Boss_Timers.csv from #dkp-database and recreate the running timer tasks."""
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print(f"[Timers] dkp-database not found in {guild.name}, cannot load active timers.")
        return

    msg = await find_csv_message(dkp_database_channel, ACTIVE_TIMERS_FILENAME)
    if msg is None:
        # nothing saved yet, nothing to load
        return

    csv_data = await download_csv(msg.attachments[0])
    if csv_data is None or len(csv_data) <= 1:
        return

    now = time.time()

    # rows: Boss Name, Timer End (epoch), Window End (epoch), Channel ID
    for row in csv_data[1:]:
        if len(row) < 3:
            continue

        boss_name = row[0].strip().lower()
        try:
            timer_end = float(row[1])
            window_end = float(row[2])
        except ValueError:
            continue

        # If the timer+window is already over, skip it
        if now >= window_end:
            continue

        # Try to restore the channel we were using
        channel = None
        if len(row) >= 4 and row[3].strip():
            ch_id = int(row[3].strip())
            channel = guild.get_channel(ch_id)

        # fallback: just use the #timers channel or any text channel
        if channel is None:
            channel = discord.utils.get(guild.text_channels, name="timers") or guild.text_channels[0]

        # Put it back in memory
        active_boss_timers[boss_name] = {
            "timer_end": timer_end,
            "window_end": window_end,
            "channel_id": channel.id if channel else None,
        }

        # Recreate the background task so it will keep counting down
        task = asyncio.create_task(
            manage_boss_timers(guild, channel, boss_name, timer_end, window_end)
        )
        active_tasks[boss_name] = task

    # After we rehydrated, refresh the #timers embed once
    await update_timers_embed_if_active(guild)
    print(f"[Timers] Restored {len(active_boss_timers)} timers for {guild.name}.")


async def find_csv_message(channel, filename):
    # Fetch the message history from the channel
    async for message in channel.history(limit=100):  # Adjust the limit as needed
        if message.attachments:
            for attachment in message.attachments:
                if attachment.filename == filename:
                    return message
    return None

async def download_csv(attachment):
    """Download and parse the CSV file."""
    async with aiohttp.ClientSession() as session:
        async with session.get(attachment.url) as response:
            if response.status == 200:
                csv_content = await response.read()
                decoded_content = csv_content.decode('utf-8')
                return list(csv.reader(io.StringIO(decoded_content)))
    return None

async def role_confirm_command(ctx_or_message, command_name):
    # Determine whether we are working with a context or message object
    if hasattr(ctx_or_message, "send"):
        send_method = ctx_or_message.send  # Use ctx.send for commands
    else:
        send_method = ctx_or_message.channel.send  # Use message.channel.send for on_message events

    # If guild is None, it's a DM. Allow commands that don't require a guild context.
    if ctx_or_message.guild is None:
        # Allow commands like help in DMs
        if command_name == "help":
            return None  # Skip role checks for the help command in DMs
        else:
            await send_method("This command cannot be used in direct messages.")
            return None

    # Fetch the DKP Database Channel (only in guilds)
    dkp_database_channel = discord.utils.get(ctx_or_message.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await send_method("The DKP database channel does not exist.")
        return None

    # Find the config.csv message in the "dkp-database" channel
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await send_method("Could not find the config.csv file.")
        return None

    # Download and parse the config.csv file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)
    if config_data is None:
        await send_method("Could not download or parse the config.csv file.")
        return None

    # Generate the role requirement setting dynamically
    role_required_setting = f"role_req_{command_name}"
    role_req = next((row[1] for row in config_data if row[0] == role_required_setting), "true")

    # Check if the role is required and if the user has it
    if role_req.lower() == "true" and "DKP Keeper" not in [role.name for role in ctx_or_message.author.roles]:
        await send_method(f"You are missing the DKP Keeper role to use the {command_name} command.")
        return None

    # Return the parsed config data for further processing
    return config_data, dkp_database_channel, config_message

@bot.command(name="dkpadd")
async def dkp_add(ctx, dkp_value: int, *names: str):
    result = await role_confirm_command(ctx, "dkpadd")
    if result is None:
        return

    # Check if at least one member or nickname is provided
    if len(names) == 0:
        await ctx.send("Usage: !dkpadd <dkp_value> <user/nickname> [additional users/nicknames]")
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if message is None:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = {}
    mains = {}

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create dictionaries for nicknames and main entries
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ") if row[1] else []
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping
                if len(row) > 2 and row[2]:  # Check if a main is assigned
                    mains[row[0]] = row[2]  # Username -> Main Nickname mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name
    updated_members = []
    for name in names:
        target_name = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
            if member:
                # Use the main nickname if it exists, otherwise default to the user's name
                target_name = mains.get(member.name, member.name)
        else:
            # Check if it's a nickname
            if name.lower() in nicknames:
                target_name = name.lower()  # Use the nickname directly
            else:
                # Fallback: Check if it's a username
                member = discord.utils.get(ctx.guild.members, name=name)
                if member:
                    target_name = member.name  # Use the username directly

        if not target_name:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found target
        updated = False
        current_balance = 0
        lifetime_balance = 0

        for row in csv_data:
            if row[0].lower() == target_name:  # Match by name or nickname
                current_balance = int(row[1]) + dkp_value
                lifetime_balance = int(row[2]) + dkp_value
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        # If the target was not found, add them
        if not updated:
            current_balance = dkp_value
            lifetime_balance = dkp_value
            csv_data.append([target_name, str(current_balance), str(lifetime_balance)])

        updated_members.append(f"{target_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}")

    if not updated_members:
        await ctx.send("No users or nicknames were found or processed for DKP.")
        return  # Exit if no members were successfully processed

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Check if logging is enabled
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is not None:
        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is not None:
            # Find the "messagetoggle_dkpadd" and "whograntedtoggle_dkpadd" settings
            log_toggle = next((row[1] for row in config_data if row[0] == "messagetoggle_dkpadd"), "true").lower()
            who_granted_toggle = next((row[1] for row in config_data if row[0] == "whograntedtoggle_dkpadd"), "false").lower()

            if log_toggle == "true":
                # Log the DKP addition in the DKP log channel
                log_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-keeping-log")
                if log_channel:
                    granted_by = f"{ctx.author.display_name} granted: " if who_granted_toggle == "true" else ""
                    await log_channel.send(
                        f"{granted_by}DKP added: {dkp_value} to:\n" + "\n".join(updated_members)
                    )

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP added to:\n" + "\n".join(updated_members))

@bot.command(name="dkpaddcurrent")
async def dkp_add_current(ctx, dkp_value: int, *names: str):
    result = await role_confirm_command(ctx, "dkpaddcurrent")
    if result is None:
        return

    # Check if at least one member or nickname is provided
    if len(names) == 0:
        await ctx.send("Usage: !dkpaddcurrent <dkp_value> <user/nickname> [additional users/nicknames]")
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if message is None:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = {}
    mains = {}

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create dictionaries for nicknames and main entries
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ") if row[1] else []
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping
                if len(row) > 2 and row[2]:  # Check if a main is assigned
                    mains[row[0]] = row[2]  # Username -> Main Nickname mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        target_name = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
            if member:
                # Use the main nickname if it exists, otherwise the username
                target_name = mains.get(member.name, member.name)
        else:
            # Check if it's a nickname
            if name.lower() in nicknames:
                target_name = name  # Use the nickname directly
            else:
                # Fallback: Check if it's a username
                member = discord.utils.get(ctx.guild.members, name=name)
                if member:
                    target_name = mains.get(member.name, member.name)  # Default to main nickname if no main is set

        if not target_name:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found target (either main nickname or username)
        updated = False
        current_balance = 0
        lifetime_balance = 0

        for row in csv_data:
            if row[0] == target_name:  # Match by name (nickname or main nickname)
                current_balance = int(row[1]) + dkp_value
                lifetime_balance = int(row[2])  # Lifetime balance remains unchanged
                row[1] = str(current_balance)  # Update only the current balance in the CSV
                updated = True
                break

        # If the target was not found in the CSV, add them
        if not updated:
            current_balance = dkp_value
            lifetime_balance = 0
            csv_data.append([target_name, str(current_balance), str(lifetime_balance)])  # Add new entry

        updated_members.append(
            f"{target_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}"
        )

    if not updated_members:
        await ctx.send("No users or nicknames were found or processed for DKP addition.")
        return  # Exit if no members were successfully processed

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Check if logging is enabled
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is not None:
        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is not None:
            # Find the "messagetoggle_dkpaddcurrent" and "whograntedtoggle_dkpaddcurrent" settings
            log_toggle = next((row[1] for row in config_data if row[0] == "messagetoggle_dkpaddcurrent"), "true").lower()
            who_granted_toggle = next((row[1] for row in config_data if row[0] == "whograntedtoggle_dkpaddcurrent"), "false").lower()

            if log_toggle == "true":
                # Log the DKP addition in the DKP log channel
                log_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-keeping-log")
                if log_channel:
                    granted_by = f"{ctx.author.display_name} granted: " if who_granted_toggle == "true" else ""
                    await log_channel.send(
                        f"{granted_by}{dkp_value} DKP added to current balances of:\n" + "\n".join(updated_members)
                    )

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP added to current balances of:\n" + "\n".join(updated_members))

# Removes DKP from just current, good for things like auctions
@bot.command(name="dkpsubtract")
async def dkp_subtract(ctx, dkp_value: int, *names: str):
    result = await role_confirm_command(ctx, "dkpsubtract")
    if result is None:
        return

    # Check if at least one member or nickname is provided
    if len(names) == 0:
        await ctx.send("Usage: !dkpsubtract <dkp_value> <user/nickname> [additional users/nicknames]")
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if message is None:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = {}
    mains = {}

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create dictionaries for nicknames and main entries
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ") if row[1] else []
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping
                if len(row) > 2 and row[2]:  # Check if a main is assigned
                    mains[row[0]] = row[2]  # Username -> Main Nickname mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        target_name = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
            if member:
                target_name = mains.get(member.name, member.name)  # Use the main nickname or username
        else:
            # Check if it's a nickname or a username directly
            if name.lower() in nicknames:
                target_name = name  # Use the nickname directly
            else:
                member = discord.utils.get(ctx.guild.members, name=name)
                if member:
                    target_name = mains.get(member.name, member.name)  # Use the main nickname or username

        if not target_name:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found target (nickname or username)
        updated = False
        current_balance = 0
        lifetime_balance = 0

        for row in csv_data:
            if row[0] == target_name:  # Match by name
                current_balance = int(row[1]) - dkp_value  # Subtract DKP from current balance
                lifetime_balance = int(row[2])  # Lifetime balance remains unchanged
                row[1] = str(current_balance)  # Update current balance
                updated = True
                break

        if not updated:
            await ctx.send(f"{target_name} not found in the Balances_Database.csv.")
            continue

        updated_members.append(
            f"{target_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}"
        )

    if not updated_members:
        await ctx.send("No users or nicknames were found or processed for DKP deduction.")
        return  # Exit if no members were successfully processed

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Check if logging is enabled
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is not None:
        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is not None:
            # Find logging settings
            log_toggle = next((row[1] for row in config_data if row[0] == "messagetoggle_dkpsubtract"), "true").lower()
            who_granted_toggle = next((row[1] for row in config_data if row[0] == "whograntedtoggle_dkpsubtract"), "false").lower()

            if log_toggle == "true":
                # Log the DKP subtraction in the DKP log channel
                log_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-keeping-log")
                if log_channel:
                    granted_by = f"{ctx.author.display_name} granted: " if who_granted_toggle == "true" else ""
                    await log_channel.send(
                        f"{granted_by}DKP deducted: {dkp_value} from:\n" + "\n".join(updated_members)
                    )

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP deducted from:\n" + "\n".join(updated_members))

@bot.command(name="dkpsubtractlifetime")
async def dkp_subtract_lifetime(ctx, dkp_value: int, *names: str):
    result = await role_confirm_command(ctx, "dkpsubtractlifetime")
    if result is None:
        return

    # Check if at least one member or nickname is provided
    if len(names) == 0:
        await ctx.send("Usage: !dkpsubtractlifetime <dkp_value> <user/nickname> [additional users/nicknames]")
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if message is None:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = {}
    mains = {}

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create dictionaries for nicknames and main entries
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ") if row[1] else []
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping
                if len(row) > 2 and row[2]:  # Check if a main is assigned
                    mains[row[0]] = row[2]  # Username -> Main Nickname mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        target_name = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
            if member:
                target_name = mains.get(member.name, member.name)  # Use the main nickname or username
        else:
            # Check if it's a nickname or a username directly
            if name.lower() in nicknames:
                target_name = name  # Use the nickname directly
            else:
                member = discord.utils.get(ctx.guild.members, name=name)
                if member:
                    target_name = mains.get(member.name, member.name)  # Use the main nickname or username

        if not target_name:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found target (nickname or username)
        updated = False
        lifetime_balance = 0
        current_balance = 0

        for row in csv_data:
            if row[0] == target_name:  # Match by name
                lifetime_balance = max(0, int(row[2]) - dkp_value)  # Subtract DKP from lifetime balance
                current_balance = int(row[1])  # Current balance remains unchanged
                row[2] = str(lifetime_balance)  # Update lifetime balance
                updated = True
                break

        if not updated:
            await ctx.send(f"{target_name} not found in the Balances_Database.csv.")
            continue

        updated_members.append(
            f"{target_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}"
        )

    if not updated_members:
        await ctx.send("No users or nicknames were found or processed for DKP deduction.")
        return  # Exit if no members were successfully processed

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Check if logging is enabled
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is not None:
        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is not None:
            # Find the logging settings
            log_toggle = next((row[1] for row in config_data if row[0] == "messagetoggle_dkpsubtractlifetime"), "true").lower()
            who_granted_toggle = next((row[1] for row in config_data if row[0] == "whograntedtoggle_dkpsubtractlifetime"), "false").lower()

            if log_toggle == "true":
                # Log the DKP subtraction in the DKP log channel
                log_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-keeping-log")
                if log_channel:
                    granted_by = f"{ctx.author.display_name} granted: " if who_granted_toggle == "true" else ""
                    await log_channel.send(
                        f"{granted_by}DKP deducted from lifetime: {dkp_value} from:\n" + "\n".join(updated_members)
                    )

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP deducted from lifetime balances of:\n" + "\n".join(updated_members))

# Removes DKP from both current and lifetime
@bot.command(name="dkpsubtractboth")
async def dkp_subtract_both(ctx, dkp_value: int, *names: str):
    result = await role_confirm_command(ctx, "dkpsubtractboth")
    if result is None:
        return

    # Check if at least one member or nickname is provided
    if len(names) == 0:
        await ctx.send("Usage: !dkpsubtractboth <dkp_value> <user/nickname> [additional users/nicknames]")
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Balances_Database.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if message is None:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = {}
    mains = {}

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create dictionaries for nicknames and main entries
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ") if row[1] else []
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping
                if len(row) > 2 and row[2]:  # Check if a main is assigned
                    mains[row[0]] = row[2]  # Username -> Main Nickname mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        target_name = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
            if member:
                target_name = mains.get(member.name, member.name)  # Use the main nickname or username
        else:
            # Use the nickname directly if found, otherwise fallback to username
            if name.lower() in nicknames:
                target_name = name  # Use the nickname directly
            else:
                member = discord.utils.get(ctx.guild.members, name=name)
                if member:
                    target_name = mains.get(member.name, member.name)  # Use the main nickname or username

        if not target_name:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found target
        updated = False
        current_balance = 0
        lifetime_balance = 0

        for row in csv_data:
            if row[0] == target_name:  # Match by name
                current_balance = max(0, int(row[1]) - dkp_value)  # Subtract from current balance
                lifetime_balance = max(0, int(row[2]) - dkp_value)  # Subtract from lifetime balance
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        if not updated:
            await ctx.send(f"{target_name} not found in the Balances_Database.csv.")
            continue

        updated_members.append(
            f"{target_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}"
        )

    if not updated_members:
        await ctx.send("No users or nicknames were found or processed for DKP deduction.")
        return  # Exit if no members were successfully processed

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Check if logging is enabled
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is not None:
        # Download and parse the config.csv file
        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is not None:
            # Find the logging settings
            log_toggle = next((row[1] for row in config_data if row[0] == "messagetoggle_dkpsubtractboth"), "true").lower()
            who_granted_toggle = next((row[1] for row in config_data if row[0] == "whograntedtoggle_dkpsubtractboth"), "false").lower()

            if log_toggle == "true":
                # Log the DKP subtraction in the DKP log channel
                log_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-keeping-log")
                if log_channel:
                    granted_by = f"{ctx.author.display_name} granted: " if who_granted_toggle == "true" else ""
                    await log_channel.send(
                        f"{granted_by}DKP deducted from both current and lifetime: {dkp_value} from:\n" + "\n".join(updated_members)
                    )

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP deducted from:\n" + "\n".join(updated_members))

@bot.command(name="bal")
async def check_balance(ctx, name: str = None):
    result = await role_confirm_command(ctx, "bal")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Load nicknames and main mappings
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = {}
    mains = {}

    if nickname_message:
        nickname_csv_data = await download_csv(nickname_message.attachments[0])
        if nickname_csv_data:
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ") if row[1] else []
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping
                if len(row) > 2 and row[2]:  # Check if a main is assigned
                    mains[row[0]] = row[2]  # Username -> Main Nickname mapping

    # Determine the user or nickname to check
    target_name = None
    user_name = None

    if not name:  # No input means check the invoker
        user_name = ctx.author.name
    elif name.startswith("<@") and name.endswith(">"):  # Mentioned user
        user_id = name.replace("<@", "").replace(">", "").replace("!", "")
        member = ctx.guild.get_member(int(user_id))
        user_name = member.name if member else None
    elif name.lower() in nicknames:  # Provided nickname
        target_name = name
    else:  # Username or invalid input
        member = discord.utils.get(ctx.guild.members, name=name)
        user_name = member.name if member else None

    if user_name:
        target_name = mains.get(user_name, user_name)

    if not target_name:
        await ctx.send(f"Could not find user or nickname: {name if name else ctx.author.display_name}")
        return

    # Adjust for account selection if multiple options exist
    user_nicknames = [
        nick for nick, user in nicknames.items() if user == user_name
    ]
    if user_name and mains.get(user_name) in user_nicknames:
        user_nicknames.remove(mains[user_name])

    if len(user_nicknames) > 1 or (len(user_nicknames) == 1 and user_name):
        options = [user_name] + user_nicknames if user_name not in mains.values() else user_nicknames

        # Create selection buttons
        view = discord.ui.View(timeout=30)
        for option in options:
            button = discord.ui.Button(label=option, custom_id=f"bal_{option}")
            view.add_item(button)

        msg = await ctx.send(f"{ctx.author.mention}, select the account to check the balance for:", view=view)

        async def button_callback(interaction):
            if interaction.user != ctx.author:
                await interaction.response.send_message("You cannot select this account.", ephemeral=True)
                return

            selected_name = interaction.data["custom_id"].split("_", 1)[1]
            await display_balance(ctx, selected_name, dkp_database_channel)
            await msg.delete()

        for button in view.children:
            button.callback = button_callback

        await view.wait()
        if not view.is_finished:
            await msg.edit(content="Timeout: No selection was made.", view=None)
        return

    # Display balance directly if no selection is needed
    await display_balance(ctx, target_name, dkp_database_channel)

async def display_balance(ctx, target_name, dkp_database_channel):
    # Fetch balances from Balances_Database.csv
    balances_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if not balances_message:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    csv_data = await download_csv(balances_message.attachments[0])
    if not csv_data:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Find the target's balance
    current_balance, lifetime_balance = 0, 0
    for row in csv_data:
        if row[0].lower() == target_name.lower():
            current_balance, lifetime_balance = int(row[1]), int(row[2])
            break

    await ctx.send(f"{target_name} has {current_balance} current DKP and {lifetime_balance} lifetime DKP.")


# Command to set the DKP value, restricted to "DKP Keeper" role
@bot.command(name="setdkp")
async def set_dkp_value(ctx, boss: str, dkp_value: int):

    result = await role_confirm_command(ctx,"setdkp")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the CSV message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Boss_DKP_Values.csv")
    if message is None:
        await ctx.send("Could not find the Boss_DKP_Values.csv file.")
        return

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the CSV file.")
        return

    # Search for the boss and update its DKP value
    updated = False
    for row in csv_data:
        if row[0] == boss:
            row[1] = str(dkp_value)
            updated = True
            break

    if not updated:
        await ctx.send(f"Boss {boss} not found in the DKP database.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Boss_DKP_Values.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    await ctx.send(f"DKP value for {boss} has been set to {dkp_value} DKP.")

    # Call the function to update the DKP values embed
    await send_dkp_values_embed(ctx.guild)

@bot.command(name="nick")
async def set_nickname(ctx, member: discord.Member, nickname: str = None):
    result = await role_confirm_command(ctx, "nick")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Nicknames.csv")

    # If Nicknames.csv doesn't exist, create it
    if message is None:
        # Create a CSV with header if it doesn't exist
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Username", "Nicknames", "Main"])  # Header with "Main"
        output.seek(0)
        nicknames_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
        await dkp_database_channel.send(file=nicknames_file)
        message = await find_csv_message(dkp_database_channel, "Nicknames.csv")  # Reload the message

    # Download and parse the Nicknames.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Nicknames.csv file.")
        return

    # If no nickname is provided, list the user's existing nicknames and main
    if nickname is None:
        for row in csv_data:
            if row[0] == member.name:  # Match by username
                existing_nicknames = row[1].split(", ") if row[1] else []
                main_nickname = row[2] if len(row) > 2 and row[2] else "None"
                if existing_nicknames:
                    await ctx.send(
                        f"{member.display_name} has the following nicknames: {', '.join(existing_nicknames)}\n"
                        f"Main nickname: {main_nickname}"
                    )
                else:
                    await ctx.send(f"{member.display_name} has no nicknames.")
                return
        await ctx.send(f"{member.display_name} has no nicknames.")
        return

    # Check for duplicate nicknames across all users
    for row in csv_data:
        if nickname.lower() in [nick.lower() for nick in row[1].split(", ")]:
            if row[0] == member.name:
                await ctx.send(f"{member.display_name} already has the nickname '{nickname}'.")
            else:
                await ctx.send(f"The nickname '{nickname}' is already taken by {row[0]}.")
            return

    # Find the user's existing nicknames
    current_nicknames = []
    for row in csv_data:
        if row[0] == member.name:  # Match by username
            current_nicknames = row[1].split(", ") if row[1] else []
            break

    # Read the double-check toggle from the config
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    config_data = await download_csv(config_message.attachments[0])
    double_check = next(
        (row[1].lower() == "true" for row in config_data if row[0] == "nick_doublecheck"), False
    )

    # Perform double-check if enabled
    if double_check:
        confirmation_msg = await ctx.send(
            f"Are you sure you want to add the nickname '{nickname}' for {member.display_name}? "
            f"Current nicknames: {', '.join(current_nicknames) if current_nicknames else 'None'}"
        )
        await confirmation_msg.add_reaction("✅")  # Checkmark
        await confirmation_msg.add_reaction("❌")  # Cross

        def check_reaction(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == confirmation_msg.id
                and str(reaction.emoji) in ["✅", "❌"]
            )

        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check_reaction)
            if str(reaction.emoji) == "❌":
                await ctx.send("Nickname addition canceled.")
                return
        except asyncio.TimeoutError:
            await ctx.send("You took too long to respond. Nickname addition canceled.")
            return

    # Modify or add the nickname for the specific user
    nickname_added = False  # Track if we added a new nickname
    updated = False
    for row in csv_data:
        if row[0] == member.name:  # Match by username
            existing_nicknames = row[1].split(", ") if row[1] else []
            if nickname not in existing_nicknames:
                existing_nicknames.append(nickname)  # Append the new nickname
                row[1] = ", ".join(existing_nicknames)  # Update the row with multiple nicknames
                nickname_added = True
            updated = True
            break

    # If the user was not found, add them to the CSV with the new nickname
    if not updated:
        csv_data.append([member.name, nickname, ""])  # Add new user with nickname and no main
        nickname_added = True  # Indicate that a nickname was added

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated Nicknames.csv to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Update Balances_Database.csv with the new nickname
    balances_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if balances_message:
        balances_csv_file = balances_message.attachments[0]
        balances_csv_data = await download_csv(balances_csv_file)

        # Add a new row for the nickname
        balances_csv_data.append([nickname, "0", "0"])

        # Create and upload the updated Balances_Database.csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(balances_csv_data)
        output.seek(0)
        updated_balances_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
        await dkp_database_channel.send(file=updated_balances_file)

        # Delete the old Balances_Database.csv message
        await balances_message.delete()

    # Send confirmation if nickname was added
    if nickname_added:
        await ctx.send(f"Nickname '{nickname}' has been added for {member.display_name}.")
    else:
        await ctx.send(f"Nickname '{nickname}' was already set for {member.display_name}.")

@bot.command(name="nickdelete")
async def delete_nickname(ctx, member: discord.Member, nickname: str = None):
    result = await role_confirm_command(ctx, "nickdelete")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    if message is None:
        await ctx.send("Could not find the Nicknames.csv file.")
        return

    # Download and parse the Nicknames.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Nicknames.csv file.")
        return

    # Read the double-check toggle from the config
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    config_data = await download_csv(config_message.attachments[0])
    double_check = next(
        (row[1].lower() == "true" for row in config_data if row[0] == "nickdelete_doublecheck"), False
    )

    # List all nicknames for the member
    user_nicknames = []
    for row in csv_data:
        if row[0] == member.name:
            user_nicknames = row[1].split(", ")
            break

    if not user_nicknames:
        await ctx.send(f"{member.display_name} does not have any nicknames to delete.")
        return

    # If no specific nickname is provided, send buttons for each nickname
    if nickname is None:
        view = View(timeout=30)  # Timeout for the buttons

        for nick in user_nicknames:
            button = Button(label=f"Delete '{nick}'", custom_id=f"delete_{nick}")

            # Use a closure to bind the current value of `nick` to the callback
            async def button_callback(interaction, nickname_to_delete=nick):
                if interaction.user != ctx.author:
                    await interaction.response.send_message("You cannot press this button.", ephemeral=True)
                    return

                if double_check:
                    await send_double_check(ctx, member, nickname_to_delete, csv_data, message, dkp_database_channel)
                else:
                    deletion_successful = await handle_nickname_deletion(
                        ctx, member, nickname_to_delete, csv_data, message, dkp_database_channel
                    )
                    if deletion_successful:
                        await interaction.response.send_message(
                            f"Nickname '{nickname_to_delete}' has been deleted for {member.display_name}."
                        )
                view.stop()

            button.callback = button_callback
            view.add_item(button)

        await ctx.send(
            f"Please select the nickname you want to delete from {member.display_name}'s list of nicknames:",
            view=view,
        )
        return

    # Otherwise, delete the provided nickname directly with or without double-check
    if double_check:
        await send_double_check(ctx, member, nickname, csv_data, message, dkp_database_channel)
    else:
        deletion_successful = await handle_nickname_deletion(
            ctx, member, nickname, csv_data, message, dkp_database_channel
        )
        if deletion_successful:
            await ctx.send(f"Nickname '{nickname}' has been deleted for {member.display_name}.")
        else:
            await ctx.send(f"Nickname '{nickname}' was not found for {member.display_name}.")

async def send_double_check(ctx, member, nickname, csv_data, message, dkp_database_channel):
    """Send a double-check message with reactions."""
    confirmation_msg = await ctx.send(
        f"Are you sure you want to remove the nickname '{nickname}' from {member.display_name}? "
        f"This will clear all DKP data associated with '{nickname}'. This cannot be reversed."
    )
    await confirmation_msg.add_reaction("✅")  # Checkmark
    await confirmation_msg.add_reaction("❌")  # Cross

    def check_reaction(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == confirmation_msg.id
            and str(reaction.emoji) in ["✅", "❌"]
        )

    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check_reaction)

        if str(reaction.emoji) == "✅":
            deletion_successful = await handle_nickname_deletion(
                ctx, member, nickname, csv_data, message, dkp_database_channel
            )
            if deletion_successful:
                await ctx.send(f"Nickname '{nickname}' has been deleted for {member.display_name}.")
            else:
                await ctx.send(f"Nickname '{nickname}' was not found for {member.display_name}.")
        else:
            await ctx.send(f"Deletion of nickname '{nickname}' has been canceled.")
    except asyncio.TimeoutError:
        await ctx.send("You took too long to respond. Nickname deletion canceled.")

async def handle_nickname_deletion(ctx, member, nickname, csv_data, message, dkp_database_channel):
    """Handle deletion of nickname from files."""
    updated = False
    nickname_lower = nickname.lower()
    for row in csv_data:
        if row[0] == member.name:
            existing_nicknames = row[1].split(", ")
            existing_nicknames_lower = [nick.lower() for nick in existing_nicknames]

            if nickname_lower in existing_nicknames_lower:
                index_to_remove = existing_nicknames_lower.index(nickname_lower)
                del existing_nicknames[index_to_remove]  # Remove the nickname
                if existing_nicknames:
                    row[1] = ", ".join(existing_nicknames)  # Update with remaining nicknames
                else:
                    csv_data.remove(row)  # Remove the row if no nicknames are left
                updated = True
                break

    if not updated:
        return False

    # Create a new CSV file with the updated nicknames data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated Nicknames.csv to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old Nicknames.csv
    await message.delete()

    # Update the Balances_Database.csv to remove the nickname entry
    balances_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if balances_message:
        balances_csv_file = balances_message.attachments[0]
        balances_csv_data = await download_csv(balances_csv_file)

        # Remove the row corresponding to the deleted nickname
        balances_csv_data = [row for row in balances_csv_data if row[0].lower() != nickname_lower]

        # Create and upload the updated Balances_Database.csv
        balances_output = io.StringIO()
        writer = csv.writer(balances_output)
        writer.writerows(balances_csv_data)
        balances_output.seek(0)
        updated_balances_file = discord.File(io.BytesIO(balances_output.getvalue().encode()), filename="Balances_Database.csv")
        await dkp_database_channel.send(file=updated_balances_file)

        # Delete the original Balances_Database.csv message
        await balances_message.delete()

    return True

@bot.command(name="setmain")
async def set_main(ctx, *args):
    result = await role_confirm_command(ctx, "setmain")
    if result is None:
        return

    member = ctx.author
    nickname = None

    # Determine if the first argument is a member or a nickname
    if len(args) > 0:
        try:
            # Attempt to resolve the first argument as a member
            member = await commands.MemberConverter().convert(ctx, args[0])
            # If a second argument exists, treat it as a nickname
            if len(args) > 1:
                nickname = args[1]
        except commands.MemberNotFound:
            # If the first argument isn't a member, treat it as a nickname
            nickname = args[0]

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    if message is None:
        await ctx.send("Could not find the Nicknames.csv file. Please create nicknames using !nick first.")
        return

    # Download and parse the Nicknames.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Nicknames.csv file.")
        return

    # Get the user's existing nicknames
    user_row = next((row for row in csv_data if row[0] == member.name), None)
    if user_row:
        existing_nicknames = user_row[1].split(", ") if user_row[1] else []
    else:
        existing_nicknames = []

    # If no nicknames exist for the user
    if not existing_nicknames:
        await ctx.send(f"{member.display_name} has no nicknames. Please create one with `!nick` or specify a main nickname with `!setmain @user nickname`.")
        return

    # If a specific nickname is provided
    if nickname:
        if nickname in existing_nicknames:
            # Update the main nickname in the CSV
            user_row[2] = nickname  # Update the "Main" column
            await update_csv_and_confirm(ctx, csv_data, dkp_database_channel, message, f"{nickname} is now the main nickname for {member.display_name}.")
        else:
            await ctx.send(f"{nickname} is not a valid nickname for {member.display_name}.")
        return

    # If no specific nickname is provided, display buttons for selection
    view = discord.ui.View(timeout=30)
    interaction_resolved = False  # Flag to track if an interaction has been handled

    for nick in existing_nicknames:
        button = discord.ui.Button(label=nick, custom_id=f"setmain_{nick}")
        view.add_item(button)

    msg = await ctx.send(f"Select the main nickname for {member.display_name}:", view=view)

    async def button_callback(interaction):
        nonlocal interaction_resolved
        if interaction.user != ctx.author:
            await interaction.response.send_message("You are not authorized to select this nickname.", ephemeral=True)
            return

        pressed_nickname = interaction.data["custom_id"].split("_")[1]  # Extract nickname from custom_id
        user_row[2] = pressed_nickname  # Update the "Main" column
        await update_csv_and_confirm(ctx, csv_data, dkp_database_channel, message, f"{pressed_nickname} is now the main nickname for {member.display_name}.")
        interaction_resolved = True
        await msg.delete()  # Delete the original embed
        #await interaction.response.send_message(f"{pressed_nickname} is now the main nickname for {member.display_name}.")
        view.stop()

    for button in view.children:
        button.callback = button_callback

    # Wait for the view to timeout
    await view.wait()

    # If the user does not respond
    if not interaction_resolved:
        await msg.edit(content="Timeout: No selection was made.", view=None)


# Helper function to update the CSV and send a confirmation message
async def update_csv_and_confirm(ctx, csv_data, dkp_database_channel, old_message, confirmation_msg):
    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated Nicknames.csv to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await old_message.delete()

    # Send confirmation
    await ctx.send(confirmation_msg)

#timer shit

@bot.command(name="togglewindows")
async def toggle_windows(ctx):

    result = await role_confirm_command(ctx,"togglewindows")
    if result is None:
        return

    # Find the "config.csv" message in the "dkp-database" channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")

    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv file
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Toggle the "togglewindows" setting
    updated = False
    for row in csv_data:
        if row[0] == "togglewindows":
            current_value = row[1].lower() == "true"  # Convert to boolean
            new_value = not current_value
            row[1] = "true" if new_value else "false"
            updated = True
            break

    if not updated:
        await ctx.send("The togglewindows setting was not found in the config.csv file.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Send confirmation to the channel
    await ctx.send(f"togglewindows has been set to {row[1]}")

@bot.command(name="toggletimers")
async def toggle_timers(ctx):

    result = await role_confirm_command(ctx,"toggletimers")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "config.csv" message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Toggle the "Active_timers" setting
    updated = False
    for row in csv_data:
        if row[0] == "Active_timers":
            current_value = row[1].lower() == "true"  # Convert to boolean
            new_value = not current_value
            row[1] = "true" if new_value else "false"
            updated = True
            break

    if not updated:
        await ctx.send("The Active_timers setting was not found in the config.csv file.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Create or delete the timers channel based on the updated value
    timers_channel = discord.utils.get(ctx.guild.text_channels, name="timers")
    if row[1] == "true":
        if timers_channel is None:
            # Create the Timers channel if it doesn't exist
            overwrites = {
                ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),  # Block everyone else
                ctx.guild.me: discord.PermissionOverwrite(send_messages=True)  # Allow bot to send messages
            }
            timers_channel = await ctx.guild.create_text_channel('timers', overwrites=overwrites)
            await update_timers_embed_if_active(ctx.guild)  # Send the boss list after creating the channel
        await ctx.send("Timers are now enabled and the Timers channel has been created.")
    else:
        if timers_channel:
            await timers_channel.delete()
        await ctx.send("Timers are now disabled and the Timers channel has been deleted.")

    # Send confirmation to the channel
    await ctx.send(f"Active_timers has been set to {row[1]}")

def format_time_left(seconds_left):
    if seconds_left >= 3600:  # More than 60 minutes
        hours = seconds_left // 3600
        minutes = (seconds_left % 3600) // 60
        return f"{int(hours)} hr {int(minutes)} mins"
    elif seconds_left >= 60:  # Between 1 minute and 60 minutes
        minutes = seconds_left // 60
        return f"{int(minutes)} mins"
    else:  # Less than 1 minute, show seconds
        return f"{int(seconds_left)} secs"

# Dictionary to store active timers
active_timers = {}

# Dictionary to store active boss timers and their end times
active_boss_timers = {}

cancelled_timers = {}  # To track canceled timers

# Dictionary to store tasks for active timers
active_tasks = {}

ACTIVE_TIMERS_FILENAME = "Active_Boss_Timers.csv"

# Add a role mapping for boss types to role names
role_mapping = {
    "dl": "dl",
    "edl": "edl",
    "legacy": "legacy",
    "world boss": "worldboss",
    "ring boss": "ringboss"
}

# Function to read specific boss channel toggle setting from config.csv
async def get_boss_channel_setting(guild, boss_type):
    # Find the "dkp-database" channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")

    if dkp_database_channel is None:
        return None

    # Find the config.csv file
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        return None

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        return None

    # Map the boss type to the toggle setting name
    toggle_mapping = {
        "DL": "toggle_dl",
        "EDL": "toggle_edl",
        "Legacy": "toggle_legacy",
        "World Boss": "toggle_worldboss",
        "Ring Boss": "toggle_ringboss"
    }

    toggle_setting = toggle_mapping.get(boss_type)

    # Search for the corresponding toggle setting
    for row in csv_data:
        if row[0] == toggle_setting:
            return row[1].lower() == "true"  # Return as boolean

    return None  # Default to None if not found

# Function to fetch and apply the role toggle
async def get_boss_role_setting(guild, boss_type):
    # Find the "dkp-database" channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if not dkp_database_channel:
        return None

    # Find the config.csv file
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if not message:
        return None

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if not csv_data:
        return None

    # Map the boss type to the role toggle setting name (case-insensitive)
    toggle_mapping = {
        "dl": "toggle_dl_role",
        "edl": "toggle_edl_role",
        "legacy": "toggle_legacy_role",
        "world boss": "toggle_worldboss_role",
        "ring boss": "toggle_ringboss_role"
    }

    toggle_setting = toggle_mapping.get(boss_type.lower())  # Make case insensitive

    # Search for the corresponding role toggle setting
    for row in csv_data:
        if row[0].lower() == toggle_setting:
            return row[1].lower() == "true"  # Return as boolean

    return None  # Default to None if not found


async def cancel_timer_logic(boss_name, guild):
    # Cancel the running timer task if it exists
    if boss_name in active_tasks:
        task = active_tasks[boss_name]
        task.cancel()
        await asyncio.sleep(0)

    # Remove from active timers and tasks
    active_boss_timers.pop(boss_name, None)
    active_tasks.pop(boss_name, None)

    # Update the timers embed
    await update_timers_embed_if_active(guild)

    # >>> NEW: persist to CSV
    await save_active_boss_timers(guild)

@bot.command(name="cancel")
async def cancel_timer(ctx, boss_name: str = None):
    result = await role_confirm_command(ctx, "cancel")
    if result is None:
        return

    # Handle the case where no boss name is provided
    if boss_name is None:
        if not active_boss_timers:
            await ctx.send("There are no currently running boss timers.")
            return

        # Paginate running timers
        timers_per_page = 23
        running_timers = list(active_boss_timers.keys())
        pages = [running_timers[i:i + timers_per_page] for i in range(0, len(running_timers), timers_per_page)]

        async def create_timer_page(page_index):
            embed = discord.Embed(
                title=f"Active Boss Timers (Page {page_index + 1}/{len(pages)})",
                description="Click a button to cancel a timer for a specific boss.",
                color=0x00ff00
            )
            view = discord.ui.View(timeout=120)

            # Add buttons for each timer on the page
            for boss_timer in pages[page_index]:
                button = discord.ui.Button(label=boss_timer, custom_id=f"cancel_{boss_timer}")
                view.add_item(button)

            # Add navigation buttons if there are multiple pages
            if len(pages) > 1:
                if page_index > 0:
                    view.add_item(discord.ui.Button(label="Previous", custom_id="previous_page", style=discord.ButtonStyle.secondary))
                if page_index < len(pages) - 1:
                    view.add_item(discord.ui.Button(label="Next", custom_id="next_page", style=discord.ButtonStyle.secondary))

            return embed, view

        current_page = 0
        embed, view = await create_timer_page(current_page)
        message = await ctx.send(embed=embed, view=view)

        # Interaction handler for the buttons
        @bot.event
        async def on_interaction(interaction: discord.Interaction):
            nonlocal current_page

            if not interaction.data or "custom_id" not in interaction.data:
                return

            custom_id = interaction.data["custom_id"]

            if interaction.user != ctx.author:
                await interaction.response.send_message("You cannot interact with this menu.", ephemeral=True)
                return

            await interaction.response.defer()

            if custom_id.startswith("cancel_"):
                boss_timer = custom_id[len("cancel_"):]
                if boss_timer in active_boss_timers:
                    await cancel_timer_logic(boss_timer, ctx.guild)
                    await interaction.followup.send(f"The timer for boss {boss_timer} has been canceled.")
                else:
                    await interaction.followup.send(f"The timer for boss {boss_timer} is no longer active.")

                # Update the embed if timers are still active
                if active_boss_timers:
                    embed, view = await create_timer_page(current_page)
                    await interaction.edit_original_response(embed=embed, view=view)
                else:
                    await interaction.edit_original_response(content="All timers have been canceled.", embed=None, view=None)

            elif custom_id == "previous_page":
                if current_page > 0:
                    current_page -= 1
                    embed, view = await create_timer_page(current_page)
                    await interaction.edit_original_response(embed=embed, view=view)

            elif custom_id == "next_page":
                if current_page < len(pages) - 1:
                    current_page += 1
                    embed, view = await create_timer_page(current_page)
                    await interaction.edit_original_response(embed=embed, view=view)

        return

    # Handle the case where a boss name is provided
    boss_command = f"!{boss_name.lower()}"
    if boss_command not in active_boss_timers:
        await ctx.send(f"There is no active timer for boss {boss_name.lower()}.")
        return

    await cancel_timer_logic(boss_command, ctx.guild)
    await ctx.send(f"The timer for boss {boss_name.lower()} has been canceled.")

async def handle_boss_timers(message):
    boss_name = message.content.lower()  # Convert boss name to lowercase
    boss_info = await get_boss_info_from_csv(message.guild, boss_name)

    # Check if there's already an active timer for this boss
    if boss_name in active_boss_timers:
        confirmation_message = await message.channel.send(
            f"There is already an active timer for {boss_name[1:]}, are you sure you want to overwrite the current timer?"
        )
        await confirmation_message.add_reaction('✅')  # Checkmark for confirmation
        await confirmation_message.add_reaction('❌')  # X for cancellation

        def check(reaction, user):
            return user == message.author and str(reaction.emoji) in ['✅', '❌'] and reaction.message.id == confirmation_message.id

        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)

            if str(reaction.emoji) == '✅':
                await message.channel.send(f"Overwriting the current timer for {boss_name[1:]}.")
                # Cancel the existing timer (reuse the cancel command logic)
                await cancel_timer_logic(boss_name, message.guild)
                await start_new_boss_timer(message, boss_name)
            else:
                await message.channel.send(f"Timer for {boss_name[1:]} remains unchanged.")
        except asyncio.TimeoutError:
            await message.channel.send("No reaction received, the existing timer will remain active.")
    else:
        # No active timer, just start a new one
        await start_new_boss_timer(message, boss_name)

async def start_new_boss_timer(message, boss_name):
    # Ensure the boss name starts with "!" and is in lowercase for case-insensitivity
    boss_name = f"!{boss_name.lower()}" if not boss_name.startswith("!") else boss_name.lower()

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(message.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await message.channel.send("The DKP database channel does not exist.")
        return

    # Find the Boss_Timers.csv message in the "dkp-database" channel
    csv_message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
    if csv_message is None:
        await message.channel.send("Could not find the Boss_Timers.csv file.")
        return

    # Download and parse the CSV file
    csv_file = csv_message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await message.channel.send("Could not download or parse the Boss_Timers.csv file.")
        return

    # Look for the boss_name in the CSV data
    boss_info = None
    for row in csv_data:
        # Convert CSV row[0] to lowercase for comparison
        if row[0].strip().lower() == boss_name:
            boss_info = {"timer": int(row[1]), "window": int(row[2]), "type": row[3]}
            break

    if boss_info is None:
        await message.channel.send(f"No boss timer found for {boss_name}.")
        return

    # Get the timer and window durations
    timer_duration = boss_info["timer"]
    window_duration = boss_info["window"]

    # Get the current time and calculate the end time for the timer and window
    timer_end = time.time() + timer_duration
    window_end = timer_end + window_duration

    # Add the boss to the active timers (now with channel id)
    active_boss_timers[boss_name] = {
        "timer_end": timer_end,
        "window_end": window_end,
        "channel_id": message.channel.id,
    }

    # Format the timer duration
    time_left_str = format_time_left(timer_duration)

    # Notify about the timer start
    await message.channel.send(
        f"The boss {boss_name[1:]} timer has started! You will be notified in {time_left_str}."
    )

    # Start the timer logic as a task and store it
    task = asyncio.create_task(manage_boss_timers(message.guild, message.channel, boss_name, timer_end, window_end))
    active_tasks[boss_name] = task

    # >>> NEW: persist to CSV
    await save_active_boss_timers(message.guild)

async def get_boss_info_from_csv(guild, boss_name):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        return None

    # Find the Boss_Timers.csv message in the "dkp-database" channel
    csv_message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
    if csv_message is None:
        return None

    # Download and parse the CSV file
    csv_file = csv_message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        return None

    # Search for the boss name in the CSV, case-insensitive
    for row in csv_data:
        if row[0].strip().lower() == boss_name:  # Ensure case-insensitive comparison
            return {
                "timer": int(row[1]),  # Timer duration
                "window": int(row[2]),  # Window duration
                "type": row[3]  # Boss type
            }

    return None  # Boss not found


async def manage_boss_timers(guild, channel, boss_name, timer_end, window_end):
    window_opened = False  # Track if the window has opened
    window_closed = False  # Track if the window has closed

    # Get the boss type from the CSV
    boss_info = await get_boss_info_from_csv(guild, boss_name)
    boss_type = boss_info.get("type", "").lower()  # Normalize to lowercase

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print(f"Could not find the dkp-database channel in {guild.name}")
        return

    # Find the config.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        print("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        print("Could not download or parse the config.csv file.")
        return

    # Check if the role for the boss type is toggled on
    role_toggle_setting = f"toggle_{boss_type}_role"
    role_toggled_on = False
    for row in csv_data:
        if row[0] == role_toggle_setting:
            role_toggled_on = row[1].lower() == "true"
            break

    # Look for the role in the server if the toggle is on
    boss_role = discord.utils.get(guild.roles, name=boss_type) if role_toggled_on else None
    if boss_role is None:
        print(f"Role {boss_type} not found or is toggled off.")

    # Check if the boss channel is toggled on
    channel_toggle_setting = f"toggle_{boss_type}"
    channel_toggled_on = False
    for row in csv_data:
        if row[0] == channel_toggle_setting:
            channel_toggled_on = row[1].lower() == "true"
            break

    # Get the boss-specific channel (e.g., #edl-boss-alerts)
    boss_channel_name = f"{boss_type}-boss-alerts"
    boss_channel = discord.utils.get(guild.text_channels, name=boss_channel_name) if channel_toggled_on else None

    # Use the boss-specific channel if it's toggled on and exists, otherwise fallback to the original channel
    target_channel = boss_channel if boss_channel else channel

    while True:
        current_time = time.time()

        # Handle notifications and embed updates in one loop
        if current_time < timer_end:
            await update_timers_embed_if_active(guild)

        elif timer_end <= current_time < window_end:
            if not window_opened:
                # Tag the role if it exists and is toggled on, otherwise use @everyone
                if boss_role:
                    await target_channel.send(f"{boss_role.mention} The window for boss {boss_name[1:]} has opened!")
                else:
                    await target_channel.send(f"@everyone The window for boss {boss_name[1:]} has opened!")
                window_opened = True
            await update_timers_embed_if_active(guild)

        elif current_time >= window_end and not window_closed:
            togglewindows = await get_togglewindows_setting(guild)
            if togglewindows:
                # Tag the role if it exists and is toggled on, otherwise use @everyone
                if boss_role:
                    await target_channel.send(f"{boss_role.mention} The window for boss {boss_name[1:]} has closed!")
                else:
                    await target_channel.send(f"@everyone The window for boss {boss_name[1:]} has closed!")
            window_closed = True

        if window_closed:
            break

        await asyncio.sleep(3 if (timer_end - current_time) <= 60 else 60)

    # Clear the boss from active timers and tasks once the timer logic completes
    active_boss_timers.pop(boss_name, None)
    active_tasks.pop(boss_name, None)

    await update_timers_embed_if_active(guild)

    await save_active_boss_timers(guild)


async def update_timers_embed_if_active(guild):
    # Fetch the "Active_timers" setting
    #active_timers = await get_active_timers_setting(guild)
    #if not active_timers:
    #    return  # Skip updating the embed if Active_timers is false

    # Fetch the "timers" channel
    timers_channel = discord.utils.get(guild.text_channels, name="timers")
    if timers_channel is None:
        print("Timers channel does not exist.")
        return

    # Find the existing embed message or send a new one
    async for message in timers_channel.history(limit=100):
        if message.embeds and message.embeds[0].title == "Timers":
            embed_message = message
            break
    else:
        # If no embed message is found, send a new one
        embed_message = await timers_channel.send(embed=Embed(title="Timers", description="Loading...", color=0x00ff00))

    # Create the embed and update it with the boss timers
    embed = Embed(title="Timers", description="", color=0x00ff00)

    current_time = time.time()

    def format_time_left(seconds_left):
        if seconds_left >= 3600:  # More than 60 minutes
            hours = seconds_left // 3600
            minutes = (seconds_left % 3600) // 60
            return f"{int(hours)} hr {int(minutes)} mins"
        elif seconds_left >= 60:  # Between 1 minute and 60 minutes
            minutes = seconds_left // 60
            return f"{int(minutes)} mins"
        else:  # Less than 1 minute, show seconds
            return f"{int(seconds_left)} secs"

    # Group bosses by type with default categories
    boss_types = {
        "dl": [],
        "edl": [],
        "world boss": [],
        "ring boss": [],
        "legacy": []
    }

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print("The DKP database channel does not exist.")
        return

    # Find and download the Boss_Timers.csv
    message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
    if message is None:
        print("Boss_Timers.csv not found.")
        return
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        print("Failed to download Boss_Timers.csv.")
        return

    # Loop over the bosses in the CSV file
    for row in csv_data[1:]:  # Skip the header row
        boss_name, timer_duration, window_duration, boss_type = row
        boss_name = boss_name.lower()  # Ensure boss names are lowercased
        boss_type = boss_type.lower()  # Normalize the boss type

        if boss_name in active_boss_timers:
            timer_end = active_boss_timers[boss_name]["timer_end"]
            window_end = active_boss_timers[boss_name]["window_end"]

            if current_time < timer_end:
                # Timer still active
                time_left = timer_end - current_time
                time_left_str = format_time_left(time_left)
            elif timer_end <= current_time < window_end:
                # Window open (counts up until window closes)
                time_since_open = current_time - timer_end
                time_left_str = f"Window open for {format_time_left(time_since_open)}"
            else:
                # Window closed
                time_left_str = "Window closed"
        else:
            # Timer is inactive
            time_left_str = "Timer inactive"

        # If boss type is not in the default categories, create a new category
        if boss_type not in boss_types:
            boss_types[boss_type] = []  # Create a new category for the unknown boss type

        # Add the boss to the appropriate type section in the embed
        boss_types[boss_type].append(f"{boss_name[1:]} — \u2003 {time_left_str}")  # Use em dash with spaces

    # Add sections for each boss type to the embed
    for boss_type, bosses in boss_types.items():
        if bosses:
            embed.add_field(name=boss_type.title(), value="\n".join(bosses), inline=False)

    # Only update the embed if the message has changed
    if embed_message.embeds[0].to_dict() != embed.to_dict():
        await embed_message.edit(embed=embed)


# timer role channel bullshit
@bot.command(name="toggle")
async def toggle_channel(ctx, channel_name: str):

    result = await role_confirm_command(ctx,"toggle")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Map the default channel names to their corresponding settings
    channel_mapping = {
        "dl": "toggle_dl",
        "edl": "toggle_edl",
        "legacy": "toggle_legacy",
        "worldboss": "toggle_worldboss",
        "ringboss": "toggle_ringboss"
    }

    # Check if the channel name is part of the default mappings or is a custom boss type
    toggle_setting = None
    if channel_name in channel_mapping:
        toggle_setting = channel_mapping[channel_name]
        channel_full_name = f"{channel_name}-boss-alerts"
    else:
        # Look for a custom boss type by searching config.csv
        channel_full_name = f"{channel_name.lower()}-boss-alerts"
        toggle_setting = f"toggle_{channel_name.lower()}"

        # Ensure we don't match role toggles (ignore settings ending with "_role")
        if any(row[0] == toggle_setting for row in csv_data) and not toggle_setting.endswith("_role"):
            pass  # We found the setting for the custom boss
        else:
            await ctx.send(f"No channel setting found for {channel_name}. Please check the boss type.")
            return

    # Fetch the channel (if it exists)
    target_channel = discord.utils.get(ctx.guild.text_channels, name=channel_full_name)

    # Find the corresponding setting and toggle it
    updated = False
    for row in csv_data:
        if row[0] == toggle_setting:
            if row[1].lower() == "true":
                row[1] = "false"
                updated = True
                # Delete the channel if it exists
                if target_channel:
                    await target_channel.delete()
                await ctx.send(f"{channel_full_name} has been disabled and the channel has been deleted.")
            else:
                row[1] = "true"
                updated = True
                # Create the channel if it doesn't exist
                if target_channel is None:
                    overwrites = {
                        ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),  # Block others
                        ctx.guild.me: discord.PermissionOverwrite(send_messages=True)  # Allow bot to send messages
                    }
                    await ctx.guild.create_text_channel(channel_full_name, overwrites=overwrites)
                await ctx.send(f"{channel_full_name} has been enabled and the channel has been created.")
            break

    if not updated:
        await ctx.send(f"Setting for {channel_name} not found in config.csv.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    await ctx.send(f"Config updated for {channel_full_name}.")

@bot.command(name="toggle_role")
async def toggle_role(ctx, role_name: str):

    result = await role_confirm_command(ctx,"toggle_role")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Check if role_name is a predefined one or a custom one
    predefined_roles = {"dl", "edl", "legacy", "worldboss", "ringboss"}
    toggle_setting = None
    full_role_name = role_name

    # Handle predefined roles
    if role_name.lower() in predefined_roles:
        toggle_setting = f"toggle_{role_name.lower()}_role"
    else:
        # Handle custom boss types by searching config.csv for custom role toggles
        toggle_setting = f"toggle_{role_name.lower()}_role"

        # Ensure we are only matching role-related settings (those that end with "_role")
        if not any(row[0] == toggle_setting for row in csv_data):
            await ctx.send(f"No role toggle found for {role_name}. Please check the boss type.")
            return

    # Fetch the role (if it exists)
    target_role = discord.utils.get(ctx.guild.roles, name=full_role_name)

    # Find the corresponding setting in the CSV and toggle it
    updated = False
    for row in csv_data:
        if row[0] == toggle_setting:
            if row[1].lower() == "true":
                row[1] = "false"
                updated = True
                # Delete the role if it exists
                if target_role:
                    await target_role.delete()
                await ctx.send(f"{full_role_name} role has been deleted.")
            else:
                row[1] = "true"
                updated = True
                # Create the role if it doesn't exist
                if target_role is None:
                    target_role = await ctx.guild.create_role(
                        name=full_role_name, mentionable=True  # Make the role mentionable
                    )
                await ctx.send(f"{full_role_name} role has been created and is now mentionable.")
            break

    if not updated:
        await ctx.send(f"Setting for {role_name} not found in config.csv.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    await ctx.send(f"Config updated for {full_role_name} role.")

    # Update the embed in the #get-timer-roles channel if it exists
    role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")
    if role_channel:
        await generate_role_embed(ctx.guild, role_channel, csv_data)


@bot.command(name="togglerolechannel")
async def toggle_role_channel(ctx):

    result = await role_confirm_command(ctx,"togglerolechannel")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Fetch the "get-timer-roles" channel (if it exists)
    role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")

    # Find the "toggle_role_channel" setting and toggle it
    updated = False
    for row in csv_data:
        if row[0] == "toggle_role_channel":
            if row[1].lower() == "true":
                row[1] = "false"
                updated = True
                # Delete the channel if it exists
                if role_channel:
                    await role_channel.delete()
                await ctx.send("The #get-timer-roles channel has been deleted.")
            else:
                row[1] = "true"
                updated = True
                # Create the channel if it doesn't exist
                if role_channel is None:
                    overwrites = {
                        ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),  # Block everyone else
                        ctx.guild.me: discord.PermissionOverwrite(send_messages=True) # Allow bot to send messages
                        #ctx.guild.default_role: discord.PermissionOverwrite(add_reactions=True)  # Allow reactions for role assignment
                    }
                    role_channel = await ctx.guild.create_text_channel('get-timer-roles', overwrites=overwrites)
                    await ctx.send("The #get-timer-roles channel has been created.")
                    await generate_role_embed(ctx.guild, role_channel, csv_data)
            break

    if not updated:
        await ctx.send("Setting for toggle_role_channel not found in config.csv.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

async def generate_role_embed(guild, channel, csv_data):
    if not csv_data:
        print("config_data is None or not passed correctly.")
        return
    """Generates or updates the existing embed message and manages role reaction emojis."""
    embed_title = "Role Notifications"

    # Search for the existing embed message in the channel
    async for message in channel.history(limit=100):
        if message.embeds and message.embeds[0].title == embed_title:
            role_embed_message = message
            break
    else:
        # If no embed message is found, send a new one
        embed = discord.Embed(title=embed_title, description="Select your notification preferences:", color=0x00ff00)
        role_embed_message = await channel.send(embed=embed)

    # Update the embed with active roles
    embed = discord.Embed(title=embed_title, description="Select your notification preferences:", color=0x00ff00)

    # Step 1: Get all available role emojis from config.csv (both default and custom)
    emoji_mapping = {}
    role_mapping = {}

    for row in csv_data:
        # Look for all rows that end with "_emoji" and map them
        if row[0].endswith("_emoji"):
            boss_type = row[0].replace("_emoji", "").lower()  # Extract boss type from row key
            emoji_mapping[boss_type] = row[1]  # Store the emoji

        # Also map the role descriptions based on boss type
        if row[0].endswith("_role"):
            boss_type = row[0].replace("toggle_", "").replace("_role", "").lower()
            role_mapping[boss_type] = f"{boss_type.capitalize()} Notifications"  # Capitalize the boss type for display

    # Step 2: Add fields for each role that is toggled on in the config file
    for row in csv_data:
        if row[0].startswith("toggle_") and row[0].endswith("_role") and row[1].lower() == "true":
            boss_type = row[0].replace("toggle_", "").replace("_role", "").lower()
            if boss_type in emoji_mapping:
                embed.add_field(name=f"Press {emoji_mapping[boss_type]} to turn on {role_mapping[boss_type]}", value="\u200b", inline=False)

    # Step 3: Fetch boss types from Boss_Timers.csv for custom boss types
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    boss_message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
    if boss_message is None:
        await channel.send("Could not find the Boss_Timers.csv file.")
        return

    # Download and parse the Boss_Timers.csv file
    boss_file = boss_message.attachments[0]
    boss_data = await download_csv(boss_file)
    if boss_data is None:
        await channel.send("Could not download or parse the Boss_Timers.csv file.")
        return

    # Step 4: Check for custom boss types that are toggled on but missing an emoji
    missing_emojis = set()  # Use a set to avoid duplicate boss types
    for boss_row in boss_data:
        boss_type = boss_row[3].strip().lower()  # Get the boss type from the row

        toggle_role_setting = f"toggle_{boss_type}_role"
        emoji_setting = f"{boss_type}_emoji"

        # Check if the role toggle is on and if an emoji exists for the boss type
        role_toggled_on = any(row[0] == toggle_role_setting and row[1].lower() == "true" for row in csv_data)
        emoji_exists = any(row[0] == emoji_setting for row in csv_data)

        # If the role is toggled on but there's no emoji, add to missing_emojis set
        if role_toggled_on and not emoji_exists:
            missing_emojis.add(boss_type.capitalize())

    # Step 5: If there are missing emojis, add a line to the embed
    if missing_emojis:
        missing_types_str = ", ".join(sorted(missing_emojis))  # Join and sort the set to make it a clean string
        embed.add_field(
            name="Missing Role Emojis",
            value=f"Role emoji for role(s): {missing_types_str} has not been assigned. Use !assign to assign an emoji.",
            inline=False
        )

    # Step 6: Edit the existing embed with the new fields and updates
    await role_embed_message.edit(embed=embed)

    # Step 7: Check current reactions and compare with the required ones
    current_reactions = {str(reaction.emoji): reaction for reaction in role_embed_message.reactions}
    required_emojis = {emoji for boss_type, emoji in emoji_mapping.items() if f"toggle_{boss_type}_role" in {row[0] for row in csv_data if row[1].lower() == "true"}}

    # Add missing reactions
    for emoji in required_emojis:
        if emoji not in current_reactions:
            await role_embed_message.add_reaction(emoji)

    # Remove reactions that are no longer needed
    for emoji, reaction in current_reactions.items():
        if emoji not in required_emojis:
            # Remove the reaction from all users (including bots)
            async for user in reaction.users():
                await reaction.remove(user)  # Remove reactions for both users and the bot

async def get_role_from_emoji(guild, emoji):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        return None

    # Find the config.csv message in the "dkp-database" channel
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        return None

    # Download and parse the config.csv file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)
    if config_data is None:
        return None

    # Check for default boss roles in config.csv
    for row in config_data:
        if row[0].endswith("_emoji") and row[1] == str(emoji):
            boss_type = row[0].replace("_emoji", "")
            role_name = boss_type  # Role name is based on the boss type

            # Find the corresponding role in the server
            return discord.utils.get(guild.roles, name=role_name)

    # If no matching emoji or role is found, return None
    return None

@bot.command(name="assign")
async def assign_emoji(ctx, boss_type: str, emoji: str):

    result = await role_confirm_command(ctx,"assign")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv message in the "dkp-database" channel
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)
    if config_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Step 1: Check if the emoji is already assigned to another boss type
    for row in config_data:
        if row[1] == emoji and row[0].endswith("_emoji"):
            assigned_boss_type = row[0].replace("_emoji", "").capitalize()
            await ctx.send(f"The emoji {emoji} is already assigned to {assigned_boss_type}. Please choose a different emoji.")
            return

    # Step 2: Check if the {boss_type}_emoji entry exists in the config.csv
    emoji_setting = f"{boss_type.lower()}_emoji"
    emoji_updated = False
    for row in config_data:
        if row[0] == emoji_setting:
            row[1] = emoji  # Update the emoji
            emoji_updated = True
            break

    # Step 3: If the emoji setting wasn't found, look in Boss_Timers.csv for the boss type
    if not emoji_updated:
        boss_message = await find_csv_message(dkp_database_channel, "Boss_Timers.csv")
        if boss_message is None:
            await ctx.send("Could not find the Boss_Timers.csv file.")
            return

        # Download and parse the Boss_Timers.csv file
        boss_file = boss_message.attachments[0]
        boss_data = await download_csv(boss_file)
        if boss_data is None:
            await ctx.send("Could not download or parse the Boss_Timers.csv file.")
            return

        # Check if the boss_type exists in Boss_Timers.csv
        boss_type_exists = any(row[3].strip().lower() == boss_type.lower() for row in boss_data)

        if boss_type_exists:
            # Add the new emoji setting to config.csv
            config_data.append([emoji_setting, emoji])
        else:
            await ctx.send(f"Boss type {boss_type} does not exist in the Boss_Timers.csv file.")
            return

    # Step 4: Create and upload the updated config.csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(config_data)
    output.seek(0)
    new_config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")

    # Send the updated CSV to the "dkp-database" channel
    await dkp_database_channel.send(file=new_config_file)

    # Delete the original message containing the old config.csv
    await config_message.delete()

    await ctx.send(f"Emoji for {boss_type} has been set to {emoji}.")

    # Step 5: Update the roles embed
    role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")
    if role_channel:
        await generate_role_embed(ctx.guild, role_channel, config_data)

@bot.command(name="toggledkpchannel")
async def toggledkpchannel(ctx):

    result = await role_confirm_command(ctx,"toggledkpchannel")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv message in the "dkp-database" channel
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)
    if config_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Find the current state of the dkp_vals_channel toggle
    dkp_channel_toggle = None
    for row in config_data:
        if row[0] == "dkp_vals_channel":
            dkp_channel_toggle = row
            break

    if dkp_channel_toggle is None:
        await ctx.send("No dkp_vals_channel setting found in the config file.")
        return

    # Determine whether to create or delete the channel based on the current value
    dkp_vals_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-vals")

    if dkp_channel_toggle[1].lower() == "false":
        # If channel doesn't exist, create it and update the config
        if dkp_vals_channel is None:
            overwrites = {
                ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False, read_messages=True),
                ctx.guild.me: discord.PermissionOverwrite(send_messages=True, read_messages=True)
            }
            dkp_vals_channel = await ctx.guild.create_text_channel("dkp-vals", overwrites=overwrites)
            await ctx.send("The DKP Values channel has been created.")

            # Toggle the value to "true" in the config
            dkp_channel_toggle[1] = "true"
        else:
            await ctx.send("The DKP Values channel already exists.")
    else:
        # If the channel exists, delete it and update the config
        if dkp_vals_channel:
            await dkp_vals_channel.delete()
            await ctx.send("The DKP Values channel has been deleted.")

            # Toggle the value to "false" in the config
            dkp_channel_toggle[1] = "false"

    # Create and upload the updated config.csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(config_data)
    output.seek(0)
    new_config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_config_file)
    await config_message.delete()

    # If the channel was created, call the function to send the embed
    if dkp_channel_toggle[1] == "true":
        await send_dkp_values_embed(ctx.guild)

    # Log success or error
    if dkp_channel_toggle[1] == "true":
        await ctx.send("The DKP Values channel has been successfully toggled on.")
    else:
        await ctx.send("The DKP Values channel has been successfully toggled off.")


async def send_dkp_values_embed(guild):
    print("Starting send_dkp_values_embed...")

    # Fetch the "dkp-vals" channel
    dkp_vals_channel = discord.utils.get(guild.text_channels, name="dkp-vals")
    if dkp_vals_channel is None:
        print("The DKP Values channel does not exist.")
        return  # Log and exit if the channel doesn't exist

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print("The DKP database channel does not exist.")
        return

    # Find the Boss_DKP_Values.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "Boss_DKP_Values.csv")
    if message is None:
        print("Could not find the Boss_DKP_Values.csv file.")
        return

    # Download and parse the Boss_DKP_Values.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None or len(csv_data) < 2:  # Check for valid data
        print("Could not download or parse the Boss_DKP_Values.csv file, or the CSV is empty.")
        return

    print("Successfully parsed Boss_DKP_Values.csv. Preparing embed...")

    # Delete all existing messages with embeds in the dkp-vals channel
    async for message in dkp_vals_channel.history(limit=100):
        if message.embeds:
            await message.delete()

    print("Deleted existing embed messages. Sending new embeds...")

    # Create the embed with boss DKP values
    embeds = []
    embed = discord.Embed(title="Boss DKP Values", description="Here are the current DKP values for bosses.", color=0x00ff00)

    # Loop through the CSV and add fields to the embed
    for row in csv_data[1:]:  # Skip the header
        boss_name = row[0]
        dkp_value = row[1]
        if boss_name and dkp_value:
            # Ensure the embed doesn't exceed the maximum allowed fields
            if len(embed.fields) < 25:
                embed.add_field(name=boss_name, value=f"DKP: {dkp_value}", inline=False)
            else:
                # Store the current embed and start a new one
                embeds.append(embed)
                embed = discord.Embed(title="Boss DKP Values", description="Here are more DKP values for bosses.", color=0x00ff00)
                embed.add_field(name=boss_name, value=f"DKP: {dkp_value}", inline=False)

    # Append the last embed if it has fields
    if embed.fields:
        embeds.append(embed)

    # Send the new embeds to the channel
    for new_embed in embeds:
        await dkp_vals_channel.send(embed=new_embed)

    print("Embeds sent successfully.")

@bot.command(name="setauctionduration")
async def set_auction_duration(ctx, duration: int):

    result = await role_confirm_command(ctx,"setauctionduration")
    if result is None:
        return

    # Fetch the "dkp-database" channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")

    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv file
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Update the "auction_duration" setting
    updated = False
    for row in csv_data:
        if row[0] == "auction_duration":
            row[1] = str(duration)
            updated = True
            break

    if not updated:
        await ctx.send("The auction_duration setting was not found in the config.csv file.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Send confirmation to the channel
    await ctx.send(f"Auction duration has been set to {duration} hours.")


# Store auction data globally or in a database
ongoing_auctions = {}


@bot.command(name="auction")
async def start_auction(ctx, *args):
    result = await role_confirm_command(ctx, "auction")
    if result is None:
        return

    if len(args) == 0:
        await ctx.send("Usage: !auction {item name} {minimum bid}. Minimum bid is optional and defaults to 0.")
        return

    # Determine minimum bid and item name
    minimum_bid = 0
    item_name_parts = []

    for arg in args:
        if arg.isdigit():  # Treat the first number as the minimum bid
            minimum_bid = int(arg)
            break
        item_name_parts.append(arg)

    item_name = ' '.join(item_name_parts).lower()

    # Check if an auction already exists for the item
    if item_name in (key.lower() for key in ongoing_auctions.keys()):
        await ctx.send(f"An auction for '{item_name}' is already ongoing.")
        return

    # Validate item name
    if not item_name:
        await ctx.send("Please provide a valid item name.")
        return

    # Retrieve auction duration from the config
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    config_data = await download_csv(config_message.attachments[0])
    auction_duration = next((int(row[1]) for row in config_data if row[0] == "auction_duration"), 24)

    # Create auction details
    auction_end_time = datetime.now() + timedelta(hours=auction_duration)
    ongoing_auctions[item_name] = {
        "minimum_bid": minimum_bid,
        "end_time": auction_end_time,
        "highest_bid": 0,
        "highest_bidder": None,
        "bids": {}
    }

    await ctx.send(f"Auction started for '{item_name}' with a minimum bid of {minimum_bid} DKP! Auction ends in {auction_duration} hours.")

    # Schedule auction end
    bot.loop.call_later(
        auction_duration * 3600,
        lambda: asyncio.ensure_future(end_auction(item_name, ctx.guild.id, ctx.channel.id))
    )

@bot.command(name="bid")
async def place_bid(ctx, *args):
    result = await role_confirm_command(ctx, "bid")
    if result is None:
        return

    if len(args) < 2:
        await ctx.send("Usage: !bid {item name} {value}.")
        return

    # Parse item name and bid value
    try:
        bid_value = int(args[-1])
    except ValueError:
        await ctx.send("Invalid bid. Ensure your bid is a valid number.")
        return

    item_name = ' '.join(args[:-1]).lower()

    # Match item name with ongoing auctions
    auction = ongoing_auctions.get(item_name)
    if auction is None:
        await ctx.send(f"No active auction for '{item_name}'.")
        return

    # Fetch user's DKP balances and nicknames
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    balances_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if balances_message is None:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    csv_data = await download_csv(balances_message.attachments[0])

    # Skip the header row
    csv_data = csv_data[1:]

    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    nicknames = []
    main_nickname = None

    if nickname_message:
        nickname_csv_data = await download_csv(nickname_message.attachments[0])
        for row in nickname_csv_data:
            if row[0] == ctx.author.name:
                nicknames = row[1].split(", ") if row[1] else []
                main_nickname = row[2] if len(row) > 2 else None
                break

    # Prepare balances for all accounts (including nicknames)
    balances = {}
    for row in csv_data:
        try:
            balances[row[0]] = int(row[1])
        except (ValueError, IndexError):
            continue  # Skip malformed rows

    # Determine the bidding accounts to show
    bidding_accounts = nicknames.copy()
    if main_nickname is None or main_nickname not in nicknames:
        bidding_accounts.insert(0, ctx.author.name)  # Include the base username if it's not the main nickname

    if len(bidding_accounts) > 1:
        # Prompt for account selection
        view = discord.ui.View(timeout=30)

        for account in bidding_accounts:
            button = discord.ui.Button(label=account, custom_id=f"bid_{account}")
            view.add_item(button)

        msg = await ctx.send(f"{ctx.author.mention}, select the account to bid from for '{item_name}':", view=view)

        async def button_callback(interaction):
            if interaction.user != ctx.author:
                await interaction.response.send_message("You cannot select this account.", ephemeral=True)
                return

            selected_account = interaction.data["custom_id"].split("_")[1]
            await handle_bid(ctx, auction, selected_account, bid_value, balances)
            await msg.delete()

        for button in view.children:
            button.callback = button_callback

        await view.wait()
        if not view.is_finished:
            await msg.edit(content="Timeout: No selection was made.", view=None)

    else:
        # Use the only available account
        account = bidding_accounts[0]
        await handle_bid(ctx, auction, account, bid_value, balances)

async def handle_bid(ctx, auction, account, bid_value, balances):
    current_bid = auction["highest_bid"]
    previous_bid = auction["bids"].get(account, 0)
    balance = balances.get(account, 0)

    if bid_value > balance:
        await ctx.send(f"Insufficient DKP! Your current balance for {account} is {balance}.")
        return

    if bid_value <= max(previous_bid, auction["minimum_bid"]):
        await ctx.send(f"Your bid must be higher than {max(previous_bid, auction['minimum_bid'])} DKP.")
        return

    auction["highest_bid"] = bid_value
    auction["highest_bidder"] = account
    auction["bids"][account] = bid_value

    # Use the item name directly from the auction dictionary
    for item_name, data in ongoing_auctions.items():
        if data == auction:
            break

    await ctx.send(f"{account} has placed the highest bid of {bid_value} DKP for '{item_name}'.")

async def end_auction(item_name, guild_id, channel_id):
    # Retrieve the auction from ongoing_auctions and remove it to prevent further attempts
    auction = ongoing_auctions.pop(item_name, None)
    if auction is None:
        return  # If the auction was already ended, do nothing

    guild = bot.get_guild(guild_id)
    if guild is None:
        print(f"Could not find guild with ID {guild_id}")
        return

    # Fetch the channel where the auction results will be announced
    result_channel = guild.get_channel(channel_id)
    if result_channel is None:
        print(f"Could not find channel with ID {channel_id}")
        return

    # Fetch the DKP database channel to update the winner's balance
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print("The DKP database channel does not exist.")
        return

    # Handle the case where there was a valid bidder
    if auction["highest_bidder"]:
        winner = auction["highest_bidder"]
        bid_value = auction["highest_bid"]

        # Handle user or nickname
        if isinstance(winner, str):  # Winner is a nickname
            winner_name = winner
        else:  # Winner is a Discord member object
            winner_name = winner.display_name

        # Announce the winner
        await result_channel.send(
            f"{winner_name} has won the auction for '{item_name}' with a bid of {bid_value} DKP!"
        )

        # Fetch the balances_database.csv message and update the winner's balance
        balances_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
        if balances_message is None:
            print("Could not find the Balances_Database.csv file.")
            return

        csv_file = balances_message.attachments[0]
        balances_data = await download_csv(csv_file)

        # Update the winner's balance in the CSV data
        updated = False
        for row in balances_data:
            if row[0] == winner_name:
                current_balance = int(row[1])
                new_balance = current_balance - bid_value  # Deduct the bid from the current balance
                row[1] = str(new_balance)  # Update the current balance
                updated = True
                break

        if not updated:
            await result_channel.send(
                f"Error: Could not find {winner_name}'s balance in the balances_database.csv file."
            )
            return

        # Create a new CSV file with the updated balances
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(balances_data)
        output.seek(0)

        # Upload the updated CSV file to the DKP database channel
        new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
        await dkp_database_channel.send(file=new_csv_file)

        # Delete the old CSV message
        await balances_message.delete()

        await result_channel.send(f"{winner_name}'s new balance is {new_balance} DKP.")
    else:
        # If no one bid, announce that the auction ended with no winner
        await result_channel.send(f"No bids were placed for '{item_name}'. Auction ended with no winner.")

# You would need a background task or a timer to call `end_auction` at the appropriate time

@bot.command(name="auctionend")
async def auction_end(ctx, *item_name_parts):
    result = await role_confirm_command(ctx, "auctionend")
    if result is None:
        return

    # If no item name is specified, show active auctions as buttons
    if not item_name_parts:
        if not ongoing_auctions:
            await ctx.send("There are no active auctions to end.")
            return

        view = discord.ui.View(timeout=30)
        auction_ended = False  # Track if an auction was successfully ended

        for item_name in ongoing_auctions.keys():
            button = discord.ui.Button(label=item_name, custom_id=f"end_{item_name}")
            view.add_item(button)

        msg = await ctx.send("Select an auction to end:", view=view)

        async def button_callback(interaction):
            nonlocal auction_ended  # Allow modification of the outer variable
            if interaction.user != ctx.author:
                await interaction.response.send_message(
                    "You are not authorized to end this auction.", ephemeral=True
                )
                return

            # Extract the item name from the custom_id
            selected_item = interaction.data["custom_id"].split("_", 1)[1]

            if selected_item in ongoing_auctions:
                await end_auction(selected_item, ctx.guild.id, ctx.channel.id)
                await interaction.response.edit_message(
                    content=f"The auction for '{selected_item}' has been manually ended.", view=None
                )
                auction_ended = True  # Mark that an auction was successfully ended
            else:
                await interaction.response.send_message(
                    f"The auction for '{selected_item}' no longer exists.", ephemeral=True
                )

            # Stop the view to prevent timeout logic
            view.stop()

        # Assign the callback to each button
        for button in view.children:
            button.callback = button_callback

        await view.wait()

        # Timeout logic: only trigger if no auction was successfully ended
        if not auction_ended:
            await msg.edit(content="Timeout: No auction was ended.", view=None)

        return

    # Combine the item name parts into a single string
    item_name = ' '.join(item_name_parts).lower()

    # Check if an auction with the normalized name exists
    matched_item = None
    for auction_item in ongoing_auctions.keys():
        if auction_item.lower() == item_name:
            matched_item = auction_item
            break

    if matched_item is None:
        await ctx.send(f"No active auction for '{item_name}'.")
        return

    # End the auction immediately
    await end_auction(matched_item, ctx.guild.id, ctx.channel.id)
    await ctx.send(f"The auction for '{matched_item}' has been manually ended.")

@bot.command(name="auctioncancel")
async def auction_cancel(ctx, *item_name_parts):
    result = await role_confirm_command(ctx, "auctioncancel")
    if result is None:
        return

    # If no item name is specified, show active auctions as buttons
    if not item_name_parts:
        if not ongoing_auctions:
            await ctx.send("There are no active auctions to cancel.")
            return

        view = discord.ui.View(timeout=30)
        auction_canceled = False  # Track if an auction was successfully canceled

        for item_name in ongoing_auctions.keys():
            button = discord.ui.Button(label=item_name, custom_id=f"cancel_{item_name}")
            view.add_item(button)

        msg = await ctx.send("Select an auction to cancel:", view=view)

        async def button_callback(interaction):
            nonlocal auction_canceled  # Allow modification of the outer variable
            if interaction.user != ctx.author:
                await interaction.response.send_message(
                    "You are not authorized to cancel this auction.", ephemeral=True
                )
                return

            # Extract the item name from the custom_id
            selected_item = interaction.data["custom_id"].split("_", 1)[1]

            if selected_item in ongoing_auctions:
                ongoing_auctions.pop(selected_item)  # Remove the auction
                await interaction.response.edit_message(
                    content=f"The auction for '{selected_item}' has been canceled.", view=None
                )
                auction_canceled = True  # Mark that an auction was successfully canceled
            else:
                await interaction.response.send_message(
                    f"The auction for '{selected_item}' no longer exists.", ephemeral=True
                )

            # Stop the view to prevent timeout logic
            view.stop()

        # Assign the callback to each button
        for button in view.children:
            button.callback = button_callback

        await view.wait()

        # Timeout logic: only trigger if no auction was successfully canceled
        if not auction_canceled:
            await msg.edit(content="Timeout: No auction was canceled.", view=None)

        return

    # Combine the item name parts into a single string
    item_name = ' '.join(item_name_parts).lower()

    # Check if an auction with the normalized name exists
    matched_item = None
    for auction_item in ongoing_auctions.keys():
        if auction_item.lower() == item_name:
            matched_item = auction_item
            break

    if matched_item is None:
        await ctx.send(f"No active auction for '{item_name}' to cancel.")
        return

    # Cancel the auction immediately
    ongoing_auctions.pop(matched_item)
    await ctx.send(f"The auction for '{matched_item}' has been canceled.")

#dkp decay code

# Global variable to control the timer loop
decay_active = False

@bot.command(name="setdecaypercent")
async def set_decay_percent(ctx, new_percent: int):

    result = await role_confirm_command(ctx,"setdecaypercent")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv file
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the CSV file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)

    if config_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Update the "decay_percent" setting in config.csv
    updated = False
    for row in config_data:
        if row[0] == "decay_percent":
            row[1] = str(new_percent)
            updated = True
            break

    if not updated:
        await ctx.send("The decay_percent setting was not found in the config.csv file.")
        return

    # Create a new CSV file with the updated data for config.csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(config_data)
    output.seek(0)

    # Send the updated config.csv to the DKP database channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await config_message.delete()

    # Update the decay_savestate.csv
    decay_savestate_message = await find_csv_message(dkp_database_channel, "decay_savestate.csv")
    if decay_savestate_message is None:
        await ctx.send("Could not find the decay_savestate.csv file.")
        return

    decay_file = decay_savestate_message.attachments[0]
    decay_data = await download_csv(decay_file)

    if decay_data is None or len(decay_data) < 2:  # Ensure there's valid data
        await ctx.send("Could not download or parse the decay_savestate.csv file.")
        return

    # Update the "Decay_Percent" value in decay_savestate.csv
    decay_data[1][0] = str(new_percent)

    # Create a new CSV file with the updated data for decay_savestate.csv
    output_decay = io.StringIO()
    writer_decay = csv.writer(output_decay)
    writer_decay.writerows(decay_data)
    output_decay.seek(0)

    # Send the updated decay_savestate.csv to the DKP database channel
    new_decay_csv_file = discord.File(io.BytesIO(output_decay.getvalue().encode()), filename="decay_savestate.csv")
    await dkp_database_channel.send(file=new_decay_csv_file)

    # Delete the original message containing the old decay_savestate.csv
    await decay_savestate_message.delete()

    # Send confirmation to the channel
    await ctx.send(f"The decay_percent has been updated to {new_percent}% in both config.csv and decay_savestate.csv.")

@bot.command(name="setdecaytimeframe")
async def set_decay_timeframe(ctx, new_timeframe: int):

    result = await role_confirm_command(ctx,"setdecaytimeframe")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv file
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the CSV file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)

    if config_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Update the "decay_timeframe" setting in config.csv
    updated = False
    for row in config_data:
        if row[0] == "decay_timeframe":
            row[1] = str(new_timeframe)
            updated = True
            break

    if not updated:
        await ctx.send("The decay_timeframe setting was not found in the config.csv file.")
        return

    # Create a new CSV file with the updated data for config.csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(config_data)
    output.seek(0)

    # Send the updated config.csv to the DKP database channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await config_message.delete()

    # Find and update decay_savestate.csv
    decay_savestate_message = await find_csv_message(dkp_database_channel, "decay_savestate.csv")
    if decay_savestate_message is None:
        await ctx.send("Could not find the decay_savestate.csv file.")
        return

    decay_file = decay_savestate_message.attachments[0]
    decay_data = await download_csv(decay_file)

    if decay_data is None or len(decay_data) < 2:
        await ctx.send("Could not download or parse the decay_savestate.csv file.")
        return

    # Calculate the current sum of decay_time_remaining and decay_time_left
    decay_time_remaining = int(decay_data[1][1])
    decay_time_left = int(decay_data[1][2])
    current_total_time = decay_time_remaining + decay_time_left

    # Adjust decay_time_left based on the new decay timeframe
    if new_timeframe < current_total_time:
        # Reduce decay_time_left to match the new timeframe
        decay_time_left -= (current_total_time - new_timeframe)
        decay_time_left = max(0, decay_time_left)  # Ensure it doesn't go below 0
    elif new_timeframe > current_total_time:
        # Increase decay_time_left to match the new timeframe
        decay_time_left += (new_timeframe - current_total_time)

    # Update the decay_savestate.csv with the new decay_time_left value
    decay_data[1][2] = str(decay_time_left)

    # Create a new CSV file with the updated data for decay_savestate.csv
    output_decay = io.StringIO()
    writer_decay = csv.writer(output_decay)
    writer_decay.writerows(decay_data)
    output_decay.seek(0)

    # Send the updated decay_savestate.csv to the DKP database channel
    new_decay_csv_file = discord.File(io.BytesIO(output_decay.getvalue().encode()), filename="decay_savestate.csv")
    await dkp_database_channel.send(file=new_decay_csv_file)

    # Delete the original message containing the old decay_savestate.csv
    await decay_savestate_message.delete()

    # Send confirmation to the channel
    await ctx.send(f"The decay_timeframe has been updated to {new_timeframe} and adjusted in the decay_savestate.csv file.")

@bot.command(name="toggledecay")
async def toggle_decay(ctx):

    result = await role_confirm_command(ctx,"toggledecay")
    if result is None:
        return

    global decay_active

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv file
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the CSV file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)

    if config_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Toggle the "toggle_decay" setting
    updated = False
    toggle_decay_value = None
    decay_percent = None
    decay_timeframe = None
    for row in config_data:
        if row[0] == "toggle_decay":
            current_value = row[1].lower() == "true"  # Convert to boolean
            new_value = not current_value  # Toggle the boolean
            row[1] = "true" if new_value else "false"
            toggle_decay_value = new_value
            updated = True
        elif row[0] == "decay_percent":
            decay_percent = row[1]
        elif row[0] == "decay_timeframe":
            decay_timeframe = row[1]

    if not updated:
        await ctx.send("The toggle_decay setting was not found in the config.csv file.")
        return

    # Create a new CSV file with the updated config data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(config_data)
    output.seek(0)

    # Send the updated config.csv to the DKP database channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original config.csv message
    await config_message.delete()

    # Send confirmation to the channel with the correct toggle state
    toggle_state = "true" if toggle_decay_value else "false"
    await ctx.send(f"The toggle_decay has been set to {toggle_state}.")

    # If toggle_decay is set to true, start the decay timer and update CSV
    if toggle_decay_value:
        # Check if the decay_savestate.csv already exists
        decay_savestate_message = await find_csv_message(dkp_database_channel, "decay_savestate.csv")
        if decay_savestate_message is not None:
            await ctx.send("The decay_savestate.csv file already exists.")
        else:
            # Create the decay_savestate.csv content
            decay_savestate_content = [
                ["decay_Percent", "decay_time_remaining", "decay_time_left"],
                [decay_percent, "0", decay_timeframe]  # Set time_left as decay_timeframe
            ]

            # Create the CSV file for decay_savestate.csv
            output_savestate = io.StringIO()
            writer_savestate = csv.writer(output_savestate)
            writer_savestate.writerows(decay_savestate_content)
            output_savestate.seek(0)

            # Upload the decay_savestate.csv to the DKP database channel
            new_savestate_file = discord.File(io.BytesIO(output_savestate.getvalue().encode()), filename="decay_savestate.csv")
            await dkp_database_channel.send(file=new_savestate_file)

            await ctx.send(f"decay_savestate.csv has been created with decay_Percent: {decay_percent} and decay_time_left: {decay_timeframe}.")

        # Start the 24-hour decay timer loop
        decay_active = True
        await decay_timer(ctx, dkp_database_channel)
    else:
        # Stop the decay timer if toggled to false
        decay_active = False
        await ctx.send("decay timer has been stopped.")

# Function to handle the 24-hour decay timer
async def decay_timer(ctx, dkp_database_channel):
    global decay_active

    while decay_active:
        # Wait for 24 hours (86400 seconds)
        await asyncio.sleep(86400)

        # Check if the decay is still active
        if not decay_active:
            break

        # Find and download decay_savestate.csv
        decay_savestate_message = await find_csv_message(dkp_database_channel, "decay_savestate.csv")
        if decay_savestate_message is None:
            await ctx.send("Could not find the decay_savestate.csv file.")
            return

        csv_file = decay_savestate_message.attachments[0]
        csv_data = await download_csv(csv_file)

        # Parse and update decay_savestate.csv values
        if len(csv_data) > 1:
            decay_time_left = int(csv_data[1][2])
            decay_time_remaining = int(csv_data[1][1])

            # If time left is greater than 0, decay and increment values
            if decay_time_left > 0:
                decay_time_left -= 1
                decay_time_remaining += 1

                # Update the CSV data
                csv_data[1][2] = str(decay_time_left)
                csv_data[1][1] = str(decay_time_remaining)

                # Create a new CSV file with the updated data
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerows(csv_data)
                output.seek(0)

                # Send the updated decay_savestate.csv to the DKP database channel
                new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="decay_savestate.csv")
                await dkp_database_channel.send(file=new_csv_file)

                # Delete the original CSV message
                await decay_savestate_message.delete()

                #await dkp_database_channel.send(f"Updated decay_savestate.csv: {decay_time_left} hours left, {decay_time_remaining} hours passed.")
            else:
                # Time has reached zero, now apply DKP decay
                # Find and download Balances_Database.csv
                balances_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
                if balances_message is None:
                    await dkp_database_channel.send("Could not find the Balances_Database.csv file.")
                    return

                balances_file = balances_message.attachments[0]
                balances_data = await download_csv(balances_file)

                # Get the decay_percent from config.csv
                config_message = await find_csv_message(dkp_database_channel, "config.csv")
                config_data = await download_csv(config_message.attachments[0])
                decay_percent = 0
                for row in config_data:
                    if row[0] == "decay_percent":
                        decay_percent = int(row[1])
                        break

                # Apply the DKP decay to all users
                for i in range(1, len(balances_data)):  # Skip the header row
                    username = balances_data[i][0]
                    current_balance = int(balances_data[i][1])
                    decay_amount = math.ceil((decay_percent / 100) * current_balance)  # Calculate the decay, round up
                    new_balance = current_balance - decay_amount if current_balance - decay_amount > 0 else 0  # Ensure DKP doesn't go negative
                    balances_data[i][1] = str(new_balance)  # Update the current balance

                # Create a new Balances_Database.csv file with updated values
                output_balances = io.StringIO()
                writer_balances = csv.writer(output_balances)
                writer_balances.writerows(balances_data)
                output_balances.seek(0)

                # Send the updated Balances_Database.csv to the DKP database channel
                new_balances_file = discord.File(io.BytesIO(output_balances.getvalue().encode()), filename="Balances_Database.csv")
                await dkp_database_channel.send(file=new_balances_file)

                # Delete the original Balances_Database.csv message
                await balances_message.delete()

                # Reset the timer in decay_savestate.csv
                csv_data[1][2] = csv_data[1][1]  # Reset time left to the original timeframe
                csv_data[1][1] = "0"  # Reset time remaining

                # Create a new decay_savestate.csv file with reset values
                output_reset = io.StringIO()
                writer_reset = csv.writer(output_reset)
                writer_reset.writerows(csv_data)
                output_reset.seek(0)

                # Send the updated decay_savestate.csv to the DKP database channel
                new_csv_file_reset = discord.File(io.BytesIO(output_reset.getvalue().encode()), filename="decay_savestate.csv")
                await dkp_database_channel.send(file=new_csv_file_reset)

                # Delete the original decay_savestate.csv message
                await decay_savestate_message.delete()

                # Send a message to notify users about the decay and reset
                await dkp_database_channel.send(f"DKP has decayed by {decay_percent}%, restarting countdown to next decay.")

                # Reset decay time_left to original timeframe and restart countdown
                decay_time_left = int(csv_data[1][2])
                decay_time_remaining = 0
        else:
            await ctx.send("Invalid data in decay_savestate.csv.")
            return

@bot.command(name="restorefromconfig")
async def restore_from_config(ctx):

    result = await role_confirm_command(ctx,"restorefromconfig")
    if result is None:
        return

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the config.csv message in the "dkp-database" channel
    message = await find_csv_message(dkp_database_channel, "config.csv")
    if message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the config.csv file.")
        return

    # Role mapping and logic to ensure roles exist
    role_mapping = {
        "dl": "toggle_dl_role",
        "edl": "toggle_edl_role",
        "legacy": "toggle_legacy_role",
        "worldboss": "toggle_worldboss_role",
        "ringboss": "toggle_ringboss_role"
    }

    # Channel mapping
    channel_mapping = {
        "dl": "toggle_dl",
        "edl": "toggle_edl",
        "legacy": "toggle_legacy",
        "worldboss": "toggle_worldboss",
        "ringboss": "toggle_ringboss"
    }

    # Iterate through each role setting and ensure roles are created/restored
    for role_name, toggle_setting in role_mapping.items():
        full_role_name = role_name

        # Find the corresponding setting in the config
        for row in csv_data:
            if row[0] == toggle_setting:
                if row[1].lower() == "true":
                    # Check if the role exists
                    target_role = discord.utils.get(ctx.guild.roles, name=full_role_name)
                    if target_role is None:
                        # Create the role if it doesn't exist
                        target_role = await ctx.guild.create_role(
                            name=full_role_name, mentionable=True  # Make the role mentionable
                        )
                        await ctx.send(f"{full_role_name} role has been restored and is now mentionable.")
                    else:
                        await ctx.send(f"{full_role_name} role already exists.")
                else:
                    # If the role should not exist, check if it does and delete it
                    target_role = discord.utils.get(ctx.guild.roles, name=full_role_name)
                    if target_role:
                        await target_role.delete()
                        await ctx.send(f"{full_role_name} role has been deleted as per the config.")
                break

    # Iterate through each channel setting and ensure channels are created/restored
    for channel_name, toggle_setting in channel_mapping.items():
        channel_full_name = f"{channel_name}-boss-alerts"

        # Find the corresponding setting in the config
        for row in csv_data:
            if row[0] == toggle_setting:
                if row[1].lower() == "true":
                    # Check if the channel exists
                    target_channel = discord.utils.get(ctx.guild.text_channels, name=channel_full_name)
                    if target_channel is None:
                        # Create the channel if it doesn't exist
                        overwrites = {
                            ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),  # Block everyone else
                            ctx.guild.me: discord.PermissionOverwrite(send_messages=True)  # Allow bot to send messages
                        }
                        await ctx.guild.create_text_channel(channel_full_name, overwrites=overwrites)
                        await ctx.send(f"{channel_full_name} has been created as per the config.")
                    else:
                        await ctx.send(f"{channel_full_name} already exists.")
                else:
                    # If the channel should not exist, check if it does and delete it
                    target_channel = discord.utils.get(ctx.guild.text_channels, name=channel_full_name)
                    if target_channel:
                        await target_channel.delete()
                        await ctx.send(f"{channel_full_name} has been deleted as per the config.")
                break

    # Restore role channel (get-timer-roles)
    for row in csv_data:
        if row[0] == "toggle_role_channel":
            role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")
            if row[1].lower() == "true":
                if role_channel is None:
                    # Create the get-timer-roles channel if it doesn't exist
                    overwrites = {
                        ctx.guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False),
                        ctx.guild.me: discord.PermissionOverwrite(send_messages=True),
                        ctx.guild.default_role: discord.PermissionOverwrite(add_reactions=True),
                    }
                    role_channel = await ctx.guild.create_text_channel('get-timer-roles', overwrites=overwrites)
                    await ctx.send("The #get-timer-roles channel has been restored.")
                    await generate_role_embed(ctx.guild, role_channel, csv_data)
                else:
                    await ctx.send("The #get-timer-roles channel already exists.")
            else:
                if role_channel:
                    await role_channel.delete()
                    await ctx.send("The #get-timer-roles channel has been deleted as per the config.")
            break

    ### Restore timers channel ###
    for row in csv_data:
        if row[0] == "Active_timers":
            timers_channel = discord.utils.get(ctx.guild.text_channels, name="timers")
            if row[1].lower() == "true":
                if timers_channel is None:
                    # Create the Timers channel if it doesn't exist
                    overwrites = {
                        ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),
                        # Block everyone else
                        ctx.guild.me: discord.PermissionOverwrite(send_messages=True)  # Allow bot to send messages
                    }
                    timers_channel = await ctx.guild.create_text_channel('timers', overwrites=overwrites)
                    await update_timers_embed_if_active(ctx.guild)  # Send the boss list after creating the channel
                await ctx.send("The timers channel has been restored and timers are now enabled.")
            else:
                if timers_channel:
                    await timers_channel.delete()
                await ctx.send("Timers are disabled and the timers channel has been deleted.")
            break

    await ctx.send("Restoration process complete.")

@bot.command(name="editcommandroles")
async def editcommandroles(ctx):
    result = await role_confirm_command(ctx, "editcommandroles")
    if result is None:
        return

    config_data, dkp_database_channel, config_message = result

    # Filter only the relevant commands
    togglable_commands = [
        row for row in config_data if row[0].startswith("role_req_")
    ]

    # Paginate the commands into chunks of 23 (leaving space for navigation buttons)
    commands_per_page = 23
    pages = [
        togglable_commands[i:i + commands_per_page]
        for i in range(0, len(togglable_commands), commands_per_page)
    ]

    # Function to create the embed and view for a specific page
    async def create_page_embed(page_index):
        page_data = pages[page_index]
        embed = discord.Embed(
            title=f"Command Role Requirements (Page {page_index + 1}/{len(pages)})",
            description="Toggle role requirements for commands.",
        )

        for row in page_data:
            command_name = row[0].replace("role_req_", "")
            embed.add_field(name=command_name, value=row[1], inline=True)

        # Create buttons for each command on the current page
        view = discord.ui.View(timeout=120)
        for row in page_data:
            command_name = row[0].replace("role_req_", "")
            button = discord.ui.Button(
                label=f"Toggle {command_name}",
                style=discord.ButtonStyle.primary,
                custom_id=row[0],
            )
            view.add_item(button)

        # Add navigation buttons
        if len(pages) > 1:
            if page_index > 0:
                view.add_item(discord.ui.Button(
                    label="Previous", style=discord.ButtonStyle.secondary, custom_id="previous_page"
                ))
            if page_index < len(pages) - 1:
                view.add_item(discord.ui.Button(
                    label="Next", style=discord.ButtonStyle.secondary, custom_id="next_page"
                ))

        return embed, view

    # Start with the first page
    current_page = 0
    embed, view = await create_page_embed(current_page)
    message = await ctx.send(embed=embed, view=view)

    # Interaction handler
    @bot.event
    async def on_interaction(interaction: discord.Interaction):
        nonlocal current_page, config_message

        # Ensure the interaction is a button press
        if interaction.data and "custom_id" in interaction.data:
            custom_id = interaction.data["custom_id"]

            # Check if the user has the "DKP Keeper" role
            if "DKP Keeper" not in [role.name for role in interaction.user.roles]:
                await interaction.response.send_message(
                    "You do not have the required role to interact with this command.", ephemeral=True
                )
                return

            # Handle command toggle
            if custom_id.startswith("role_req_"):
                # Update the config data
                for row in config_data:
                    if row[0] == custom_id:
                        row[1] = "false" if row[1].lower() == "true" else "true"
                        break

                # Save updated config.csv
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerows(config_data)
                output.seek(0)

                new_config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")

                # Delete the old config.csv message and replace it
                await config_message.delete()
                new_config_message = await dkp_database_channel.send(file=new_config_file)

                # Update the reference to the new message
                config_message = new_config_message

                # Refresh the current page
                embed, view = await create_page_embed(current_page)
                await interaction.response.edit_message(embed=embed, view=view)

            # Handle page navigation
            elif custom_id == "previous_page":
                current_page -= 1
                embed, view = await create_page_embed(current_page)
                await interaction.response.edit_message(embed=embed, view=view)

            elif custom_id == "next_page":
                current_page += 1
                embed, view = await create_page_embed(current_page)
                await interaction.response.edit_message(embed=embed, view=view)

@bot.command(name="toggles")
async def toggles(ctx):
    result = await role_confirm_command(ctx, "toggles")
    if result is None:
        return

    config_data, dkp_database_channel, config_message = result

    # Filter relevant toggleable settings
    toggleable_settings = [
        row for row in config_data if row[0].startswith("toggle")
        or row[0].startswith("messagetoggle")
        or row[0].startswith("whograntedtoggle")
        or row[0] in {"Active_timers", "dkp_vals_channel", "nick_doublecheck", "nickdelete_doublecheck"}
    ]

    # Map toggle settings to corresponding channels or roles
    channel_mapping = {
        "toggle_dl": "dl-boss-alerts",
        "toggle_edl": "edl-boss-alerts",
        "toggle_legacy": "legacy-boss-alerts",
        "toggle_ringboss": "ringboss-boss-alerts",
        "toggle_worldboss": "worldboss-boss-alerts"
    }
    role_mapping = {
        "toggle_dl_role": "dl",
        "toggle_edl_role": "edl",
        "toggle_legacy_role": "legacy",
        "toggle_ringboss_role": "ringboss",
        "toggle_worldboss_role": "worldboss"
    }
    other_channel_commands = {
        "Active_timers",
        "dkp_vals_channel",
        "toggle_role_channel"

    }

    # Paginate the settings into chunks of 23 (leaving space for navigation buttons)
    settings_per_page = 23
    pages = [
        toggleable_settings[i:i + settings_per_page]
        for i in range(0, len(toggleable_settings), settings_per_page)
    ]

    async def create_page_embed(page_index):
        page_data = pages[page_index]
        embed = discord.Embed(
            title=f"Toggle Settings (Page {page_index + 1}/{len(pages)})",
            description="Toggle settings for bot functionality.",
        )
        for row in page_data:
            setting_name = row[0]
            embed.add_field(name=setting_name, value=row[1], inline=True)

        view = discord.ui.View(timeout=120)
        for row in page_data:
            setting_name = row[0]
            button = discord.ui.Button(
                label=setting_name,
                style=discord.ButtonStyle.primary,
                custom_id=setting_name,
            )
            view.add_item(button)

        if len(pages) > 1:
            if page_index > 0:
                view.add_item(discord.ui.Button(
                    label="Previous", style=discord.ButtonStyle.secondary, custom_id="previous_page"
                ))
            if page_index < len(pages) - 1:
                view.add_item(discord.ui.Button(
                    label="Next", style=discord.ButtonStyle.secondary, custom_id="next_page"
                ))

        return embed, view

    current_page = 0
    embed, view = await create_page_embed(current_page)
    message = await ctx.send(embed=embed, view=view)

    @bot.event
    async def on_interaction(interaction: discord.Interaction):
        nonlocal current_page, config_message

        # >>> IMPORTANT: only handle clicks that belong to *this* toggles message
        if not interaction.message or interaction.message.id != message.id:
            return

        if interaction.data and "custom_id" in interaction.data:
            custom_id = interaction.data["custom_id"]
            if ctx.author.id != interaction.user.id:
                await interaction.response.send_message(
                    "Only the user who initiated the command can interact with these buttons.",
                    ephemeral=True,
                )
                return

            await interaction.response.defer()

            # Handle pagination navigation
            if custom_id == "previous_page":
                if current_page > 0:
                    current_page -= 1
                    embed, view = await create_page_embed(current_page)
                    await interaction.edit_original_response(embed=embed, view=view)
                return

            elif custom_id == "next_page":
                if current_page < len(pages) - 1:
                    current_page += 1
                    embed, view = await create_page_embed(current_page)
                    await interaction.edit_original_response(embed=embed, view=view)
                return
            # General handler for unhandled toggles
            for row in config_data:
                if row[0] == custom_id and custom_id not in channel_mapping and custom_id not in role_mapping and custom_id not in other_channel_commands:
                    current_value = row[1].lower() == "true"
                    row[1] = "false" if current_value else "true"
                    break

            # Handle boss channel toggles
            if custom_id in channel_mapping.keys():
                channel_name = channel_mapping[custom_id]
                target_channel = discord.utils.get(ctx.guild.text_channels, name=channel_name)

                for row in config_data:
                    if row[0] == custom_id:
                        current_value = row[1].lower() == "true"
                        row[1] = "false" if current_value else "true"
                        break

                if current_value:  # Currently enabled, toggle off
                    if target_channel:
                        await target_channel.delete()
                        await interaction.followup.send(f"The {channel_name} channel has been deleted.")
                else:  # Currently disabled, toggle on
                    if target_channel is None:
                        overwrites = {
                            ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),
                            ctx.guild.me: discord.PermissionOverwrite(send_messages=True)
                        }
                        await ctx.guild.create_text_channel(channel_name, overwrites=overwrites)
                        await interaction.followup.send(f"The {channel_name} channel has been created.")
            # Handle Active_timers toggle
            if custom_id == "Active_timers":
                for row in config_data:
                    if row[0] == "Active_timers":
                        current_value = row[1].lower() == "true"
                        row[1] = "false" if current_value else "true"
                        break

                timers_channel = discord.utils.get(ctx.guild.text_channels, name="timers")

                if current_value:  # Currently enabled, toggle off
                    if timers_channel:
                        await timers_channel.delete()
                        await interaction.followup.send("Timers are now disabled and the Timers channel has been deleted.")
                else:  # Currently disabled, toggle on
                    if timers_channel is None:
                        overwrites = {
                            ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),
                            ctx.guild.me: discord.PermissionOverwrite(send_messages=True)
                        }
                        timers_channel = await ctx.guild.create_text_channel("timers", overwrites=overwrites)
                        await update_timers_embed_if_active(ctx.guild)  # Send the timers embed
                        await interaction.followup.send("Timers are now enabled and the Timers channel has been created.")

            # Handle role toggles
            if custom_id in role_mapping.keys():
                for row in config_data:
                    if row[0] == custom_id:
                        current_value = row[1].lower() == "true"
                        row[1] = "false" if current_value else "true"
                        break

                role_name = role_mapping[custom_id]
                target_role = discord.utils.get(ctx.guild.roles, name=role_name)

                if current_value:  # Currently enabled, toggle off
                    if target_role:
                        await target_role.delete()
                        await interaction.followup.send(f"The {role_name} role has been deleted.")
                else:  # Currently disabled, toggle on
                    if target_role is None:
                        await ctx.guild.create_role(name=role_name, mentionable=True)
                        await interaction.followup.send(
                            f"The {role_name} role has been created and is now mentionable.")

                # Update the embed in the #get-timer-roles channel
                role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")
                if role_channel:
                    await generate_role_embed(ctx.guild, role_channel, config_data)

            # Handle toggle_role_channel
            if custom_id == "toggle_role_channel":
                for row in config_data:
                    if row[0] == "toggle_role_channel":
                        current_value = row[1].lower() == "true"
                        row[1] = "false" if current_value else "true"
                        break

                role_channel = discord.utils.get(ctx.guild.text_channels, name="get-timer-roles")

                if current_value:  # Currently enabled, toggle off
                    if role_channel:
                        await role_channel.delete()
                else:  # Currently disabled, toggle on
                    if role_channel is None:
                        overwrites = {
                            ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False),
                            ctx.guild.me: discord.PermissionOverwrite(send_messages=True)
                        }
                        role_channel = await ctx.guild.create_text_channel('get-timer-roles',
                                                                                   overwrites=overwrites)
                        await generate_role_embed(ctx.guild, role_channel, config_data)

            # Handle dkp_vals_channel toggle
            if custom_id == "dkp_vals_channel":
                for row in config_data:
                    if row[0] == "dkp_vals_channel":
                        current_value = row[1].lower() == "true"
                        row[1] = "false" if current_value else "true"
                        break

                dkp_vals_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-vals")

                if current_value:  # Currently enabled, toggle off
                    if dkp_vals_channel:
                        await dkp_vals_channel.delete()
                        await interaction.followup.send("The DKP Values channel has been deleted.")
                else:  # Currently disabled, toggle on
                    if dkp_vals_channel is None:
                        overwrites = {
                            ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False, read_messages=True),
                            ctx.guild.me: discord.PermissionOverwrite(send_messages=True, read_messages=True)
                        }
                        dkp_vals_channel = await ctx.guild.create_text_channel("dkp-vals", overwrites=overwrites)
                        await interaction.followup.send("The DKP Values channel has been created.")
                        await send_dkp_values_embed(ctx.guild)

        # Save updated config.csv only if not navigating pages
            if custom_id not in {"previous_page", "next_page"}:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerows(config_data)
                output.seek(0)

                new_config_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="config.csv")
                await config_message.delete()
                new_config_message = await dkp_database_channel.send(file=new_config_file)
                config_message = new_config_message

                # Update the current page embed to reflect changes
                embed, view = await create_page_embed(current_page)
                await interaction.edit_original_response(embed=embed, view=view)


@bot.command(name="removemain")
async def remove_main(ctx, member: discord.Member = None):
    result = await role_confirm_command(ctx, "removemain")
    if result is None:
        return

    # Use the invoking user's name if no member is specified
    target_user = member.name if member else ctx.author.name

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Find the "Nicknames.csv" message in the "dkp-database" channel
    nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
    if nickname_message is None:
        await ctx.send("Could not find the Nicknames.csv file.")
        return

    # Download and parse the Nicknames.csv file
    nickname_csv_file = nickname_message.attachments[0]
    nickname_csv_data = await download_csv(nickname_csv_file)

    if nickname_csv_data is None:
        await ctx.send("Could not download or parse the Nicknames.csv file.")
        return

    # Modify the data to remove the main nickname
    updated = False
    for row in nickname_csv_data:
        if row[0] == target_user:
            if len(row) > 2 and row[2]:  # Check if a main is assigned
                row[2] = ""  # Clear the main nickname field
                updated = True
            break

    if not updated:
        await ctx.send(f"No main nickname is assigned to {target_user}.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(nickname_csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await nickname_message.delete()

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"The main nickname for {target_user} has been removed.")

@remove_main.error
async def remove_main_error(ctx, error):
    if isinstance(error, commands.MemberNotFound):
        await ctx.send("The specified member could not be found. Please check the name and try again.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: `!removemain` or `!removemain @user`. Specify a user only if not removing your own main nickname.")

help_dict = {
    'k': """!k

Usage: !k{boss lvl or name}/{boss stars}
Use !k (example: !k155/4 to report the killing of the 4* lvl 155 DL boss) as a method to report boss attendance.
Whenever the command is used, the bot will react to the message with a crossed swords emoji (⚔️). Pressing this crossed swords emoji will give you DKP for attending that kill. It will also send those who have reacted to the kill command to the #dkp-keeping-log channel.
An example of the log message would be: notbetaorbiter has attended !k155/5 and earned 2 DKP.
When a reaction has been removed, the bot automatically removes the DKP from the player and logs this in the log channel.
An example of the DKP removal message: notbetaorbiter has revoked attendance to !k155/4 and lost 1 DKP.
By default this command does not require any roles to use, but this can be changed via the !editcommandroles command.""",
    'a': """!a
Usage: !a{boss lvl or name}/{boss stars} user1 user2 user3
Use !a for another method of DKP Keeping. An example would be: "!a155/4 @ notbetaorbiter", which the bot will then reply to with "1 DKP added for attending 155/4 to: notbetaorbiter". You can use as many members after the boss name as were present at the boss to reduce effort.
Use of nicknames works as follows: !a155/4 nickname. Nicknames must be set using the !nick command.
Both nicknames and @user can be used to track attendance.
See !nick for more information.
By default this command requires the DKP Keeper role to use.""",
    'nick': """Usage: !nick @user nickname
Use !nick to create nicknames for users in the server. This allows you to track attendance more easily without needing to memorize their @user. A good practice is to !nick every person in the server with their in-game name for easier tracking.
Multiple nicknames can be added to a single user, this allows for handling if they have multiple accounts as each of the nicknames has a separate dkp pool.To remove a nickname see !nickdelete help page.
You can set a "main" nickname with the !setmain command. This is highly recommended if the user only has 1 account.
The nicknames work for the following commands:
!a
!dkpadd
!dkpsubtract
!dkpsubtractboth
!dkpaddcurrent
!dkpsubtractlifetime
!bal
And is used in many other circumstances like auctions and !k. For even more info see !setmain.
By default this command requires the DKP Keeper role to use.""",
    'dkpadd': """Usage: !dkpadd # user1 user2 user3 etc

DKP Keepers can use this command to manually add DKP to either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct DKP values or grant awards of DKP. This will increase both current DKP and lifetime DKP.
Nicknames can be used for this command. See !nick for more information.
By default this command requires the DKP Keeper role to use.""",
    'dkpsubtract': """Usage: !dkpsubtract # user1 user2 user3 etc

DKP Keepers can use this command to manually remove DKP from either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct DKP values or take away DKP due to winning an item, etc. This will reduce current DKP and have no effect on lifetime DKP. To change both, see !dkpsubtractboth.
Nicknames can be used for this command. See !nick for more information.
By default this command requires the DKP Keeper role to use.""",

    'dkpsubtractboth': """Usage: !dkpsubtractboth # user1 user2 user3 etc

DKP Keepers can use this command to manually remove DKP from either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct DKP values. This will reduce both current DKP and lifetime DKP.
Nicknames can be used for this command. See !nick for more information.
By default this command requires the DKP Keeper role to use.""",
    'setdkp': """Usage: !setdkp {boss lvl or name}/{boss stars} #
Example: !setdkp 155/4 10

This command is used to change the DKP values for each boss from the defaults stored in the Boss_DKP_Values.csv located in the #dkp-database text channel. 
This will be saved across sessions; restarting the bot will not reset these values as they are stored in Boss_DKP_Values.csv.
By default this command requires the DKP Keeper role to use.""",

    'bal': """Usages:
!bal → returns current and lifetime DKP of the user who sent the message
!bal user1 → returns current and lifetime DKP of the tagged user or nickname used as user1

Example: !bal
Bot replies: notbetaorbiter has 10 current DKP and 10 lifetime DKP.

Nicknames can be used for this command. See !nick for more information.""",

    'togglewindows': """Toggle windows is on by default. When true, the bot timers will notify the server when the boss spawn window opens and closes. When false, the bot will notify only when the spawn window opens.
This command toggles the togglewindows value between true and false in the config.csv located in the #dkp-database text channel.
By default this command requires the DKP Keeper role to use.""",

    'toggletimers': """Toggle timers is off by default. When true, the #timers channel will be generated, and the timers embed will be sent. When false, the #timers channel will be deleted, and the message removed.
The #timers channel cannot have messages sent in it except by administrators and the bot itself. To make use of the timers embed, see !{boss} commands.
By default this command requires the DKP Keeper role to use.""",

    '{boss}': """Usage: !{boss} or for example !155

If the same command is used while that timer is already running, the user will be prompted to ask if they want to override the current running timer.
These commands are used to start timers for all boss types. Supported bosses include:
!155, !160, !165, !170, !180, !185, !190, !195, !200, !205, !210, !215
!prot, !necro, !mord, !dino, !hrung, !aggy, !gele
!lich, !reaver
!erb, !nrb, !mrb, !srb

To cancel timers, see !cancel. 
By default, the timers will @ everyone on window open and closed. Use !togglewindows to change this to only @ everyone on window open.
This can also be changed from @ everyone to only tag those with the boss type role using the !toggle_role command.
For help assigning roles, see the help page for the !togglerolechannel command.""",
    'timers': """Usage: !{boss} or for example !155

If the same command is used while that timer is already running, the user will be prompted to ask if they want to override the current running timer.
These commands are used to start timers for all boss types. Supported bosses include:
!155, !160, !165, !170, !180, !185, !190, !195, !200, !205, !210, !215
!prot, !necro, !mord, !dino, !hrung, !aggy, !gele
!lich, !reaver
!erb, !nrb, !mrb, !srb

To cancel timers, see !cancel. 
By default, the timers will @ everyone on window open and closed. Use !togglewindows to change this to only @ everyone on window open.
This can also be changed from @ everyone to only tag those with the boss type role using the !toggle_role command.
For help assigning roles, see the help page for the !togglerolechannel command.""",
    'cancel': """Usage: !cancel {boss} or !cancel
For example: !cancel 155
Doing !cancel without specifying a {boss} will allow for the user to see all currently running timers to choose from. Pressing their corresponding button will cancel their timer.
If there is a currently running timer for a boss, using !cancel {boss} will cancel the timer for said boss. To see a list of supported timers, see the help entry for !{boss}.""",

    'toggle': """Usage: !toggle {boss type}

Where boss types are: dl, edl, ringboss, worldboss, legacy

By default, all boss type text channels are off. 
This command toggles on and off boss type specific text channels for timer alerts. 
For example, using !toggle dl will create a text channel called #dl-boss-alerts. Alerts for DL timers will then be sent in that channel.
By default this command requires the DKP Keeper role to use.""",

    'toggle_role': """Usage: !toggle_role {boss type}

Where default boss types are: dl, edl, ringboss, worldboss, legacy. Custom boss types can be added via !timeradd.

By toggling on, users with these roles are the only ones notified when a timer for a boss of that type goes off instead of the default @ everyone.
For example, when on, the !155 timer command will result in: @dl The window for boss 155 has opened!
By default, all boss type roles are off.
To help assign users these roles, see the help page for the !togglerolechannel.
By default this command requires the DKP Keeper role to use.""",

    'restorefromconfig': """THIS COMMAND IS ONLY PARTIALLY COMPLETE AFTER UPDATE. This command is used to bring configuration choices across servers. 
To use: find and download config.csv file from the #dkp-database text channel of the server whose configuration you want to import.
Delete the current config.csv file in the server you are importing new settings to (if you do not have one in the #dkp-database channel, skip this).
Send the config.csv of the server whose configuration you want to import into the #dkp-database text channel after adding the bot to the server.
Then send !restorefromconfig.
By default this command requires the DKP Keeper role to use.""",

    'createbackup': """This command creates a timestamped backup of the current DKP values of everyone in the server. 
See help page on !restorefrombackup for more information about using these backups.
By default this command requires the DKP Keeper role to use.""",

    'restorefrombackup': """This command can only be used after the !createbackup command has been run and there is at least one backup file. 
This command can also be used to move DKP values across servers via a downloaded backup of the Balances_Database.csv.
If there is more than one backup file, you will be given the choice of which one to restore from. 
This command includes a check to ensure that you mean to override the data in the Balances_Database.csv, as they cannot be recovered after the restore.
By default this command requires the DKP Keeper role to use.""",
    'togglerolechannel': """This command toggles on and off a channel that allows a user to assign boss timer roles to themselves via clicking on a reaction. If you do not see any roles active you must turn them on via the !toggle_role command. Read about the help page for that command for more information on it.

This channel automatically has an embed within that updates on changes to the active boss timer roles. This approach is suggested if you are using roles to notify users to boss spawns. See !assign if you are adding custom boss types.

By default this command requires the DKP Keeper role to use.""",
    'toggledecay': """Usage: !toggledecay
The goal of DKP decay is to incentivize players to remain active as every set amount of time (default of 30 days) their dkp is reduced by a set %  (default 4%).

The value for the number of days until the DKP decays can be changed by using the !setdecaytimeframe command.

The value of the % of DKP that is decayed can be changed by using the !setdecaypercent command.

This feature is off by default.

This feature applies to all users and only current DKP and has no effect on lifetime DKP.

By default this command requires the DKP Keeper role to use.""",
    'setdecaypercent': """Usage: !setdecaypercent {integer}

For example !setdecaypercent 5

This command sets the % at which the DKP of all users decays over time. For more information read the help page for !toggledecay.

The default value for this setting is 4%.

By default this command requires the DKP Keeper role to use.""",
    'setdecaytimeframe': """Usage: !setdecaytimeframe {integer}

For example !setdecaytimeframe 5

This command sets the number of days it takes for the DKP of all users to decay. For more information read the help page for !toggledecay.

The default value for this setting is 30 days.

By default this command requires the DKP Keeper role to use.""",
    'auction': """Usage: !auction {item name} {minimum bid}

For example: !auction rare totem of earth 69

Or: !auction godly sub

If you do not populate the {minimum bid} field it will assume that the minimum bid is 0 DKP.

The default length of an auction is 24 hours but can be changed with !setauctionduration.

Users can bid with the !bid command

You can end any auction prematurely by using the !auctionend command.

You can cancel any auction by using the !auctioncancel command.

Note: the item name can have multiple words as long as it does not contain numbers.

By default this command requires the DKP Keeper role to use.""",
    'bid': """Usage: !bid {item name} {amount}

For example: !bid rare totem of earth 420

This can be used to bid on any auction as long as the user has enough DKP and it is both over the minimum bid and over their previous bids.""",
    'auctionend': """Usage: !auctionend {item name}

Ends an auction prematurely. This is not the same as canceling an auction. See !auctioncancel.

Whoever is highest bidding when this command is used will win that auction.

By default this command requires the DKP Keeper role to use.""",
    'auctioncancel': """Usage: !auctioncancel {item name}

Cancels an auction. This is not the same as Ending an auction. See !auctionend.

This will stop an auction entirely with no winners possible.

By default this command requires the DKP Keeper role to use.""",
    'setauctionduration': """Usage: !setauctionduration {integer}

Command to set the number of hours an auction lasts for. By default the length of auctions is 24 hours.

By default this command requires the DKP Keeper role to use.""",
    'toggledkpchannel': """This command toggles on or off a channel that is used to display the DKP values for all bosses that give DKP.

The default for this toggle is false.

This command requires DKP Keeper role to use.""",
    'bossadd': """Usage: !bossadd {boss} {DKP values}

For example: !bossadd 155/4 69

Adds a boss to the list of bosses that give DKP. Therefore it can be used with the !a and !k commands which are designed to track attends.

This can be used to add custom bosses that do not give DKP by default.

By default this command requires the DKP Keeper role to use.""",
    'bossdelete': """Usage: !bossdelete {boss}

For example: !bossdelete 155/4

Removes a boss from the list of bosses that give DKP.

By default this command requires the DKP Keeper role to use.""",
    'nickdelete': """Usage: !nickdelete @{user} {nickname}

For example: !nickdelete @notbetaorbiter notbeta

Removes a nickname from a user.

By default this command requires the DKP Keeper role to use.""",
    'timeradd': """Usage: !timeradd {boss} {respawn time} {window time} {boss type}

For example: !timeradd 155 60000 180 DL

Supports custom boss types outside of just the standard CH ones, can be populated by any word.

Timer for the new boss can then be triggered by doing !{boss} and is displayed in the #timers channel which is toggled via the !toggletimers command. 

By default this command requires the DKP Keeper role to use.""",
    'timerdelete': """Usage: !timerdelete {boss}

For example: !timerdelete 155

Removes a timer from the list of timers. Used to remove custom bosses that you may be timing currently but will not use in the future. 

By default this command requires the DKP Keeper role to use.""",
    'timeredit': """Usage: !timeredit {boss} {respawn time} {window time} {boss type}

For example: !timeredit 155 60000 180 DL

Note: all time values are in seconds.

This command is used to edit existing timers to correct errors or cope with game updates.

By default this command requires the DKP Keeper role to use.""",
    'assign': """Usage: !assign {boss type} 😀

Example: !assign dl 💀

The assign command is used to assign an emoji to a taggable role. This is to be used in conjunction with the role channel added by !togglerolechannel. Note: when assigning an emoji to a boss type you must toggle the role on first for that boss type via the !toggle_role command. This command works with custom boss types added through the !timeradd command.

By default this command requires the DKP Keeper role to use.""",
    'editcommandroles': """Usage: !editcommandroles
This command allows for any command to be toggled between needing the DKP Keeper role or not. True means the command requires the DKP Keeper role, False means the command can be used by anyone.

By default this command requires the DKP Keeper role to use.""",
    'dkpsubtractlifetime': """Usage: !dkpsubtractlifetime <dkp_value> <user/nickname> [additional users/nicknames]
Example: !dkpsubtractlifetime 5 pie @jeff (note multiple users are supported but not required)
DKP Keepers can use this command to manually remove lifetime DKP from either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct / remove lifetime DKP. This will reduce lifetime DKP and have no effect on current DKP. To change both, see !dkpsubtractboth.
Nicknames can be used for this command. See !nick for more information.
By default this command requires the DKP Keeper role to use.""",
    'dkpaddcurrent': """Usage: !dkpaddcurrent <dkp_value> <user/nickname> [additional users/nicknames]

Example: !dkpadcurrent 10 pie jeff @eggroll (note multiple users are supported but not required)
DKP Keepers can use this command to manually add only current DKP to either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct DKP values or grant awards of DKP. This will increase ONLY current DKP and have no effect on lifetime DKP.
Nicknames can be used for this command. See !nick for more information.
By default this command requires the DKP Keeper role to use.""",
    'removemain': """Usage: !removemain @user, !removemain

Example: !removemain unassigns their current main nickname, !removemain @user removes that user's current main nickname
'Main' nicknames are used if a server owner prefers to keep all dkp earned across accounts saved as the person instead of each account having its own individual DKP tally.
Mains allow for easier refrencing of a user without the need to tag their @user account each time.
NOTE: this command does not delete that account, just unassigns it as their "main". To delete a nickname use !nickdelete
By default this command requires the DKP Keeper role to use.""",
    'setmain': """Usage: !setmain @user {nickname}, !setmain @user, !setmain {nickname}, !setmain

Examples: !setmain will give a list of current usernames to pick from, which is similar to the usage of !setmain @user, which will show the list of that @user's nicknames to pick from.
'Main' nicknames are used if a server owner prefers to keep all dkp earned across accounts saved as the person instead of each account having its own individual DKP tally.
Mains allow for easier refrencing of a user without the need to tag their @user account each time.
To remove the main of a user see the help page for !removemain.
By default this command requires the DKP Keeper role to use.""",
    'toggles': """Usage: !toggles

This command allows for the user to edit all toggles. This is one of the most important commands as it allows the user to quickly edit all toggleable settings for bot function.
By default this command requires the DKP Keeper role to use.""",
    'tutorials': """
    Video tutorials / info to  help server owners configure the bot
https://youtube.com/playlist?list=PL1_mX4n1t8JcdqhVZRLtA93bPlODE_NIZ&si=2PhZ87mffAjihWK6""",
}

@bot.command(name="help")
async def help_command(ctx, command: str = None):
    # Determine if the command is invoked in a DM or a guild
    is_dm = ctx.guild is None

    # Role check is skipped in DMs since there's no guild context
    if not is_dm:
        result = await role_confirm_command(ctx, "help")
        if result is None:
            return

    # Fetch toggle_public_help_messages from the config (only for guilds)
    toggle_public_help_messages = False
    if not is_dm:
        dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
        if dkp_database_channel is None:
            await ctx.send("The DKP database channel does not exist.")
            return

        config_message = await find_csv_message(dkp_database_channel, "config.csv")
        if config_message is None:
            await ctx.send("Could not find the config.csv file.")
            return

        config_file = config_message.attachments[0]
        config_data = await download_csv(config_file)

        if config_data is None:
            await ctx.send("Could not download or parse the config.csv file.")
            return

        toggle_public_help_messages = next(
            (row[1] for row in config_data if row[0] == "toggle_public_help_messages"), "false"
        ).lower() == "true"

    # If a specific command is provided, display its help
    if command:
        response = help_dict.get(command, "Command not found. Please use `!help` for a list of commands.")
        if is_dm or not toggle_public_help_messages:
            await ctx.send(response, ephemeral=True)
        else:
            await ctx.send(response)
        return

    # Generate the paginated help menu
    commands = list(help_dict.keys())
    buttons_per_page = 23  # Max buttons per page, leaving room for navigation buttons
    pages = [commands[i:i + buttons_per_page] for i in range(0, len(commands), buttons_per_page)]

    async def create_help_page(page_index):
        embed = discord.Embed(
            title=f"Help Menu (Page {page_index + 1}/{len(pages)})",
            description="Click a button to view details about a command.",
        )
        page_commands = pages[page_index]

        embed.add_field(
            name="Commands",
            value="\n".join(page_commands),
            inline=False,
        )

        view = discord.ui.View(timeout=120)

        # Add a button for each command on the page
        for command_name in page_commands:
            button = discord.ui.Button(label=command_name, custom_id=f"help_{command_name}")
            view.add_item(button)

        # Navigation buttons
        if len(pages) > 1:
            if page_index > 0:
                view.add_item(discord.ui.Button(label="Previous", custom_id="previous_page", style=discord.ButtonStyle.secondary))
            if page_index < len(pages) - 1:
                view.add_item(discord.ui.Button(label="Next", custom_id="next_page", style=discord.ButtonStyle.secondary))

        return embed, view

    current_page = 0
    embed, view = await create_help_page(current_page)

    # Send the help menu as ephemeral if in DM or based on the toggle
    if is_dm or not toggle_public_help_messages:
        message = await ctx.send(embed=embed, view=view, ephemeral=True)
    else:
        message = await ctx.send(embed=embed, view=view)

    @bot.event
    async def on_interaction(interaction: discord.Interaction):
        nonlocal current_page

        # Ensure the interaction is a button press
        if not interaction.data or "custom_id" not in interaction.data:
            return

        custom_id = interaction.data["custom_id"]

        # Ensure the user interacting is the one who initiated the command
        if interaction.user != ctx.author:
            await interaction.response.send_message("You cannot interact with this help menu.", ephemeral=True)
            return

        # Handle command help button
        if custom_id.startswith("help_"):
            # Extract the full command name (handles multi-word commands)
            command_name = custom_id[len("help_"):]
            response = help_dict.get(command_name, f"Command `{command_name}` not found.")
            await interaction.response.send_message(response, ephemeral=True)

        # Handle navigation
        elif custom_id == "previous_page":
            current_page -= 1
            embed, view = await create_help_page(current_page)
            await interaction.response.edit_message(embed=embed, view=view)
        elif custom_id == "next_page":
            current_page += 1
            embed, view = await create_help_page(current_page)
            await interaction.response.edit_message(embed=embed, view=view)

# Start the bot using your bot token
bot.run('PUT BOT TOKEN HERE')
