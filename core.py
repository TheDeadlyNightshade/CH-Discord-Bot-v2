import discord
from discord.ext import commands
from discord import Embed
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


@bot.event
async def on_ready():
    # Set the bot's status
    await bot.change_presence(activity=discord.Game(name="Made by Notbetaorbiter / Nightshade / Pie123"))
    global decay_active  # Declare the global variable here
    print(f'Bot is ready and logged in as {bot.user}')

    for guild in bot.guilds:
        # Step 1: Check if the "DKP Keeper" role exists
        role = discord.utils.get(guild.roles, name="DKP Keeper")

        if role is None:
            print(f'Creating role "DKP Keeper" in {guild.name}')
            role = await guild.create_role(name="DKP Keeper")
        else:
            print(f'Role "DKP Keeper" already exists in {guild.name}')

        # Step 3: Check if the "dkp-keeping-log" channel exists
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

        # Step 4: Check if the "dkp-database" channel exists
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

        # Step 5: Check if the "Boss_DKP_Values.csv" exists
        message = await find_csv_message(db_channel, "Boss_DKP_Values.csv")
        if message is None:
            print(f'Creating Boss_DKP_Values.csv in {guild.name}')
            await create_dkp_values_csv(guild)
        else:
            print(f"Boss_DKP_Values.csv already exists in {guild.name}")

        # Step 6: Check if the "Balances_Database.csv" exists
        message = await find_csv_message(db_channel, "Balances_Database.csv")
        if message is None:
            print(f'Creating Balances_Database.csv in {guild.name}')
            await create_balances_csv(guild)
        else:
            print(f"Balances_Database.csv already exists in {guild.name}")
        # Step 7: Check if the "Boss_Timers.csv" exists
        message = await find_csv_message(db_channel, "Boss_Timers.csv")
        if message is None:
            print(f'Creating Boss_Timers.csv in {guild.name}')
            await create_timers_csv(guild)
        else:
            print(f"Boss_Timers.csv already exists in {guild.name}")
        # Step 8: Check if the "config.csv" exists
        message = await find_csv_message(db_channel, "config.csv")
        if message is None:
            print(f'Creating config.csv in {guild.name}')
            await create_config_csv(guild)
        else:
            print(f"config.csv already exists in {guild.name}")
            # Check if Active_timers is enabled in config.csv
        # New Step 8.5?: Check if toggle_decay is set to true and restart the decay timer if necessary
            config_data = await download_csv(message.attachments[0])
            if config_data is not None:
                for row in config_data:
                    if row[0] == "toggle_decay" and row[1].lower() == "true":
                        print(f"toggle_decay is set to true in {guild.name}. Restarting decay timer.")
                        decay_active = True  # Set decay_active to true
                        await decay_timer(None, db_channel)  # Start the decay timer
                        break

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

    # Loop through all members of the server and set their initial balances to 0
    for member in guild.members:
        writer.writerow([member.name, 0, 0])

    # Seek to the beginning of the StringIO buffer
    output.seek(0)

    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is not None:
        # Create a discord.File object from the CSV data
        dkp_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Balances_Database.csv")
        # Send the file to the dkp-database channel without a message
        await dkp_database_channel.send(file=dkp_file)

async def create_config_csv(guild):
    # Create the content for the config.csv file, including the new emoji fields
    config_data = [
        ["Setting", "Choice"],
        ["togglewindows", "true"],
        ["Active_timers", "false"],
        ["toggle_dl", "false"],
        ["toggle_edl", "false"],
        ["toggle_legacy", "false"],
        ["toggle_worldboss", "false"],
        ["toggle_ringboss", "false"],
        ["toggle_dl_role", "false"],
        ["toggle_edl_role", "false"],
        ["toggle_legacy_role", "false"],
        ["toggle_worldboss_role", "false"],
        ["toggle_ringboss_role", "false"],
        ["toggle_role_channel", "false"],
        ["dkp_vals_channel", "false"],
        ["auction_duration", "24"],
        ["decay_timeframe", "30"],
        ["decay_percent", "4"],
        ["toggle_decay", "false"],
        ["dl_emoji", "🐉"],
        ["edl_emoji", "🤖"],
        ["legacy_emoji", "🦵"],
        ["worldboss_emoji", "👹"],
        ["ringboss_emoji", "💍"]
    ]

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

    # Load the valid commands from the CSV file dynamically
    valid_commands = await load_valid_commands()

    # Debugging: print the message to check if it's captured correctly
    #print(f"Received message: {message.content}")

    # Check if the message starts with either !k or !a followed by a valid boss command
    if message.content.lower().startswith("!k") or message.content.lower().startswith("!a"):
        # Extract the boss command part (e.g., "155/4" in "!k155/4")
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

@bot.event
async def on_raw_reaction_add(payload):
    guild = bot.get_guild(payload.guild_id)
    channel = bot.get_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    member = guild.get_member(payload.user_id)

    if member.bot:
        return  # Ignore bot reactions

    # Load valid commands from the Boss_DKP_Values.csv dynamically
    valid_commands = await load_valid_commands()

    # Check if the reaction is in the "get-timer-roles" channel
    if channel.name == "get-timer-roles":
        role = await get_role_from_emoji(guild, payload.emoji)
        if role:
            await member.add_roles(role)  # Assign the role to the user

    # Check if it's a DKP command message
    elif str(payload.emoji) == '⚔️' and message.content.lower() in valid_commands:
        command = message.content.lower()

        # Strip the "!k" prefix to log the boss name without it
        boss_name = command.replace("!k", "")

        # Get the DKP value from the CSV
        dkp_value = await get_dkp_value_for_boss(command, guild)
        if dkp_value is None:
            print(f"Could not find DKP value for {command}.")
            return

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

        # Modify the data (Add DKP to both current and lifetime)
        updated = False
        for row in csv_data:
            if row[0] == member.name:  # Match by username
                current_balance = int(row[1]) + dkp_value
                lifetime_balance = int(row[2]) + dkp_value
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        # If the user was not found, add them to the CSV
        if not updated:
            csv_data.append([member.name, str(dkp_value), str(dkp_value)])  # Add new user

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

        # Log to the DKP log channel
        log_channel = discord.utils.get(guild.text_channels, name="dkp-keeping-log")
        if log_channel:
            await log_channel.send(f"{member.display_name} has attended {boss_name} and earned {dkp_value} DKP.")


@bot.event
async def on_raw_reaction_remove(payload):
    guild = bot.get_guild(payload.guild_id)
    channel = bot.get_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    member = guild.get_member(payload.user_id)

    if member.bot:
        return  # Ignore bot reactions

    # Load valid commands from the Boss_DKP_Values.csv dynamically
    valid_commands = await load_valid_commands()

    # Check if the reaction is in the "get-timer-roles" channel
    if channel.name == "get-timer-roles":
        role = await get_role_from_emoji(guild, payload.emoji)
        if role:
            await member.remove_roles(role)  # Remove the role from the user

    # Check if it's a DKP command message
    elif str(payload.emoji) == '⚔️' and message.content.lower() in valid_commands:
        command = message.content.lower()

        # Strip the "!k" prefix to log the boss name without it
        boss_name = command.replace("!k", "")

        # Get the DKP value from the CSV
        dkp_value = await get_dkp_value_for_boss(command, guild)
        if dkp_value is None:
            print(f"Could not find DKP value for {command}.")
            return

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

        # Modify the data (Subtract DKP from both current and lifetime)
        updated = False
        for row in csv_data:
            if row[0] == member.name:  # Match by username
                current_balance = int(row[1]) - dkp_value
                lifetime_balance = int(row[2]) - dkp_value
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        if not updated:
            print(f"User {member.name} not found in the CSV.")
            return

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

        # Log to the DKP log channel
        log_channel = discord.utils.get(guild.text_channels, name="dkp-keeping-log")
        if log_channel:
            await log_channel.send(f"{member.display_name} has revoked attendance to {boss_name} and lost {dkp_value} DKP.")

@bot.command(name="createbackup")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def create_backup(ctx):
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

# Error handler for MissingRole
@create_backup.error
async def create_backup_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")

@bot.command(name="restorebackup")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def restore_backup(ctx):
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

# Error handler for MissingRole
@restore_backup.error
async def role_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")

@bot.command(name="backup")
@commands.has_role("DKP Keeper")  # Ensure only DKP Keeper role can use this command
async def restore_specific_backup(ctx, backup_number: int):
    # Fetch the selected backup file from the available backups
    backup_index = backup_number - 1

    if 0 <= backup_index < len(available_backups):
        filename, file_url = available_backups[backup_index]
        await restore_specific_backup_file(ctx, filename, file_url)
    else:
        await ctx.send("Invalid backup selection. Please choose a valid backup number.")

# Error handler for MissingRole
@restore_specific_backup.error
async def restore_specific_backup_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")

# Command to generate and send the Balances_Database CSV
@bot.command(name="generatebalances")
async def generate_balances(ctx):
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

@bot.command(name="dkpadd")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def dkp_add(ctx, dkp_value: int, *names: str):
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

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create a dictionary of usernames mapped to their nicknames
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ")
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        member = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(ctx.guild.members, name=name)

            # If no user by that name, check if it's a nickname
            if member is None and name.lower() in nicknames:
                real_username = nicknames[name.lower()]
                member = discord.utils.get(ctx.guild.members, name=real_username)

        if member is None:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found member
        updated = False
        current_balance = 0
        lifetime_balance = 0

        for row in csv_data:
            if row[0] == member.name:  # Match by username
                current_balance = int(row[1]) + dkp_value
                lifetime_balance = int(row[2]) + dkp_value
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        # If the user was not found in the CSV, add them
        if not updated:
            current_balance = dkp_value
            lifetime_balance = dkp_value
            csv_data.append([member.name, str(current_balance), str(lifetime_balance)])  # Add new user

        updated_members.append(
            f"{member.display_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}")

    if not updated_members:
        await ctx.send("No users were found or processed for DKP.")
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

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP added to:\n" + "\n".join(updated_members))

# Error handler for MissingRole and BadArgument
@dkp_add.error
async def dkp_add_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Usage: !dkpadd <dkp_value> <user/nickname> [additional users/nicknames]")

# Removes DKP from just current, good for things like auctions
@bot.command(name="dkpsubtract")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def dkp_subtract(ctx, dkp_value: int, *names: str):
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

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create a dictionary of usernames mapped to their nicknames
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ")
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        member = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(ctx.guild.members, name=name)

            # If no user by that name, check if it's a nickname
            if member is None and name.lower() in nicknames:
                real_username = nicknames[name.lower()]
                member = discord.utils.get(ctx.guild.members, name=real_username)

        if member is None:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found member
        updated = False
        current_balance = 0
        lifetime_balance = 0

        for row in csv_data:
            if row[0] == member.name:  # Match by username
                current_balance = int(row[1]) - dkp_value  # Subtract DKP from current balance
                lifetime_balance = int(row[2])  # Lifetime balance remains the same
                row[1] = str(current_balance)  # Update only the current balance in the CSV
                updated = True
                break

        if not updated:
            await ctx.send(f"User {member.name} not found in the CSV.")
            return

        updated_members.append(f"{member.display_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}")

    if not updated_members:
        await ctx.send("No users were found or processed for DKP deduction.")
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

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP deducted from:\n" + "\n".join(updated_members))

# Error handler for MissingRole and BadArgument
@dkp_subtract.error
async def dkp_subtract_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Usage: !dkpsubtract <dkp_value> <user/nickname> [additional users/nicknames]")

# Removes DKP from both current and lifetime
@bot.command(name="dkpsubtractboth")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def dkp_subtract_both(ctx, dkp_value: int, *names: str):
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

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create a dictionary of usernames mapped to their nicknames
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ")
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        member = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(ctx.guild.members, name=name)

            # If no user by that name, check if it's a nickname
            if member is None and name.lower() in nicknames:
                real_username = nicknames[name.lower()]
                member = discord.utils.get(ctx.guild.members, name=real_username)

        if member is None:
            await ctx.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found member
        updated = False
        current_balance = 0
        lifetime_balance = 0

        for row in csv_data:
            if row[0] == member.name:  # Match by username
                current_balance = int(row[1]) - dkp_value  # Subtract from current balance
                lifetime_balance = int(row[2]) - dkp_value  # Subtract from lifetime balance
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        if not updated:
            await ctx.send(f"User {member.name} not found in the CSV.")
            return

        updated_members.append(f"{member.display_name} - current balance: {current_balance} - lifetime balance: {lifetime_balance}")

    if not updated_members:
        await ctx.send("No users were found or processed for DKP deduction.")
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

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"{dkp_value} DKP deducted from:\n" + "\n".join(updated_members))

# Error handler for MissingRole and BadArgument
@dkp_subtract_both.error
async def dkp_subtract_both_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Usage: !dkpsubtractboth <dkp_value> <user/nickname> [additional users/nicknames]")


@bot.command(name="bal")
async def check_balance(ctx, name: str = None):
    # If no name is provided, default to the user who invoked the command
    if name is None:
        member = ctx.author
    else:
        # Try to find the user by mention or nickname
        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(ctx.guild.members, name=name)

            # Fetch the DKP Database Channel to find nicknames
            dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
            if dkp_database_channel is None:
                await ctx.send("The DKP database channel does not exist.")
                return

            # Find the "Nicknames.csv" message in the "dkp-database" channel
            nickname_message = await find_csv_message(dkp_database_channel, "Nicknames.csv")
            nicknames = {}

            if nickname_message is not None:
                # Download and parse the Nicknames.csv file
                nickname_csv_file = nickname_message.attachments[0]
                nickname_csv_data = await download_csv(nickname_csv_file)

                if nickname_csv_data is not None:
                    # Create a dictionary of usernames mapped to their nicknames
                    for row in nickname_csv_data:
                        nickname_list = row[1].split(", ")
                        for nick in nickname_list:
                            nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping

                # If the name is a nickname, get the actual username
                if name.lower() in nicknames:
                    member_name = nicknames[name.lower()]
                    member = discord.utils.get(ctx.guild.members, name=member_name)

        if member is None:
            await ctx.send(f"Could not find user or nickname: {name}")
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

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Search for the user's data in the CSV
    found_user = False
    current_balance = 0
    lifetime_balance = 0
    for row in csv_data:
        if row[0] == member.name:  # Match by username
            current_balance = row[1]
            lifetime_balance = row[2]
            found_user = True
            break

    # If the user was not found in the CSV, add them with default values
    if not found_user:
        current_balance = 0
        lifetime_balance = 0
        csv_data.append([member.name, str(current_balance), str(lifetime_balance)])  # Add new user

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

    # Send the user's current and lifetime DKP
    await ctx.send(f"{member.display_name} has {current_balance} current DKP and {lifetime_balance} lifetime DKP.")

# Error handler for BadArgument
@check_balance.error
async def check_balance_error(ctx, error):
    if isinstance(error, commands.BadArgument):
        await ctx.send("Usage: !bal [user/nickname]")

# Command to set the DKP value, restricted to "DKP Keeper" role
@bot.command(name="setdkp")
@commands.has_role("DKP Keeper")
async def set_dkp_value(ctx, boss: str, dkp_value: int):
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

# Error handler for MissingRole
@set_dkp_value.error
async def set_dkp_value_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")


@bot.command(name="nick")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def set_nickname(ctx, member: discord.Member, nickname: str):
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
        writer.writerow(["Username", "Nicknames"])  # Header
        output.seek(0)
        nicknames_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
        await dkp_database_channel.send(file=nicknames_file)
        message = await find_csv_message(dkp_database_channel, "Nicknames.csv")  # Reload the message

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Nicknames.csv file.")
        return

    # Check for duplicate nicknames across all users
    for row in csv_data:
        if nickname in row[1].split(", "):
            if row[0] == member.name:
                await ctx.send(f"{member.display_name} already has the nickname '{nickname}'.")
            else:
                await ctx.send(f"The nickname '{nickname}' is already taken by {row[0]}.")
            return

    # Modify or add the nickname for the specific user
    nickname_added = False  # Track if we added a new nickname
    updated = False
    for row in csv_data:
        if row[0] == member.name:  # Match by username
            existing_nicknames = row[1].split(", ")
            if nickname not in existing_nicknames:
                existing_nicknames.append(nickname)  # Append the new nickname
                row[1] = ", ".join(existing_nicknames)  # Update the row with multiple nicknames
                nickname_added = True
            updated = True
            break

    # If the user was not found, add them to the CSV with the new nickname
    if not updated:
        csv_data.append([member.name, nickname])  # Add new user with nickname
        nickname_added = True  # Indicate that a nickname was added

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Send confirmation if nickname was added
    if nickname_added:
        await ctx.send(f"Nickname '{nickname}' has been added for {member.display_name}.")
    else:
        await ctx.send(f"Nickname '{nickname}' was already set for {member.display_name}.")

# Error handler for MissingRole
@set_nickname.error
async def set_nickname_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")

@bot.command(name="nickdelete")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def delete_nickname(ctx, member: discord.Member, nickname: str):
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

    # Download and parse the CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)
    if csv_data is None:
        await ctx.send("Could not download or parse the Nicknames.csv file.")
        return

    # Modify or delete the nickname
    updated = False
    for row in csv_data:
        if row[0] == member.name:
            existing_nicknames = row[1].split(", ")
            if nickname in existing_nicknames:
                existing_nicknames.remove(nickname)  # Remove the nickname
                if existing_nicknames:
                    row[1] = ", ".join(existing_nicknames)  # Update with remaining nicknames
                else:
                    csv_data.remove(row)  # Remove the row if no nicknames are left
                updated = True
                break

    if not updated:
        await ctx.send(f"Nickname '{nickname}' not found for {member.display_name}.")
        return

    # Create a new CSV file with the updated data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(csv_data)
    output.seek(0)

    # Send the updated CSV to the "dkp-database" channel
    new_csv_file = discord.File(io.BytesIO(output.getvalue().encode()), filename="Nicknames.csv")
    await dkp_database_channel.send(file=new_csv_file)

    # Delete the original message containing the old CSV
    await message.delete()

    # Send confirmation to the channel where the command was issued
    await ctx.send(f"Nickname '{nickname}' has been deleted for {member.display_name}.")

# Error handler for MissingRole
@delete_nickname.error
async def delete_nickname_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use the !nickdelete command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Usage: `!nickdelete user nickname`.")
    else:
        await ctx.send("An unexpected error occurred while deleting the nickname.")


async def handle_attendance_command(message):
    # Load valid commands dynamically from the Boss_DKP_Values.csv
    valid_commands = await load_valid_commands()

    # Check if the user has the DKP Keeper role
    role = discord.utils.get(message.author.roles, name="DKP Keeper")
    if role is None:
        await message.channel.send("You need the DKP Keeper role to use this command.")
        return

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

    if nickname_message is not None:
        # Download and parse the Nicknames.csv file
        nickname_csv_file = nickname_message.attachments[0]
        nickname_csv_data = await download_csv(nickname_csv_file)

        if nickname_csv_data is not None:
            # Create a dictionary of nicknames mapped to their usernames
            for row in nickname_csv_data:
                nickname_list = row[1].split(", ")
                for nick in nickname_list:
                    nicknames[nick.lower()] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    balance_csv_file = balance_message.attachments[0]
    balance_csv_data = await download_csv(balance_csv_file)

    if balance_csv_data is None:
        await message.channel.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        member = None

        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = message.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(message.guild.members, name=name)

            # If no user by that name, check if it's a nickname
            if member is None and name.lower() in nicknames:
                real_username = nicknames[name.lower()]
                member = discord.utils.get(message.guild.members, name=real_username)

        if member is None:
            await message.channel.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found member
        updated = False
        for row in balance_csv_data:
            if row[0] == member.name:  # Match by username
                current_balance = int(row[1]) + dkp_value
                lifetime_balance = int(row[2]) + dkp_value
                row[1] = str(current_balance)
                row[2] = str(lifetime_balance)
                updated = True
                break

        # If the user was not found, add them to the CSV
        if not updated:
            balance_csv_data.append([member.name, str(dkp_value), str(dkp_value)])  # Add new user

        updated_members.append(f"{member.display_name}")

    if not updated_members:
        await message.channel.send("No users were found or processed for attendance.")
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

    # Log the attendance in the DKP log channel
    log_channel = discord.utils.get(message.guild.text_channels, name="dkp-keeping-log")
    if log_channel:
        await log_channel.send(f"{', '.join(updated_members)} have attended {boss_command} and earned {dkp_value} DKP.")
    else:
        print(f"Error: 'dkp-keeping-log' channel not found in guild {message.guild.name}")

    # Send confirmation to the channel where the command was issued
    await message.channel.send(f"{dkp_value} DKP added for attending {boss_command} to:\n" + "\n".join(updated_members))

#timer shit

@bot.command(name="togglewindows")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def toggle_windows(ctx):
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

# Error handler for MissingRole
@toggle_windows.error
async def toggle_windows_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")

@bot.command(name="toggletimers")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def toggle_timers(ctx):
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

# Error handler for MissingRole
@toggle_timers.error
async def toggle_timers_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")

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
        task.cancel()  # Cancel the running task
        await asyncio.sleep(0)  # Give time for the cancellation to propagate

    # Remove from active timers and tasks
    active_boss_timers.pop(boss_name, None)
    active_tasks.pop(boss_name, None)

    # Update the timers embed
    await update_timers_embed_if_active(guild)

@bot.command(name="cancel")
async def cancel_timer(ctx, boss_name: str):
    # Add a prefix to match the format used in the dictionary (e.g., !155)
    # Convert the boss_name to lowercase to make the command case-insensitive
    boss_command = f"!{boss_name.lower()}"

    # Check if the boss timer is active
    if boss_command not in active_boss_timers:
        await ctx.send(f"There is no active timer for boss {boss_name.lower()}.")
        return

    # Cancel the timer task and remove it from active timers
    await cancel_timer_logic(boss_command, ctx.guild)

    # Notify the channel that the timer has been canceled
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

    # Add the boss to the active timers
    active_boss_timers[boss_name] = {"timer_end": timer_end, "window_end": window_end}

    # Format the timer duration
    time_left_str = format_time_left(timer_duration)

    # Notify about the timer start
    await message.channel.send(
        f"The boss {boss_name[1:]} timer has started! You will be notified in {time_left_str}."
    )

    # Start the timer logic as a task and store it
    task = asyncio.create_task(manage_boss_timers(message.guild, message.channel, boss_name, timer_end, window_end))
    active_tasks[boss_name] = task

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


async def update_timers_embed_if_active(guild):
    # Fetch the "Active_timers" setting
    active_timers = await get_active_timers_setting(guild)
    if not active_timers:
        return  # Skip updating the embed if Active_timers is false

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
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def toggle_channel(ctx, channel_name: str):
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

# Error handler for MissingRole
@toggle_channel.error
async def toggle_channel_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")

@bot.command(name="toggle_role")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def toggle_role(ctx, role_name: str):
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

# Error handler for MissingRole
@toggle_role.error
async def toggle_role_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")



@bot.command(name="togglerolechannel")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def toggle_role_channel(ctx):
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

# Error handler for MissingRole
@toggle_role_channel.error
async def toggle_role_channel_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")


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
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def assign_emoji(ctx, boss_type: str, emoji: str):
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

# Error handler for MissingRole
@assign_emoji.error
async def assign_emoji_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use the !assign command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Usage: `!assign boss_type emoji`.")
    else:
        await ctx.send("An unexpected error occurred while assigning the emoji.")

@bot.command(name="toggledkpchannel")
@commands.has_role("DKP Keeper")
async def toggledkpchannel(ctx):
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

# Error handler for MissingRole
@toggledkpchannel.error
async def toggle_dkp_channel_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")


async def send_dkp_values_embed(guild):
    # Fetch the DKP Database Channel
    dkp_database_channel = discord.utils.get(guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        print("The DKP database channel does not exist.")
        return

    # Check the config.csv for the dkp_vals_channel toggle
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        print("Could not find the config.csv file.")
        return

    # Download and parse the config.csv file
    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)
    if config_data is None:
        print("Could not download or parse the config.csv file.")
        return

    # Check if the "dkp_vals_channel" is set to true in the config
    dkp_vals_channel_enabled = False
    for row in config_data:
        if row[0] == "dkp_vals_channel" and row[1].lower() == "true":
            dkp_vals_channel_enabled = True
            break

    if not dkp_vals_channel_enabled:
        print("DKP Values channel is disabled in the config.")
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

    # Debugging: print the CSV data
    #print("CSV Data:", csv_data)

    # Fetch the "dkp-vals" channel
    dkp_vals_channel = discord.utils.get(guild.text_channels, name="dkp-vals")
    if dkp_vals_channel is None:
        print("The DKP Values channel does not exist.")
        return

    # Delete all existing messages with embeds in the dkp-vals channel
    async for message in dkp_vals_channel.history(limit=100):
        if message.embeds:
            await message.delete()

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

@bot.command(name="setauctionduration")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def set_auction_duration(ctx, duration: int):
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

# Error handler for MissingRole
@set_auction_duration.error
async def set_auction_duration_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Please provide a valid integer for the auction duration.")


# Store auction data globally or in a database
ongoing_auctions = {}


@bot.command(name="auction")
@commands.has_role("DKP Keeper")
async def start_auction(ctx, *args):
    if len(args) == 0:
        await ctx.send("Usage: !auction {item name} {minimum bid}. Minimum bid is optional and defaults to 0.")
        return

    # Find the minimum bid and the item name
    minimum_bid = 0
    item_name_parts = []

    for arg in args:
        if arg.isdigit():  # If we find a number, treat it as the minimum bid
            minimum_bid = int(arg)
            break
        item_name_parts.append(arg)

    item_name = ' '.join(item_name_parts).lower()  # Combine the words into the item name and normalize to lowercase

    # Check if an auction for this item already exists
    if item_name in (key.lower() for key in ongoing_auctions.keys()):
        await ctx.send(f"An auction for '{item_name}' is already ongoing.")
        return

    # Check if an item name was provided
    if not item_name:
        await ctx.send("Please provide a valid item name.")
        return

    # Fetch auction duration from config and set auction details
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    # Retrieve auction duration from config.csv
    config_message = await find_csv_message(dkp_database_channel, "config.csv")
    if config_message is None:
        await ctx.send("Could not find the config.csv file.")
        return

    config_file = config_message.attachments[0]
    config_data = await download_csv(config_file)

    auction_duration = 24  # default auction duration
    for row in config_data:
        if row[0] == "auction_duration":
            auction_duration = int(row[1])
            break

    # Start the auction
    auction_end_time = datetime.now() + timedelta(hours=auction_duration)
    ongoing_auctions[item_name] = {
        "minimum_bid": minimum_bid,
        "end_time": auction_end_time,
        "highest_bid": 0,
        "highest_bidder": None,
        "bids": {}
    }

    await ctx.send(f"Auction started for '{item_name}' with a minimum bid of {minimum_bid} DKP! Auction ends in {auction_duration} hours.")

    # Schedule the auction end
    bot.loop.call_later(auction_duration * 3600,
                        lambda: asyncio.ensure_future(end_auction(item_name, ctx.guild.id, ctx.channel.id)))


# Error handling for the auction command
@start_auction.error
async def start_auction_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You need the DKP Keeper role to start an auction.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: !auction {item name} {minimum bid}. Minimum bid is optional and defaults to 0.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Please ensure the minimum bid is a valid number.")


@bot.command(name="bid")
async def place_bid(ctx, *args):
    if len(args) < 2:
        await ctx.send("Usage: !bid {item name} {value}.")
        return

    # Split the arguments into item name and bid value
    try:
        bid_value = int(args[-1])  # The last argument should be the bid value
    except ValueError:
        await ctx.send("Invalid bid. Ensure your bid is a valid number.")
        return

    # The rest of the arguments form the item name
    item_name = ' '.join(args[:-1]).lower()

    # Check if an auction with the normalized name exists
    matched_item = None
    for auction_item in ongoing_auctions.keys():
        if auction_item.lower() == item_name:
            matched_item = auction_item
            break

    if matched_item is None:
        await ctx.send(f"No active auction for {item_name}.")
        return

    # Fetch user's DKP balance from Balances_Database.csv
    dkp_database_channel = discord.utils.get(ctx.guild.text_channels, name="dkp-database")
    if dkp_database_channel is None:
        await ctx.send("The DKP database channel does not exist.")
        return

    balances_message = await find_csv_message(dkp_database_channel, "Balances_Database.csv")
    if balances_message is None:
        await ctx.send("Could not find the Balances_Database.csv file.")
        return

    csv_file = balances_message.attachments[0]
    balances_data = await download_csv(csv_file)

    # Find user's current balance
    user_balance = None
    for row in balances_data:
        if row[0] == str(ctx.author):
            user_balance = int(row[1])
            break

    if user_balance is None:
        await ctx.send(f"You don't have a DKP balance recorded.")
        return

    # Validate bid
    auction = ongoing_auctions[matched_item]
    current_bid = auction["highest_bid"]
    previous_bid = auction["bids"].get(ctx.author.id, 0)

    if bid_value > user_balance:
        await ctx.send(f"You don't have enough DKP! Your current balance is {user_balance}.")
        return

    if bid_value < auction["minimum_bid"]:
        await ctx.send(f"Your bid must be higher than the minimum bid of {auction['minimum_bid']}.")
        return

    if bid_value <= previous_bid:
        await ctx.send(f"Your bid must be higher than your previous bid of {previous_bid}.")
        return

    if bid_value > current_bid:
        # Update auction data
        auction["highest_bid"] = bid_value
        auction["highest_bidder"] = ctx.author
        auction["bids"][ctx.author.id] = bid_value
        await ctx.send(f"{ctx.author.name} has placed the highest bid of {bid_value} DKP for {matched_item}!")
    else:
        await ctx.send(f"Your bid must be higher than the current highest bid of {current_bid} DKP.")

# Error handling for the bid command
@place_bid.error
async def place_bid_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: !bid {item name} {value}.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid bid. Ensure your bid is a valid number.")


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

        # Announce the winner
        await result_channel.send(f"{winner.mention} has won the auction for {item_name} with a bid of {bid_value} DKP!")

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
            if row[0] == str(winner):
                current_balance = int(row[1])
                new_balance = current_balance - bid_value  # Deduct the bid from the current balance
                row[1] = str(new_balance)  # Update the current balance
                updated = True
                break

        if not updated:
            await result_channel.send(f"Error: Could not find {winner}'s balance in the balances_database.csv file.")
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

        await result_channel.send(f"{winner.mention}'s new balance is {new_balance} DKP.")
    else:
        # If no one bid, announce that the auction ended with no winner
        await result_channel.send(f"No bids were placed for {item_name}. Auction ended with no winner.")

# You would need a background task or a timer to call `end_auction` at the appropriate time

@bot.command(name="auctionend")
@commands.has_role("DKP Keeper")
async def auction_end(ctx, *item_name_parts):
    # Combine the item name parts into a single string
    item_name = ' '.join(item_name_parts).lower()

    # Check if an auction with the normalized name exists
    matched_item = None
    for auction_item in ongoing_auctions.keys():
        if auction_item.lower() == item_name:
            matched_item = auction_item
            break

    if matched_item is None:
        await ctx.send(f"No active auction for {item_name}.")
        return

    # End the auction immediately with the correct case-preserved item name
    await end_auction(matched_item, ctx.guild.id, ctx.channel.id)

    await ctx.send(f"The auction for {matched_item} has been manually ended.")

# Error handling for the auctionend command
@auction_end.error
async def auction_end_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You need the DKP Keeper role to end an auction.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: !auctionend {item name}.")

@bot.command(name="auctioncancel")
@commands.has_role("DKP Keeper")
async def auction_cancel(ctx, *item_name_parts):
    # Combine the item name parts into a single string
    item_name = ' '.join(item_name_parts).lower()

    # Check if an auction with the normalized name exists
    matched_item = None
    for auction_item in ongoing_auctions.keys():
        if auction_item.lower() == item_name:
            matched_item = auction_item
            break

    if matched_item is None:
        await ctx.send(f"No active auction for {item_name} to cancel.")
        return

    # Remove the matched auction from the ongoing_auctions list
    ongoing_auctions.pop(matched_item)

    await ctx.send(f"The auction for {matched_item} has been canceled.")

# Error handling for the auctioncancel command
@auction_cancel.error
async def auction_cancel_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You need the DKP Keeper role to cancel an auction.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Usage: !auctioncancel {item name}.")

#dkp decay code

# Global variable to control the timer loop
decay_active = False

@bot.command(name="setdecaypercent")
@commands.has_role("DKP Keeper")
async def set_decay_percent(ctx, new_percent: int):
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

# Error handling for the set_decay_percent command
@set_decay_percent.error
async def set_decay_percent_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You need the DKP Keeper role to use this command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Please provide a valid integer for the decay percent.")


@bot.command(name="setdecaytimeframe")
@commands.has_role("DKP Keeper")
async def set_decay_timeframe(ctx, new_timeframe: int):
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

# Error handling for the set_decay_timeframe command
@set_decay_timeframe.error
async def set_decay_timeframe_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You need the DKP Keeper role to use this command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument. Please provide a valid integer for the decay timeframe.")

@bot.command(name="toggledecay")
@commands.has_role("DKP Keeper")
async def toggle_decay(ctx):
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

# Error handling for the toggle_decay command
@toggle_decay.error
async def toggle_decay_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You need the DKP Keeper role to use this command.")

@bot.command(name="restorefromconfig")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def restore_from_config(ctx):
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

# Error handler for MissingRole
@restore_from_config.error
async def restore_from_config_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use this command.")


help_dict = {
    'k': """!k

Usage: !k{boss lvl or name}/{boss stars}
Use !k (example: !k155/4 to report the killing of the 4* lvl 155 DL boss) as a method to report boss attendance.
Whenever the command is used, the bot will react to the message with a crossed swords emoji (⚔️). Pressing this crossed swords emoji will give you DKP for attending that kill. It will also send those who have reacted to the kill command to the #dkp-keeping-log channel.
An example of the log message would be: notbetaorbiter has attended !k155/5 and earned 2 DKP.
When a reaction has been removed, the bot automatically removes the DKP from the player and logs this in the log channel.
An example of the DKP removal message: notbetaorbiter has revoked attendance to !k155/4 and lost 1 DKP.
This command does not require any roles to use.""",
    'a': """!a
Usage: !a{boss lvl or name}/{boss stars} user1 user2 user3
Use !a for another method of DKP Keeping. An example would be: "!a155/4 @ notbetaorbiter", which the bot will then reply to with "1 DKP added for attending 155/4 to: notbetaorbiter". You can use as many members after the boss name as were present at the boss to reduce effort.
Use of nicknames works as follows: !a155/4 nickname. Nicknames must be set using the !nick command.
Both nicknames and @user can be used to track attendance.
See !nick for more information.
This command requires the DKP keeper role to use.""",
    'nick': """Usage: !nick @user nickname
Use !nick to create nicknames for users in the server. This allows you to track attendance more easily without needing to memorize their @user. A good practice is to !nick every person in the server with their in-game name for easier tracking.
Multiple nicknames can be added to a single user, this allows for handling if they have multiple accounts. To remove a nickname see !nickdelete help page.
The nicknames work for the following commands:
!a
!dkpadd
!dkpsubtract
!dkpsubtractboth
!bal
This command requires the DKP keeper role to use.""",
    'dkpadd': """Usage: !dkpadd # user1 user2 user3 etc

DKP Keepers can use this command to manually add DKP to either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct DKP values or grant awards of DKP. This will increase both current DKP and lifetime DKP.
Nicknames can be used for this command. See !nick for more information.
This command requires the DKP keeper role to use.""",
    'dkpsubtract': """Usage: !dkpsubtract # user1 user2 user3 etc

DKP Keepers can use this command to manually remove DKP from either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct DKP values or take away DKP due to winning an item, etc. This will reduce current DKP and have no effect on lifetime DKP. To change both, see !dkpsubtractboth.
Nicknames can be used for this command. See !nick for more information.
This command requires the DKP keeper role to use.""",

    'dkpsubtractboth': """Usage: !dkpsubtractboth # user1 user2 user3 etc

DKP Keepers can use this command to manually remove DKP from either a single user or a set of users defined by the user1 user2 user3 arguments. This can be used to correct DKP values. This will reduce both current DKP and lifetime DKP.
Nicknames can be used for this command. See !nick for more information.
This command requires the DKP keeper role to use.""",

    'setdkp': """Usage: !setdkp {boss lvl or name}/{boss stars} #
Example: !setdkp 155/4 10

This command is used to change the DKP values for each boss from the defaults stored in the Boss_DKP_Values.csv located in the #dkp-database text channel. 
This will be saved across sessions; restarting the bot will not reset these values as they are stored in Boss_DKP_Values.csv.
This command requires the DKP keeper role to use.""",

    'bal': """Usages:
!bal → returns current and lifetime DKP of the user who sent the message
!bal user1 → returns current and lifetime DKP of the tagged user or nickname used as user1

Example: !bal
Bot replies: notbetaorbiter has 10 current DKP and 10 lifetime DKP.

Nicknames can be used for this command. See !nick for more information.""",

    'togglewindows': """Toggle windows is on by default. When true, the bot timers will notify the server when the boss spawn window opens and closes. When false, the bot will notify only when the spawn window opens.
This command toggles the togglewindows value between true and false in the config.csv located in the #dkp-database text channel.
This command requires the DKP keeper role to use.""",

    'toggletimers': """Toggle timers is off by default. When true, the #timers channel will be generated, and the timers embed will be sent. When false, the #timers channel will be deleted, and the message removed.
The #timers channel cannot have messages sent in it except by administrators and the bot itself. To make use of the timers embed, see !{boss} commands.
This command requires the DKP keeper role to use.""",

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
    'cancel': """Usage: !cancel {boss}
For example: !cancel 155

If there is a currently running timer for a boss, using !cancel {boss} will cancel the timer for said boss. To see a list of supported timers, see the help entry for !{boss}.""",

    'toggle': """Usage: !toggle {boss type}

Where boss types are: dl, edl, ringboss, worldboss, legacy

By default, all boss type text channels are off. 
This command toggles on and off boss type specific text channels for timer alerts. 
For example, using !toggle dl will create a text channel called #dl-boss-alerts. Alerts for DL timers will then be sent in that channel.
This command requires the DKP keeper role to use.""",

    'toggle_role': """Usage: !toggle_role {boss type}

Where default boss types are: dl, edl, ringboss, worldboss, legacy. Custom boss types can be added via !timeradd.

By toggling on, users with these roles are the only ones notified when a timer for a boss of that type goes off instead of the default @ everyone.
For example, when on, the !155 timer command will result in: @dl The window for boss 155 has opened!
By default, all boss type roles are off.
To help assign users these roles, see the help page for the !togglerolechannel.
This command requires the DKP keeper role to use.""",

    'restorefromconfig': """THIS COMMAND IS ONLY PARTIALLY COMPLETE AFTER UPDATE. This command is used to bring configuration choices across servers. 
To use: find and download config.csv file from the #dkp-database text channel of the server whose configuration you want to import.
Delete the current config.csv file in the server you are importing new settings to (if you do not have one in the #dkp-database channel, skip this).
Send the config.csv of the server whose configuration you want to import into the #dkp-database text channel after adding the bot to the server.
Then send !restorefromconfig.
This command requires the DKP keeper role to use.""",

    'createbackup': """This command creates a timestamped backup of the current DKP values of everyone in the server. 
See help page on !restorefrombackup for more information about using these backups.
This command requires the DKP keeper role to use.""",

    'restorefrombackup': """This command can only be used after the !createbackup command has been run and there is at least one backup file. 
This command can also be used to move DKP values across servers via a downloaded backup of the Balances_Database.csv.
If there is more than one backup file, you will be given the choice of which one to restore from. 
This command includes a check to ensure that you mean to override the data in the Balances_Database.csv, as they cannot be recovered after the restore.
This command requires the DKP keeper role to use.""",
    'togglerolechannel': """This command toggles on and off a channel that allows a user to assign boss timer roles to themselves via clicking on a reaction. If you do not see any roles active you must turn them on via the !toggle_role command. Read about the help page for that command for more information on it.

This channel automatically has an embed within that updates on changes to the active boss timer roles. This approach is suggested if you are using roles to notify users to boss spawns. See !assign if you are adding custom boss types.

This command requires the DKP keeper role to use.""",
    'toggledecay': """Usage: !toggledecay
The goal of DKP decay is to incentivize players to remain active as every set amount of time (default of 30 days) their dkp is reduced by a set %  (default 4%).

The value for the number of days until the DKP decays can be changed by using the !setdecaytimeframe command.

The value of the % of DKP that is decayed can be changed by using the !setdecaypercent command.

This feature is off by default.

This feature applies to all users and only current DKP and has no effect on lifetime DKP.

This command requires DKP Keeper role to use.
""",
    'setdecaypercent': """Usage: !setdecaypercent {integer}

For example !setdecaypercent 5

This command sets the % at which the DKP of all users decays over time. For more information read the help page for !toggledecay.

The default value for this setting is 4%.

This command requires DKP Keeper role to use.""",
    'setdecaytimeframe': """Usage: !setdecaytimeframe {integer}

For example !setdecaytimeframe 5

This command sets the number of days it takes for the DKP of all users to decay. For more information read the help page for !toggledecay.

The default value for this setting is 30 days.

This command requires DKP Keeper role to use.""",
    'auction': """Usage: !auction {item name} {minimum bid}

For example: !auction rare totem of earth 69

Or: !auction godly sub

If you do not populate the {minimum bid} field it will assume that the minimum bid is 0 DKP.

The default length of an auction is 24 hours but can be changed with !setauctionduration.

Users can bid with the !bid command

You can end any auction prematurely by using the !auctionend command.

You can cancel any auction by using the !auctioncancel command.

Note: the item name can have multiple words as long as it does not contain numbers.""",
    'bid': """Usage: !bid {item name} {amount}

For example: !bid rare torem of earth 420

This can be used to bid on any auction as long as the user has enough DKP and it is both over the minimum bid and over their previous bids.""",
    'auctionend': """Usage: !auctionend {item name}

Ends an auction prematurely. This is not the same as canceling an auction. See !auctioncancel.

Whoever is highest bidding when this command is used will win that auction.""",
    'auctioncancel': """Usage: !auctioncancel {item name}

Cancels an auction. This is not the same as Ending an auction. See !auctionend.

This will stop an auction entirely with no winners possible.""",
    'setauctionduration': """Usage: !setauctionduration {integer}

Command to set the number of hours an auction lasts for. By default the length of auctions is 24 hours.""",
    'toggledkpchannel': """This command toggles on or off a channel that is used to display the DKP values for all bosses that give DKP.

The default for this toggle is false.

This command requires DKP Keeper role to use.""",
    'bossadd': """Usage: !bossadd {boss} {DKP values}

For example: !bossadd 155/4 69

Adds a boss to the list of bosses that give DKP. Therefore it can be used with the !a and !k commands which are designed to track attends.

This can be used to add custom bosses that do not give DKP by default.

This command requires DKP Keeper role to use.""",
    'bossdelete': """Usage: !bossdelete {boss}

For example: !bossdelete 155/4

Removes a boss from the list of bosses that give DKP.

This command requires DKP Keeper role to use.""",
    'nickdelete': """Usage: !nickdelete @{user} {nickname}

For example: !nickdelete @notbetaorbiter notbeta

Removes a nickname from a user.

Requires the DKP Keeper role to use.""",
    'timeradd': """Usage: !timeradd {boss} {respawn time} {window time} {boss type}

For example: !timeradd 155 60000 180 DL

Supports custom boss types outside of just the standard CH ones, can be populated by any word.

Timer for the new boss can then be triggered by doing !{boss} and is displayed in the #timers channel which is toggled via the !toggletimers command. 

Requires the DKP Keeper role to use.""",
    'timerdelete': """Usage: !timerdelete {boss}

For example: !timerdelete 155

Removes a timer from the list of timers. Used to remove custom bosses that you may be timing currently but will not use in the future. 

Requires the DKP Keeper role to use.""",
    'timeredit': """Usage: !timeredit {boss} {respawn time} {window time} {boss type}

For example: !timeredit 155 60000 180 DL

Note: all time values are in seconds.

This command is used to edit existing timers to correct errors or cope with game updates.

Requires the DKP Keeper role to use.""",
    'assign': """Usage: !assign {boss type} 😀

Example: !assign dl 💀

The assign command is used to assign an emoji to a taggable role. This is to be used in conjunction with the role channel added by !togglerolechannel. Note: when assigning an emoji to a boss type you must toggle the role on first for that boss type via the !toggle_role command. This command works with custom boss types added through the !timeradd command.

This command requires the DKP Keeper role to use.""",
}


@bot.command(name="help")
async def help_command(ctx, command: str = None):
    if command is None:
        help_message = """
        **Help usage:**
        Send `!help` followed by one of the following commands:

        `k`
        `a`
        `nick`
        `dkpadd`
        `dkpsubtract`
        `dkpsubtractboth`
        `bal`
        `setdkp`
        `togglewindows`
        `toggletimers`
        `{boss}`
        `timers`
        `cancel`
        `toggle`
        `toggle_role`
        `togglerolechannel`
        `auction`
        `bid`
        `auctionend`
        `auctioncancel`
        `setauctionduration`
        `toggledkpchannel`
        `bossadd`
        `bossdelete`
        `nickdelete`
        `timeradd`
        `timerdelete`
        `timeredit`
        `assign`
        `setdecaypercent`
        `setdecaytimeframe`
        `toggledecay`
        `restorefromconfig`
        `createbackup`
        `restorefrombackup`
        """
        await ctx.send(help_message)
    else:
        response = help_dict.get(command, "Command not found. Please use `!help` for a list of commands.")
        await ctx.send(response)


# Start the bot using your bot token
bot.run('put bot token here')
