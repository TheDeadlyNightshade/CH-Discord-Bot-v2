import discord
from discord.ext import commands
from discord import Embed
import csv
import io
import aiohttp
import asyncio
from datetime import datetime
import time

#Made By Nightshade / Pie123 for Celtic Heroes Discord Servers
intents = discord.Intents.default()
intents.message_content = True  # Enable reading message content
intents.guilds = True  # Enable access to guild (server) information
intents.reactions = True  # Enable handling of reactions
intents.members = True  # Enable fetching members

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# Store the valid commands in a set for efficient lookups
valid_commands = {"!k155/4", "!k155/5", "!k155/6",
                  "!k160/4", "!k160/5", "!k160/6",
                  "!k165/4", "!k165/5", "!k165/6",
                  "!k170/4", "!k170/5", "!k170/6",
                  "!k180/4", "!k180/5", "!k180/6",
                  "!k185/4", "!k185/5", "!k185/6",
                  "!k190/4", "!k190/5", "!k190/6",
                  "!k195/4", "!k195/5", "!k195/6",
                  "!k200/4", "!k200/5", "!k200/6",
                  "!k205/4", "!k205/5", "!k205/6",
                  "!k210/4", "!k210/5", "!k210/6",
                  "!k215/4", "!k215/5", "!k215/6",
                  "!kaggy", "!klich", "!kreaver",
                  "!kgele", "!kprot", "!kbt",
                  "!knecro", "!khrung", "!krb/5",
                  "!krb/6", "!kdino", "!kcook"}

@bot.event
async def on_ready():
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

        # Step 7: Check if the "config.csv" exists
        message = await find_csv_message(db_channel, "config.csv")
        if message is None:
            print(f'Creating config.csv in {guild.name}')
            await create_config_csv(guild)
        else:
            print(f"config.csv already exists in {guild.name}")
            # Check if Active_timers is enabled in config.csv


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
    # Create the content for the config.csv file
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
        ["toggle_role_channel", "false"]  # Add the setting for the role channel toggle
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

    # Check if the message content is one of the valid kill commands
    if message.content in valid_commands:
        await message.add_reaction('⚔️')
        return

    # Check if the message content starts with !a and call the attendance function
    if message.content.startswith("!a"):
        await handle_attendance_command(message)
        return

    # Check if the message content matches a valid boss timer command (e.g., !155, !200)
    if message.content in boss_timers:
        # Call the unified function that handles both notifications and embed updates
        await handle_boss_timers(message)
        return

    # Allow the bot to process other commands after on_message event
    await bot.process_commands(message)

@bot.event
async def on_raw_reaction_add(payload):
    guild = bot.get_guild(payload.guild_id)
    channel = bot.get_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    member = guild.get_member(payload.user_id)

    if member.bot:
        return  # Ignore bot reactions

    # Check if the reaction is in the "get-timer-roles" channel
    if channel.name == "get-timer-roles":
        role = get_role_from_emoji(guild, payload.emoji)
        if role:
            await member.add_roles(role)  # Assign the role to the user

    # Check if it's a DKP command message
    elif str(payload.emoji) == '⚔️' and message.content in valid_commands:
        command = message.content

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
            await log_channel.send(
                f"{member.display_name} has attended {command} and earned {dkp_value} DKP.")


@bot.event
async def on_raw_reaction_remove(payload):
    #Handles removing roles and DKP reactions
    guild = bot.get_guild(payload.guild_id)
    channel = bot.get_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    member = guild.get_member(payload.user_id)

    if member.bot:
        return  # Ignore bot reactions

    # Check if the reaction is in the "get-timer-roles" channel
    if channel.name == "get-timer-roles":
        role = get_role_from_emoji(guild, payload.emoji)
        if role:
            await member.remove_roles(role)

    # Check if it's a DKP command message
    elif str(payload.emoji) == '⚔️' and message.content in valid_commands:
        command = message.content

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
            await log_channel.send(
                f"{member.display_name} has revoked attendance to {command} and lost {dkp_value} DKP.")

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
        await ctx.send("Please provide at least one user or nickname.")
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
                nicknames[row[1]] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(ctx.guild.members, name=name)

            # If the name is not a user mention, check if it's a nickname
            if member is None and name in nicknames:
                member_name = nicknames[name]  # Get the corresponding username
                member = discord.utils.get(ctx.guild.members, name=member_name)

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

# Error handler for MissingRole
@dkp_add.error
async def dkp_add_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")

# Removes DKP from just current, good for things like auctions
@bot.command(name="dkpsubtract")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def dkp_subtract(ctx, dkp_value: int, *names: str):
    # Check if at least one member or nickname is provided
    if len(names) == 0:
        await ctx.send("Please provide at least one user or nickname.")
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
                nicknames[row[1]] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(ctx.guild.members, name=name)

            # If the name is not a user mention, check if it's a nickname
            if member is None and name in nicknames:
                member_name = nicknames[name]  # Get the corresponding username
                member = discord.utils.get(ctx.guild.members, name=member_name)

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

# Error handler for MissingRole
@dkp_subtract.error
async def dkp_subtract_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")

# Removes DKP from both current and lifetime
@bot.command(name="dkpsubtractboth")
@commands.has_role("DKP Keeper")  # Restrict the command to users with the "DKP Keeper" role
async def dkp_subtract_both(ctx, dkp_value: int, *names: str):
    # Check if at least one member or nickname is provided
    if len(names) == 0:
        await ctx.send("Please provide at least one user or nickname.")
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
                nicknames[row[1]] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    csv_file = message.attachments[0]
    csv_data = await download_csv(csv_file)

    if csv_data is None:
        await ctx.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = ctx.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(ctx.guild.members, name=name)

            # If the name is not a user mention, check if it's a nickname
            if member is None and name in nicknames:
                member_name = nicknames[name]  # Get the corresponding username
                member = discord.utils.get(ctx.guild.members, name=member_name)

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

# Error handler for MissingRole
@dkp_subtract_both.error
async def dkp_subtract_both_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")


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
                        nicknames[row[1]] = row[0]  # Nickname -> Username mapping

                # If the name is a nickname, get the actual username
                if name in nicknames:
                    member_name = nicknames[name]
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
        writer.writerow(["Username", "Nickname"])  # Header
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

    # Modify or add the nickname
    updated = False
    for row in csv_data:
        if row[0] == member.name:  # Match by username
            row[1] = nickname  # Update the nickname
            updated = True
            break

    # If the user was not found, add them to the CSV
    if not updated:
        csv_data.append([member.name, nickname])  # Add new user with nickname

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
    await ctx.send(f"Nickname for {member.display_name} has been set to {nickname}.")


# Error handler for MissingRole
@set_nickname.error
async def set_nickname_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("DKP Keeper role is required to use that command.")

async def handle_attendance_command(message):
    # Check if the user has the DKP Keeper role
    role = discord.utils.get(message.author.roles, name="DKP Keeper")
    if role is None:
        await message.channel.send("You need the DKP Keeper role to use this command.")
        return

    # Extract boss and users from message content
    parts = message.content.split(" ")
    boss_command = parts[0][2:]  # Extract the boss part (e.g., 155/4)
    names = parts[1:]  # Get the remaining parts as names

    if len(names) == 0:
        await message.channel.send("Please mention at least one user or nickname.")
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
            # Create a dictionary of usernames mapped to their nicknames
            for row in nickname_csv_data:
                nicknames[row[1]] = row[0]  # Nickname -> Username mapping

    # Download and parse the Balances CSV file
    balance_csv_file = balance_message.attachments[0]
    balance_csv_data = await download_csv(balance_csv_file)

    if balance_csv_data is None:
        await message.channel.send("Could not download or parse the Balances_Database.csv file.")
        return

    # Modify the data for each name (which could be a username, mention, or a nickname)
    updated_members = []
    for name in names:
        # Check if the name is a mention
        if name.startswith("<@") and name.endswith(">"):
            # Extract user ID from the mention format
            user_id = name.replace("<@", "").replace(">", "").replace("!", "")
            member = message.guild.get_member(int(user_id))
        else:
            # If the name is not a mention, check if it's a nickname or username
            member = discord.utils.get(message.guild.members, name=name)

            # If the name is not a user mention, check if it's a nickname
            if member is None and name in nicknames:
                member_name = nicknames[name]  # Get the corresponding username
                member = discord.utils.get(message.guild.members, name=member_name)

        if member is None:
            await message.channel.send(f"Could not find user or nickname: {name}")
            continue

        # Update DKP for the found member
        updated = False
        current_balance = 0
        lifetime_balance = 0

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
            current_balance = dkp_value
            lifetime_balance = dkp_value
            balance_csv_data.append([member.name, str(current_balance), str(lifetime_balance)])  # Add new user

        updated_members.append(f"{member.display_name}")

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



# Dictionary to store active timers
active_timers = {}

# Dictionary to store active boss timers and their end times
active_boss_timers = {}

cancelled_timers = {}  # To track canceled timers

# Dictionary to store tasks for active timers
active_tasks = {}

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

# Function to read specific boss role toggle setting from config.csv
async def get_boss_role_setting(guild, boss_type):
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

    # Map the boss type to the role toggle setting name
    toggle_mapping = {
        "DL": "toggle_dl_role",
        "EDL": "toggle_edl_role",
        "Legacy": "toggle_legacy_role",
        "World Boss": "toggle_worldboss_role",
        "Ring Boss": "toggle_ringboss_role"
    }

    toggle_setting = toggle_mapping.get(boss_type)

    # Search for the corresponding role toggle setting
    for row in csv_data:
        if row[0] == toggle_setting:
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
    boss_command = f"!{boss_name}"

    # Check if the boss timer is active
    if boss_command not in active_boss_timers:
        await ctx.send(f"There is no active timer for boss {boss_name}.")
        return

    # Cancel the timer task and remove it from active timers
    await cancel_timer_logic(boss_command, ctx.guild)

    # Notify the channel that the timer has been canceled
    await ctx.send(f"The timer for boss {boss_name} has been canceled.")

async def handle_boss_timers(message):
    boss_name = message.content
    boss_info = boss_timers[boss_name]

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
    boss_info = boss_timers[boss_name]
    timer_duration = boss_info["timer"]
    window_duration = boss_info["window"]

    # Get the current time and calculate the end time for the timer and window
    timer_end = time.time() + timer_duration
    window_end = timer_end + window_duration

    # Add the boss to the active timers
    active_boss_timers[boss_name] = {"timer_end": timer_end, "window_end": window_end}

    # Format the timer duration
    minutes = timer_duration // 60
    if minutes >= 60:
        hours = minutes // 60
        remaining_minutes = minutes % 60
        time_left_str = f"{hours} hr {remaining_minutes} mins"
    else:
        time_left_str = f"{minutes} mins"

    # Notify about the timer start
    await message.channel.send(
        f"The boss {boss_name[1:]} timer has started! You will be notified in {time_left_str}."
    )

    # Start the timer logic as a task and store it
    task = asyncio.create_task(manage_boss_timers(message.guild, message.channel, boss_name, timer_end, window_end))
    active_tasks[boss_name] = task

async def manage_boss_timers(guild, channel, boss_name, timer_end, window_end):
    window_opened = False  # Track if the window has opened
    window_closed = False  # Track if the window has closed

    # Get the boss type from the boss_info
    boss_info = boss_timers.get(boss_name, {})
    boss_type = boss_info.get("type", "Unknown")

    # Check if the boss channel for the type is toggled on
    boss_channel_toggled_on = await get_boss_channel_setting(guild, boss_type)

    # Fetch the boss-specific channel if toggled on
    boss_channel_name = boss_type.lower().replace(" ", "-") + "-boss-alerts"  # Example: "dl-boss-alerts"
    boss_channel = discord.utils.get(guild.text_channels, name=boss_channel_name) if boss_channel_toggled_on else None

    # Use the specific boss channel if it's toggled on and exists, otherwise fallback to the original channel
    target_channel = boss_channel if boss_channel else channel

    # Check if the boss role for the type is toggled on
    boss_role_toggled_on = await get_boss_role_setting(guild, boss_type)

    # Fetch the boss-specific role if toggled on
    boss_role_name = boss_type.lower()  # Example: "dl", "edl", etc.
    boss_role = discord.utils.get(guild.roles, name=boss_role_name) if boss_role_toggled_on else None

    while True:
        current_time = time.time()

        # Handle notifications and embed updates in one loop
        if current_time < timer_end:
            await update_timers_embed_if_active(guild)

        elif timer_end <= current_time < window_end:
            if not window_opened:
                # Tag the role if it exists, otherwise use @everyone
                if boss_role:
                    await target_channel.send(f"{boss_role.mention} The window for boss {boss_name[1:]} has opened!")
                else:
                    await target_channel.send(f"@everyone The window for boss {boss_name[1:]} has opened!")
                window_opened = True
            await update_timers_embed_if_active(guild)

        elif current_time >= window_end and not window_closed:
            togglewindows = await get_togglewindows_setting(guild)
            if togglewindows:
                # Tag the role if it exists, otherwise use @everyone
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

    # Group bosses by type
    boss_types = {"DL": [], "EDL": [], "World Boss": [], "Ring Boss": [], "Legacy": []}

    # Loop over all bosses in the boss_timers dictionary
    for boss_name, boss_info in boss_timers.items():
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

        # Group the boss by its type
        boss_type = boss_info.get("type", "Unknown")
        boss_types[boss_type].append(f"{boss_name[1:]} — \u2003 {time_left_str}")  # Use em dash with more spaces

    # Add sections for each boss type to the embed
    for boss_type, bosses in boss_types.items():
        if bosses:
            embed.add_field(name=boss_type, value="\n".join(bosses), inline=False)

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

    # Map the channel names to the corresponding settings
    channel_mapping = {
        "dl": "toggle_dl",
        "edl": "toggle_edl",
        "legacy": "toggle_legacy",
        "worldboss": "toggle_worldboss",
        "ringboss": "toggle_ringboss"
    }

    # Verify the channel name is valid
    if channel_name not in channel_mapping:
        await ctx.send("Invalid channel name. Available options are: dl, edl, legacy, worldboss, ringboss.")
        return

    toggle_setting = channel_mapping[channel_name]
    channel_full_name = f"{channel_name}-boss-alerts"

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
                        ctx.guild.default_role: discord.PermissionOverwrite(send_messages=True),  # Block everyone else
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
    # Map the role names to proper settings and roles
    role_mapping = {
        "dl": "toggle_dl_role",
        "edl": "toggle_edl_role",
        "legacy": "toggle_legacy_role",
        "worldboss": "toggle_worldboss_role",
        "ringboss": "toggle_ringboss_role"
    }

    # Verify if the role name is valid
    if role_name not in role_mapping:
        await ctx.send("Invalid role name. Available options are: dl, edl, legacy, worldboss, ringboss.")
        return

    toggle_setting = role_mapping[role_name]
    full_role_name = role_name

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

    # Fetch the role (if it exists)
    target_role = discord.utils.get(ctx.guild.roles, name=full_role_name)

    # Find the corresponding setting and toggle it
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
    emoji_mapping = {
        "toggle_dl_role": "🐉",
        "toggle_edl_role": "🤖",
        "toggle_legacy_role": "🦵",
        "toggle_worldboss_role": "👹",
        "toggle_ringboss_role": "💍"
    }
    role_mapping = {
        "toggle_dl_role": "DL Notifications",
        "toggle_edl_role": "EDL Notifications",
        "toggle_legacy_role": "Legacy Notifications",
        "toggle_worldboss_role": "World Boss Notifications",
        "toggle_ringboss_role": "Ring Boss Notifications"
    }

    # Add fields for each role that is toggled on in the config file
    for row in csv_data:
        if row[0] in emoji_mapping and row[1].lower() == "true":
            embed.add_field(name=f"Press {emoji_mapping[row[0]]} to turn on {role_mapping[row[0]]}", value="\u200b", inline=False)

    # Edit the existing embed
    await role_embed_message.edit(embed=embed)

    # Clear existing reactions
    await role_embed_message.clear_reactions()

    # Add reactions for active roles
    for row in csv_data:
        if row[0] in emoji_mapping and row[1].lower() == "true":
            await role_embed_message.add_reaction(emoji_mapping[row[0]])


def get_role_from_emoji(guild, emoji):
    # Map emojis to roles
    emoji_role_mapping = {
        '🐉': "dl",
        '🤖': "edl",
        '🦵': "legacy",
        '👹': "worldboss",
        '💍': "ringboss"
    }

    # Check if the emoji is in the mapping and return the corresponding role
    role_name = emoji_role_mapping.get(str(emoji))
    if role_name:
        return discord.utils.get(guild.roles, name=role_name)
    return None

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

Where boss types are: dl, edl, ringboss, worldboss, legacy

By toggling on, users with these roles are the only ones notified when a timer for a boss of that type goes off instead of the default @ everyone.
For example, when on, the !155 timer command will result in: @dl The window for boss 155 has opened!
By default, all boss type roles are off.
To help assign users these roles, see the help page for the !togglerolechannel.
This command requires the DKP keeper role to use.""",

    'restorefromconfig': """This command is used to bring configuration choices across servers. 
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

This channel automatically has an embed within that updates on changes to the active boss timer roles. This approach is suggested if you are using roles to notify users to boss spawns.

This command requires the DKP keeper role to use."""
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
        `restorefromconfig`
        `createbackup`
        `restorefrombackup`
        """
        await ctx.send(help_message)
    else:
        response = help_dict.get(command, "Command not found. Please use `!help` for a list of commands.")
        await ctx.send(response)


# Start the bot using your bot token
bot.run('Put Bot Token Here')
