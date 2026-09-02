import asyncio
import binascii
import json
import locale
import os
import queue
import random
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta

import aiohttp
import discord
import requests
import streamlit as st
import websockets
from aiohttp.client import ClientTimeout
from discord import app_commands
from discord.ext import commands, tasks
from discord.utils import get
from dotenv import load_dotenv
from google.protobuf.internal.encoder import _VarintBytes
from streamlit.proto.ForwardMsg_pb2 import ForwardMsg
from yarl import URL

import server
from guild import *

load_dotenv()

if "log_queue" not in st.session_state:
    st.session_state["log_queue"] = queue.Queue()

if "logs" not in st.session_state:
    st.session_state["logs"] = []

if "task_running" not in st.session_state:
    st.session_state["task_running"] = False
GUILD_ID = 1122707918177960047
BOT_NAME = "appvvrite"
SESSION_ID = None
SESSION_ID_OLD = None
LAST_UPDATE = None
LAST_MSG = None
HEADERS = []

RESULT = None
URL_STREAM = "https://keep-sl-online-d7bnwfpjbw9cw23yreygwk.streamlit.app/"
RESTART_LOOP = random.randrange(12, 18, 1)
NEXT_TIME = False
timeout = 30


def _emit(log_queue, level, message):
    """Push a log line to the Streamlit UI queue (safe no-op if queue is None)."""
    print(f"[{level}] {message}")
    if log_queue is not None:
        try:
            log_queue.put((level, f"{datetime.now()} {message}"))
        except Exception:
            pass


def myStyle(log_queue):
    APPVVRITE_API = os.getenv("appvvrite_api")
    APPVVRITE_PROJECT_ID = os.getenv("appvvwrite_project_id")
    APPVVRITE_TABLE_ID = os.getenv("appvvwrite_table_id")
    BOT_TOKEN = os.getenv("bot_token")

    MY_GUILD = discord.Object(id=GUILD_ID)  # replace with your guild id

    class MyClient(discord.Client):
        # Suppress error on the User attribute being None since it fills up later
        user: discord.ClientUser

        def __init__(self, *, intents: discord.Intents):
            super().__init__(intents=intents)
            # A CommandTree is a special type that holds all the application command
            # state required to make it work. This is a separate class because it
            # allows all the extra state to be opt-in.
            # Whenever you want to work with application commands, your tree is used
            # to store and work with them.
            # Note: When using commands.Bot instead of discord.Client, the bot will
            # maintain its own tree instead.
            self.tree = app_commands.CommandTree(self)

        # In this basic example, we just synchronize the app commands to one guild.
        # Instead of specifying a guild to every command, we copy over our global commands instead.
        # By doing so, we don't have to wait up to an hour until they are shown to the end-user.
        async def setup_hook(self):
            # This copies the global commands over to your guild.
            self.tree.copy_global_to(guild=MY_GUILD)
            await self.tree.sync(guild=MY_GUILD)

    intents = discord.Intents.default()
    client = MyClient(intents=intents)
    log_queue.put(("info", "Starting process data..."))
    intents = discord.Intents.all()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        global RESULT
        try:
            req = requests.get("http://localhost:8888")
            log_queue.put(
                (
                    "info",
                    f"Phát hiện instance khác đang chạy (status {req.status_code}), dừng instance này",
                )
            )
            await client.close()
            return
        except Exception as error:
            print(error)
            server.b()
            if not keepLive.is_running():
                keepLive.start()

    @tasks.loop(seconds=30)
    async def keepLive():
        url = f"https://sgp.cloud.appwrite.io/v1/tablesdb/{APPVVRITE_TABLE_ID}/tables/sessions/rows"
        headers = {
            "X-Appwrite-Project": APPVVRITE_PROJECT_ID,
            "Content-Type": "application/json",
            "X-Appwrite-Key": APPVVRITE_API,
        }
        async with aiohttp.ClientSession(
            cookie_jar=aiohttp.CookieJar(), timeout=ClientTimeout(30)
        ) as session:
            async with session.post(
                url,
                headers=headers,
                json={
                    "databaseId": APPVVRITE_TABLE_ID,
                    "tableId": "sessions",
                    "rowId": "hello",
                    "data": {"pingAt": "hello"},
                },
            ) as res:
                if res.status >= 400:
                    _emit(
                        log_queue, "error", f"keepLive create row error: {res.status}"
                    )
                else:
                    _emit(
                        log_queue,
                        "success",
                        f"keepLive create row success: {res.status}",
                    )
                async with session.delete(
                    url,
                    headers=headers,
                    json={
                        "databaseId": APPVVRITE_TABLE_ID,
                        "tableId": "sessions",
                        "rowId": "hello",
                    },
                ) as res:
                    if res.status >= 400:
                        _emit(
                            log_queue,
                            "error",
                            f"keepLive delete row error: {res.status}",
                        )
                    else:
                        _emit(
                            log_queue,
                            "success",
                            f"keepLive delete row success: {res.status}",
                        )

    client.run(BOT_TOKEN)


def run_bot_forever(log_queue):
    """Watchdog: chạy myStyle trong loop, tự restart nếu thread chết vì lỗi."""
    retry_count = 0
    while True:
        try:
            log_queue.put(("info", f"Khởi động bot (lần thử #{retry_count + 1})"))
            myStyle(log_queue)  # đây là hàm blocking (client.run() bên trong)
            # nếu chạy tới đây tức là client.run() thoát êm (VD: client.close() bình thường)
            log_queue.put(("info", "Bot đã dừng bình thường"))
        except SystemExit as e:
            log_queue.put(("error", f"Bot bị SystemExit: {e}"))
        except Exception as e:
            log_queue.put(("error", f"Bot crash: {e}"))

        retry_count += 1
        wait = min(60, 2 ** min(retry_count, 6))  # backoff: 2,4,8,...tối đa 60s
        log_queue.put(("info", f"Tự động restart sau {wait}s..."))
        time.sleep(wait)


thread = None


@st.cache_resource
def initialize_heavy_stuff():
    global thread
    # Đây là phần chỉ chạy ĐÚNG 1 LẦN khi server khởi động (hoặc khi cache miss)
    with st.spinner("running your scripts..."):
        thread = threading.Thread(
            target=run_bot_forever,  # <-- đổi từ myStyle sang wrapper này
            args=(st.session_state.log_queue,),
            daemon=True,  # để thread không giữ process sống khi app tắt
        )
        thread.start()
        print(
            "Heavy initialization running..."
        )  # bạn sẽ thấy log này chỉ 1 lần trong console/cloud log

        return {
            "model": "loaded_successfully",
            "timestamp": time.time(),
            "db_status": "connected",
        }


# Trong phần chính của app
st.title("my style")

# Dòng này đảm bảo: chạy 1 lần duy nhất, mọi user đều dùng chung kết quả
result = initialize_heavy_stuff()

st.success("The system is ready!")
st.write("Result:")
st.json(result)
with st.status("Processing...", expanded=True) as status:
    placeholder = st.empty()
    logs = []
    while (thread and thread.is_alive()) or not st.session_state.log_queue.empty():
        try:
            level, message = st.session_state.log_queue.get_nowait()
            logs.append((level, message))

            with placeholder.container():
                for lvl, msg in logs:
                    if lvl == "info":
                        st.write(msg)
                    elif lvl == "success":
                        st.success(msg)
                    elif lvl == "error":
                        st.error(msg)

            time.sleep(0.2)
        except queue.Empty:
            time.sleep(0.3)

    status.update(label="Hoàn thành!", state="complete", expanded=False)
