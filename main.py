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

APPVVRITE_API = os.getenv("appvvrite_api")
APPVVRITE_PROJECT_ID = os.getenv("appvvwrite_project_id")
APPVVRITE_TABLE_ID = os.getenv("appvvwrite_table_id")
BOT_TOKEN = os.getenv("bot_token")


def _emit(log_queue, level, message):
    """Push a log line to the Streamlit UI queue (safe no-op if queue is None)."""
    print(f"[{level}] {message}")
    if log_queue is not None:
        try:
            log_queue.put((level, message))
        except Exception:
            pass


def myStyle(log_queue):
    intents = discord.Intents.all()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        global RESULT
        try:
            req = requests.get("http://localhost:8501")
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
            cookie_jar=aiohttp.CookieJar(), timeout=timeout
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


@st.cache_resource
def initialize_heavy_stuff():
    """
    Runs exactly once per server process (cache_resource). The thread object
    itself is returned as part of the cached result - that's the only way to
    keep a reference to it across Streamlit reruns, since every top-level
    variable in this script gets re-executed (and reset) on every rerun.
    """
    with st.spinner("running your scripts..."):
        t = threading.Thread(
            target=myStyle, args=(st.session_state.log_queue,), daemon=True
        )
        t.start()
        print("Heavy initialization running...")
        return {
            "thread": t,
            "model": "loaded_successfully",
            "timestamp": time.time(),
            "db_status": "connected",
        }


st.title("my style")

result = initialize_heavy_stuff()
thread = result["thread"]

st.success("The system is ready!")
st.write("Result:")
st.json({k: v for k, v in result.items() if k != "thread"})

with st.status("Processing...", expanded=True) as status:
    placeholder = st.empty()
    logs = []
    while thread.is_alive() or not st.session_state.log_queue.empty():
        try:
            level, message = st.session_state.log_queue.get_nowait()
            logs.append((level, message))
            with placeholder.container():
                for lvl, msg in logs[-200:]:
                    if lvl == "info":
                        st.write(msg)
                    elif lvl == "success":
                        st.success(msg)
                    elif lvl == "error":
                        st.error(msg)
        except queue.Empty:
            time.sleep(0.3)

    status.update(label="Bot is running", state="complete", expanded=False)
