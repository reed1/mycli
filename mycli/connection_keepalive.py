import os
import pymysql
import threading
import time

state = {
    "connection": None,
    "thread": None,
}


def keepalive(conn: pymysql.connections.Connection):
    state["connection"] = conn
    if state["thread"] is None:
        state["thread"] = threading.Thread(target=keepalive_thread, daemon=True)
        state["thread"].start()


def keepalive_thread():
    while True:
        time.sleep(30)
        if state["connection"]:
            state["connection"].ping()
