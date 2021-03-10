import asyncio
import requests
from util.db import db
from util.config import config
from datetime import datetime
from flask import Flask, json
import threading

app = Flask(__name__)

loop = asyncio.get_event_loop()
history = db["history"]


async def query_one(name, addr):
    future = loop.run_in_executor(
        None, requests.get, "http://{}/api/gpu_info".format(addr))
    r = await future
    if r.status_code == 200:
        gpu_info = r.json()
        history.insert_one({"name": name, "stat": gpu_info,
                            "time": datetime.utcnow()})


async def query():
    for client in config["client"]:
        loop.create_task(
            query_one(client["name"], client["address"]))


async def query_loop():
    while True:
        asyncio.create_task(query())
        await asyncio.sleep(300)


@app.route("/api/history", methods=["GET"])
def get_history():
    result = {}
    for client in config["client"]:
        name = client["name"]
        result[name] = []
        for item in history.find({"name": name}, {"_id": 0}).sort(
                [("_id", -1)]).limit(30):
            result[name].append(item)
    return json.dumps(result)


def main():
    loop.create_task(query_loop())
    threading.Thread(target=loop.run_forever).start()
    app.run(host="0.0.0.0", port=12046, debug=False)


main()
