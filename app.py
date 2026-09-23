from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pathlib import Path
import json

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent

clients = {}


@app.get("/")
async def home():
    return FileResponse(BASE_DIR / "static" / "index.html")


async def broadcast(data):
    disconnected = []

    for websocket in list(clients.keys()):
        try:
            await websocket.send_text(
                json.dumps(data, ensure_ascii=False)
            )
        except Exception:
            disconnected.append(websocket)

    for websocket in disconnected:
        clients.pop(websocket, None)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    await websocket.accept()

    username = None

    try:
        # Primer mensaje = nombre del usuario
        raw = await websocket.receive_text()
        data = json.loads(raw)

        if data.get("type") != "join":
            await websocket.close()
            return

        username = data.get("username", "").strip()

        if not username:
            username = "Anónimo"

        username = username[:30]

        clients[websocket] = username

        print(f"{username} conectado")

        await broadcast({
            "type": "system",
            "text": f"{username} se unió al chat."
        })

        while True:

            raw = await websocket.receive_text()
            data = json.loads(raw)

            if data.get("type") == "message":

                text = data.get("text", "").strip()

                if not text:
                    continue

                text = text[:2000]

                print(f"{username}: {text}")

                await broadcast({
                    "type": "message",
                    "username": username,
                    "text": text
                })

    except WebSocketDisconnect:
        pass

    except Exception as e:
        print("Error:", e)

    finally:

        if websocket in clients:
            clients.pop(websocket)

        if username:

            print(f"{username} desconectado")

            await broadcast({
                "type": "system",
                "text": f"{username} salió del chat."
            })
