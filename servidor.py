import asyncio
import json
import os

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


clients = {}


async def broadcast(data):
    if not clients:
        return

    message = json.dumps(data, ensure_ascii=False)

    await asyncio.gather(
        *(client.send(message) for client in list(clients)),
        return_exceptions=True
    )


async def handler(websocket):

    username = None

    try:
        # El primer mensaje debe ser el registro del usuario
        raw = await websocket.recv()
        data = json.loads(raw)

        if data.get("type") != "join":
            await websocket.close(
                code=1008,
                reason="Debes identificarte primero."
            )
            return

        username = data.get("username", "").strip()

        if not username:
            username = "Anónimo"

        # Limitar tamaño del nombre
        username = username[:30]

        clients[websocket] = username

        print(f"{username} conectado.")

        await broadcast({
            "type": "system",
            "text": f"{username} se conectó al chat."
        })

        # Escuchar mensajes del cliente
        async for raw in websocket:

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if data.get("type") != "message":
                continue

            text = data.get("text", "").strip()

            if not text:
                continue

            # Limitar mensajes
            text = text[:2000]

            print(f"{username}: {text}")

            await broadcast({
                "type": "message",
                "username": username,
                "text": text
            })

    except ConnectionClosed:
        pass

    finally:

        if websocket in clients:
            del clients[websocket]

        if username:

            print(f"{username} desconectado.")

            await broadcast({
                "type": "system",
                "text": f"{username} salió del chat."
            })


async def main():

    # Render asigna el puerto automáticamente
    port = int(os.environ.get("PORT", 8765))

    print(f"Servidor iniciado en puerto {port}")

    async with serve(
        handler,
        "0.0.0.0",
        port,
        ping_interval=20,
        ping_timeout=20,
        max_size=65536
    ):

        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
