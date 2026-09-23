import asyncio
import json

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


# CAMBIAREMOS ESTA DIRECCIÓN CUANDO SUBAMOS EL SERVIDOR
SERVER = "ws://localhost:8765"


async def receive_messages(websocket):

    try:

        async for raw in websocket:

            data = json.loads(raw)

            if data.get("type") == "message":

                username = data.get("username")
                text = data.get("text")

                print(f"\n{username}: {text}")

            elif data.get("type") == "system":

                print(f"\n*** {data.get('text')} ***")

    except ConnectionClosed:
        print("\nConexión perdida.")


async def send_messages(websocket):

    while True:

        text = await asyncio.to_thread(input, "")

        if text.lower() == "/quit":

            await websocket.close()
            return

        await websocket.send(
            json.dumps({
                "type": "message",
                "text": text
            })
        )


async def chat():

    username = input("Nombre de usuario: ").strip()

    print(f"Conectando a {SERVER}...")

    try:

        async with connect(
            SERVER,
            ping_interval=20,
            ping_timeout=20
        ) as websocket:

            # Enviar identificación
            await websocket.send(
                json.dumps({
                    "type": "join",
                    "username": username
                })
            )

            print("Conectado.")
            print("Escribe /quit para salir.\n")

            receiver = asyncio.create_task(
                receive_messages(websocket)
            )

            sender = asyncio.create_task(
                send_messages(websocket)
            )

            done, pending = await asyncio.wait(
                [receiver, sender],
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

    except Exception as error:

        print("No se pudo conectar al servidor.")
        print(error)


async def main():
    await chat()


if __name__ == "__main__":
    asyncio.run(main())