import asyncio
import aiohttp
import json
from channels.generic.websocket import AsyncWebsocketConsumer
from django.utils import timezone
import os

ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://localhost:8000/api/orders/mergepot/")
FRANCHISE_SERVICE_URL = os.getenv("FRANCHISE_SERVICE_URL", "http://localhost:8000/api/franchises/mergepot/")

class MergePotOrderConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.franchise_connections = {}
        self.session = None
        self._stop = False

    async def connect(self):
        try:
            if not self.session or self.session.closed:
                self.session = aiohttp.ClientSession()

            async with self.session.get(FRANCHISE_SERVICE_URL, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    franchises = data.get("data", [])
                else:
                    await self.close()
                    return

            # Start WS connections for each franchise with an API key
            for f in franchises:
                franchise_id = f.get("id")
                api_key = f.get("mergepot_api_key")

                if not api_key:
                    continue

                self.franchise_connections[franchise_id] = {
                    "api_key": api_key,
                    "ws_task": asyncio.create_task(self._ws_reconnect_loop(franchise_id, api_key)),
                    "keepalive_task": None,
                }

            if not self.franchise_connections:
                await self.close()
                return

        except Exception:
            await self.close()
            return

        await self.accept()
        self._stop = False

        await self.send(text_data=json.dumps({
            'type': 'connection_established',
            'message': f'Connected to Gateway WS for franchises: {list(self.franchise_connections.keys())}'
        }))

    async def disconnect(self, close_code):
        self._stop = True
        # Cancel all franchise WS + keepalive tasks
        for f_id, conn in self.franchise_connections.items():
            if conn.get("ws_task"):
                conn["ws_task"].cancel()
            if conn.get("keepalive_task"):
                conn["keepalive_task"].cancel()

        if self.session:
            try:
                await self.session.close()
            except Exception:
                pass

    async def _ws_reconnect_loop(self, franchise_id, api_key):
        backoff = 1
        max_backoff = 30

        while not self._stop:
            try:
                await self.listen_mergepot_ws(franchise_id, api_key)
                backoff = 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.send(text_data=json.dumps({
                    'type': 'error',
                    'message': f'WS listener crashed for franchise {franchise_id}: {str(e)}'
                }))

            if self._stop:
                break

            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

    async def listen_mergepot_ws(self, franchise_id, api_key):
        if not api_key:
            raise ValueError(f"API key is missing for franchise {franchise_id}")

        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()

        url = "wss://socket.ordering.mergeport.com/v4"

        try:
            async with self.session.ws_connect(url, protocols=[api_key], timeout=30) as ws:
                # Start keepalive
                if self.franchise_connections[franchise_id].get("keepalive_task"):
                    self.franchise_connections[franchise_id]["keepalive_task"].cancel()
                self.franchise_connections[franchise_id]["keepalive_task"] = asyncio.create_task(self._keepalive_sender(ws))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue

                        await self.send(text_data=json.dumps({
                            "type": "mergepot_payload",
                            "franchise_id": franchise_id,
                            "payload": payload
                        }))

                        # Normalize order status to "new" if order exists
                        if "data" in payload and "order" in payload["data"]:
                            payload_for_db = payload.copy()
                            payload_for_db["data"]["order_status_for_db"] = "new"

                        await self.forward_to_order_service(franchise_id, payload)

                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break

                # Cleanup keepalive
                if self.franchise_connections[franchise_id].get("keepalive_task"):
                    self.franchise_connections[franchise_id]["keepalive_task"].cancel()
                    self.franchise_connections[franchise_id]["keepalive_task"] = None

        except asyncio.CancelledError:
            if self.franchise_connections[franchise_id].get("keepalive_task"):
                self.franchise_connections[franchise_id]["keepalive_task"].cancel()
                self.franchise_connections[franchise_id]["keepalive_task"] = None
            raise
        except Exception as e:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Error connecting/reading Mergepot WS for franchise {franchise_id}: {str(e)}'
            }))
            if self.franchise_connections[franchise_id].get("keepalive_task"):
                self.franchise_connections[franchise_id]["keepalive_task"].cancel()
                self.franchise_connections[franchise_id]["keepalive_task"] = None
            raise

    async def _keepalive_sender(self, ws):
        try:
            while True:
                await asyncio.sleep(60)
                try:
                    await ws.send_json({"action": "ka", "data": "ping"})
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    async def forward_to_order_service(self, franchise_id, payload):
        try:
            async with self.session.post(
                ORDER_SERVICE_URL,
                json={"franchise_id": franchise_id, "payload": payload},
                timeout=10
            ) as resp:
                if resp.status == 200:
                    await self.send(text_data=json.dumps({
                        'type': 'order_forwarded',
                        'status': 'success',
                        'franchise_id': franchise_id,
                        'payload': payload,
                        'timestamp': timezone.now().isoformat()
                    }))
                else:
                    error_text = await resp.text()
                    await self.send(text_data=json.dumps({
                        'type': 'error',
                        'message': f'Order service rejected: {error_text}',
                        'franchise_id': franchise_id,
                        'payload': payload
                    }))
        except Exception as e:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Failed to forward to Order Service for franchise {franchise_id}: {str(e)}',
                'franchise_id': franchise_id,
                'payload': payload
            }))
