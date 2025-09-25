from django.urls import re_path
from .consumers import MergePotOrderConsumer

websocket_urlpatterns = [
    re_path(r"socket/mergepot/orders/$", MergePotOrderConsumer.as_asgi()),
]
