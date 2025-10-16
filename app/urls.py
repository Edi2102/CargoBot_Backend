from django.urls import path
from . import views

urlpatterns = [
    path("api/active-products/", views.active_products),
    path("api/deleted-products/", views.deleted_products),
    path("api/cargo/ids", views.get_user_ids),
    path("api/cargo/meta", views.get_cargo_meta),
    path("api/page-ping/", views.page_ping),
    path("api/greenlight/", views.greenlight_check),
    path("api/press-ack/", views.press_ack),
    path("api/greenlight/set", views.set_greenlight),
    path("api/ping/active", views.ping_active),
    path("api/ping/deleted", views.ping_deleted),
    path("api/greenlight/delete", views.delete_greenlight),
]
