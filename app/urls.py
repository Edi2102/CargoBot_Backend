from django.urls import path
from .views import active_products

urlpatterns = [
    path('active-products/', active_products),
]
