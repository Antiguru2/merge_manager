from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin


class SuperuserRequiredTemplateView(LoginRequiredMixin, UserPassesTestMixin, TemplateView):
    """Базовый класс для представлений, доступных только суперпользователям"""
    
    def test_func(self):
        """Проверяет, является ли пользователь суперпользователем"""
        return self.request.user.is_superuser