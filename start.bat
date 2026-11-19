@echo off
chcp 65001 >nul
echo ========================================
echo   Подслушано - Telegram Bot Launcher
echo ========================================
echo.

if not exist "venv\" (
    echo Создание виртуального окружения...
    python -m venv venv
    if errorlevel 1 (
        echo Ошибка: Python не найден. Установите Python 3.10+
        pause
        exit /b 1
    )
)

call venv\Scripts\activate.bat

echo Установка зависимостей...
pip install -r requirements.txt --quiet

echo.
echo Запуск бота...
echo Для остановки нажмите Ctrl+C
echo.

python bot.py

pause
