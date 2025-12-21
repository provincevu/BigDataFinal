@echo off
REM =====================================================
REM RETAIL BIG DATA PIPELINE - STOP SCRIPT
REM =====================================================
echo.
echo =====================================================
echo    STOPPING ALL SERVICES...
echo =====================================================
echo.

docker-compose down

echo.
echo [INFO] All services stopped.
echo.
echo [TIP] Run 'start.bat' to start services again
echo [TIP] Run 'docker-compose down -v' to also remove volumes
echo.
pause
