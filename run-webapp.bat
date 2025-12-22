@echo off
REM =====================================================
REM RUN RETAIL ANALYTICS WEB APPLICATION
REM =====================================================
echo.
echo =====================================================
echo    STARTING RETAIL ANALYTICS WEB APPLICATION
echo =====================================================
echo.

echo [INFO] Building and starting webapp container...
docker-compose up -d --build webapp

echo.
echo [INFO] Waiting for webapp to start...
timeout /t 10 /nobreak > nul

echo.
echo =====================================================
echo    WEB APPLICATION STARTED!
echo =====================================================
echo.
echo    URL: http://localhost:5000
echo.
echo    Cac trang co san:
echo    - Dashboard:     http://localhost:5000/
echo    - Khach Hang:    http://localhost:5000/customers
echo    - Phan Khuc:     http://localhost:5000/segments
echo    - San Pham:      http://localhost:5000/products
echo    - Doanh Thu:     http://localhost:5000/revenue
echo    - Quoc Gia:      http://localhost:5000/countries
echo    - Goi Y:         http://localhost:5000/recommendations
echo.
echo    API Endpoints:
echo    - /api/stats
echo    - /api/customers
echo    - /api/segments
echo    - /api/revenue/monthly
echo    - /api/products/top
echo    - /api/countries
echo.
echo =====================================================
pause
