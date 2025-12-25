@echo off
REM =====================================================
REM RUN RETAIL ANALYTICS WEB APPLICATION
REM =====================================================
echo.
echo =====================================================
echo    BAT DAU KHOI TAO RETAIL WEB APPLICATION
echo =====================================================
echo.

echo [INFO] Dang xay dung va khoi dong container webapp...
docker-compose up -d --build webapp

echo.
echo [INFO] Dang cho webapp khoi dong...
timeout /t 10 /nobreak > nul

echo.
echo =====================================================
echo    WEB APPLICATION DA DUOC KHOI DONG!
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
