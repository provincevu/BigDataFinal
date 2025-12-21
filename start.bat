@echo off
REM =====================================================
REM RETAIL BIG DATA PIPELINE - STARTUP SCRIPT
REM =====================================================
echo.
echo =====================================================
echo    RETAIL BIG DATA PIPELINE - STARTING UP
echo =====================================================
echo.

REM Kiểm tra Docker đang chạy
docker info > nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not running! Please start Docker Desktop first.
    pause
    exit /b 1
)

echo [INFO] Docker is running...
echo.

REM Di chuyển dữ liệu vào thư mục data
if not exist "data" mkdir data
if exist "online_retail.csv" (
    echo [INFO] Moving online_retail.csv to data folder...
    copy "online_retail.csv" "data\online_retail.csv" > nul
)

REM Tạo các thư mục cần thiết
echo [INFO] Creating required directories...
if not exist "config" mkdir config
if not exist "spark-apps" mkdir spark-apps
if not exist "notebooks" mkdir notebooks
if not exist "mongo-init" mkdir mongo-init
if not exist "hive-queries" mkdir hive-queries

echo.
echo [INFO] Starting Docker Compose services...
echo [INFO] This may take several minutes on first run...
echo.

REM Start services
docker-compose up -d

echo.
echo [INFO] Waiting for services to be ready...
timeout /t 30 /nobreak > nul

echo.
echo =====================================================
echo    SERVICES STARTED SUCCESSFULLY!
echo =====================================================
echo.
echo Access the following URLs:
echo.
echo   📊 HDFS NameNode:        http://localhost:9870
echo   💾 HDFS DataNode:        http://localhost:9864
echo   ⚡ Spark Master:         http://localhost:8080
echo   ⚡ Spark Worker:         http://localhost:8081
echo   🌐 Hue (Web GUI):        http://localhost:8888
echo   📒 Jupyter Notebook:     http://localhost:8889
echo   🍃 MongoDB Express:      http://localhost:8082
echo.
echo MongoDB Credentials:
echo   - Username: admin
echo   - Password: admin123
echo.
echo Hive Server:
echo   - Host: localhost
echo   - Port: 10000
echo.
echo =====================================================
echo.
echo [TIP] Run 'docker-compose logs -f' to view logs
echo [TIP] Run 'stop.bat' to stop all services
echo.
pause
