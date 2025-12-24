@echo off
chcp 65001 >nul 2>&1
REM =====================================================
REM RETAIL BIG DATA PIPELINE - SCRIPT KHOI DONG
REM =====================================================

echo.
echo =====================================================
echo    HE THONG BIG DATA BAN LE - DANG KHOI DONG
echo =====================================================
echo.

REM Kiem tra Docker dang chay
echo [1/5] Kiem tra Docker Desktop...
docker version >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Docker Desktop khong chay!
    echo Vui long mo Docker Desktop va cho no khoi dong xong.
    pause
    exit /b 1
)
echo [OK] Docker Desktop dang chay
echo.

REM Kiem tra file du lieu
echo [2/5] Kiem tra file du lieu...
if not exist "data" mkdir data
if exist "online_retail.csv" (
    if not exist "data\online_retail.csv" (
        copy "online_retail.csv" "data\online_retail.csv" >nul
        echo [OK] Da sao chep online_retail.csv vao thu muc data
    )
)
echo [OK] File du lieu san sang
echo.

REM Dung container cu neu co
echo [3/5] Don dep container cu...
docker-compose down >nul 2>&1
echo [OK] Da don dep
echo.

REM Khoi dong Docker services
echo [4/5] Khoi dong Docker Compose services...
echo Lan dau chay co the mat 3-5 phut de tai images...
echo.
docker-compose up -d
if %errorlevel% neq 0 (
    echo [LOI] Khong the khoi dong services!
    echo Chay 'docker-compose logs' de xem chi tiet loi.
    pause
    exit /b 1
)
echo.
echo [OK] Docker services da khoi dong
echo.

REM Doi services san sang
echo [5/5] Doi services khoi tao (90 giay)...
echo Dang khoi tao Hadoop, Spark, MongoDB...
timeout /t 90 /nobreak
echo.
echo [OK] Services da san sang!
echo.

REM Hien thi trang thai
echo =====================================================
echo    TRANG THAI SERVICES
echo =====================================================
docker-compose ps
echo.

REM Hien thi thong tin truy cap
echo =====================================================
echo    THONG TIN TRUY CAP
echo =====================================================
echo.
echo  HADOOP HDFS:
echo    - NameNode UI:     http://localhost:9870
echo    - DataNode 1 UI:     http://localhost:9864
echo    - DataNode 2 UI:     http://localhost:9865
echo.
echo  SPARK:
echo    - Master UI:       http://localhost:8580
echo    - Worker 1 UI:     http://localhost:8581
echo    - Worker 2 UI:     http://localhost:8582
echo.
echo  MONGODB:
echo    - Mongo Express:   http://localhost:8290
echo      User: admin / Pass: admin123
echo.
echo  WEB APPLICATION:
echo    - Retail Analytics: http://localhost:5555
echo.
echo.
echo =====================================================
echo    LENH HUU ICH
echo =====================================================
echo.
echo  Xem logs:        docker-compose logs -f
echo  Dung he thong:   docker-compose down
echo  Chay ETL:        run-etl.bat
echo  Chay Clustering: run-clustering.bat
echo.
echo =====================================================
echo    KHOI DONG HOAN TAT!
echo =====================================================
echo.
pause