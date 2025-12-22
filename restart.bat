@echo off
REM =====================================================
REM SCRIPT XOA VA CHAY LAI HOAN TOAN
REM =====================================================
setlocal enabledelayedexpansion

color 0C
echo.
echo CANH BAO: THAO TAC NAY SE XOA MOI DU LIEU!
echo.
echo Ban sap xoa HOAN TOAN he thong Big Data gom:
echo - Tat ca containers
echo - Tat ca volumes (HDFS, PostgreSQL, MongoDB)
echo - Tat ca du lieu trong thu muc data/
echo.
set /p "confirm=Ban co CHAC CHAN muon xoa hoan toan? (nhap 'YES' de xac nhan): "

if not "!confirm!"=="YES" (
    echo.
    echo Da huy thao tac.
    pause
    exit /b 0
)

echo.
echo =====================================================
echo    DANG XOA HOAN TOAN HE THONG...
echo =====================================================
echo.

REM 1. Dung va xoa containers
echo [1/7] Dung va xoa tat ca containers...
docker-compose down -v --remove-orphans
echo [OK] Da xoa containers
echo.

REM 2. Xoa volumes
echo [2/7] Xoa Docker volumes...
for /f "tokens=*" %%v in ('docker volume ls -q ^| findstr "bigdatafinal"') do (
    echo   Xoa volume: %%v
    docker volume rm %%v 2>nul
)
echo [OK] Da xoa volumes
echo.

REM 3. Xoa network
echo [3/7] Xoa Docker network...
docker network rm bigdatafinal_bigdata-network 2>nul
echo [OK] Da xoa network
echo.

REM 4. Xoa thu muc du lieu
echo [4/7] Xoa thu muc du lieu...
if exist "data" (
    echo   Xoa: data\
    rmdir /s /q "data"
)
if exist "hue-logs" (
    echo   Xoa: hue-logs\
    rmdir /s /q "hue-logs"
)
if exist "hue-data" (
    echo   Xoa: hue-data\
    rmdir /s /q "hue-data"
)
echo [OK] Da xoa thu muc du lieu
echo.

REM 5. Xu ly config (co the xoa hoac giu)
echo [5/7] Xu ly thu muc config...
set /p "giu_config=Giu lai file cau hinh? (y/N): "
if /i "!giu_config!"=="y" (
    echo [INFO] Giu lai thu muc config
) else (
    if exist "config" (
        echo   Xoa: config\
        rmdir /s /q "config"
    )
    echo [OK] Da xoa config
)
echo.

REM 6. Xoa images (tuy chon)
echo [6/7] Xu ly Docker images...
set /p "xoa_images=Xoa Docker images de tai lai? (y/N): "
if /i "!giu_images!"=="y" (
    echo Dang xoa images...
    docker rmi bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 2>nul
    docker rmi bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 2>nul
    docker rmi bde2020/hive:2.3.2-postgresql-metastore 2>nul
    docker rmi bde2020/spark-master:2.4.8-hadoop3.2 2>nul
    docker rmi bde2020/spark-worker:2.4.8-hadoop3.2 2>nul
    docker rmi mongo:6.0 2>nul
    docker rmi mongo-express:latest 2>nul
    docker rmi gethue/hue:4.10.0 2>nul
    docker rmi jupyter/pyspark-notebook:spark-2.4.8 2>nul
    docker rmi postgres:14-alpine 2>nul
    echo [OK] Da xoa images
)
echo.

REM 7. Don dep he thong
echo [7/7] Don dep he thong Docker...
docker system prune -f
echo [OK] Da don dep he thong
echo.

echo =====================================================
echo    DA XOA HOAN TOAN THANH CONG!
echo =====================================================
echo.
echo Bay gio ban co the:
echo 1. Chay 'start.bat' de khoi dong lai tu dau
echo 2. Hoac tai lai du lieu moi
echo.
echo Luu y: Tat ca du lieu HDFS, Hive, MongoDB da bi xoa!
echo.
pause

REM Tu dong chay lai neu muon
echo.
set /p "chay_lai=Ban muon chay lai he thong ngay bay gio? (y/N): "
if /i "!chay_lai!"=="y" (
    echo.
    echo =====================================================
    echo    DANG KHOI DONG LAI HE THONG...
    echo =====================================================
    echo.
    call start.bat
)