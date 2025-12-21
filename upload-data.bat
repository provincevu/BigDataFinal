@echo off
REM =====================================================
REM UPLOAD DATA TO HDFS
REM =====================================================
echo.
echo =====================================================
echo    UPLOADING DATA TO HDFS
echo =====================================================
echo.

REM Chờ HDFS sẵn sàng
echo [INFO] Checking HDFS availability...
timeout /t 5 /nobreak > nul

REM Tạo thư mục trên HDFS
echo [INFO] Creating HDFS directories...
docker exec namenode hdfs dfs -mkdir -p /user/retail
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse

REM Upload dữ liệu
echo [INFO] Uploading online_retail.csv to HDFS...
docker exec namenode hdfs dfs -put -f /data/online_retail.csv /user/retail/

REM Kiểm tra
echo.
echo [INFO] Verifying upload...
docker exec namenode hdfs dfs -ls /user/retail/

echo.
echo =====================================================
echo    DATA UPLOADED SUCCESSFULLY!
echo =====================================================
echo.
echo Data location: hdfs://namenode:9000/user/retail/online_retail.csv
echo.
pause
