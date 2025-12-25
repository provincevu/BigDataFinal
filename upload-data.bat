@echo off
REM =====================================================
REM TAI DU LIEU LEN HDFS
REM =====================================================
echo.
echo =====================================================
echo        TAI DU LIEU LEN HDFS
echo =====================================================
echo.

REM Chờ HDFS sẵn sàng
echo [INFO] Dang kiem tra tinh san sang cua HDFS...
timeout /t 5 /nobreak > nul

REM Tạo thư mục trên HDFS
echo [INFO] Dang tao cac thu muc tren HDFS...
docker exec namenode hdfs dfs -mkdir -p /user/retail
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse

REM Upload dữ liệu
echo [INFO] dang tai online_retail.csv len HDFS...
docker exec namenode hdfs dfs -put -f /data/online_retail.csv /user/retail/

REM Kiểm tra
echo.
echo [INFO] Dang kiem tra viec tai len...
docker exec namenode hdfs dfs -ls /user/retail/

echo.
echo =====================================================
echo    DU LIEU DA DUOC TAI LEN HDFS THANH CONG!
echo =====================================================
echo.
echo Vi tri du lieu: hdfs://namenode:9000/user/retail/online_retail.csv
echo.
pause
