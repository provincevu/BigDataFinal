@echo off
REM =====================================================
REM CHAY SPARK ETL PIPELINE
REM =====================================================
echo.
echo =====================================================
echo    DANG CHAY SPARK ETL PIPELINE
echo =====================================================
echo.

echo [INFO] Dang gui Spark job...
echo.

docker exec spark-master /spark/bin/spark-submit ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/retail_etl_simple.py

echo.
echo =====================================================
echo    ETL PIPELINE DA HOAN THANH!
echo =====================================================
echo.
pause
