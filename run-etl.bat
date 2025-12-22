@echo off
REM =====================================================
REM RUN SPARK ETL PIPELINE (Simple Version)
REM =====================================================
echo.
echo =====================================================
echo    RUNNING SPARK ETL PIPELINE (Simple)
echo =====================================================
echo.

echo [INFO] Submitting Spark job...
echo.

docker exec spark-master /spark/bin/spark-submit ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/retail_etl_simple.py

echo.
echo =====================================================
echo    ETL PIPELINE COMPLETED!
echo =====================================================
echo.
pause
