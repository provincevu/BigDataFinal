@echo off
REM =====================================================
REM RUN SPARK ETL PIPELINE
REM =====================================================
echo.
echo =====================================================
echo    RUNNING SPARK ETL PIPELINE
echo =====================================================
echo.

echo [INFO] Submitting Spark job...
echo.

docker exec spark-master /spark/bin/spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode client ^
    --driver-memory 2g ^
    --executor-memory 2g ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 ^
    /spark-apps/retail_etl_pipeline.py

echo.
echo =====================================================
echo    ETL PIPELINE COMPLETED!
echo =====================================================
echo.
pause
