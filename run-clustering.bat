@echo off
REM =====================================================
REM RUN CUSTOMER CLUSTERING
REM =====================================================
echo.
echo =====================================================
echo    RUNNING CUSTOMER CLUSTERING
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
    /spark-apps/customer_clustering.py

echo.
echo =====================================================
echo    CLUSTERING COMPLETED!
echo =====================================================
echo.
pause
