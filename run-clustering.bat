@echo off
REM =====================================================
REM RUN CUSTOMER CLUSTERING/SEGMENTATION
REM =====================================================
echo.
echo =====================================================
echo    RUNNING CUSTOMER SEGMENTATION (RFM-Based)
echo =====================================================
echo.

echo [INFO] Submitting Spark job...
echo.

docker exec spark-master /spark/bin/spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode client ^
    --driver-memory 2g ^
    --executor-memory 2g ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/customer_clustering_simple.py

echo.
echo =====================================================
echo    CLUSTERING COMPLETED!
echo =====================================================
echo.
pause
