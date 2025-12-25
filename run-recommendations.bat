@echo off
REM =====================================================
REM CHAY PRODUCT RECOMMENDATIONS (FP-Growth MLlib)
REM =====================================================
echo.
echo =====================================================
echo    DANG CHAY PRODUCT RECOMMENDATIONS (FP-Growth)
echo =====================================================
echo.

echo [INFO] Kiem tra va cai dat (numpy)...
docker exec spark-master apk add --no-cache py3-numpy >nul 2>&1
echo [INFO] Dependencies ready.
echo.

echo [INFO] Dang gui Spark job...
echo.

docker exec spark-master /spark/bin/spark-submit ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/product_recommendations.py

echo.
echo =====================================================
echo    PRODUCT RECOMMENDATIONS DA HOAN THANH!
echo =====================================================
echo.
echo Ket qua da duoc luu vao:
echo   - MongoDB: product_associations, product_recommendations
echo   - HDFS: /user/retail/analysis/product_*
echo.
pause
