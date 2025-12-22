@echo off
REM =====================================================
REM RETAIL BIG DATA PIPELINE - SCRIPT KHOI DONG
REM =====================================================
setlocal enabledelayedexpansion

echo.
echo =====================================================
echo    HE THONG BIG DATA BAN LE - DANG KHOI DONG
echo =====================================================
echo.

REM Thiet lap mau sac cho de nhin
color 0A

REM Kiem tra Docker dang chay
echo [1/7] Kiem tra trang thai Docker Desktop...
docker version > nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Docker Desktop khong chay!
    echo.
    echo Vui long:
    echo 1. Mo Docker Desktop
    echo 2. Doi no khoi dong xong (bieu tuong ca voi ngung nhap nhay)
    echo 3. Chay lai script nay
    echo.
    pause
    exit /b 1
)
echo [OK] Docker Desktop dang chay
echo.

REM Tao cac thu muc can thiet
echo [2/7] Tao cac thu muc can thiet...
set "dirs=config spark-apps notebooks mongo-init hive-queries hue-logs hue-data data"
for %%d in (%dirs%) do (
    if not exist "%%d" (
        mkdir "%%d"
        echo   Da tao: %%d
    ) else (
        echo   Da co:  %%d
    )
)
echo.

REM Kiem tra file du lieu
echo [3/7] Kiem tra file du lieu...
if exist "online_retail.csv" (
    if not exist "data\online_retail.csv" (
        copy "online_retail.csv" "data\online_retail.csv" > nul
        echo [OK] Da sao chep online_retail.csv vao thu muc data
    ) else (
        echo [THONG TIN] File du lieu da ton tai
    )
) else (
    echo [CANH BAO] Khong tim thay online_retail.csv trong thu muc goc
    echo [THONG TIN] Ban co the tai tu: https://archive.ics.uci.edu/ml/datasets/online+retail
    echo [THONG TIN] Dat file tai: D:\MyProject\BigDataFinal\
)
echo.

REM Kiem tra file cau hinh
echo [4/7] Kiem tra file cau hinh...
if not exist "config\hadoop.env" (
    echo [CANH BAO] Khong tim thay config\hadoop.env!
    echo [THONG TIN] Dang tao hadoop.env mac dinh...
    (
        echo HADOOP_HEAPSIZE_MAX=512
        echo HADOOP_HEAPSIZE_MIN=256
        echo CORE_CONF_fs_defaultFS=hdfs://namenode:9000
        echo HDFS_CONF_dfs_replication=1
        echo YARN_CONF_yarn_resourcemanager_hostname=resourcemanager
        echo YARN_CONF_yarn_resourcemanager_address=resourcemanager:8032
        echo YARN_CONF_yarn_nodemanager_resource_memory_mb=2048
        echo YARN_CONF_yarn_scheduler_maximum-allocation-mb=2048
        echo YARN_CONF_yarn_scheduler_minimum-allocation-mb=512
        echo YARN_CONF_yarn_nodemanager_resource_cpu_vcores=2
        echo MAPRED_CONF_mapred_child_java_opts=-Xmx1024m
        echo MAPRED_CONF_mapreduce_map_memory_mb=1024
        echo MAPRED_CONF_mapreduce_reduce_memory_mb=1024
        echo MAPRED_CONF_mapreduce_map_java_opts=-Xmx768m
        echo MAPRED_CONF_mapreduce_reduce_java_opts=-Xmx768m
        echo MAPRED_CONF_yarn_app_mapreduce_am_resource_mb=1024
        echo MAPRED_CONF_yarn_app_mapreduce_am_command_opts=-Xmx768m
    ) > "config\hadoop.env"
    echo [OK] Da tao hadoop.env mac dinh
)

if not exist "config\hive-site.xml" (
    echo [CANH BAO] Khong tim thay config\hive-site.xml!
    echo [THONG TIN] Dang tao hive-site.xml mac dinh...
    copy /y "hive-site-template.xml" "config\hive-site.xml" > nul 2>&1
    if %errorlevel% neq 0 (
        echo [LOI] Vui long tao config\hive-site.xml thu cong
    )
)

if not exist "config\hue.ini" (
    echo [CANH BAO] Khong tim thay config\hue.ini!
    echo [THONG TIN] Dang tao hue.ini mac dinh...
    copy /y "hue-template.ini" "config\hue.ini" > nul 2>&1
    if %errorlevel% neq 0 (
        echo [LOI] Vui long tao config\hue.ini thu cong
    )
)
echo.

REM Khoi dong Docker services
echo [5/7] Dang khoi dong Docker Compose services...
echo [THONG TIN] Lan dau chay co the mat 3-5 phut...
echo [THONG TIN] Vui long doi trong giay lat...
echo.

docker-compose down > nul 2>&1
echo [THONG TIN] Dang don dep container cu...

docker-compose up -d
if %errorlevel% neq 0 (
    echo [LOI] Khong the khoi dong Docker Compose services!
    echo.
    echo Cac buoc khac phuc:
    echo 1. Kiem tra port da duoc su dung
    echo 2. Chay 'docker-compose logs' de xem chi tiet
    echo 3. Dam bao Docker Desktop co du tai nguyen
    echo.
    pause
    exit /b 1
)
echo [OK] Docker services da khoi dong thanh cong
echo.

REM Doi services san sang
echo [6/7] Dang doi services khoi tao (60 giay)...
echo [THONG TIN] Dang khoi tao Hadoop, Hive va MongoDB...
for /l %%i in (1,1,60) do (
    set /a "phut=%%i/60"
    set /a "giay=%%i%%60"
    set "tien_trinh=%%i"
    if %%i leq 60 set "tien_trinh=["
    for /l %%j in (1,1,%%i) do set "tien_trinh=!tien_trinh!█"
    for /l %%j in (%%i,1,59) do set "tien_trinh=!tien_trinh!░"
    set "tien_trinh=!tien_trinh!]"
    <nul set /p "=Dang doi: !tien_trinh! !phut!ph!giay!giay\r"
    timeout /t 1 /nobreak > nul
)
echo.
echo [OK] Services da san sang
echo.

REM Hien thi trang thai services
echo [7/7] Kiem tra trang thai services...
echo.
echo Dich vu               Trang thai        URL
echo ====================  ==============    ================================

REM Kiem tra tung service
set "services=namenode datanode hive-metastore hive-server spark-master mongodb hue jupyter"
for %%s in (%services%) do (
    docker ps --filter "name=%%s" --format "{{.Names}}\t{{.Status}}" | findstr /c:"%%s" > nul
    if !errorlevel! equ 0 (
        for /f "tokens=1,2" %%a in ('docker ps --filter "name=%%s" --format "{{.Names}} {{.Status}}" ^| findstr /c:"%%s"') do (
            set "trang_thai=%%b"
        )
        echo %%~20s        !trang_thai!      http://localhost:!port!
    ) else (
        echo %%~20s        [KHONG CHAY]
    )
    
    REM Thiet lap port cho tung service
    if "%%s"=="namenode" set "port=9870"
    if "%%s"=="datanode" set "port=9864"
    if "%%s"=="spark-master" set "port=8580"
    if "%%s"=="spark-worker" set "port=8581"
    if "%%s"=="hue" set "port=8788"
    if "%%s"=="jupyter" set "port=8889"
    if "%%s"=="mongo-express" set "port=8290"
)
echo.

REM Hien thi thong tin truy cap
echo =====================================================
echo    THONG TIN TRUY CAP
echo =====================================================
echo.
echo 1. HADOOP HDFS
echo    - Giao dien NameNode:    http://localhost:9870
echo    - Giao dien DataNode:    http://localhost:9864
echo.
echo 2. SPARK
echo    - Giao dien Master:      http://localhost:8580
echo    - Giao dien Worker:      http://localhost:8581
echo.
echo 3. KHO DU LIEU (DATA WAREHOUSE)
echo    - Hue (Web GUI):         http://localhost:8788
echo      Tai khoan: admin (lan dau, sau do tao tai khoan rieng)
echo      Mat khau: admin
echo.
echo 4. JUPYTER NOTEBOOK
echo    - JupyterLab:            http://localhost:8889
echo      Token: (xem logs bang 'docker logs jupyter')
echo.
echo 5. MONGODB
echo    - Mongo Express:         http://localhost:8290
echo      Tai khoan: admin
echo      Mat khau: admin123
echo    - MongoDB ket noi:       mongodb://admin:admin123@localhost:27017
echo.
echo 6. HIVE
echo    - HiveServer2:           jdbc:hive2://localhost:10000
echo    - Dung Beeline:          docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
echo.
echo =====================================================
echo    LENH NHANH
echo =====================================================
echo.
echo Xem logs:          docker-compose logs -f [ten_service]
echo Dung tat ca:       docker-compose down
echo Khoi dong lai:     docker-compose restart [ten_service]
echo Truy cap shell:    docker exec -it [ten_container] bash
echo.
echo =====================================================
echo.

REM Tao script dung services
(
echo @echo off
echo echo Dang dung He thong Big Data Ban le...
echo docker-compose down
echo echo Da dung tat ca dich vu.
echo pause
) > "dung.bat"

echo [THONG TIN] Da tao dung.bat - dung de dung tat ca services
echo [THONG TIN] Khoi dong hoan tat thanh cong!
echo.

REM Hoi co muon xem logs khong
set /p "xem_logs=Ban co muon xem logs dich vu? (y/N): "
if /i "!xem_logs!"=="y" (
    echo.
    echo Dang hien thi logs tat ca dich vu (Nhan Ctrl+C de thoat)...
    timeout /t 3 /nobreak
    docker-compose logs -f
) else (
    echo.
    echo Co the xem logs bat ky luc nao voi: docker-compose logs -f
)

pause