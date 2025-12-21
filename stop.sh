#!/bin/bash
# =====================================================
# RETAIL BIG DATA PIPELINE - STOP SCRIPT
# =====================================================

echo ""
echo "====================================================="
echo "   STOPPING ALL SERVICES..."
echo "====================================================="
echo ""

docker-compose down

echo ""
echo "[INFO] All services stopped."
echo ""
echo "[TIP] Run './start.sh' to start services again"
echo "[TIP] Run 'docker-compose down -v' to also remove volumes"
echo ""
