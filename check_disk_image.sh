#!/bin/bash
# Check what's in the disk image

DISK_IMAGE="/Users/ahmet/Documents/AllDiskImages/plstestimg_disk/pls_test_data/android_turbo.db"

echo "==========================================="
echo "Checking Disk Image"
echo "==========================================="
echo ""

# Check if file exists
if [ ! -e "$DISK_IMAGE" ]; then
    echo "❌ Disk image does not exist: $DISK_IMAGE"
    exit 1
fi

echo "✅ Disk image exists"
echo ""

# Check file info
echo "File info:"
ls -lh "$DISK_IMAGE"
echo ""

echo "File type:"
file "$DISK_IMAGE"
echo ""

# Try running log2timeline WITHOUT http-endpoint to see if it generates events
echo "==========================================="
echo "Testing if log2timeline can parse the disk image at all"
echo "==========================================="
echo ""

cd /Users/ahmet/X/Projects/Binalyze/plaso

# Run log2timeline in regular mode to a temp .plaso file
echo "Running log2timeline to create .plaso file (this will show if events are generated)..."

PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
timeout 30 python3 plaso/scripts/log2timeline.py \
  --vss_stores none \
  /tmp/test_disk.plaso \
  "$DISK_IMAGE" 2>&1 | head -50

echo ""
echo "==========================================="
echo "Checking if .plaso file was created and has events..."
echo "==========================================="

if [ -f /tmp/test_disk.plaso ]; then
    ls -lh /tmp/test_disk.plaso
    echo ""
    echo "Running pinfo to see event count..."
    PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
    python3 plaso/scripts/pinfo.py /tmp/test_disk.plaso 2>&1 | grep -A 5 "Events"
    
    rm -f /tmp/test_disk.plaso
else
    echo "❌ No .plaso file created - disk image might not have parseable content"
fi

echo ""
echo "Done!"

