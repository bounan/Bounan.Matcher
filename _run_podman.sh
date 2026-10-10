podman build -t matcher:latest .

podman run \
  -d \
  --name matcher \
  --restart=always \
  --device nvidia.com/gpu=all \
  -e LD_LIBRARY_PATH="/usr/lib/wsl/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64" \
  -e PATH="/usr/lib/wsl/lib:/opt/venv/bin:/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
  -v ".env:/app/.env" \
  matcher:latest \
  python3 -u runner.py
