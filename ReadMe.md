# BoUnAn Matcher

Python worker service that detects openings and endings of episodes.

## Description

This is a part of the BoUnAn project.

## Podman with CUDA on Windows

When Podman runs through a WSL machine, CUDA is exposed through WSL's `/dev/dxg` device and the driver files under `/usr/lib/wsl`. The NVIDIA container runtime is not installed in the default Podman machine, so create a CDI device spec inside the machine once:

```sh
podman machine ssh podman-machine-default
sudo mkdir -p /etc/cdi
sudo tee /etc/cdi/wsl-nvidia.yaml >/dev/null <<'EOF'
cdiVersion: 0.5.0
kind: nvidia.com/gpu
devices:
  - name: all
    containerEdits:
      env:
        - LD_LIBRARY_PATH=/usr/lib/wsl/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64
        - PATH=/usr/lib/wsl/lib:/opt/venv/bin:/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
      deviceNodes:
        - path: /dev/dxg
          type: c
          major: 10
          minor: 125
          permissions: rw
      mounts:
        - hostPath: /usr/lib/wsl
          containerPath: /usr/lib/wsl
          options:
            - ro
            - rbind
EOF
exit
```

Validate GPU access from Windows:

```sh
podman run --rm --device nvidia.com/gpu=all docker.io/nvidia/cuda:12.9.2-cudnn-devel-ubuntu24.04 nvidia-smi
```

Run the worker with:

```sh
./_run_podman.sh
```

The run script passes `--device nvidia.com/gpu=all` and the WSL CUDA paths explicitly. If the Podman machine is recreated, repeat the CDI setup because `/etc/cdi/wsl-nvidia.yaml` lives inside the machine.

The worker waits for SQS notifications when it starts with no pending videos. After handling videos, it finishes all available work and exits when the API reports no more videos to match. The run script uses `--restart=always` so Podman restarts it with a fresh process before it waits for the next notification. An existing container must be recreated with the updated run script to receive this restart policy and code.
