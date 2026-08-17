#!/usr/bin/env bash
set -e
DIR="$(cd "$(dirname "$0")/.." && pwd)"; RUNUSER="$(whoami)"
echo "installing into $DIR as $RUNUSER"
sudo apt-get update -y
sudo apt-get install -y python3-pip
pip3 install --quiet -r "$DIR/requirements.txt"
sudo tee /etc/systemd/system/doo4730-realtime.service >/dev/null <<UNIT
[Unit]
Description=DOO4730 real-time failure monitor
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
WorkingDirectory=$DIR
ExecStart=/usr/bin/python3 $DIR/instant_monitor.py
Restart=always
RestartSec=5
User=$RUNUSER
[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable doo4730-realtime
sudo systemctl restart doo4730-realtime
echo "done. logs: journalctl -u doo4730-realtime -f"
