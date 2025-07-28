#!/bin/bash

# Update system
yum update -y

# Install required packages
yum install -y python3 python3-pip git docker amazon-cloudwatch-agent

# Start and enable Docker
systemctl start docker
systemctl enable docker

# Add ec2-user to docker group
usermod -a -G docker ec2-user

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Create application directory
mkdir -p /opt/gr3-entity-search
cd /opt/gr3-entity-search

# Clone the application (you'll need to set up deployment keys or use HTTPS)
# For now, we'll assume the code is deployed via other means
# git clone https://github.com/yourusername/gr3-new.git .

# Create environment file
cat > .env << EOF
DB_HOST=${db_host}
DB_NAME=${db_name}
DB_USER=${db_user}
DB_PASSWORD=${db_pass}
SERVER_PORT=${app_port}
ENVIRONMENT=production
EOF

# Install Python dependencies (assuming requirements.txt exists)
pip3 install -r requirements.txt

# Create systemd service file
cat > /etc/systemd/system/gr3-entity-search.service << EOF
[Unit]
Description=GR3 Entity Search Application
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/gr3-entity-search
Environment=PYTHONPATH=/opt/gr3-entity-search
EnvironmentFile=/opt/gr3-entity-search/.env
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

# Set up CloudWatch agent configuration
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << EOF
{
    "logs": {
        "logs_collected": {
            "files": {
                "collect_list": [
                    {
                        "file_path": "/opt/gr3-entity-search/logs/app.log",
                        "log_group_name": "/aws/ec2/gr3-entity-search",
                        "log_stream_name": "{instance_id}/application",
                        "timestamp_format": "%Y-%m-%d %H:%M:%S"
                    }
                ]
            }
        }
    },
    "metrics": {
        "namespace": "GR3/EntitySearch",
        "metrics_collected": {
            "cpu": {
                "measurement": [
                    "cpu_usage_idle",
                    "cpu_usage_iowait",
                    "cpu_usage_user",
                    "cpu_usage_system"
                ],
                "metrics_collection_interval": 60
            },
            "disk": {
                "measurement": [
                    "used_percent"
                ],
                "metrics_collection_interval": 60,
                "resources": [
                    "*"
                ]
            },
            "mem": {
                "measurement": [
                    "mem_used_percent"
                ],
                "metrics_collection_interval": 60
            }
        }
    }
}
EOF

# Start CloudWatch agent
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s

# Enable and start the application service
systemctl daemon-reload
systemctl enable gr3-entity-search
systemctl start gr3-entity-search

# Create log rotation
cat > /etc/logrotate.d/gr3-entity-search << EOF
/opt/gr3-entity-search/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0644 ec2-user ec2-user
    postrotate
        systemctl reload gr3-entity-search
    endscript
}
EOF

# Set up health check script
cat > /opt/gr3-entity-search/health_check.sh << 'EOF'
#!/bin/bash
curl -f http://localhost:${app_port}/health > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "Health check passed"
    exit 0
else
    echo "Health check failed"
    exit 1
fi
EOF

chmod +x /opt/gr3-entity-search/health_check.sh

# Add health check to cron
echo "*/1 * * * * /opt/gr3-entity-search/health_check.sh" | crontab -

echo "User data script completed successfully"