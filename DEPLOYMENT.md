# AWS Deployment Guide for GR3 Entity Search

This guide provides complete instructions for deploying the GR3 Entity Search application to AWS using Terraform.

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **Terraform** installed (>= 1.0)
3. **AWS CLI** configured with credentials
4. **Key Pair** created in AWS Console
5. **Database** (RDS or external) accessible from AWS

## Quick Start

### 1. Clone and Prepare

```bash
git clone <your-repo-url> gr3-entity-search
cd gr3-entity-search
```

### 2. Configure Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your configuration:

```hcl
# AWS Configuration
aws_region = "us-west-2"
project_name = "gr3-entity-search"
environment = "production"

# Security - IMPORTANT: Replace with your IP
ssh_allowed_cidrs = ["YOUR_IP_ADDRESS/32"]
key_pair_name = "your-key-pair-name"

# Database Configuration
db_host = "your-database-host.amazonaws.com"
db_name = "gr3_entity_db"
db_user = "your_db_username"
db_password = "your_secure_password"

# Instance Configuration
instance_type = "t3.medium"
min_instances = 1
max_instances = 3
desired_instances = 2
```

### 3. Deploy Infrastructure

```bash
# Initialize Terraform
terraform init

# Plan deployment
terraform plan

# Deploy (this will take 5-10 minutes)
terraform apply
```

### 4. Access Application

After deployment completes:

```bash
# Get the application URL
terraform output application_url

# Get health check URL
terraform output health_check_url
```

## Detailed Configuration

### Infrastructure Components

The Terraform configuration creates:

- **VPC** with public/private subnets across 2 AZs
- **Application Load Balancer** with health checks
- **Auto Scaling Group** with launch template
- **Security Groups** for ALB and instances
- **IAM Roles** for EC2 instances
- **CloudWatch Logs** for monitoring

### Health Check Endpoints

The application provides two health check endpoints:

1. **`/health`** - Basic health check with database connectivity test
2. **`/health/ready`** - Readiness check for application initialization

Response format:
```json
{
  "status": "healthy",
  "timestamp": 1699123456.789,
  "database": "healthy",
  "service": "gr3-entity-search",
  "version": "1.0.0"
}
```

### Auto Scaling Configuration

- **Min Instances**: 1
- **Max Instances**: 3
- **Desired**: 2
- **Health Check Type**: ELB
- **Health Check Grace Period**: 300 seconds

### Load Balancer Health Check

- **Path**: `/health`
- **Healthy Threshold**: 2
- **Unhealthy Threshold**: 2
- **Timeout**: 5 seconds
- **Interval**: 30 seconds

## Security Configuration

### Network Security

- **ALB Security Group**: Allows HTTP (80) and HTTPS (443) from internet
- **App Security Group**: Allows app port (8080) from ALB only
- **SSH Access**: Restricted to specified CIDR blocks

### Database Security

Database credentials are:
- Stored as Terraform variables (marked sensitive)
- Passed to instances via user data
- Not logged or exposed in outputs

### Instance Security

- **IAM Role**: Minimal permissions for CloudWatch logging
- **System Updates**: Automatic via user data script
- **Application User**: Non-root user for service

## Monitoring and Logging

### CloudWatch Integration

- **Application Logs**: `/aws/ec2/gr3-entity-search`
- **System Metrics**: CPU, Memory, Disk utilization
- **Custom Metrics**: Application-specific metrics
- **Log Retention**: 14 days (configurable)

### Health Monitoring

- **ALB Health Checks**: Automatic instance replacement
- **CloudWatch Alarms**: (can be added for advanced monitoring)
- **Application Logs**: Structured logging for debugging

## Scaling and Performance

### Vertical Scaling

Change instance type in `terraform.tfvars`:
```hcl
instance_type = "t3.large"  # or t3.xlarge, m5.large, etc.
```

### Horizontal Scaling

Adjust Auto Scaling Group settings:
```hcl
min_instances = 2
max_instances = 5
desired_instances = 3
```

### Database Performance

For high-traffic deployments:
- Use RDS with Multi-AZ deployment
- Enable connection pooling
- Consider read replicas for read-heavy workloads

## Troubleshooting

### Common Issues

1. **Instance Fails Health Checks**
   ```bash
   # SSH to instance and check logs
   sudo journalctl -u gr3-entity-search -f
   
   # Check application health directly
   curl http://localhost:8080/health
   ```

2. **Database Connection Issues**
   ```bash
   # Verify database connectivity from instance
   telnet your-db-host 5432
   
   # Check environment variables
   sudo systemctl show gr3-entity-search --property=Environment
   ```

3. **Application Not Starting**
   ```bash
   # Check Python dependencies
   pip3 list
   
   # Check application logs
   tail -f /opt/gr3-entity-search/logs/app.log
   
   # Restart service
   sudo systemctl restart gr3-entity-search
   ```

### Log Locations

- **Application Logs**: `/opt/gr3-entity-search/logs/`
- **System Logs**: `/var/log/messages`
- **Service Logs**: `journalctl -u gr3-entity-search`
- **CloudWatch**: AWS Console → CloudWatch → Log Groups

## Updating the Application

### Code Updates

1. **Update Launch Template**:
   ```bash
   # Modify user data script or AMI
   terraform apply
   ```

2. **Rolling Update**:
   ```bash
   # Terminate instances one by one
   # ASG will launch new instances with updated configuration
   ```

### Database Migrations

1. **Backup Database** before any schema changes
2. **Test Migrations** in staging environment
3. **Apply Migrations** during maintenance window

## Cost Optimization

### Right-sizing

- **Monitor CPU/Memory** usage via CloudWatch
- **Adjust instance types** based on actual usage
- **Use Reserved Instances** for predictable workloads

### Auto Scaling

- **Scale down** during low-traffic periods
- **Use spot instances** for dev/test environments
- **Enable detailed monitoring** only when needed

## Security Best Practices

### Network Security

- **Restrict SSH access** to specific IP ranges
- **Use VPN** for administrative access
- **Enable VPC Flow Logs** for network monitoring

### Data Security

- **Encrypt data at rest** (RDS encryption)
- **Use SSL/TLS** for data in transit
- **Regular security updates** via user data script

### Access Control

- **Least privilege** IAM policies
- **Regular key rotation** for database credentials
- **Audit access logs** regularly

## Disaster Recovery

### Backup Strategy

- **Database Backups**: Automated RDS backups
- **Configuration**: Terraform state backup
- **Application Code**: Git repository

### Recovery Procedures

1. **Database Recovery**: Restore from RDS backup
2. **Infrastructure**: `terraform apply` from backed-up state
3. **Application**: Deploy from Git repository

## Support and Maintenance

### Regular Tasks

- **Monitor health checks** and application logs
- **Update system packages** regularly
- **Review CloudWatch metrics** for performance issues
- **Test disaster recovery** procedures periodically

### Scaling for Growth

- **Monitor request patterns** and plan capacity
- **Consider CDN** for static content
- **Implement caching** for database queries
- **Use managed services** (RDS, ElastiCache) for scaling

## Advanced Configuration

### SSL/TLS Certificate

Add HTTPS support:
```hcl
# In main.tf, add SSL certificate
resource "aws_acm_certificate" "app" {
  domain_name       = "your-domain.com"
  validation_method = "DNS"
  
  lifecycle {
    create_before_destroy = true
  }
}

# Update ALB listener for HTTPS
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.main.arn
  port              = "443"
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS-1-2-2017-01"
  certificate_arn   = aws_acm_certificate.app.arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.app.arn
  }
}
```

### Custom Domain

Configure Route53 for custom domain:
```hcl
resource "aws_route53_record" "app" {
  zone_id = var.hosted_zone_id
  name    = "your-domain.com"
  type    = "A"

  alias {
    name                   = aws_lb.main.dns_name
    zone_id                = aws_lb.main.zone_id
    evaluate_target_health = true
  }
}
```

## Contact and Support

For deployment issues or questions:
1. Check application logs first
2. Review CloudWatch metrics
3. Consult this deployment guide
4. Contact system administrator

---

**Note**: Replace placeholder values (YOUR_IP_ADDRESS, your-key-pair-name, etc.) with your actual configuration values before deployment.