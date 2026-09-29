import boto3
import os

# ==============================================================================
# AWS Lambda EC2 Scheduler for ULLTR System
# ==============================================================================
# Triggers: 
# 1. EventBridge Rule (Start): cron(0 3 ? * MON-FRI *) -> 03:00 AM UTC (08:30 AM IST)
#    - Boots the VM before the ULLTR Expiry Manager does options scanning at 08:45 AM.
# 2. EventBridge Rule (Stop): cron(0 11 ? * MON-FRI *) -> 11:00 AM UTC (04:30 PM IST)
#    - Shuts down the VM after the market closes (03:30 PM) and afternoon rollover completes (03:45 PM).
#
# Environment Variables:
# - INSTANCE_ID: The EC2 Instance ID of the ULLTR VM (e.g. i-0abcd1234abcd1234)
# - ACTION: 'START' or 'STOP' (passed from EventBridge Constant JSON input)
# ==============================================================================

def lambda_handler(event, context):
    region = os.environ.get('AWS_REGION', 'ap-south-1')
    ec2 = boto3.client('ec2', region_name=region)
    
    # Read instance ID
    instance_id = os.environ.get('INSTANCE_ID')
    if not instance_id:
        instance_id = event.get('instance_id')
        
    action = event.get('action', os.environ.get('ACTION', '')).upper()
    
    if not instance_id:
        print("❌ Error: No INSTANCE_ID specified in environment or event payload.")
        return {"statusCode": 400, "body": "Missing INSTANCE_ID"}
        
    if action not in ['START', 'STOP']:
        print(f"❌ Error: Invalid action '{action}'. Action must be 'START' or 'STOP'.")
        return {"statusCode": 400, "body": "Invalid Action"}
        
    try:
        if action == 'START':
            print(f"🚀 Attempting to START ULLTR instance: {instance_id} in {region}")
            response = ec2.start_instances(InstanceIds=[instance_id])
            current_state = response['StartingInstances'][0]['CurrentState']['Name']
            print(f"✅ Success! Instance state is now: {current_state}")
            return {"statusCode": 200, "body": f"Started instance {instance_id}. Current state: {current_state}"}
            
        elif action == 'STOP':
            print(f"🛑 Attempting to STOP ULLTR instance: {instance_id} in {region}")
            response = ec2.stop_instances(InstanceIds=[instance_id], Force=False)
            current_state = response['StoppingInstances'][0]['CurrentState']['Name']
            print(f"✅ Success! Instance state is now: {current_state}")
            return {"statusCode": 200, "body": f"Stopped instance {instance_id}. Current state: {current_state}"}
            
    except Exception as e:
        print(f"❌ Operation failed: {str(e)}")
        return {"statusCode": 500, "body": f"Failed to execute EC2 action: {str(e)}"}
