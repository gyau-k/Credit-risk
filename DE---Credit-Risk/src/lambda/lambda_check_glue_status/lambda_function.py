"""
Lambda function to check if a Glue job is currently running.
Used by Step Functions to coordinate Glue job execution.
"""
import json
import os
import boto3
import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Glue client
glue_client = boto3.client('glue')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Check if the specified Glue job is currently running.

    Args:
        event: Dictionary containing:
            - glue_job_name: Name of the Glue job to check
        context: Lambda context object

    Returns:
        Dictionary with:
            - isRunning: Boolean indicating if job is running
            - jobName: Name of the checked job
            - runningJobIds: List of running job run IDs (if any)
            - status: Status message
    """
    try:
        # Get Glue job name from event or environment variable
        glue_job_name = event.get('glue_job_name') or os.environ.get('GLUE_JOB_NAME')

        if not glue_job_name:
            logger.error("Glue job name not provided in event or environment variables")
            raise ValueError("glue_job_name is required")

        logger.info(f"Checking status for Glue job: {glue_job_name}")

        # Get recent job runs
        response = glue_client.get_job_runs(
            JobName=glue_job_name,
            MaxResults=10  # Check last 10 runs
        )

        # Check if any runs are in RUNNING or STARTING state
        running_states = ['RUNNING', 'STARTING', 'STOPPING']
        running_job_ids = []

        for job_run in response.get('JobRuns', []):
            job_state = job_run.get('JobRunState')
            job_run_id = job_run.get('Id')

            if job_state in running_states:
                running_job_ids.append(job_run_id)
                logger.info(f"Found job run {job_run_id} in state: {job_state}")

        is_running = len(running_job_ids) > 0

        result = {
            'isRunning': is_running,
            'jobName': glue_job_name,
            'runningJobIds': running_job_ids,
            'status': f"Job is {'running' if is_running else 'available'}",
            'requestId': context.aws_request_id
        }

        logger.info(f"Check result: {json.dumps(result)}")
        return result

    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f"Glue job '{glue_job_name}' not found")
        return {
            'isRunning': False,
            'jobName': glue_job_name,
            'runningJobIds': [],
            'status': 'Job not found',
            'error': 'EntityNotFoundException'
        }

    except Exception as e:
        logger.error(f"Error checking Glue job status: {str(e)}", exc_info=True)
        raise
