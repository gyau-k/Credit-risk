import json
import logging
import time
import requests
import config

logger = logging.getLogger()

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5  # Wait 5 seconds between retries


def fetch_transactions():
    """
    Fetches transaction data from the API with retry logic.
    Retries up to 3 times with 5-second delays between attempts.
    Handles Lambda response wrapper format with statusCode and body.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                logger.info(f"Retry attempt {attempt}/{MAX_RETRIES}")
            else:
                logger.info(f"Polling API: {config.API_URL}")

            # Prepare headers
            headers = {'Content-Type': 'application/json'}

            # Make API request
            response = requests.get(
                config.API_URL,
                headers=headers,
                timeout=config.API_TIMEOUT
            )

            # Raise exception for bad status codes (4xx, 5xx)
            response.raise_for_status()

            logger.info(f"API response status: {response.status_code}")

            data = response.json()

            # Handle Lambda response wrapper
            if isinstance(data, dict) and 'statusCode' in data:
                # Lambda response format
                status_code = data.get('statusCode')

                if status_code != 200:
                    logger.error(f"API returned non-200 status code: {status_code}")
                    return None

                # Parse the body (it's a JSON string in Lambda responses)
                body = data.get('body', '[]')

                if isinstance(body, str):
                    transactions = json.loads(body)
                else:
                    transactions = body

            elif isinstance(data, list):
                # Direct list response
                transactions = data
            else:
                logger.error(f"Unexpected API response format: {type(data)}")
                return None

            logger.info(f"Successfully fetched {len(transactions)} transactions from API")
            return transactions

        except requests.exceptions.Timeout:
            logger.warning(f"API request timed out after {config.API_TIMEOUT} seconds (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                logger.info(f"Waiting {RETRY_DELAY_SECONDS} seconds before retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error(f"All {MAX_RETRIES} retry attempts failed due to timeout")
                return None

        except requests.exceptions.ConnectionError:
            logger.warning(f"Failed to connect to API - connection error (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                logger.info(f"Waiting {RETRY_DELAY_SECONDS} seconds before retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error(f"All {MAX_RETRIES} retry attempts failed due to connection error")
                return None

        except requests.exceptions.HTTPError as e:
            logger.error(f"API returned error status: {e}")
            logger.error(f"Response content: {e.response.text if hasattr(e.response, 'text') else 'N/A'}")
            # Don't retry on HTTP errors (4xx, 5xx) as they're unlikely to succeed on retry
            return None

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            # Don't retry on JSON parsing errors
            return None

        except Exception as e:
            logger.error(f"Unexpected error fetching from API: {e}", exc_info=True)
            if attempt < MAX_RETRIES:
                logger.info(f"Waiting {RETRY_DELAY_SECONDS} seconds before retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error(f"All {MAX_RETRIES} retry attempts failed")
                return None

    return None
