from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import stripe
import datetime

def validate_configuration(configuration: dict):
    """Validate the configuration dictionary."""
    if "stripe_api_key" not in configuration:
        raise ValueError("Missing required configuration value: stripe_api_key")

def schema(configuration: dict):
    """Define the schema for Stripe events."""
    return [
        {
            "table": "stripe_events",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "type": "STRING",
                "api_version": "STRING",
                "created": "UTC_DATETIME",
                "data": "JSON",
                "livemode": "BOOLEAN",
                "pending_webhooks": "INT",
                "request": "JSON", # Can be null
            },
        }
    ]

def update(configuration: dict, state: dict):
    """Fetch Stripe events and send them to Fivetran."""
    validate_configuration(configuration)
    stripe.api_key = configuration["stripe_api_key"]

    last_event_id = state.get("last_event_id")
    start_date_str = configuration.get("start_date")

    params = {"limit": 100} # Max limit is 100

    if last_event_id:
        params["starting_after"] = last_event_id
    elif start_date_str:
        try:
            # Convert start_date to Unix timestamp
            start_datetime = datetime.datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            params["created"] = {"gte": int(start_datetime.timestamp())}
        except ValueError:
            log.error("Invalid start_date format. Please use YYYY-MM-DDTHH:MM:SSZ. Ignoring start_date.")
            pass # Continue without created filter if date is invalid


    has_more = True
    latest_event_id_in_batch = None

    while has_more:
        try:
            events = stripe.Event.list(**params)
        except Exception as e:
            log.error(f"Error fetching events from Stripe: {e}")
            raise

        if not events.data:
            has_more = False
            break

        for event in events.data:
            # Stripe's `created` is a Unix timestamp, Fivetran SDK expects datetime string
            event_data = event.to_dict()
            event_data["created"] = datetime.datetime.fromtimestamp(event.created, tz=datetime.timezone.utc).isoformat()

            # Ensure data and request are serializable
            if 'data' in event_data and event_data['data'] is not None:
                event_data['data'] = event_data['data'].to_dict_recursive()
            else:
                event_data['data'] = {} # Or some other sensible default if it can be None

            if 'request' in event_data and event_data['request'] is not None:
                 # request can be an object or just an id string.
                if hasattr(event_data['request'], 'to_dict_recursive'):
                    event_data['request'] = event_data['request'].to_dict_recursive()
            else:
                # It's okay for request to be null based on Stripe API, so handle if it's not present or None
                event_data['request'] = None


            yield op.upsert(table="stripe_events", data=event_data)
            latest_event_id_in_batch = event.id

        if latest_event_id_in_batch:
            state["last_event_id"] = latest_event_id_in_batch
            yield op.checkpoint(state)
            params["starting_after"] = latest_event_id_in_batch

        has_more = events.has_more

    if latest_event_id_in_batch: # Final checkpoint if any events were processed
        state["last_event_id"] = latest_event_id_in_batch
        yield op.checkpoint(state)

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    import json
    # Load configuration from configuration.json
    # Important: Create a configuration.json file in the same directory as this script
    # with your Stripe API key and an optional start_date.
    # Example configuration.json:
    # {
    #   "stripe_api_key": "sk_test_YOURKEY",
    #   "start_date": "2023-01-01T00:00:00Z"
    # }
    try:
        with open("configuration.json", 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        log.error("configuration.json not found. Please create it.")
        raise
    except json.JSONDecodeError:
        log.error("Error decoding configuration.json. Please ensure it's valid JSON.")
        raise

    # To test with a saved state, create a state.json file
    # Example state.json:
    # {
    #  "last_event_id": "evt_xxxxxxxxxxxxxxx"
    # }
    current_state = {}
    try:
        with open("state.json", 'r') as f:
            current_state = json.load(f)
    except FileNotFoundError:
        log.info("state.json not found, starting with empty state.")
    except json.JSONDecodeError:
        log.warning("Error decoding state.json, starting with empty state.")

    connector.debug(configuration=config, state=current_state)

    # After debug run, the new state can be found in the logs or you can modify
    # the debug call to capture it if needed for manual inspection.
    # For example, the SDK might print the final state to stdout.
