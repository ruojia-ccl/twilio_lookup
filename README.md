# Twilio Lookup

This project performs phone number lookups using the Twilio Lookup API with multi-layer caching to minimize API calls, reduce latency, and reduce overall lookup costs.

## Features

- Twilio phone number lookups
- Optional Line Status Intelligence support (V2)
- Automatic fallback to standard lookup behavior if Line Status permissions are unavailable
- Snowflake-backed lookup cache
- Local Parquet cache for faster repeated access
- Automatic cache refresh handling for stale records
- Environment-variable based credential management using `.env`

---

# Cache Strategy

The lookup pipeline uses multiple cache layers to avoid unnecessary API calls and improve performance.

## Local Parquet Cache

The application first checks a local Parquet cache before querying external systems.

- Fastest lookup path
- Reduces repeated Snowflake queries
- Local cache automatically refreshes when data is more than 48 hours old

## Snowflake Cache

If data is not available locally, the application checks the Snowflake cache.

- Prevents duplicate Twilio lookups across environments and users
- Reduces Twilio API usage and associated costs
- Snowflake cache entries refresh every 12 months to ensure data accuracy over time

## Twilio Lookup API

If the lookup does not exist in either cache layer, the application performs a live Twilio lookup request.

For V2 lookups:

- Attempts to retrieve Line Status Intelligence data
- Automatically falls back to standard lookup behavior if the account lacks the required permissions or entitlements

---

# Configuration

Create a `.env` file in the project root with the required credentials.

Example:

```env
# Twilio
TWILIO_ACCOUNT_SID=your_account_sid
TWILIO_AUTH_TOKEN=your_auth_token

# Snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
