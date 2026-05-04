import re
import os
import warnings
import asyncio
import aiohttp
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dataclasses import dataclass
from typing import Dict, List
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv

load_dotenv()

TWILIO_LOOKUP_BASE = "https://lookups.twilio.com/v1/PhoneNumbers"

# =============================================================================
# Config dataclasses
# =============================================================================

@dataclass
class TwilioConfig:
    sid: str
    token: str
    concurrency: int = 50
    timeout_seconds: int = 10
    max_retries: int = 3
    backoff_base: int = 2


@dataclass
class SnowflakeConfig:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str

    def connect(self):
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )


# =============================================================================
# Snowflake Cache
# =============================================================================

class SnowflakeCache:
    CACHE_DIR = "./snowflake_cache"
    PARQUET_PATH = os.path.join(CACHE_DIR, "validated_numbers.parquet")

    def __init__(self, snowflake_config: SnowflakeConfig | None = None, conn=None, parquet_refresh = 48):
        self._numbers: dict | None = None
        self._conn = conn
        self._parquet_refresh = parquet_refresh
        self._sf_config = snowflake_config or SnowflakeConfig(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PW"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        os.makedirs(self.CACHE_DIR, exist_ok=True)

    def _get_conn(self):
        return self._conn or self._sf_config.connect()

    @staticmethod
    def _extract_digits(phone) -> str:
        return "".join(re.findall(r"\d+", str(phone)))

    def _to_digits(self, phone) -> int:
        digits = self._extract_digits(phone)
        if len(digits) == 11:
            country_code, digits = digits[0], digits[1:]
            if country_code != "1":
                warnings.warn(f"Unexpected country code '{country_code}' for phone: {phone}")
        elif len(digits) > 11:
            country_code, digits = digits[:-10], digits[-10:]
            if country_code != "1":
                warnings.warn(f"Unexpected country code '{country_code}' for phone: {phone}")
        return int(digits)

    def _load_from_snowflake(self) -> dict:
        conn = self._get_conn()
        close_conn = conn is not self._conn
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM TWILIO_LOOKUPS.PUBLIC.TWILIO_VALIDATED_NUMBERS")
            row_count = cur.fetchone()[0]
            print(f"Snowflake TWILIO_VALIDATED_NUMBERS has {row_count:,} rows.")

            cur.execute("SELECT NUMBER_FORMATTED, PHONE_CARRIER, PHONE_TYPE, REACHABLE_STATUS FROM TWILIO_LOOKUPS.PUBLIC.TWILIO_VALIDATED_NUMBERS")
            rows = cur.fetchall()

            df = pd.DataFrame(
                rows,
                columns=[
                    "NUMBER_FORMATTED",
                    "PHONE_CARRIER",
                    "PHONE_TYPE",
                    "REACHABLE_STATUS",
                ],
            )
            df.to_parquet(self.PARQUET_PATH, index=False)
            print(f"Cached {len(df):,} numbers to {self.PARQUET_PATH}")

            numbers = df.set_index("NUMBER_FORMATTED")["PHONE_CARRIER"].to_dict()
            return numbers
        
        finally:
            cur.close()
            if close_conn:
                conn.close()

    def _load_from_parquet(self) -> dict:
        df = pd.read_parquet(self.PARQUET_PATH, columns=["NUMBER_FORMATTED", "PHONE_CARRIER"])
        numbers = dict(zip(df["NUMBER_FORMATTED"], df["PHONE_CARRIER"]))
        print(f"Loaded {len(numbers):,} validated numbers from Parquet cache.")
        return numbers

    def _is_parquet_stale(self, max_age_hours: int) -> bool:
        if not os.path.exists(self.PARQUET_PATH):
            return True
        age = pd.Timestamp.now() - pd.Timestamp(os.path.getmtime(self.PARQUET_PATH), unit="s")
        return age.total_seconds() > max_age_hours * 3600

    def _ensure_loaded(self):
        if self._numbers is not None:
            return
        if os.path.exists(self.PARQUET_PATH) and not self._is_parquet_stale(self._parquet_refresh):
            self._numbers = self._load_from_parquet()
        else:
            self._numbers = self._load_from_snowflake()

    def refresh(self):
        """Force a fresh pull from Snowflake, regenerate Parquet, reload dict."""
        self._numbers = self._load_from_snowflake()
        print("Cache refreshed from Snowflake.")

    def is_validated(self, phone) -> bool:
        self._ensure_loaded()
        return self._to_digits(phone) in self._numbers

    def return_carrier(self, phone):
        """Return the carrier name for a phone number, or False if not found."""
        self._ensure_loaded()
        return self._numbers.get(self._to_digits(phone), False)

    def upload(self, df, conn=None):
        """Upload a DataFrame with NUMBER_FORMATTED, PHONE_CARRIER, PHONE_TYPE to Snowflake."""
        required = {"NUMBER_FORMATTED", "PHONE_CARRIER", "PHONE_TYPE"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame is missing columns: {missing}")

        df = df.copy()
        df["NUMBER_FORMATTED"] = df["NUMBER_FORMATTED"].astype(int)
        df["PHONE_CARRIER"] = df["PHONE_CARRIER"].astype(str).str.strip()
        df["PHONE_TYPE"] = df["PHONE_TYPE"].astype(str).str.strip()
        df["REACHABLE_STATUS"] = None #### THIS IS A PLACEHOLDER FOR WHEN WE UPDATE THE API CONFIG ######
        df["SID"] = self.twilio.sid

        before = len(df)
        df = df[df["NUMBER_FORMATTED"] > 0]
        dropped = before - len(df)
        if dropped > 0:
            print(f"Dropped {dropped} rows with invalid NUMBER_FORMATTED values.")

        if df.empty:
            print("No valid rows to upload.")
            return

        conn = conn or self._get_conn()
        close_conn = conn is not self._conn
        try:
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name="TWILIO_VALIDATED_NUMBERS",
                database="TWILIO_LOOKUPS",
                schema="PUBLIC",
                overwrite=False,
            )
            if success:
                print(f"Successfully uploaded {nrows} rows in {nchunks} chunk(s).")
            else:
                print("Upload failed — no rows written.")
        except Exception as e:
            print(f"Error uploading to Snowflake: {e}")
            raise
        finally:
            if close_conn:
                conn.close()

    def stale_numbers(self, months: int = 6, conn=None) -> List[str]:
        """Return phone numbers whose UPDATED_AT is older than `months` months."""
        conn = conn or self._get_conn()
        close_conn = conn is not self._conn
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT NUMBER_FORMATTED FROM TWILIO_LOOKUPS.PUBLIC.TWILIO_VALIDATED_NUMBERS "
                "WHERE UPDATED_AT < DATEADD(month, -%s, CURRENT_TIMESTAMP())",
                (months,),
            )
            numbers = [str(r[0]) for r in cur.fetchall()]
            print(f"Found {len(numbers):,} numbers older than {months} months.")
            return numbers
        finally:
            cur.close()
            if close_conn:
                conn.close()

    @property
    def numbers(self) -> dict:
        self._ensure_loaded()
        return self._numbers

    def __len__(self):
        self._ensure_loaded()
        return len(self._numbers)

    def __contains__(self, phone):
        return self.is_validated(phone)


# =============================================================================
# Twilio Lookup
# =============================================================================

class TwilioLookup:
    STAGING_PATH = os.path.join(SnowflakeCache.CACHE_DIR, "twilio_staging.csv")

    def __init__(
        self,
        cache: SnowflakeCache | None = None,
        twilio_config: TwilioConfig | None = None,
        conn=None,
    ):
        self.cache = cache or SnowflakeCache(conn=conn)
        self.twilio = twilio_config or TwilioConfig(
            sid=os.getenv("CCL_TWILIO_SID"),
            token=os.getenv("CCL_TWILIO_TOKEN"),
        )
        self._conn = conn

    def _upload_staging_file(self, conn=None):
        if not os.path.exists(self.STAGING_PATH):
            return 0

        df = pd.read_csv(self.STAGING_PATH, dtype=str)
        if df.empty:
            os.remove(self.STAGING_PATH)
            return 0

        before = len(df)
        df = df.dropna(subset=["NUMBER_FORMATTED", "PHONE_CARRIER", "PHONE_TYPE"])
        df = df[df["NUMBER_FORMATTED"].apply(lambda x: len(SnowflakeCache._extract_digits(x)) == 10)]
        skipped = before - len(df)

        if skipped > 0:
            print(f"Staging: skipped {skipped} rows (missing values or bad format).")

        if df.empty:
            os.remove(self.STAGING_PATH)
            print("Staging file had no valid rows to upload.")
            return 0

        df["NUMBER_FORMATTED"] = df["NUMBER_FORMATTED"].astype(int)
        self.cache.upload(df, conn)
        os.remove(self.STAGING_PATH)
        print(f"Uploaded {len(df)} staged records and removed staging file.")
        return len(df)

    async def _check_phone(self, session, phone, semaphore):
        digits = SnowflakeCache._extract_digits(phone)
        clean_number = f"+1{digits}"
        url = f"{TWILIO_LOOKUP_BASE}/{clean_number}?Type=carrier"

        async with semaphore:
            for attempt in range(self.twilio.max_retries):
                try:
                    async with session.get(url) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(self.twilio.backoff_base ** attempt)
                            continue

                        data = await resp.json()
                        carrier = data.get("carrier", {})

                        return {
                            "phone": phone,
                            "clean_phone": clean_number,
                            "phone_type": carrier.get("type"),
                            "carrier": carrier.get("name"),
                            "status": resp.status,
                            "valid": resp.status == 200,
                        }

                except (aiohttp.ClientError, asyncio.TimeoutError):
                    await asyncio.sleep(self.twilio.backoff_base ** attempt)

        return {
            "phone": phone,
            "clean_phone": clean_number,
            "phone_type": None,
            "carrier": None,
            "status": 500,
            "valid": False,
        }

    @staticmethod
    def _is_valid_format(phone) -> bool:
        digits = "".join(re.findall(r"\d+", str(phone)))
        return len(digits) == 10

    async def validate(self, phones: List[str], conn=None) -> Dict[str, pd.DataFrame]:
        """Validate a list of phone numbers. Checks cache first, calls Twilio for misses,
        stages results to CSV, uploads to Snowflake, then cleans up."""

        conn = conn or self._conn

        # --- Hard guard: Snowflake or Parquet must be available before proceeding ---
        parquet_available = os.path.exists(self.cache.PARQUET_PATH) and not self.cache._is_parquet_stale(max_age_hours=48)
        snowflake_available = False

        if not parquet_available:
            try:
                test_conn = self.cache._get_conn()
                test_cur = test_conn.cursor()
                test_cur.execute("SELECT 1")
                test_cur.close()
                snowflake_available = True
            except Exception as e:
                print(f"Snowflake connection failed: {e}")

        if not parquet_available and not snowflake_available:
            raise RuntimeError(
                "Twilio validation blocked: neither a valid Parquet cache nor a "
                "Snowflake connection is available. Resolve connectivity before proceeding."
            )

        # --- Load cache (parquet or snowflake) ---
        try:
            self.cache._ensure_loaded()
        except Exception as e:
            raise RuntimeError(f"Cache failed to load: {e}") from e

        # --- If a staging file exists from a previous run, upload it first ---
        uploaded = self._upload_staging_file(conn=conn)
        if uploaded > 0:
            self.cache.refresh()

        # --- Format check ---
        format_valid = [p for p in phones if self._is_valid_format(p)]
        format_invalid = [p for p in phones if not self._is_valid_format(p)]

        format_invalid_df = pd.DataFrame({
            "phone": format_invalid,
            "clean_phone": [None] * len(format_invalid),
            "phone_type": [None] * len(format_invalid),
            "carrier": [None] * len(format_invalid),
            "status": ["invalid_format"] * len(format_invalid),
        })

        unique_phones = list(dict.fromkeys(format_valid))

        # --- Split cached vs uncached ---
        cached_results = []
        cached_invalid = []
        uncached_phones = []

        for p in unique_phones:
            carrier = self.cache.return_carrier(p)
            if carrier is not False:
                digits = SnowflakeCache._extract_digits(p)
                if carrier == "Not Valid":
                    cached_invalid.append({
                        "phone": p,
                        "clean_phone": f"+1{digits}",
                        "phone_type": None,
                        "carrier": "Not Valid",
                        "status": 404,
                    })
                else:
                    cached_results.append({
                        "phone": p,
                        "clean_phone": f"+1{digits}",
                        "phone_type": "mobile",
                        "carrier": carrier,
                        "status": 200,
                    })
            else:
                uncached_phones.append(p)

        print(f"Input phones:  {len(phones)}")
        print(f"Unique phones: {len(unique_phones)}")
        print(f"  Cached:      {len(cached_results)}")
        print(f"  Cached (inv):{len(cached_invalid)}")
        print(f"  Uncached:    {len(uncached_phones)}")

        # --- Twilio lookup for uncached phones ---
        twilio_results = []
        if uncached_phones:
            semaphore = asyncio.Semaphore(self.twilio.concurrency)
            connector = aiohttp.TCPConnector(limit=self.twilio.concurrency)
            timeout = aiohttp.ClientTimeout(total=self.twilio.timeout_seconds)

            async with aiohttp.ClientSession(
                auth=aiohttp.BasicAuth(self.twilio.sid, self.twilio.token),
                connector=connector,
                timeout=timeout,
                headers={"User-Agent": "PhoneValidator"},
            ) as session:
                tasks = [self._check_phone(session, p, semaphore) for p in uncached_phones]
                twilio_results = await tqdm_asyncio.gather(*tasks, desc="Validating (uncached)", total=len(tasks))

            # --- Write Twilio results to staging file (valid + invalid) ---
            staging_rows = []
            for r in twilio_results:
                digits = SnowflakeCache._extract_digits(r["phone"])
                if r["status"] == 200 and r["phone_type"] and r["carrier"]:
                    staging_rows.append({
                        "NUMBER_FORMATTED": digits,
                        "PHONE_CARRIER": r["carrier"],
                        "PHONE_TYPE": r["phone_type"],
                    })
                elif 400 <= r["status"] <= 499:
                    staging_rows.append({
                        "NUMBER_FORMATTED": digits,
                        "PHONE_CARRIER": "Not Valid",
                        "PHONE_TYPE": "invalid",
                    })

            if staging_rows:
                pd.DataFrame(staging_rows).to_csv(self.STAGING_PATH, index=False)
                print(f"Wrote {len(staging_rows)} new results to staging file.")

            # --- Upload staging to Snowflake, then delete ---
            self._upload_staging_file(conn=conn)

            # --- Update cache dict in memory ---
            for r in twilio_results:
                digits = int(SnowflakeCache._extract_digits(r["phone"]))
                if r["status"] == 200 and r["carrier"]:
                    self.cache._numbers[digits] = r["carrier"]
                elif 400 <= r["status"] <= 499:
                    self.cache._numbers[digits] = "Not Valid"

        # --- Combine cached + fresh results ---
        all_results = cached_results + [
            {k: v for k, v in r.items() if k != "valid"} for r in twilio_results
        ]

        if not all_results and not cached_invalid:
            empty = pd.DataFrame(columns=["phone", "clean_phone", "phone_type", "carrier", "status"])
            return {"valid": empty, "invalid": format_invalid_df, "failed": empty}

        df = pd.DataFrame(all_results) if all_results else pd.DataFrame()

        valid_df = df[df["status"] == 200].copy() if not df.empty else pd.DataFrame()
        new_invalid_df = df[df["status"].between(400, 499)].copy() if not df.empty else pd.DataFrame()
        failed_df = df[~df["status"].between(200, 499)].copy() if not df.empty else pd.DataFrame()

        all_invalid = pd.concat(
            [new_invalid_df, pd.DataFrame(cached_invalid), format_invalid_df],
            ignore_index=True,
        )

        print(f"\nResults:")
        print(f"  Valid:   {len(valid_df)} ({len(cached_results)} cached, {len(valid_df) - len(cached_results)} new)")
        print(f"  Invalid: {len(all_invalid)} ({len(cached_invalid)} cached, {len(new_invalid_df)} new, {len(format_invalid_df)} bad format)")
        print(f"  Failed:  {len(failed_df)}")

        return {
            "valid": valid_df,
            "invalid": all_invalid,
            "failed": failed_df,
        }
