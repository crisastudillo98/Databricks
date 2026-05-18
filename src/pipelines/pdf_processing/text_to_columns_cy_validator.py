import argparse
import logging
from pyspark.sql import SparkSession, functions as F, types as T

# ============================================================
# Logger
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CY_Validator")

# ============================================================
# 0. Arguments
# ============================================================
parser = argparse.ArgumentParser(description="CY Extractor Validator")
parser.add_argument(
    "--source_table", required=True,
    help="Spark table to validate (e.g. logistics.bronze.truckr_loads_cy)"
)
args = parser.parse_args()
source_table = args.source_table

# ============================================================
# 1. Spark Session
# ============================================================
spark = SparkSession.builder.appName("CY_Extractor_Validator").getOrCreate()

# ============================================================
# 2. Schema
# ============================================================
fields = [
    "source_file",
    # Broker
    "broker_name", "broker_rep", "broker_phone", "broker_fax",
    "broker_email", "broker_address", "broker_city", "broker_state", "broker_zipcode",
    # Load / carrier
    "loadConfirmationNumber", "equipment_type",
    "carrier_name", "carrier_usdot", "carrier_phone", "carrier_email", "carrier_fax",
    # Charges
    "totalCarrierPay", "flat_rate_amount", "fuel_surcharge_amount",
    # Stop 1
    "pickup_customer_1", "pickup_address_1", "pickup_city_1",
    "pickup_state_1", "pickup_zipcode_1",
    "pickup_start_datetime_1", "pickup_end_datetime_1",
    "pickup_po_1", "pickup_commodity_1", "pickup_weight_1",
    "delivery_customer_1", "delivery_address_1", "delivery_city_1",
    "delivery_state_1", "delivery_zipcode_1",
    "delivery_start_datetime_1", "delivery_end_datetime_1",
    "delivery_po_1", "delivery_commodity_1", "delivery_weight_1",
]
schema = T.StructType([T.StructField(f, T.StringType()) for f in fields])

# ============================================================
# 3. Ground Truth Records
#    Built from the 5 real CY txt files validated locally.
#    Add more records here as you process new batches.
# ============================================================
truth_records = [
    # ── FL-GA: Load 28861101 ─────────────────────────────────────────────────
    {
        "source_file": "dbfs:/Volumes/logistics/bronze/raw/txt/source=CY/1679676366011_CY%20FL-GA.txt",
        "broker_name": "Coyote Logistics, LLC",
        "broker_rep": "Tamaz Bazgadze",
        "broker_phone": "+1 (423) 385 3805 x2246",
        "broker_fax": "+1 (847) 810 4891",
        "broker_email": "CarrierInvoices@coyote.com",
        "broker_address": "960 Northpoint Parkway",
        "broker_city": "Alpharetta",
        "broker_state": "GA",
        "broker_zipcode": "30005",
        "loadConfirmationNumber": "28861101",
        "equipment_type": "Van, 53'",
        "carrier_name": "GTT Freight Corp",
        "carrier_usdot": "3723304",
        "carrier_phone": "None",
        "carrier_email": "gtt.expresscorp@gmail.com",
        "carrier_fax": "None",
        "totalCarrierPay": "600.00",
        "flat_rate_amount": "276.60",
        "fuel_surcharge_amount": "323.40",
        "pickup_customer_1": "United Sugars",
        "pickup_address_1": "450 SONORA DRIVE GATE D",
        "pickup_city_1": "Clewiston",
        "pickup_state_1": "FL",
        "pickup_zipcode_1": "33440",
        "pickup_start_datetime_1": "2023-03-29T08:00:00",
        "pickup_end_datetime_1": "2023-03-29T13:00:00",
        "pickup_po_1": "PO-1162791",
        "pickup_commodity_1": "Miscellaneous",
        "pickup_weight_1": "44455",
        "delivery_customer_1": "Batory Foods",
        "delivery_address_1": "885 DOUGLAS HILLS RD",
        "delivery_city_1": "Lithia Springs",
        "delivery_state_1": "GA",
        "delivery_zipcode_1": "30122",
        "delivery_start_datetime_1": "2023-03-30T09:30:00",
        "delivery_end_datetime_1": "",
        "delivery_po_1": "PO-1162791",
        "delivery_commodity_1": "Miscellaneous",
        "delivery_weight_1": "44455",
    },
    # ── GA-FL: Load 29630485 ─────────────────────────────────────────────────
    {
        "source_file": "dbfs:/Volumes/logistics/bronze/raw/txt/source=CY/1691428773382_CY%20GA-FL.txt",
        "broker_name": "Coyote Logistics, LLC",
        "broker_rep": "Danny Matkovic",
        "broker_phone": "+1 (773) 365 6256 x6256",
        "broker_fax": "+1 (773) 365 4256",
        "broker_email": "CarrierInvoices@coyote.com",
        "broker_address": "960 Northpoint Parkway",
        "broker_city": "Alpharetta",
        "broker_state": "GA",
        "broker_zipcode": "30005",
        "loadConfirmationNumber": "29630485",
        "equipment_type": "Van, 53' x 102 x 110",
        "carrier_name": "GTT Freight Corp",
        "carrier_usdot": "3723304",
        "carrier_phone": "None",
        "carrier_email": "gtt.expresscorp@gmail.com",
        "carrier_fax": "None",
        "totalCarrierPay": "1100.00",
        "flat_rate_amount": "883.46",
        "fuel_surcharge_amount": "216.54",
        "pickup_customer_1": "Cott Beverages",
        "pickup_address_1": "4801 CARGO DR",
        "pickup_city_1": "Columbus",
        "pickup_state_1": "GA",
        "pickup_zipcode_1": "31907",
        "pickup_start_datetime_1": "2023-08-08T13:00:00",
        "pickup_end_datetime_1": "",
        "pickup_po_1": "0085070634; 0052900784",
        "pickup_commodity_1": "Miscellaneous",
        "pickup_weight_1": "20000",
        "delivery_customer_1": "Refresco Lakeland Plant",
        "delivery_address_1": "2090 BARTOW RD",
        "delivery_city_1": "Lakeland",
        "delivery_state_1": "FL",
        "delivery_zipcode_1": "33801",
        "delivery_start_datetime_1": "2023-08-09T10:00:00",
        "delivery_end_datetime_1": "",
        "delivery_po_1": "0085070634",
        "delivery_commodity_1": "Miscellaneous",
        "delivery_weight_1": "20000",
    },
    # ── SC-FL: Load 29630660 ─────────────────────────────────────────────────
    {
        "source_file": "dbfs:/Volumes/logistics/bronze/raw/txt/source=CY/1690978355931_CY%20SC-FL.txt",
        "broker_name": "Coyote Logistics, LLC",
        "broker_rep": "Danny Matkovic",
        "broker_phone": "+1 (773) 365 6256 x6256",
        "broker_fax": "+1 (773) 365 4256",
        "broker_email": "CarrierInvoices@coyote.com",
        "broker_address": "960 Northpoint Parkway",
        "broker_city": "Alpharetta",
        "broker_state": "GA",
        "broker_zipcode": "30005",
        "loadConfirmationNumber": "29630660",
        "equipment_type": "Van, 53'",
        "carrier_name": "GTT Freight Corp",
        "carrier_usdot": "3723304",
        "carrier_phone": "None",
        "carrier_email": "gtt.expresscorp@gmail.com",
        "carrier_fax": "None",
        "totalCarrierPay": "1200.00",
        "flat_rate_amount": "973.20",
        "fuel_surcharge_amount": "226.80",
        "pickup_customer_1": "Mars Pet Care",
        "pickup_address_1": "451 Prosperity Drive",
        "pickup_city_1": "Orangeburg",
        "pickup_state_1": "SC",
        "pickup_zipcode_1": "29115",
        "pickup_start_datetime_1": "2023-08-05T10:00:00",
        "pickup_end_datetime_1": "",
        "pickup_po_1": "28561277; 4529291977",
        "pickup_commodity_1": "General Merchandise",
        "pickup_weight_1": "41459",
        "delivery_customer_1": "WALMART/SAMS GROC DC 6071 EDI",
        "delivery_address_1": "5600 State Road 544",
        "delivery_city_1": "Winter Haven",
        "delivery_state_1": "FL",
        "delivery_zipcode_1": "33881",
        "delivery_start_datetime_1": "2023-08-06T07:00:00",
        "delivery_end_datetime_1": "",
        "delivery_po_1": "4529291977; 22082889",
        "delivery_commodity_1": "General Merchandise",
        "delivery_weight_1": "41459",
    },
    # ── TX-FL: Load 29587264 ─────────────────────────────────────────────────
    {
        "source_file": "dbfs:/Volumes/logistics/bronze/raw/txt/source=CY/1690231789167_CY%20TX-FL.txt",
        "broker_name": "Coyote Logistics, LLC",
        "broker_rep": "Danny Matkovic",
        "broker_phone": "+1 (773) 365 6256 x6256",
        "broker_fax": "+1 (773) 365 4256",
        "broker_email": "CarrierInvoices@coyote.com",
        "broker_address": "960 Northpoint Parkway",
        "broker_city": "Alpharetta",
        "broker_state": "GA",
        "broker_zipcode": "30005",
        "loadConfirmationNumber": "29587264",
        "equipment_type": "Van, 53'",
        "carrier_name": "GTT Freight Corp",
        "carrier_usdot": "3723304",
        "carrier_phone": "None",
        "carrier_email": "gtt.expresscorp@gmail.com",
        "carrier_fax": "None",
        "totalCarrierPay": "2400.00",
        "flat_rate_amount": "1830.72",
        "fuel_surcharge_amount": "569.28",
        "pickup_customer_1": "Wrist USA",
        "pickup_address_1": "1485 E. Sam Houston Suite 100",
        "pickup_city_1": "Pasadena",
        "pickup_state_1": "TX",
        "pickup_zipcode_1": "77501",
        "pickup_start_datetime_1": "2023-07-25T13:00:00",
        "pickup_end_datetime_1": "",
        "pickup_po_1": "49125",
        "pickup_commodity_1": "Misc",
        "pickup_weight_1": "44500",
        "delivery_customer_1": "USply LLC",
        "delivery_address_1": "9400 NW 104TH ST",
        "delivery_city_1": "Medley",
        "delivery_state_1": "FL",
        "delivery_zipcode_1": "33178",
        "delivery_start_datetime_1": "2023-07-27T08:00:00",
        "delivery_end_datetime_1": "2023-07-27T12:00:00",
        "delivery_po_1": "49125",
        "delivery_commodity_1": "",
        "delivery_weight_1": "",
    },
    # ── GA-AL: Load 29677913 ─────────────────────────────────────────────────
    {
        "source_file": "dbfs:/Volumes/logistics/bronze/raw/txt/source=CY/1691681755269_CY%20GA-AL.txt",
        "broker_name": "Coyote Logistics, LLC",
        "broker_rep": "Danny Matkovic",
        "broker_phone": "+1 (773) 365 6256 x6256",
        "broker_fax": "+1 (773) 365 4256",
        "broker_email": "CarrierInvoices@coyote.com",
        "broker_address": "960 Northpoint Parkway",
        "broker_city": "Alpharetta",
        "broker_state": "GA",
        "broker_zipcode": "30005",
        "loadConfirmationNumber": "29677913",
        "equipment_type": "Van, 53'",
        "carrier_name": "GTT Freight Corp",
        "carrier_usdot": "3723304",
        "carrier_phone": "None",
        "carrier_email": "gtt.expresscorp@gmail.com",
        "carrier_fax": "None",
        "totalCarrierPay": "650.00",
        "flat_rate_amount": "559.34",
        "fuel_surcharge_amount": "90.66",
        "pickup_customer_1": "Del Monte Corp",
        "pickup_address_1": "4475 S FULTON PKWY",
        "pickup_city_1": "Atlanta",
        "pickup_state_1": "GA",
        "pickup_zipcode_1": "30349",
        "pickup_start_datetime_1": "2023-08-11T19:15:00",
        "pickup_end_datetime_1": "",
        "pickup_po_1": "K13324501230808_12046871; 81502555",
        "pickup_commodity_1": "Food Product",
        "pickup_weight_1": "39755",
        "delivery_customer_1": "Publix Super Markets",
        "delivery_address_1": "7200 Jefferson Metro Pkwy",
        "delivery_city_1": "Mc Calla",
        "delivery_state_1": "AL",
        "delivery_zipcode_1": "35111",
        "delivery_start_datetime_1": "2023-08-12T11:00:00",
        "delivery_end_datetime_1": "",
        "delivery_po_1": "K13324501230808_12046871",
        "delivery_commodity_1": "Food Product",
        "delivery_weight_1": "39755",
    },
]

truth_df = spark.createDataFrame(truth_records, schema=schema)

# ============================================================
# 4. Load target table
# ============================================================
target_df = spark.table(source_table)

# ============================================================
# 5. Normalize (trim + lowercase for comparison)
# ============================================================
def normalize(df):
    str_cols = [c for c, t in df.dtypes if t == "string"]
    return df.select(*[
        F.trim(F.lower(F.col(c))).alias(c) if c in str_cols else F.col(c)
        for c in df.columns
    ])

truth_norm  = normalize(truth_df)
target_norm = normalize(target_df)

# ============================================================
# 6. Field-by-field comparison per load
# ============================================================
all_results = []

for truth_record in truth_records:
    load_id = truth_record["loadConfirmationNumber"]
    rows = target_norm.filter(
        F.col("loadConfirmationNumber") == load_id.lower()
    ).collect()

    if not rows:
        logger.error(f"[{load_id}] ❌ Record NOT FOUND in table")
        for f in fields:
            all_results.append((load_id, f, "❌ Missing record", truth_record.get(f), None))
        continue

    logger.info(f"[{load_id}] Record found — comparing fields")
    target_vals = rows[0].asDict()

    for f in fields:
        truth_val  = str(truth_record.get(f) or "").strip().lower()
        target_val = str(target_vals.get(f) or "").strip().lower()
        status = "✅ Match" if truth_val == target_val else "❌ Mismatch"
        all_results.append((load_id, f, status, truth_record.get(f), target_vals.get(f)))

# ============================================================
# 7. Log results
# ============================================================
errors = []
for load_id, field, status, truth, target in all_results:
    msg = f"[{load_id}] {field:42} | {status} | expected='{truth}' | got='{target}'"
    if status.startswith("✅"):
        logger.info(msg)
    else:
        logger.error(msg)
        errors.append((load_id, field, truth, target))

# ============================================================
# 8. Summary
# ============================================================
total   = len(all_results)
matched = sum(1 for r in all_results if r[2].startswith("✅"))
logger.info(f"\n{'='*60}")
logger.info(f"VALIDATION SUMMARY")
logger.info(f"  Records checked : {len(truth_records)}")
logger.info(f"  Fields checked  : {total}")
logger.info(f"  Passed          : {matched}")
logger.info(f"  Failed          : {len(errors)}")
logger.info(f"{'='*60}")

# ============================================================
# 9. Fail pipeline if errors
# ============================================================
if errors:
    for load_id, field, truth, target in errors:
        logger.error(f"  [{load_id}] {field}: expected='{truth}' got='{target}'")
    raise ValueError(f"CY Validation failed: {len(errors)} field(s) did not match across {len(truth_records)} records.")

logger.info("✅ All fields match. CY extraction validated successfully.")
