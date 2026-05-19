import os
import re
import json
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType

# ============================================================
# Logger
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================
# Base Extractor
# ============================================================
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, text: str) -> dict:
        pass


# ============================================================
# UTB Extractor — calibrado para output de pdfplumber
# ============================================================
class UTBExtractor(BaseExtractor):
    """
    Extracts broker, carrier, pickup, and delivery data from
    USA Truck Brokers load confirmation text files converted
    with pdfplumber.

    Key differences vs pymupdf4llm output:
      - No markdown formatting (no ** or ##)
      - Two-column PDF layout causes right-column text to be
        appended to left-column lines (e.g. 'Terms:' content
        appended to broker address lines, 'Service Notes Payment'
        appended to carrier name line, 'Total To Pay' appended
        to carrier address line).
      - Regex patterns include explicit cutoffs for known
        right-column contamination phrases.
    """

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        # pdfplumber output has no markdown — just clean whitespace
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        text = re.sub(r"\r\n?", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"(?<=\w)\n(?=[A-Z0-9])", " ", text)
        text = "\n".join([ln.strip() for ln in text.splitlines() if ln.strip()])
        return text.strip()

    def _combine_dt(self, date_str: str, time_str: str) -> str:
        if not date_str or not time_str:
            return ""
        for fmt in [
            "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %I:%M %p",    "%m/%d/%Y %H:%M",
        ]:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        return ""

    def _extract_stop_blocks(self, text: str, stop_type: str) -> list:
        stop_type = stop_type.lower()
        pattern = re.compile(rf"{stop_type.title()}\s*#\s*\d+", re.I)
        markers = list(pattern.finditer(text))

        if not markers:
            alt = re.compile(
                rf"{stop_type.title()}\b(.*?)(?=(Pickup|Delivery|Send Invoice|Total|$))",
                re.I | re.S
            )
            return [m.group(1).strip() for m in alt.finditer(text)]

        blocks = []
        for i, m in enumerate(markers):
            start = m.end()
            end = markers[i + 1].start() if i + 1 < len(markers) else len(text)
            sub = text[start:end]
            cutoff = re.search(r"(Send Invoice|Special Instructions|Total|Please Sign)", sub, re.I)
            blocks.append(sub[:cutoff.start()].strip() if cutoff else sub.strip())
        return blocks

    def extract(self, text: str) -> dict:
        data = {f: "" for f in EXTRACTION_FIELDS}
        if not text:
            return data

        text = self._normalize(text)

        # ── Load / Trip number ───────────────────────────────────────────────
        if m := re.search(r"Trip\s*#[:\s]*([A-Z0-9\-]+)", text, re.I):
            data["loadConfirmationNumber"] = m.group(1).strip()

        # ── Total carrier pay ────────────────────────────────────────────────
        if m := re.search(r"Total\s*(?:To\s*Pay|Carrier\s*Pay)[:\s\$]*([\d,]+\.\d{2})", text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()

        # ── Broker info ──────────────────────────────────────────────────────
        # pdfplumber: broker block is in 'Send Invoice to:' section.
        # Right column ('Terms:') text gets appended to left-column lines,
        # so we cut aggressively at known right-column phrases.
        broker_chunk = re.search(
            r"Send Invoice to:(.*?)(?=CONTRACT|ADDENDUM|$)", text, re.I | re.S
        )
        if broker_chunk:
            btext = broker_chunk.group(1)

            if m := re.search(r"(USA Truck Brokers Inc\.?)", btext, re.I):
                data["broker_name"] = m.group(1).strip()

            # Phone: may have right-column text after it — stop at first non-digit
            if m := re.search(r"Tel[:\s]*([\d\-]+)", btext, re.I):
                data["broker_phone"] = m.group(1).strip()

            if m := re.search(r"Fax[:\s]*([\d\-]+)", btext, re.I):
                data["broker_fax"] = m.group(1).strip()

            if emails := re.findall(r"[\w\.\-]+@[\w\.\-]+\.\w+", btext):
                data["broker_email"] = "; ".join(sorted(set(emails)))

            # Address line: 'NNNN Street Name Suite NNN and <right-col text>'
            # Strategy: match up to and including Suite/Ste/Floor/Unit NNN
            # If no suite, match number + street words up to conjunction or excess text
            addr_m = re.search(
                r"(\d{3,6}[^,\n]+?(?:Suite|Ste|Unit|Floor)\s*\d+)",
                btext, re.I
            )
            if not addr_m:
                # Fallback: address without suite — stop at 2+ spaces or conjunction
                addr_m = re.search(
                    r"(\d{3,6}\s+[\w\s#\.,]+?)(?=\s+(?:and|please|or|we|if|in|for|the)\b|\s{3,}|$)",
                    btext, re.I | re.M
                )
            if addr_m:
                data["broker_address"] = re.sub(r"\s+", " ", addr_m.group(1)).strip()

            # City, State ZIP — may have right-column text after zip
            if m := re.search(r"([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5})", btext, re.I):
                city = re.sub(r"\bLake,s\b", "Lakes", m.group(1), flags=re.I).strip()
                data["broker_city"]    = city
                data["broker_state"]   = m.group(2).strip()
                data["broker_zipcode"] = m.group(3).strip()

        # ── Carrier info ─────────────────────────────────────────────────────
        # pdfplumber: 'Carrier GTT FREIGHT CORP LLC Service Notes Payment'
        # — cut before 'Service Notes' or 2+ spaces
        carrier_block = re.search(
            r"Trailer VIN Number.*?(?=Send Invoice|Please Sign|$)",
            text, re.I | re.S
        )
        if carrier_block:
            ctext = carrier_block.group(0)

            # carrier_name: stop before 'Service Notes' or extra whitespace
            if m := re.search(
                r"Carrier\s+([A-Z0-9\s&\.\-]+?)(?=\s+Service\s+Notes|\s{3,}|\n|MC\s*#)",
                ctext, re.I
            ):
                data["carrier_name"] = m.group(1).strip()

            if m := re.search(r"MC\s*#[:\s]*(\d+)", ctext, re.I):
                data["carrier_mc"] = m.group(1).strip()

            # carrier_address: stop before 'Total To Pay' or extra whitespace
            if m := re.search(
                r"Address\s+([^\n]+?)(?=\s+Total\s+To\s+Pay|\s{3,}|$)",
                ctext, re.I | re.M
            ):
                data["carrier_address"] = m.group(1).strip()

            if m := re.search(r"City\s+([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5})", ctext, re.I):
                data["carrier_city"]    = m.group(1).strip()
                data["carrier_state"]   = m.group(2).strip()
                data["carrier_zipcode"] = m.group(3).strip()

            if m := re.search(r"Phone\s+([\d\-\s\(\)]+)", ctext, re.I):
                data["carrier_phone"] = m.group(1).strip()

            if m := re.search(r"Fax\s+([\d\-\s\(\)]*)", ctext, re.I):
                data["carrier_fax"] = m.group(1).strip()

            if m := re.search(
                r"Contact\s+([A-Za-z\s\-\.,\(\)]+?)(?=\s*(?:Truck|Trailer|Total|Please|$))",
                ctext, re.I
            ):
                data["carrier_contact"] = m.group(1).strip()

        # ── Pickups ──────────────────────────────────────────────────────────
        pickups = self._extract_stop_blocks(text, "pickup")
        for i, block in enumerate(pickups[:3], start=1):
            if m := re.search(r"Customer[:\s]*([A-Z0-9 &\.\-]+?)(?=\s*(?:Pick|Address|City|Zip|Phone|$))", block, re.I):
                data[f"pickup_customer_{i}"] = m.group(1).strip()
            if m := re.search(r"Address\s*#1[:\s]*([0-9A-Za-z\.,#\-\s]+?)(?:\n|Pick(?:up)?\s*Time|$)", block, re.I):
                data[f"pickup_address_{i}"] = m.group(1).strip()
            if m := re.search(r"City[:\s]*([A-Za-z\s]+),\s*([A-Z]{2})", block, re.I):
                data[f"pickup_city_{i}"]  = m.group(1).strip()
                data[f"pickup_state_{i}"] = m.group(2).strip()
            if m := re.search(r"Zip(?:\s*Code)?[:\s]*([0-9]{5})", block, re.I):
                data[f"pickup_zipcode_{i}"] = m.group(1).strip()
            date_m = re.search(r"Pick(?:up)?\s*Date[:\s]*([0-9/]+)", block, re.I)
            times_m = re.search(r"(?:Pick(?:up)?\s*Time|Time)[:\s]*([0-9:]+(?:\s*[AP]M)?)\s*-\s*([0-9:]+(?:\s*[AP]M)?)", block, re.I)
            if date_m and times_m:
                data[f"pickup_start_datetime_{i}"] = self._combine_dt(date_m.group(1), times_m.group(1))
                data[f"pickup_end_datetime_{i}"]   = self._combine_dt(date_m.group(1), times_m.group(2))

        # ── Deliveries ───────────────────────────────────────────────────────
        deliveries = self._extract_stop_blocks(text, "delivery")
        for i, block in enumerate(deliveries[:3], start=1):
            if m := re.search(r"Customer[:\s]*([A-Z0-9 &\.\-]+?)(?=\s*(?:Delivery|Address|City|Zip|Phone|$))", block, re.I):
                data[f"delivery_customer_{i}"] = m.group(1).strip()
            if m := re.search(r"Address\s*#1[:\s]*([0-9A-Za-z\.,#\-\s]+)", block, re.I):
                val = re.sub(r"\bDelivery\s*Time[:\s]*[0-9: -ETapm]*", "", m.group(1), flags=re.I).strip()
                data[f"delivery_address_{i}"] = val
            if m := re.search(r"City[:\s]*([A-Za-z\s]+),\s*([A-Z]{2})", block, re.I):
                data[f"delivery_city_{i}"]  = m.group(1).strip()
                data[f"delivery_state_{i}"] = m.group(2).strip()
            if m := re.search(r"Zip(?:\s*Code)?[:\s]*([0-9]{5})", block, re.I):
                data[f"delivery_zipcode_{i}"] = m.group(1).strip()
            date_m = re.search(r"Delivery\s*Date[:\s]*([0-9/]+)", block, re.I)
            times_m = re.search(r"(?:Delivery\s*Time|Time)[:\s]*([0-9:]+(?:\s*[AP]M)?)\s*-\s*([0-9:]+(?:\s*[AP]M)?)", block, re.I)
            if date_m and times_m:
                data[f"delivery_start_datetime_{i}"] = self._combine_dt(date_m.group(1), times_m.group(1))
                data[f"delivery_end_datetime_{i}"]   = self._combine_dt(date_m.group(1), times_m.group(2))

        return data


# ============================================================
# Schema fields
# ============================================================
EXTRACTION_FIELDS = [
    "broker_name", "broker_phone", "broker_fax", "broker_address",
    "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay",
    "carrier_name", "carrier_mc", "carrier_address", "carrier_city",
    "carrier_state", "carrier_zipcode", "carrier_phone",
    "carrier_fax", "carrier_contact",
    *[f"{p}_{i}" for p in [
        "pickup_customer", "pickup_address", "pickup_city",
        "pickup_state", "pickup_zipcode",
        "pickup_start_datetime", "pickup_end_datetime",
    ] for i in range(1, 4)],
    *[f"{p}_{i}" for p in [
        "delivery_customer", "delivery_address", "delivery_city",
        "delivery_state", "delivery_zipcode",
        "delivery_start_datetime", "delivery_end_datetime",
    ] for i in range(1, 4)],
    "processed_at",
]


# ============================================================
# Spark UDF wrapper
# ============================================================
def extract_fields_udf():
    extractor = UTBExtractor()

    def _extract(text):
        result = extractor.extract(text or "")
        result["processed_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        return result

    return udf(_extract, MapType(StringType(), StringType()))


# ============================================================
# Main
# ============================================================
def main(p):
    spark = SparkSession.builder.appName("TruckR UTB pdfplumber Extraction").getOrCreate()
    logger.info("Starting UTB (pdfplumber) extraction process")

    df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.txt")
        .option("recursiveFileLookup", "false")
        .load(os.path.join(p["source_path"], "*.txt"))
        .select(col("_metadata.file_path").alias("source_file"), col("content"))
    )
    df = df.withColumn("text", col("content").cast("string")).drop("content")
    logger.info(f"Files detected: {df.count()}")

    extract_udf = extract_fields_udf()
    df = df.withColumn("extracted", extract_udf(col("text")))

    for field in EXTRACTION_FIELDS:
        df = df.withColumn(field, col("extracted").getItem(field))

    df = df.drop("text", "extracted")

    logger.info(f"Writing {df.count()} records to {p['target_table']}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(p["target_table"])
    )
    logger.info("UTB pdfplumber extraction completed successfully.")


# ============================================================
# CLI entry
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UTB pdfplumber Extraction Parameters")
    parser.add_argument("--source_path", required=True)
    parser.add_argument("--target_table", required=True)
    args = parser.parse_args()
    main({"source_path": args.source_path, "target_table": args.target_table})
