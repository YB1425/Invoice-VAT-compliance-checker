import streamlit as st
import pandas as pd
import requests
import time
import datetime
from io import BytesIO

# ==== CONFIG ====
st.set_page_config(page_title="Invoice Compliance Checker", layout="centered")
st.title("🚗 Invoice Compliance Checker")

MAX_MB = 75
MAX_BYTES = MAX_MB * 1024 * 1024
MAX_FILES = 8

INSTANCE = st.secrets["DATABRICKS_INSTANCE"]
TOKEN = st.secrets["DATABRICKS_TOKEN"]
VOLUME_PATH = st.secrets["VOLUME_PATH"]
ARCHIVE_PATH = st.secrets["ARCHIVE_PATH"]
JOB_ID = st.secrets["JOB_ID"]           # VAT_Checker_Notebook job id
WAREHOUSE_ID = st.secrets["WAREHOUSE_ID"]

headers = {"Authorization": f"Bearer {TOKEN}"}

# ==== HELPERS ====
def upload_to_volume(file_name, file_bytes):
    url = f"{INSTANCE}/api/2.0/fs/files{VOLUME_PATH}/{file_name}"
    resp = requests.put(url, headers=headers, data=file_bytes)
    resp.raise_for_status()
    return {"status": "uploaded", "file": file_name}

def run_parse_job():
    url = f"{INSTANCE}/api/2.1/jobs/run-now"
    resp = requests.post(url, headers=headers, json={"job_id": JOB_ID})
    resp.raise_for_status()
    return resp.json()["run_id"]

def wait_for_result(run_id):
    url = f"{INSTANCE}/api/2.1/jobs/runs/get?run_id={run_id}"
    while True:
        resp = requests.get(url, headers=headers).json()
        state = resp["state"]["life_cycle_state"]
        if state == "TERMINATED":
            return resp
        time.sleep(5)

def run_sql(sql: str):
    submit_url = f"{INSTANCE}/api/2.0/sql/statements/"
    payload = {"statement": sql, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "30s"}
    resp = requests.post(submit_url, headers=headers, json=payload).json()

    if "statement_id" not in resp:
        st.error(f"SQL submission failed: {resp}")
        return pd.DataFrame()

    statement_id = resp["statement_id"]

    while True:
        res = requests.get(f"{submit_url}{statement_id}", headers=headers).json()
        state = res["status"]["state"]
        if state in ["SUCCEEDED", "FAILED", "CANCELED"]:
            break
        time.sleep(2)

    if res["status"]["state"] != "SUCCEEDED":
        # Show detailed error if available
        if "error" in res:
            st.error(f"SQL execution failed: {res['error'].get('message', res)}")
        else:
            st.error("SQL execution failed: " + str(res))
        return pd.DataFrame()

    cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
    rows = [[c["value"] for c in r] for r in res["result"]["data_array"]]
    return pd.DataFrame(rows, columns=cols)

def list_files(volume_path):
    url = f"{INSTANCE}/api/2.0/fs/files{volume_path}?recursive=true"
    resp = requests.get(url, headers=headers).json()
    return resp.get("files", [])

def copy_to_archive(folder):
    files = list_files(VOLUME_PATH)
    if not files:
        return "No files to archive."

    for f in files:
        src_url = f"{INSTANCE}/api/2.0/fs/files{f['path']}"
        dest_path = f"{ARCHIVE_PATH}/{folder}/{f['name']}"
        dest_url = f"{INSTANCE}/api/2.0/fs/files{dest_path}"

        data = requests.get(src_url, headers=headers).content
        resp = requests.put(dest_url, headers=headers, data=data)
        resp.raise_for_status()

    return f"Archived {len(files)} files to {ARCHIVE_PATH}/{folder}"

def clear_volume():
    files = list_files(VOLUME_PATH)
    for f in files:
        url = f"{INSTANCE}/api/2.0/fs/files{f['path']}"
        requests.delete(url, headers=headers)
    return f"Cleared {len(files)} files from {VOLUME_PATH}"

def df_to_excel(df_dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
    return output.getvalue()

# ==== CONNECTION TEST ====
st.subheader("🔌 Databricks Connection Check")
test_sql = "SELECT current_date() AS today"
try:
    df_test = run_sql(test_sql)
    if not df_test.empty:
        st.success(f"✅ SQL Warehouse connected! Today's date = {df_test.iloc[0]['today']}")
    else:
        st.error("❌ SQL Warehouse test failed. No data returned.")
except Exception as e:
    st.error(f"❌ SQL Warehouse connection error: {e}")

# ==== STREAMLIT UI ====
batch_name_input = st.text_input("📦 Enter a batch name (optional)", placeholder="e.g. Sept14_Invoices")

# normalize batch name or fallback to timestamp
if batch_name_input and batch_name_input.strip():
    BATCH_NAME = batch_name_input.strip().replace(" ", "_")
else:
    BATCH_NAME = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

uploads = st.file_uploader("Upload up to 8 invoice PDFs", type=["pdf"], accept_multiple_files=True)

if uploads:
    if len(uploads) > MAX_FILES:
        st.error(f"⚠️ You can only upload up to {MAX_FILES} files at once. Please remove extra files.")
    else:
        too_big = [f for f in uploads if f.size > MAX_BYTES]
        ok = [f for f in uploads if f.size <= MAX_BYTES]

        if too_big:
            st.error(f"{len(too_big)} file(s) exceed {MAX_MB} MB and were skipped: "
                     + ", ".join(f.name for f in too_big))

        if ok:
            st.success(f"Received {len(ok)} file(s).")
            st.dataframe(pd.DataFrame(
                [{"File": f.name, "Size (MB)": round(f.size / 1024 / 1024, 2)} for f in ok]
            ))

            if st.button("🚀 Run VAT Compliance Check"):
                # Upload files
                with st.spinner("Uploading files to Databricks..."):
                    for f in ok:
                        upload_to_volume(f.name, f.read())

                # Trigger job
                with st.spinner("Running Databricks VAT_Checker_Notebook..."):
                    run_id = run_parse_job()
                    wait_for_result(run_id)

                st.success("✅ Job completed! Fetching results...")

                # --- Summary ---
                summary_sql = """
                SELECT path, invoice_number, issue_date, final_decision
                FROM dev_uc_catalog.default.zatca_invoices_head
                ORDER BY path;
                """
                df_summary = run_sql(summary_sql)
                st.subheader("📄 Invoice Summary")
                st.dataframe(df_summary)

                # --- Detailed failures ---
                details_sql = """
                SELECT h.path, h.invoice_number, h.issue_date, h.final_decision,
                       c.id AS failed_rule_id, c.name AS failed_rule_name, c.reason AS failed_reason
                FROM dev_uc_catalog.default.zatca_invoices_head h
                JOIN dev_uc_catalog.default.zatca_checks_flat c
                  ON h.path = c.path
                WHERE c.result = 'fail'
                ORDER BY h.path, c.id;
                """
                df_details = run_sql(details_sql)
                if not df_details.empty:
                    st.subheader("⚠️ Failed Checks")
                    st.dataframe(df_details)
                else:
                    st.success("🎉 All invoices passed compliance checks!")

                # --- Export buttons ---
                st.subheader("📥 Export Results")
                excel_data = df_to_excel({"Summary": df_summary, "Failed Checks": df_details})
                st.download_button("⬇️ Download Excel",
                                   data=excel_data,
                                   file_name=f"vat_compliance_results_{BATCH_NAME}.xlsx")

                st.download_button("⬇️ Download Summary CSV",
                                   data=df_summary.to_csv(index=False).encode("utf-8"),
                                   file_name=f"vat_summary_{BATCH_NAME}.csv",
                                   mime="text/csv")

                if not df_details.empty:
                    st.download_button("⬇️ Download Failed Checks CSV",
                                       data=df_details.to_csv(index=False).encode("utf-8"),
                                       file_name=f"vat_failed_checks_{BATCH_NAME}.csv",
                                       mime="text/csv")

                # --- Archive & Reset ---
                with st.spinner("Archiving processed files..."):
                    msg = copy_to_archive(BATCH_NAME)
                    st.success(msg)

                with st.spinner("Resetting volume for next batch..."):
                    msg = clear_volume()
                    st.info(msg)

else:
    st.write("Upload one or more PDF files (≤ 75 MB each, max 8 files).")
