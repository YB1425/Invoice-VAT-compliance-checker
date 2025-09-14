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
JOB_ID = st.secrets["JOB_ID"]
WAREHOUSE_ID = st.secrets["WAREHOUSE_ID"]

headers = {"Authorization": f"Bearer {TOKEN}"}

# ==== HELPERS ====
def upload_to_volume(file_name, file_bytes, dest_path):
    url = f"{INSTANCE}/api/2.0/fs/files{dest_path}/{file_name}"
    resp = requests.put(url, headers=headers, data=file_bytes)
    resp.raise_for_status()

def run_parse_job():
    url = f"{INSTANCE}/api/2.1/jobs/run-now"
    resp = requests.post(url, headers=headers, json={"job_id": JOB_ID})
    resp.raise_for_status()
    return resp.json()["run_id"]

def wait_for_result(run_id):
    url = f"{INSTANCE}/api/2.1/jobs/runs/get?run_id={run_id}"
    while True:
        resp = requests.get(url, headers=headers).json()
        if resp["state"]["life_cycle_state"] == "TERMINATED":
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
        st.error("SQL execution failed: " + str(res))
        return pd.DataFrame()

    # If result key missing, return empty dataframe
    if "result" not in res or "data_array" not in res["result"]:
        return pd.DataFrame()

    cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
    rows = []
    for r in res["result"]["data_array"]:
        row = []
        for c in r:
            if isinstance(c, dict) and "value" in c:
                row.append(c["value"])
            else:
                row.append(c)
        rows.append(row)

    return pd.DataFrame(rows, columns=cols)

def df_to_excel(df_dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
    return output.getvalue()

# ==== CONNECTION TEST ====
st.subheader("🔌 Databricks Connection Check")
test_sql = "SELECT current_date() AS today"
df_test = run_sql(test_sql)
if not df_test.empty and "today" in df_test.columns:
    st.success(f"✅ SQL Warehouse connected! Today's date = {df_test.at[0, 'today']}")
else:
    st.error("❌ SQL Warehouse test failed.")

# ==== TABS ====
tab1, tab2, tab3 = st.tabs(["📥 New Compliance Check", "📂 Archived Invoices", "📂 Archived Failed Checks"])

with tab1:
    batch_name_input = st.text_input("📦 Enter a batch name (optional)", placeholder="e.g. Sept14_Invoices")
    if batch_name_input and batch_name_input.strip():
        BATCH_NAME = batch_name_input.strip().replace(" ", "_")
    else:
        BATCH_NAME = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")

    uploads = st.file_uploader("Upload up to 8 invoice PDFs", type=["pdf"], accept_multiple_files=True)

    if uploads:
        if len(uploads) > MAX_FILES:
            st.error(f"⚠️ You can only upload up to {MAX_FILES} files at once.")
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
                    # Upload files (working + archive immediately)
                    with st.spinner("Uploading files..."):
                        for f in ok:
                            file_bytes = f.read()
                            upload_to_volume(f.name, file_bytes, VOLUME_PATH)   # working
                            upload_to_volume(f.name, file_bytes, f"{ARCHIVE_PATH}/{BATCH_NAME}")  # archive

                    # Run job
                    with st.spinner("Running Databricks job..."):
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

                    # --- Archive results in SQL ---
                    with st.spinner("Archiving SQL results..."):
                        run_sql(f"""
                            INSERT INTO dev_uc_catalog.default.zatca_invoices_head_archive
                            SELECT *, '{BATCH_NAME}' AS batch_name
                            FROM dev_uc_catalog.default.zatca_invoices_head
                        """)
                        run_sql(f"""
                            INSERT INTO dev_uc_catalog.default.zatca_checks_flat_archive
                            SELECT *, '{BATCH_NAME}' AS batch_name
                            FROM dev_uc_catalog.default.zatca_checks_flat
                        """)

                    # --- Cleanup working tables ---
                    with st.spinner("Cleaning up..."):
                        run_sql("TRUNCATE TABLE dev_uc_catalog.default.zatca_invoices_head")
                        run_sql("TRUNCATE TABLE dev_uc_catalog.default.zatca_checks_flat")
                        run_sql("TRUNCATE TABLE dev_uc_catalog.default.zatca_invoice_check_parsed")

                    st.success("Session archived and reset ✅")

with tab2:
    st.subheader("📂 Archived Invoices")
    batch_list = run_sql("SELECT DISTINCT batch_name FROM dev_uc_catalog.default.zatca_invoices_head_archive ORDER BY batch_name DESC")
    if not batch_list.empty:
        selected_batch = st.selectbox("Choose a batch", batch_list["batch_name"])
        df_archive_invoices = run_sql(f"""
            SELECT * FROM dev_uc_catalog.default.zatca_invoices_head_archive
            WHERE batch_name = '{selected_batch}'
            ORDER BY path
        """)
        st.dataframe(df_archive_invoices)
        st.download_button("⬇️ Download Invoices Archive CSV",
                           data=df_archive_invoices.to_csv(index=False).encode("utf-8"),
                           file_name=f"invoices_{selected_batch}.csv",
                           mime="text/csv")
    else:
        st.info("No archived invoices found yet.")

with tab3:
    st.subheader("📂 Archived Failed Checks")
    batch_list = run_sql("SELECT DISTINCT batch_name FROM dev_uc_catalog.default.zatca_checks_flat_archive ORDER BY batch_name DESC")
    if not batch_list.empty:
        selected_batch = st.selectbox("Choose a batch", batch_list["batch_name"])
        df_archive_checks = run_sql(f"""
            SELECT * FROM dev_uc_catalog.default.zatca_checks_flat_archive
            WHERE batch_name = '{selected_batch}'
            ORDER BY path, id
        """)
        st.dataframe(df_archive_checks)
        st.download_button("⬇️ Download Checks Archive CSV",
                           data=df_archive_checks.to_csv(index=False).encode("utf-8"),
                           file_name=f"checks_{selected_batch}.csv",
                           mime="text/csv")
    else:
        st.info("No archived checks found yet.")

# ==== DISCLAIMER ====
st.markdown("""
---
⚠️ **Disclaimer:**  
This program is a **proof-of-concept tool**.  
- Results may be inaccurate or incomplete.  
- It does **not** validate electronic VAT **QR codes** or **UBL XML** compliance.  
For official ZATCA compliance, always use certified solutions.
""")
