import streamlit as st
import pandas as pd
import requests
import time
import datetime
from io import BytesIO

# ==== CONFIG ====
st.set_page_config(page_title="Invoice Compliance Checker", layout="centered")

MAX_MB = 75
MAX_BYTES = MAX_MB * 1024 * 1024
MAX_FILES = 8

INSTANCE = st.secrets["DATABRICKS_INSTANCE"]
TOKEN = st.secrets["DATABRICKS_TOKEN"]
VOLUME_PATH = st.secrets["VOLUME_PATH"]
ARCHIVE_PATH = st.secrets["ARCHIVE_PATH"]
JOB_ID = st.secrets["JOB_ID"]
WAREHOUSE_ID = st.secrets["WAREHOUSE_ID"]

MAIN_PASSWORD = st.secrets["MAIN_PASSWORD"]
FINANCE_PASSWORD = st.secrets["FINANCE_PASSWORD"]

headers = {"Authorization": f"Bearer {TOKEN}"}

# ==== SESSION STATE ====
if "role" not in st.session_state:
    st.session_state.role = None  # "main" or "finance"

if "language" not in st.session_state:
    st.session_state.language = "en"  # default English

# ==== LANGUAGE STRINGS ====
STRINGS = {
    "en": {
        "title": "🚗 Invoice Compliance Checker",
        "main_tab": "📥 New Compliance Check",
        "inv_tab": "📂 Archived Invoices",
        "fail_tab": "📂 Archived Failed Checks",
        "password_prompt": "🔑 Enter password to access this section",
        "finance_prompt": "Finance-only access. Please enter the finance password in Tab 1.",
        "logout": "🚪 Logout",
        "batch_name": "📦 Enter a batch name (optional)",
        "upload_label": "Upload up to 8 invoice PDFs",
        "received": "Received {n} file(s).",
        "too_big": "{n} file(s) exceed {mb} MB and were skipped: {files}",
        "run_check": "🚀 Run VAT Compliance Check",
        "summary": "📄 Invoice Summary",
        "failed": "⚠️ Failed Checks",
        "all_passed": "🎉 All invoices passed compliance checks!",
        "export": "📥 Export Results",
        "download_excel": "⬇️ Download Excel",
        "download_inv_csv": "⬇️ Download Invoices Archive CSV",
        "download_fail_csv": "⬇️ Download Checks Archive CSV",
        "no_archives": "No archived data found yet.",
        "connection_ok": "✅ SQL Warehouse connected! Today's date = {date}",
        "connection_fail": "❌ SQL Warehouse test failed.",
        "wrong_password": "❌ Incorrect password. Please try again.",
        "disclaimer": """
---
⚠️ **Disclaimer:**  
This program is a **proof-of-concept tool**.  
- Results may be inaccurate or incomplete.  
- It does **not** validate electronic VAT **QR codes** or **UBL XML** compliance.  
For official ZATCA compliance, always use certified solutions.
"""
    },
    "ar": {
        "title": "🚗 أداة التحقق من مطابقة الفواتير",
        "main_tab": "📥 التحقق من الفواتير الجديدة",
        "inv_tab": "📂 الفواتير المؤرشفة",
        "fail_tab": "📂 الإخفاقات المؤرشفة",
        "password_prompt": "🔑 أدخل كلمة المرور للوصول إلى هذا القسم",
        "finance_prompt": "الوصول خاص بقسم المالية. يرجى إدخال كلمة مرور المالية في التبويب الأول.",
        "logout": "🚪 تسجيل الخروج",
        "batch_name": "📦 أدخل اسم الدفعة (اختياري)",
        "upload_label": "قم برفع ما يصل إلى 8 ملفات PDF للفواتير",
        "received": "تم استلام {n} ملف(ات).",
        "too_big": "{n} ملف(ات) تتجاوز {mb} ميغابايت وتم تجاهلها: {files}",
        "run_check": "🚀 تشغيل التحقق من مطابقة ضريبة القيمة المضافة",
        "summary": "📄 ملخص الفواتير",
        "failed": "⚠️ الفحوصات الفاشلة",
        "all_passed": "🎉 جميع الفواتير اجتازت التحقق من المطابقة!",
        "export": "📥 تنزيل النتائج",
        "download_excel": "⬇️ تنزيل ملف Excel",
        "download_inv_csv": "⬇️ تنزيل أرشيف الفواتير CSV",
        "download_fail_csv": "⬇️ تنزيل أرشيف الإخفاقات CSV",
        "no_archives": "لا توجد بيانات مؤرشفة حتى الآن.",
        "connection_ok": "✅ تم الاتصال بـ SQL Warehouse! تاريخ اليوم = {date}",
        "connection_fail": "❌ فشل اختبار الاتصال بـ SQL Warehouse.",
        "wrong_password": "❌ كلمة المرور غير صحيحة. يرجى المحاولة مرة أخرى.",
        "disclaimer": """
---
⚠️ **تنويه:**  
هذه الأداة مجرد **إثبات مفهوم**.  
- قد تكون النتائج غير دقيقة أو غير كاملة.  
- لا تتحقق من **رموز QR الإلكترونية** أو **UBL XML** الخاصة بضريبة القيمة المضافة.  
للحصول على مطابقة رسمية مع هيئة الزكاة والضريبة والجمارك، يرجى استخدام الحلول المعتمدة.
"""
    }
}

# ==== LANG SELECTOR + SIDEBAR LOGOUT ====
with st.sidebar:
    lang = st.radio("🌐 Language / اللغة", ["English", "العربية"])
    st.session_state.language = "ar" if lang == "العربية" else "en"

    if st.session_state.role:
        st.success(f"Logged in as: {st.session_state.role}")
        if st.button(STRINGS[st.session_state.language]["logout"]):
            st.session_state.role = None
            st.rerun()

T = STRINGS[st.session_state.language]

st.title(T["title"])

# ==== HELPERS ====
@st.cache_data(ttl=60)
def run_sql(sql: str):
    submit_url = f"{INSTANCE}/api/2.0/sql/statements/"
    payload = {"statement": sql, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "30s"}
    resp = requests.post(submit_url, headers=headers, json=payload).json()
    if "statement_id" not in resp:
        return pd.DataFrame()
    statement_id = resp["statement_id"]

    while True:
        res = requests.get(f"{submit_url}{statement_id}", headers=headers).json()
        if res["status"]["state"] in ["SUCCEEDED", "FAILED", "CANCELED"]:
            break
        time.sleep(2)

    if res["status"]["state"] != "SUCCEEDED":
        return pd.DataFrame()
    if "result" not in res or "data_array" not in res["result"]:
        return pd.DataFrame()

    cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
    rows = []
    for r in res["result"]["data_array"]:
        row = []
        for c in r:
            row.append(c.get("value") if isinstance(c, dict) else c)
        rows.append(row)
    return pd.DataFrame(rows, columns=cols)

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

def df_to_excel(df_dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
    return output.getvalue()

def cleanup_volume(path, batch_name):
    # Delete the whole batch folder in one request
    folder_path = f"{path}/{batch_name}"
    url = f"{INSTANCE}/api/2.0/fs/files{folder_path}?recursive=true"
    resp = requests.delete(url, headers=headers)
    resp.raise_for_status()
    return f"Deleted folder {folder_path}"
# ==== TABS ====
tab1, tab2, tab3 = st.tabs([T["main_tab"], T["inv_tab"], T["fail_tab"]])

# --- Main Tab ---
with tab1:
    if st.session_state.role not in ["main", "finance"]:
        pw = st.text_input(T["password_prompt"], type="password", key="main_pw")
        if pw:
            if pw == MAIN_PASSWORD:
                st.session_state.role = "main"
                st.success("Access granted ✅")
            elif pw == FINANCE_PASSWORD:
                st.session_state.role = "finance"
                st.success("Finance access granted ✅")
            else:
                st.error(T["wrong_password"])
                st.stop()
        else:
            st.stop()
    batch_name_input = st.text_input(T["batch_name"], placeholder="e.g. Sept14_Invoices")
    if batch_name_input and batch_name_input.strip():
        BATCH_NAME = batch_name_input.strip().replace(" ", "_")
    else:
        BATCH_NAME = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")

    uploads = st.file_uploader(T["upload_label"], type=["pdf"], accept_multiple_files=True)

    if uploads:
        if len(uploads) > MAX_FILES:
            st.error(f"⚠️ You can only upload up to {MAX_FILES} files at once.")
        else:
            too_big = [f for f in uploads if f.size > MAX_BYTES]
            ok = [f for f in uploads if f.size <= MAX_BYTES]

            if too_big:
                st.error(T["too_big"].format(n=len(too_big), mb=MAX_MB, files=", ".join(f.name for f in too_big)))

            if ok:
                st.success(T["received"].format(n=len(ok)))
                st.dataframe(pd.DataFrame(
                    [{"File": f.name, "Size (MB)": round(f.size / 1024 / 1024, 2)} for f in ok]
                ))

                if st.button(T["run_check"]):
                    # Upload files (working + archive immediately)
                    with st.spinner("Uploading files..."):
                        for f in ok:
                            file_bytes = f.read()
                            upload_to_volume(f.name, file_bytes, f"{VOLUME_PATH}/{BATCH_NAME}")   # working
                            upload_to_volume(f.name, file_bytes, f"{ARCHIVE_PATH}/{BATCH_NAME}")  # archive

                    # Run job
                    with st.spinner("Running Databricks job..."):
                        run_id = run_parse_job()
                        wait_for_result(run_id)

                    st.success("✅ Job completed! Fetching results...")

                    # --- Summary ---
                    df_summary = run_sql("""
                        SELECT path, invoice_number, issue_date, final_decision
                        FROM dev_uc_catalog.default.zatca_invoices_head
                        ORDER BY path
                    """)
                    st.subheader(T["summary"])
                    st.dataframe(df_summary)

                    # --- Failed checks ---
                    df_details = run_sql("""
                        SELECT h.path, h.invoice_number, h.issue_date, h.final_decision,
                               c.id AS failed_rule_id, c.name AS failed_rule_name, c.reason AS failed_reason
                        FROM dev_uc_catalog.default.zatca_invoices_head h
                        JOIN dev_uc_catalog.default.zatca_checks_flat c
                          ON h.path = c.path
                        WHERE c.result = 'fail'
                        ORDER BY h.path, c.id
                    """)
                    if not df_details.empty:
                        st.subheader(T["failed"])
                        st.dataframe(df_details)
                    else:
                        st.success(T["all_passed"])

                    # --- Export buttons ---
                    st.subheader(T["export"])
                    excel_data = df_to_excel({"Summary": df_summary, "Failed Checks": df_details})
                    st.download_button(T["download_excel"],
                                       data=excel_data,
                                       file_name=f"vat_compliance_results_{BATCH_NAME}.xlsx")

                    # Archive & reset DB
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
                    run_sql("TRUNCATE TABLE dev_uc_catalog.default.zatca_invoices_head")
                    run_sql("TRUNCATE TABLE dev_uc_catalog.default.zatca_checks_flat")
                    run_sql("TRUNCATE TABLE dev_uc_catalog.default.zatca_invoice_check_parsed")

                    msg = cleanup_volume(VOLUME_PATH, BATCH_NAME)
                    st.success(f"Session archived and reset ✅ ({msg})")

# --- Archived Invoices (Finance only) ---
with tab2:
    if st.session_state.role != "finance":
        st.warning(T["finance_prompt"])
        st.stop()

    st.subheader(T["inv_tab"])
    batch_list = run_sql("SELECT DISTINCT batch_name FROM dev_uc_catalog.default.zatca_invoices_head_archive ORDER BY batch_name DESC")
    if not batch_list.empty:
        selected_batch = st.selectbox("Choose a batch", batch_list["batch_name"], key="batch_invoices")
        df_archive_invoices = run_sql(f"""
            SELECT * FROM dev_uc_catalog.default.zatca_invoices_head_archive
            WHERE batch_name = '{selected_batch}'
            ORDER BY path
        """)
        st.dataframe(df_archive_invoices)
        st.download_button(T["download_inv_csv"],
                           data=df_archive_invoices.to_csv(index=False).encode("utf-8"),
                           file_name=f"invoices_{selected_batch}.csv",
                           mime="text/csv")
    else:
        st.info(T["no_archives"])

# --- Archived Failed Checks (Finance only) ---
with tab3:
    if st.session_state.role != "finance":
        st.warning(T["finance_prompt"])
        st.stop()

    st.subheader(T["fail_tab"])
    batch_list = run_sql("SELECT DISTINCT batch_name FROM dev_uc_catalog.default.zatca_checks_flat_archive ORDER BY batch_name DESC")
    if not batch_list.empty:
        selected_batch = st.selectbox("Choose a batch", batch_list["batch_name"], key="batch_checks")
        df_archive_checks = run_sql(f"""
            SELECT * FROM dev_uc_catalog.default.zatca_checks_flat_archive
            WHERE batch_name = '{selected_batch}'
            ORDER BY path, id
        """)
        st.dataframe(df_archive_checks)
        st.download_button(T["download_fail_csv"],
                           data=df_archive_checks.to_csv(index=False).encode("utf-8"),
                           file_name=f"checks_{selected_batch}.csv",
                           mime="text/csv")
    else:
        st.info(T["no_archives"])

# ==== DISCLAIMER ====
st.markdown(T["disclaimer"])
