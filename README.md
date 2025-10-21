# **Invoice VAT Compliance Checker**

## **Overview**
The Invoice VAT Compliance Checker is a Streamlit web application developed to automate the validation of supplier invoices for VAT compliance.  
It uses a **custom Large Language Model (LLM)** integrated with **Azure Databricks** pipelines to intelligently extract, interpret, and assess invoice content for regulatory accuracy.

The project was built during my **Digital Transformation and IT Internship at Abdul Latif Jameel Enterprises**, focusing on AI-driven business automation and financial process improvement.

---

## **Key Objectives**
- Automate invoice compliance review using natural language understanding  
- Detect VAT and invoice irregularities with improved accuracy  
- Reduce manual auditing time and enhance financial transparency  
- Support scalable enterprise integration through Databricks pipelines  

---

## **System Workflow**
1. **Data Upload** – Users upload supplier invoices in PDF or text format  
2. **LLM Processing** – Custom model extracts and interprets invoice details (supplier info, VAT number, total, date, etc.)  
3. **Compliance Validation** – Model checks extracted data against VAT compliance rules and flags anomalies  
4. **Databricks Integration** – Results are stored, validated, and aggregated through a Databricks SQL workflow  
5. **Streamlit Dashboard** – Displays flagged invoices, compliance metrics, and review summaries  

---

## **Features**
- AI-driven invoice validation using a custom LLM  
- Automated compliance checks with contextual reasoning  
- Real-time analytics and visual summaries in Streamlit  
- Integration with Databricks Jobs and SQL Warehouses  
- CSV export of validated and non-compliant invoices  

---

## **Tech Stack**
- **Languages:** Python  
- **Frameworks:** Streamlit  
- **Cloud Platform:** Azure Databricks  
- **AI Components:** Custom LLM Model, Natural Language Processing  
- **Libraries:** Databricks SDK, pandas, regex, csv  
- **Storage:** Databricks SQL Warehouse  

---

## **Impact**
- Cut invoice validation time by **18%**  
- Reduced workload on finance teams  
- Detected non-compliance earlier in the purchasing process  
- Improved detection accuracy for VAT non-compliance cases  
- Reduced financial risk exposure through automated checks  
- Enabled transparent and scalable review workflows for finance teams  

---

## **Project Structure**
```
Invoice-VAT-compliance-checker/
│
├── app.py                 # Streamlit application
├── requirements.txt       # Dependencies
├── app.yaml               # Configuration file
├── assets/                # UI elements and resources
└── README.md              # Project documentation
```

---

## **How It Works**
1. Launch the Streamlit app  
2. Upload invoices in PDF or text format  
3. The system extracts key data using a fine-tuned LLM  
4. Compliance logic runs automatically  
5. A dashboard displays validated and flagged invoices  

---

## **Future Enhancements**
- Integration with Power BI for extended analytics  
- Fine-tuning the LLM for multi-language invoice formats  
- Real-time API deployment for enterprise use  
