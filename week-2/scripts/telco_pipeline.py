# ============================================
# WEEK 2 — MODULE 6 CAPSTONE:
# Telco Churn Analysis Pipeline
# GetSkills Network DE Bootcamp
# ============================================

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json
import logging
import os
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "Telco Churn Analysis Pipeline"
PIPELINE_VERSION = "1.0.0"
AUTHOR = "Lawrence Koomson"

RAW_DATA = Path(__file__).parent.parent / \
    "data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv"
PROCESSED_DIR = Path(
    __file__).parent.parent / "data/processed"
CHARTS_DIR = Path(
    __file__).parent.parent / "data/charts"
LOGS_DIR = Path(__file__).parent.parent / "logs"

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            LOGS_DIR / 'capstone_pipeline.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ─────────────────────────────────────────────
class PipelineError(Exception):
    pass

class DataLoadError(PipelineError):
    pass

class ValidationError(PipelineError):
    pass

# ─────────────────────────────────────────────
# STEP 1: EXTRACT
# ─────────────────────────────────────────────
def extract(filepath):
    """Load raw CSV into DataFrame."""
    logger.info(f"Extracting data from {filepath}")
    try:
        df = pd.read_csv(filepath)
        logger.info(
            f"Extracted {df.shape[0]:,} rows x "
            f"{df.shape[1]} columns")
        return df
    except FileNotFoundError:
        raise DataLoadError(
            f"File not found: {filepath}")
    except Exception as e:
        raise DataLoadError(
            f"Error loading data: {e}")

# ─────────────────────────────────────────────
# STEP 2: VALIDATE
# ─────────────────────────────────────────────
def validate(df):
    """Validate raw dataset quality."""
    logger.info("Validating raw dataset")
    issues = []

    required_cols = [
        'customerID', 'Churn',
        'Contract', 'tenure',
        'MonthlyCharges', 'TotalCharges',
        'InternetService', 'PaymentMethod']

    for col in required_cols:
        if col not in df.columns:
            issues.append(
                f"Missing column: {col}")

    if df.duplicated(
            subset='customerID').sum() > 0:
        issues.append("Duplicate customerIDs found")

    if df['tenure'].min() < 0:
        issues.append("Negative tenure values")

    if df['MonthlyCharges'].min() <= 0:
        issues.append(
            "Invalid MonthlyCharges values")

    if issues:
        for issue in issues:
            logger.warning(f"Validation: {issue}")
    else:
        logger.info(
            "Validation passed — no issues found")

    return issues

# ─────────────────────────────────────────────
# STEP 3: TRANSFORM
# ─────────────────────────────────────────────
def transform(df):
    """Clean and transform the dataset."""
    logger.info("Transforming dataset")
    df = df.copy()

    # Fix TotalCharges
    df['TotalCharges'] = pd.to_numeric(
        df['TotalCharges'].replace(' ', np.nan),
        errors='coerce').fillna(0.0)

    # Fix SeniorCitizen
    df['SeniorCitizen'] = df[
        'SeniorCitizen'].map({0: 'No', 1: 'Yes'})

    # Strip strings
    str_cols = df.select_dtypes(
        include=['object', 'str']
    ).columns.tolist()
    str_cols = [
        c for c in str_cols
        if c != 'customerID']
    for col in str_cols:
        df[col] = df[col].str.strip()

    # Convert Churn to bool
    df['Churn'] = df['Churn'].map(
        {'Yes': True, 'No': False})

    # Feature engineering
    df['tenure_group'] = pd.cut(
        df['tenure'],
        bins=[0, 12, 24, 48, 72],
        labels=[
            '0-12 months', '13-24 months',
            '25-48 months', '49+ months'],
        include_lowest=True)

    df['charge_band'] = pd.cut(
        df['MonthlyCharges'],
        bins=[0, 35, 65, 90, 200],
        labels=[
            'Low (<$35)', 'Medium ($35-65)',
            'High ($65-90)', 'Premium (>$90)'],
        include_lowest=True)

    df['is_auto_pay'] = df[
        'PaymentMethod'].str.contains(
        'automatic', case=False)

    df['lifetime_value'] = (
        df['MonthlyCharges'] *
        df['tenure']).round(2)

    logger.info(
        f"Transform complete: "
        f"{df.shape[0]:,} rows, "
        f"{df.shape[1]} columns")
    return df

# ─────────────────────────────────────────────
# STEP 4: ANALYSE
# ─────────────────────────────────────────────
def analyse(df):
    """Run churn analysis and return insights."""
    logger.info("Running churn analysis")

    total = len(df)
    churned = df['Churn'].sum()
    churn_rate = churned / total * 100

    # By contract
    contract_churn = df.groupby(
        'Contract')['Churn'].mean() * 100

    # By tenure group
    tenure_churn = df.groupby(
        'tenure_group',
        observed=True)['Churn'].mean() * 100

    # By internet service
    internet_churn = df.groupby(
        'InternetService')['Churn'].mean() * 100

    # By payment method
    payment_churn = df.groupby(
        'PaymentMethod')['Churn'].mean() * 100

    # Revenue impact
    monthly_at_risk = df[
        df['Churn'] == True
    ]['MonthlyCharges'].sum()

    # Avg charges
    avg_churned = df[
        df['Churn'] == True
    ]['MonthlyCharges'].mean()
    avg_retained = df[
        df['Churn'] == False
    ]['MonthlyCharges'].mean()

    insights = {
        "total_customers": total,
        "churned_customers": int(churned),
        "retained_customers": int(total - churned),
        "churn_rate_pct": round(churn_rate, 2),
        "monthly_revenue_at_risk": round(
            monthly_at_risk, 2),
        "avg_monthly_charge_churned": round(
            avg_churned, 2),
        "avg_monthly_charge_retained": round(
            avg_retained, 2),
        "churn_by_contract": contract_churn.round(
            1).to_dict(),
        "churn_by_tenure": tenure_churn.round(
            1).to_dict(),
        "churn_by_internet": internet_churn.round(
            1).to_dict(),
        "churn_by_payment": payment_churn.round(
            1).to_dict(),
        "top_risk_factor": contract_churn.idxmax(),
        "safest_contract": contract_churn.idxmin(),
    }

    logger.info(
        f"Analysis complete: "
        f"{churn_rate:.1f}% churn rate, "
        f"${monthly_at_risk:,.2f} at risk")
    return insights

# ─────────────────────────────────────────────
# STEP 5: VISUALISE
# ─────────────────────────────────────────────
def visualise(df, output_dir):
    """Generate and save analysis charts."""
    logger.info("Generating visualisations")

    fig, axes = plt.subplots(
        2, 2, figsize=(14, 10))
    fig.suptitle(
        f'{PIPELINE_NAME}\n'
        f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        fontsize=14, fontweight='bold')

    RED = '#E74C3C'
    GREEN = '#27AE60'
    BLUE = '#2980B9'
    ORANGE = '#E67E22'

    # Chart 1 — Churn by Contract
    contract_churn = df.groupby(
        'Contract')['Churn'].mean() * 100
    contract_churn = contract_churn.sort_values(
        ascending=False)
    colors1 = [RED if v > 30 else
               ORANGE if v > 10 else
               GREEN for v in contract_churn]
    bars = axes[0,0].bar(
        contract_churn.index,
        contract_churn.values,
        color=colors1, edgecolor='white')
    axes[0,0].set_title(
        'Churn Rate by Contract',
        fontweight='bold')
    axes[0,0].set_ylabel('Churn Rate (%)')
    for bar, val in zip(
            bars, contract_churn.values):
        axes[0,0].text(
            bar.get_x() + bar.get_width()/2,
            bar.get_height() + 0.5,
            f'{val:.1f}%', ha='center',
            fontweight='bold')

    # Chart 2 — Churn by Tenure
    tenure_order = [
        '0-12 months', '13-24 months',
        '25-48 months', '49+ months']
    tenure_churn = df.groupby(
        'tenure_group',
        observed=True)['Churn'].mean() * 100
    tenure_churn = tenure_churn.reindex(
        tenure_order)
    colors2 = [RED if v > 30 else
               ORANGE if v > 15 else
               GREEN for v in tenure_churn]
    bars2 = axes[0,1].bar(
        tenure_churn.index,
        tenure_churn.values,
        color=colors2, edgecolor='white')
    axes[0,1].set_title(
        'Churn Rate by Tenure',
        fontweight='bold')
    axes[0,1].set_ylabel('Churn Rate (%)')
    axes[0,1].tick_params(
        axis='x', rotation=15)
    for bar, val in zip(
            bars2, tenure_churn.values):
        axes[0,1].text(
            bar.get_x() + bar.get_width()/2,
            bar.get_height() + 0.5,
            f'{val:.1f}%', ha='center',
            fontweight='bold')

    # Chart 3 — Monthly Charges Histogram
    axes[1,0].hist(
        df[df['Churn'] == False][
            'MonthlyCharges'],
        bins=25, alpha=0.6,
        color=GREEN, label='Retained',
        edgecolor='white')
    axes[1,0].hist(
        df[df['Churn'] == True][
            'MonthlyCharges'],
        bins=25, alpha=0.6,
        color=RED, label='Churned',
        edgecolor='white')
    axes[1,0].set_title(
        'Monthly Charges Distribution',
        fontweight='bold')
    axes[1,0].set_xlabel('Monthly Charges ($)')
    axes[1,0].set_ylabel('Customers')
    axes[1,0].legend()

    # Chart 4 — Internet Service Churn
    internet_churn = df.groupby(
        'InternetService')['Churn'].mean() * 100
    internet_churn = internet_churn.sort_values(
        ascending=False)
    colors4 = [RED if v > 30 else
               ORANGE if v > 15 else
               GREEN for v in internet_churn]
    bars4 = axes[1,1].bar(
        internet_churn.index,
        internet_churn.values,
        color=colors4, edgecolor='white',
        width=0.4)
    axes[1,1].set_title(
        'Churn Rate by Internet Service',
        fontweight='bold')
    axes[1,1].set_ylabel('Churn Rate (%)')
    for bar, val in zip(
            bars4, internet_churn.values):
        axes[1,1].text(
            bar.get_x() + bar.get_width()/2,
            bar.get_height() + 0.5,
            f'{val:.1f}%', ha='center',
            fontweight='bold')

    plt.tight_layout()
    chart_path = output_dir / \
        'capstone_dashboard.png'
    plt.savefig(
        chart_path, dpi=150,
        bbox_inches='tight')
    plt.close()

    logger.info(
        f"Charts saved to {chart_path}")
    return str(chart_path)

# ─────────────────────────────────────────────
# STEP 6: LOAD (Save outputs)
# ─────────────────────────────────────────────
def load(df, insights, output_dir):
    """Save all pipeline outputs."""
    logger.info("Saving pipeline outputs")

    # Save clean data
    clean_path = output_dir / \
        'capstone_clean.csv'
    df.to_csv(clean_path, index=False)
    logger.info(
        f"Clean data saved: {clean_path}")

    # Save insights report
    report = {
        "pipeline_name": PIPELINE_NAME,
        "version": PIPELINE_VERSION,
        "author": AUTHOR,
        "run_timestamp": datetime.now(
            ).strftime("%Y-%m-%d %H:%M:%S"),
        "insights": insights,
        "recommendations": [
            "Convert month-to-month customers "
            "to longer contracts — reduces churn "
            "from 42.7% to 2.8%",
            "Focus retention on first 12 months "
            "— 47.4% churn rate in year 1",
            "Investigate Fiber optic quality "
            "— 41.9% churn despite premium price",
            "Offer auto-pay incentives "
            "— electronic check = 45.3% churn",
            "Target senior citizens with "
            "dedicated support packages"
        ]
    }

    report_path = output_dir / \
        'capstone_report.json'
    with open(report_path, 'w',
              encoding='utf-8') as f:
        json.dump(report, f,
                  indent=4, default=str)
    logger.info(
        f"Report saved: {report_path}")

    return clean_path, report_path

# ─────────────────────────────────────────────
# MAIN PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────
def run_pipeline():
    """Run the complete ETL + Analysis pipeline."""
    start_time = datetime.now()

    logger.info("=" * 50)
    logger.info(f"  {PIPELINE_NAME}")
    logger.info(f"  Version: {PIPELINE_VERSION}")
    logger.info(f"  Author:  {AUTHOR}")
    logger.info("=" * 50)

    stats = {
        "status": "RUNNING",
        "validation_issues": [],
        "rows_processed": 0,
        "errors": []
    }

    try:
        # Step 1 — Extract
        logger.info("▶ Step 1: Extract")
        df_raw = extract(RAW_DATA)
        stats["rows_processed"] = len(df_raw)

        # Step 2 — Validate
        logger.info("▶ Step 2: Validate")
        issues = validate(df_raw)
        stats["validation_issues"] = issues
        if issues:
            logger.warning(
                f"{len(issues)} validation "
                f"issues found")

        # Step 3 — Transform
        logger.info("▶ Step 3: Transform")
        df_clean = transform(df_raw)

        # Step 4 — Analyse
        logger.info("▶ Step 4: Analyse")
        insights = analyse(df_clean)

        # Step 5 — Visualise
        logger.info("▶ Step 5: Visualise")
        chart_path = visualise(
            df_clean, CHARTS_DIR)

        # Step 6 — Load
        logger.info("▶ Step 6: Load")
        clean_path, report_path = load(
            df_clean, insights, PROCESSED_DIR)

        stats["status"] = "SUCCESS"

    except (DataLoadError,
            ValidationError) as e:
        logger.error(f"Pipeline error: {e}")
        stats["status"] = "FAILED"
        stats["errors"].append(str(e))

    except Exception as e:
        logger.error(
            f"Unexpected error: {e}")
        stats["status"] = "FAILED"
        stats["errors"].append(str(e))

    finally:
        duration = (
            datetime.now() - start_time
        ).total_seconds()
        logger.info(
            f"Pipeline {stats['status']} "
            f"in {duration:.2f}s")

    return stats, insights if \
        stats["status"] == "SUCCESS" \
        else {}

# ─────────────────────────────────────────────
# RUN & DISPLAY RESULTS
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🚀 Starting Telco Churn Pipeline...")
    print("=" * 55)

    results, insights = run_pipeline()

    print("\n" + "=" * 55)
    print("   PIPELINE RESULTS")
    print("=" * 55)
    print(f"\n   Status:     {results['status']}")
    print(f"   Rows:       "
          f"{results['rows_processed']:,}")

    if insights:
        print(f"\n   📊 KEY INSIGHTS:")
        print(f"   Churn Rate:      "
              f"{insights['churn_rate_pct']}%")
        print(f"   Churned:         "
              f"{insights['churned_customers']:,}")
        print(f"   Retained:        "
              f"{insights['retained_customers']:,}")
        print(f"   Revenue at Risk: "
              f"${insights['monthly_revenue_at_risk']:,}")
        print(f"   Top Risk Factor: "
              f"{insights['top_risk_factor']}")
        print(f"   Safest Contract: "
              f"{insights['safest_contract']}")

        print(f"\n   📋 CHURN BY CONTRACT:")
        for k, v in insights[
                'churn_by_contract'].items():
            bar = '🔴' if v > 30 else \
                  '🟡' if v > 10 else '🟢'
            print(f"   {bar} {k:<20} {v}%")

        print(f"\n   📋 CHURN BY INTERNET:")
        for k, v in insights[
                'churn_by_internet'].items():
            bar = '🔴' if v > 30 else \
                  '🟡' if v > 10 else '🟢'
            print(f"   {bar} {k:<20} {v}%")

    print(f"\n   📁 OUTPUT FILES:")
    output_files = [
        PROCESSED_DIR / 'capstone_clean.csv',
        PROCESSED_DIR / 'capstone_report.json',
        CHARTS_DIR / 'capstone_dashboard.png',
        LOGS_DIR / 'capstone_pipeline.log'
    ]
    for f in output_files:
        if f.exists():
            size = f.stat().st_size
            print(f"   📄 {f.name:<35} "
                  f"{size:,} bytes")

    print(f"\n✅ Week 2 Capstone Complete!")
    print(f"   Telco Churn Pipeline — "
          f"Production Ready!")