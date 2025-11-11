"""
Glue Job: Build Gold Analytics Layer (Silver → Gold)
Purpose: Read transformed silver data, build dimensions + facts + KPIs, write to gold bucket
"""

import sys
import pandas as pd
import s3fs
from datetime import datetime
import numpy as np

# S3 filesystem
fs = s3fs.S3FileSystem()

# S3 Paths
SILVER_BUCKET = "s3://creditrisk-silver"
GOLD_BUCKET = "s3://creditrisk-gold01"

# Input paths
LOAN_APPLICATIONS_SILVER = f"{SILVER_BUCKET}/transformed/loan_applications/"
LOAN_REPAYMENTS_SILVER = f"{SILVER_BUCKET}/transformed/loan_repayments/"
CREDIT_BUREAU_SILVER = f"{SILVER_BUCKET}/transformed/credit_bureau/"

# Output paths - Dimensions and Facts go to Silver, KPIs go to Gold
DIM_CUSTOMER_OUTPUT = f"{SILVER_BUCKET}/dimensions/dim_customer/"
DIM_LOAN_OUTPUT = f"{SILVER_BUCKET}/dimensions/dim_loan/"
FACT_LOAN_PERFORMANCE_OUTPUT = f"{SILVER_BUCKET}/facts/fact_loan_performance/"
KPI_PORTFOLIO_OUTPUT = f"{GOLD_BUCKET}/kpis/kpi_portfolio/"
KPI_REGIONAL_OUTPUT = f"{GOLD_BUCKET}/kpis/kpi_regional_delinquency/"
KPI_CUSTOMER_DRILLDOWN_OUTPUT = f"{GOLD_BUCKET}/kpis/kpi_customer_drilldown/"


def get_latest_transformed_data(s3_path: str) -> pd.DataFrame:
    """Read the most recent transformed parquet files from silver bucket."""
    try:
        # Get all parquet files
        files = fs.glob(f"{s3_path}**/*.parquet")
        if not files:
            raise FileNotFoundError(f"No parquet files found in {s3_path}")

        # Read all parquet files and concatenate
        dfs = []
        for file in files:
            df = pd.read_parquet(f"s3://{file}", filesystem=fs)
            dfs.append(df)

        combined = pd.concat(dfs, ignore_index=True)
        print(f" Loaded {len(combined)} records from {len(files)} file(s)")
        return combined

    except Exception as e:
        print(f"Error reading transformed data: {str(e)}")
        raise



#  DIM_CUSTOMER


def build_dim_customer(loan_applications: pd.DataFrame, credit_bureau: pd.DataFrame) -> pd.DataFrame:
    """
    Build DimCustomer by LEFT JOIN loan_applications + credit_bureau on customer_id.
    """
    print("\n" + "="*60)
    print("BUILDING DIM_CUSTOMER")
    print("="*60)

    # Select relevant columns from loan applications
    customer_demo = loan_applications[[
        'customer_id', 'full_name', 'email', 'phone_number',
        'address', 'date_of_birth'
    ]].drop_duplicates(subset=['customer_id'], keep='last')

    print(f"Customer demographics: {len(customer_demo)} unique customers")

    # Select credit profile columns
    credit_profile = credit_bureau[[
        'customer_id', 'ssn', 'credit_score', 'total_open_loans',
        'total_defaults', 'inquiries_last_6m', 'effective_date'
    ]].drop_duplicates(subset=['customer_id'], keep='last')

    print(f"Credit bureau data: {len(credit_profile)} unique customers")

    # LEFT JOIN on customer_id
    dim_customer = customer_demo.merge(
        credit_profile,
        on='customer_id',
        how='left'
    )

    # Add timestamps
    dim_customer['created_date'] = datetime.now()
    dim_customer['updated_date'] = datetime.now()

    print(f" DimCustomer built: {len(dim_customer)} records")
    return dim_customer


def apply_dim_customer_upsert(new_data: pd.DataFrame) -> pd.DataFrame:
    """Upsert logic: merge with existing, keep latest by updated_date."""
    try:
        # Check if any parquet files exist in the output path
        files = fs.glob(f"{DIM_CUSTOMER_OUTPUT}**/*.parquet")
        if files:
            print("Existing DimCustomer found. Applying upsert logic...")
            # Read all existing parquet files
            existing_dfs = []
            for file in files:
                df = pd.read_parquet(f"s3://{file}", filesystem=fs)
                existing_dfs.append(df)
            existing_data = pd.concat(existing_dfs, ignore_index=True)

            combined = pd.concat([existing_data, new_data], ignore_index=True)
            combined = combined.sort_values('updated_date', ascending=False)
            final_data = combined.drop_duplicates(subset=['customer_id'], keep='first')

            print(f"Upserted: {len(existing_data)} existing + {len(new_data)} new = {len(final_data)} final")
            return final_data
        else:
            print("No existing DimCustomer. Writing initial data.")
            return new_data
    except Exception as e:
        print(f"Error during upsert: {str(e)}")
        return new_data



# DIMENSION: DIM_LOAN


def build_dim_loan(loan_repayments: pd.DataFrame, loan_applications: pd.DataFrame) -> pd.DataFrame:
    """
    Build DimLoan by LEFT JOIN loan_repayments + loan_applications on customer_id.
    Filter to approved loans only.
    """
    print("\n" + "="*60)
    print("BUILDING DIM_LOAN")
    print("="*60)

    # Get unique active loans from repayments
    active_loans = loan_repayments[[
        'loan_id', 'customer_id'
    ]].drop_duplicates(subset=['loan_id'], keep='last')

    print(f"Active loans from repayments: {len(active_loans)} unique loans")

    # Get loan characteristics from applications (approved only)
    approved_apps = loan_applications[loan_applications['approval_status'] == 'APPROVED'].copy()

    loan_chars = approved_apps[[
        'application_id', 'customer_id', 'loan_amount', 'loan_type',
        'term_months', 'interest_rate', 'approval_status',
        'application_date', 'approval_date'
    ]].drop_duplicates(subset=['customer_id'], keep='last')

    print(f"Approved applications: {len(loan_chars)} unique customers")

    # LEFT JOIN on customer_id
    dim_loan = active_loans.merge(
        loan_chars,
        on='customer_id',
        how='left'
    )

    # Add match quality flag
    dim_loan['has_approved_application'] = dim_loan['application_id'].notna()

    # Add timestamps
    dim_loan['created_date'] = datetime.now()
    dim_loan['updated_date'] = datetime.now()

    print(f" DimLoan built: {len(dim_loan)} records")
    print(f"  - With approved application: {dim_loan['has_approved_application'].sum()}")
    print(f"  - Without application: {(~dim_loan['has_approved_application']).sum()}")

    return dim_loan


def apply_dim_loan_upsert(new_data: pd.DataFrame) -> pd.DataFrame:
    """Upsert logic: merge with existing, keep latest by updated_date."""
    try:
        # Check if any parquet files exist in the output path
        files = fs.glob(f"{DIM_LOAN_OUTPUT}**/*.parquet")
        if files:
            print("Existing DimLoan found. Applying upsert logic...")
            # Read all existing parquet files
            existing_dfs = []
            for file in files:
                df = pd.read_parquet(f"s3://{file}", filesystem=fs)
                existing_dfs.append(df)
            existing_data = pd.concat(existing_dfs, ignore_index=True)

            combined = pd.concat([existing_data, new_data], ignore_index=True)
            combined = combined.sort_values('updated_date', ascending=False)
            final_data = combined.drop_duplicates(subset=['loan_id'], keep='first')

            print(f"Upserted: {len(existing_data)} existing + {len(new_data)} new = {len(final_data)} final")
            return final_data
        else:
            print("No existing DimLoan. Writing initial data.")
            return new_data
    except Exception as e:
        print(f"Error during upsert: {str(e)}")
        return new_data



# FACT: FACT_LOAN_PERFORMANCE

def calculate_amortization(df: pd.DataFrame, dim_loan: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate principal_paid, interest_paid, and outstanding_balance using amortization formula.
    """
    print("Calculating amortization metrics...")

    # Merge with dim_loan to get loan characteristics
    fact = df.merge(
        dim_loan[['loan_id', 'loan_amount', 'interest_rate', 'term_months']],
        on='loan_id',
        how='left'
    )

    # Sort by loan_id and payment_date for sequential calculation
    fact = fact.sort_values(['loan_id', 'payment_date'])

    # Initialize balance tracking
    fact['outstanding_balance'] = 0.0
    fact['principal_paid'] = 0.0
    fact['interest_paid'] = 0.0

    # Calculate for each loan
    for loan_id in fact['loan_id'].unique():
        loan_mask = fact['loan_id'] == loan_id
        loan_data = fact[loan_mask].copy()

        # Get loan parameters
        initial_balance = loan_data['loan_amount'].iloc[0]
        annual_rate = loan_data['interest_rate'].iloc[0]

        if pd.isna(initial_balance) or pd.isna(annual_rate):
            # If missing loan data, estimate
            fact.loc[loan_mask, 'outstanding_balance'] = 0
            fact.loc[loan_mask, 'principal_paid'] = loan_data['amount_paid']
            fact.loc[loan_mask, 'interest_paid'] = 0
            continue

        monthly_rate = annual_rate / 100 / 12
        running_balance = initial_balance

        for idx in loan_data.index:
            # Calculate interest for this period
            interest = running_balance * monthly_rate
            amount_paid = fact.loc[idx, 'amount_paid']

            # Split payment into interest and principal
            interest_paid = min(amount_paid, interest)
            principal_paid = amount_paid - interest_paid

            # Update balance
            running_balance -= principal_paid
            running_balance = max(0, running_balance)  # Don't go negative

            # Store calculated values
            fact.loc[idx, 'interest_paid'] = interest_paid
            fact.loc[idx, 'principal_paid'] = principal_paid
            fact.loc[idx, 'outstanding_balance'] = running_balance

    print(" Amortization calculations complete")
    return fact


def build_fact_loan_performance(loan_repayments: pd.DataFrame, dim_loan: pd.DataFrame) -> pd.DataFrame:
    """
    Build FactLoanPerformance with calculated metrics.
    """
    print("\n" + "="*60)
    print("BUILDING FACT_LOAN_PERFORMANCE")
    print("="*60)

    # Start with repayments
    fact = loan_repayments.copy()

    # Calculate amortization
    fact = calculate_amortization(fact, dim_loan)

    # Add default flag (status = MISSED indicates defaulted loan)
    fact['is_defaulted'] = (fact['status'] == 'MISSED')

    # Add processing timestamp
    fact['processing_date'] = datetime.now()

    # Select final columns
    fact_columns = [
        'repayment_id', 'loan_id', 'customer_id',
        'due_date', 'payment_date', 'amount_paid', 'status',
        'days_past_due', 'delinquency_bucket',
        'principal_paid', 'interest_paid', 'outstanding_balance',
        'is_defaulted', 'processing_date', 'source_file'
    ]

    fact = fact[fact_columns]

    print(f" FactLoanPerformance built: {len(fact)} records")
    return fact


def apply_fact_loan_performance_upsert(new_facts: pd.DataFrame) -> pd.DataFrame:
    """
    Upsert logic for fact table: merge and recalculate balances.
    Key: repayment_id (assuming unique per repayment event)
    """
    try:
        # Check if any parquet files exist in the output path
        files = fs.glob(f"{FACT_LOAN_PERFORMANCE_OUTPUT}**/*.parquet")
        if files:
            print("Existing FactLoanPerformance found. Applying upsert logic...")
            # Read all existing parquet files
            existing_dfs = []
            for file in files:
                df = pd.read_parquet(f"s3://{file}", filesystem=fs)
                existing_dfs.append(df)
            existing_facts = pd.concat(existing_dfs, ignore_index=True)

            # Combine and deduplicate on repayment_id
            combined = pd.concat([existing_facts, new_facts], ignore_index=True)
            combined = combined.sort_values('payment_date', ascending=False)
            final_facts = combined.drop_duplicates(subset=['repayment_id'], keep='first')

            # Re-sort chronologically for balance consistency
            final_facts = final_facts.sort_values(['loan_id', 'payment_date'])

            print(f"Upserted: {len(existing_facts)} existing + {len(new_facts)} new = {len(final_facts)} final")
            return final_facts
        else:
            print("No existing FactLoanPerformance. Writing initial data.")
            return new_facts
    except Exception as e:
        print(f"Error during upsert: {str(e)}")
        return new_facts



# KPI: PORTFOLIO LEVEL


def calculate_portfolio_kpis(fact_df: pd.DataFrame, dim_customer: pd.DataFrame, dim_loan: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate 7 portfolio-level KPIs with corrected business logic.
    """
    print("\n" + "="*60)
    print("CALCULATING PORTFOLIO KPIs")
    print("="*60)

    # Merge fact data with dim_loan to get loan characteristics
    fact_with_loan = fact_df.merge(
        dim_loan[['loan_id', 'loan_amount', 'interest_rate', 'term_months', 'approval_status']],
        on='loan_id',
        how='left'
    )

    # Group by loan_id to get loan-level aggregates
    loan_summary = fact_with_loan.groupby('loan_id').agg({
        'amount_paid': 'sum',  # Total amount paid per loan
        'status': 'last',  # Latest status per loan
        'due_date': 'min',  # Earliest due date (loan start)
        'days_past_due': 'max',  # Max days past due
        'loan_amount': 'first',
        'interest_rate': 'first',
        'term_months': 'first',
        'approval_status': 'first',
        'customer_id': 'first'
    }).reset_index()

    # Filter for approved loans only (active loans)
    active_loans = loan_summary[loan_summary['approval_status'] == 'APPROVED'].copy()
    total_active_loans = len(active_loans)
    print(f"Total active loans (APPROVED): {total_active_loans}")

    # Calculate loan-level metrics
    # Outstanding balance = loan_amount - total amount_paid
    active_loans['outstanding_balance'] = active_loans['loan_amount'] - active_loans['amount_paid']
    active_loans['outstanding_balance'] = active_loans['outstanding_balance'].clip(lower=0)

    # Calculate elapsed months from earliest due_date to now
    today = datetime.now()
    active_loans['elapsed_months'] = active_loans['due_date'].apply(
        lambda x: ((today.year - x.year) * 12 + (today.month - x.month)) if pd.notna(x) else 0
    )
    active_loans['elapsed_months'] = active_loans['elapsed_months'].clip(lower=0)

    # Calculate interest earned per loan: loan_amount × interest_rate × (elapsed_months / 12)
    active_loans['interest_earned'] = (
        active_loans['loan_amount'] *
        (active_loans['interest_rate'] / 100) *
        (active_loans['elapsed_months'] / 12)
    )

    #  Default Rate (%) = (Number of Defaulted Loans / Total Active Loans) × 100
    # Defaulted = status 'MISSED' only (loans that have been missed completely)
    defaulted_loans = active_loans[active_loans['status'] == 'MISSED']
    num_defaulted = len(defaulted_loans)
    default_rate = ((num_defaulted / total_active_loans) * 100) if total_active_loans > 0 else 0
    print(f"1. Default Rate: {default_rate:.2f}% ({num_defaulted} defaulted / {total_active_loans} active)")

    # Portfolio Yield (%) = (Total Interest Earned / Total Outstanding Principal) × 100
    total_interest_earned = active_loans['interest_earned'].sum()
    total_outstanding_principal = active_loans['outstanding_balance'].sum()
    portfolio_yield = ((total_interest_earned / total_outstanding_principal) * 100) if total_outstanding_principal > 0 else 0
    print(f"2. Portfolio Yield: {portfolio_yield:.2f}%")

    #  Early Delinquency Ratio (%) = (Loans 1-90 days overdue / Total Loans) × 100
    # Balance 1-90 days past due (captures early signs of delinquency)
    latest_repayments = fact_df.sort_values('payment_date').groupby('loan_id').last().reset_index()
    early_delinquent_loans = latest_repayments[
        (latest_repayments['days_past_due'] >= 1) & (latest_repayments['days_past_due'] <= 90)
    ]['loan_id'].nunique()
    early_delinquency_ratio = (early_delinquent_loans / total_active_loans * 100) if total_active_loans > 0 else 0
    print(f"3. Early Delinquency Ratio: {early_delinquency_ratio:.2f}% ({early_delinquent_loans} loans 1-90 days overdue / {total_active_loans} total)")

    # Exposure at Default (EAD) - Outstanding balance of defaulted loans (status='MISSED')
    missed_loans = active_loans[active_loans['status'] == 'MISSED']
    ead = missed_loans['outstanding_balance'].sum()
    print(f"4. Exposure at Default: ${ead:,.2f}")

    #  Customer Risk Score - Credit score adjusted by delinquency and repayment history
    # Calculate repayment history score: (count of PAID status / total repayments) per customer
    repayment_history = fact_df.groupby('customer_id').agg({
        'status': lambda x: (x == 'PAID').sum() / len(x) * 100 if len(x) > 0 else 0
    }).reset_index()
    repayment_history.columns = ['customer_id', 'repayment_history_score']

    # Merge with customer data
    customer_risk_df = active_loans.merge(
        dim_customer[['customer_id', 'credit_score']],
        on='customer_id',
        how='left'
    ).merge(
        repayment_history,
        on='customer_id',
        how='left'
    )

    avg_credit_score = customer_risk_df['credit_score'].mean()
    avg_repayment_history = customer_risk_df['repayment_history_score'].mean()
    avg_days_past_due = active_loans['days_past_due'].fillna(0).mean()

    # Transaction Score Calculation:
    # The transaction score represents how "healthy" the customer's loan portfolio is in terms of timeliness of repayment.
    # days_past_due measures how late the customer is on payments (i.e., delinquency).
    # Converting days_past_due to a 0-100 scale gives a transaction score:
    #   - 0 days past due → score 100 (perfect repayment behavior)
    #   - 90 days past due → score 0 (very risky)
    transaction_score = max(0, 100 - (avg_days_past_due / 90) * 100)  # Scale to 0-100 (inverted)

    # Normalize credit score to 0-100 scale (assuming credit scores range 300-850)
    credit_score_norm = ((avg_credit_score - 300) / (850 - 300)) * 100 if not pd.isna(avg_credit_score) else 0
    credit_score_norm = max(0, min(100, credit_score_norm))  # Clip to 0-100

    # Customer Risk Score (weighted formula):
    # = 0.5 × credit_score_norm + 0.3 × transaction_score + 0.2 × repayment_history_score
    customer_risk_score = (
        0.5 * credit_score_norm +
        0.3 * transaction_score +
        0.2 * avg_repayment_history
    ) if not pd.isna(avg_credit_score) else 0
    print(f"5. Customer Risk Score: {customer_risk_score:.2f}")

    # NPL Ratio (%) = (Balance of Loans >90 Days Overdue / Total Outstanding Balance) × 100
    npl_loans = active_loans[active_loans['days_past_due'] > 90]
    npl_balance = npl_loans['outstanding_balance'].sum()
    npl_ratio = (npl_balance / total_outstanding_principal * 100) if total_outstanding_principal > 0 else 0
    print(f"6. NPL Ratio: {npl_ratio:.2f}%")

    #  Expected Loss - EAD × PD × LGD 
    pd_probability = default_rate / 100
    lgd = 0.45  # Loss Given Default globally recognized figure
    expected_loss = ead * pd_probability * lgd
    print(f"7. Expected Loss: ${expected_loss:,.2f}")

    # Create KPI dataframe
    kpi_df = pd.DataFrame({
        'calculation_date': [datetime.now().date()],
        'default_rate_pct': [default_rate],
        'portfolio_yield_pct': [portfolio_yield],
        'early_delinquency_ratio_pct': [early_delinquency_ratio],
        'exposure_at_default': [ead],
        'customer_risk_score': [customer_risk_score],
        'npl_ratio_pct': [npl_ratio],
        'expected_loss': [expected_loss],
        'total_active_loans': [total_active_loans],
        'total_defaulted_loans': [num_defaulted],
        'total_outstanding_balance': [total_outstanding_principal],
        'total_interest_earned': [total_interest_earned],
        'avg_repayment_history_score': [avg_repayment_history],
        'processing_timestamp': [datetime.now()]
    })

    print(f" Portfolio KPIs calculated")
    return kpi_df


def calculate_regional_delinquency_kpis(fact_df: pd.DataFrame, dim_customer: pd.DataFrame, dim_loan: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate delinquency metrics by region for dashboard drilldown.
    Extracts region from customer address and calculates delinquency stats.
    """
    print("\n" + "="*60)
    print("CALCULATING REGIONAL DELINQUENCY KPIs")
    print("="*60)

    # Merge fact data with dim_loan to get loan details (exclude customer_id as it's already in fact_df)
    fact_with_loan = fact_df.merge(
        dim_loan[['loan_id', 'loan_amount', 'approval_status']],
        on='loan_id',
        how='left'
    )

    # merge with dim_customer to get address
    fact_with_details = fact_with_loan.merge(
        dim_customer[['customer_id', 'address']],
        on='customer_id',
        how='left'
    )

    # Extract region from address ("123 Street, City, State ZIP")
    # Extract state as region
    fact_with_details['region'] = fact_with_details['address'].apply(
        lambda x: x.split(',')[-2].strip() if pd.notna(x) and len(x.split(',')) >= 2 else 'Unknown'
    )

    # Filter for approved loans only
    active_fact = fact_with_details[fact_with_details['approval_status'] == 'APPROVED'].copy()

    # Get latest repayment per loan
    latest_repayments = active_fact.sort_values('payment_date').groupby('loan_id').last().reset_index()

    # Group by region and calculate metrics
    regional_stats = []
    for region in latest_repayments['region'].unique():
        region_loans = latest_repayments[latest_repayments['region'] == region]
        total_loans = len(region_loans)

        # Delinquent loans (1-90 days)
        early_delinquent = region_loans[
            (region_loans['days_past_due'] >= 1) & (region_loans['days_past_due'] <= 90)
        ]
        num_early_delinquent = len(early_delinquent)
        early_delinquency_rate = (num_early_delinquent / total_loans * 100) if total_loans > 0 else 0

        # NPL (>90 days)
        npl_loans = region_loans[region_loans['days_past_due'] > 90]
        num_npl = len(npl_loans)
        npl_rate = (num_npl / total_loans * 100) if total_loans > 0 else 0

        # Defaulted loans (MISSED status)
        defaulted = region_loans[region_loans['status'] == 'MISSED']
        num_defaulted = len(defaulted)
        default_rate = (num_defaulted / total_loans * 100) if total_loans > 0 else 0

        # Calculate total outstanding balance and exposure at default
        region_loans_outstanding = active_fact[active_fact['region'] == region].groupby('loan_id').agg({
            'loan_amount': 'first',
            'amount_paid': 'sum'
        }).reset_index()
        region_loans_outstanding['outstanding_balance'] = (
            region_loans_outstanding['loan_amount'] - region_loans_outstanding['amount_paid']
        ).clip(lower=0)
        total_outstanding = region_loans_outstanding['outstanding_balance'].sum()

        regional_stats.append({
            'region': region,
            'total_loans': total_loans,
            'early_delinquent_loans': num_early_delinquent,
            'early_delinquency_rate_pct': early_delinquency_rate,
            'npl_loans': num_npl,
            'npl_rate_pct': npl_rate,
            'defaulted_loans': num_defaulted,
            'default_rate_pct': default_rate,
            'total_outstanding_balance': total_outstanding,
            'calculation_date': datetime.now().date(),
            'processing_timestamp': datetime.now()
        })

    regional_kpi_df = pd.DataFrame(regional_stats)
    print(f" Regional KPIs calculated for {len(regional_kpi_df)} regions")
    return regional_kpi_df


def calculate_customer_drilldown_kpis(fact_df: pd.DataFrame, dim_customer: pd.DataFrame, dim_loan: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate customer-level risk metrics for dashboard drilldown.
    Includes customer name, ID, and detailed risk metrics.
    """
    print("\n" + "="*60)
    print("CALCULATING CUSTOMER-LEVEL DRILLDOWN KPIs")
    print("="*60)

    # Merge fact data with dim_loan to get loan details (exclude customer_id as it's already in fact_df)
    fact_with_loan = fact_df.merge(
        dim_loan[['loan_id', 'loan_amount', 'approval_status']],
        on='loan_id',
        how='left'
    )

    # Then merge with dim_customer to get customer details
    fact_with_details = fact_with_loan.merge(
        dim_customer[['customer_id', 'full_name', 'address', 'credit_score']],
        on='customer_id',
        how='left'
    )

    # Filter for approved loans only
    active_fact = fact_with_details[fact_with_details['approval_status'] == 'APPROVED'].copy()

    # Extract region from address
    active_fact['region'] = active_fact['address'].apply(
        lambda x: x.split(',')[-2].strip() if pd.notna(x) and len(x.split(',')) >= 2 else 'Unknown'
    )

    # Calculate customer-level metrics
    customer_stats = []
    for customer_id in active_fact['customer_id'].unique():
        customer_data = active_fact[active_fact['customer_id'] == customer_id]
        customer_name = customer_data['full_name'].iloc[0]
        customer_region = customer_data['region'].iloc[0]
        customer_credit_score = customer_data['credit_score'].iloc[0]

        # Get latest repayment status per loan
        latest_repayments = customer_data.sort_values('payment_date').groupby('loan_id').last()

        total_loans = len(latest_repayments)
        total_loan_amount = customer_data.groupby('loan_id')['loan_amount'].first().sum()
        total_amount_paid = customer_data.groupby('loan_id')['amount_paid'].sum().sum()
        outstanding_balance = max(0, total_loan_amount - total_amount_paid)

        # Delinquency status
        max_days_past_due = latest_repayments['days_past_due'].max()
        is_delinquent = max_days_past_due > 0
        is_npl = max_days_past_due > 90
        is_defaulted = (latest_repayments['status'] == 'MISSED').any()

        # Repayment history score
        repayment_history_score = (customer_data['status'] == 'PAID').sum() / len(customer_data) * 100 if len(customer_data) > 0 else 0

        # Transaction score (inverted delinquency: 0 days past due = 100, 90+ days = 0)
        transaction_score = max(0, 100 - (max_days_past_due / 90) * 100) if pd.notna(max_days_past_due) else 100

        # Normalize credit score to 0-100 scale (assuming credit scores range 300-850)
        credit_score_norm = ((customer_credit_score - 300) / (850 - 300)) * 100 if pd.notna(customer_credit_score) else 0
        credit_score_norm = max(0, min(100, credit_score_norm))  # Clip to 0-100

        # Customer Risk Score (weighted formula):
        # = 0.5 × credit_score_norm + 0.3 × transaction_score + 0.2 × repayment_history_score
        customer_risk_score = (
            0.5 * credit_score_norm +
            0.3 * transaction_score +
            0.2 * repayment_history_score
        ) if pd.notna(customer_credit_score) else 0

        customer_stats.append({
            'customer_id': customer_id,
            'customer_name': customer_name,
            'region': customer_region,
            'credit_score': customer_credit_score,
            'total_loans': total_loans,
            'total_loan_amount': total_loan_amount,
            'outstanding_balance': outstanding_balance,
            'max_days_past_due': max_days_past_due if pd.notna(max_days_past_due) else 0,
            'is_delinquent': is_delinquent,
            'is_npl': is_npl,
            'is_defaulted': is_defaulted,
            'repayment_history_score': repayment_history_score,
            'customer_risk_score': customer_risk_score,
            'calculation_date': datetime.now().date(),
            'processing_timestamp': datetime.now()
        })

    customer_drilldown_df = pd.DataFrame(customer_stats)
    print(f" Customer drilldown KPIs calculated for {len(customer_drilldown_df)} customers")
    return customer_drilldown_df



# WRITE TO GOLD


def write_to_gold(df: pd.DataFrame, output_path: str, partition: bool = True) -> None:
    """Write dataframe to gold bucket as partitioned Parquet."""
    try:
        if partition:
            now = datetime.now()
            # Create partition path with filename
            partition_path = f"{output_path}year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            full_path = f"{partition_path}data.parquet"
        else:
            # For non-partitioned, ensure path ends with .parquet
            if not output_path.endswith('.parquet'):
                full_path = f"{output_path}data.parquet"
            else:
                full_path = output_path

        print(f"Writing to: {full_path}")

        # Write using s3fs directly with proper path handling
        with fs.open(full_path, 'wb') as f:
            df.to_parquet(
                f,
                engine='pyarrow',
                compression='snappy',
                index=False
            )

        print(f" Successfully wrote {len(df)} records to {full_path}")

    except Exception as e:
        print(f"Error writing to S3: {str(e)}")
        raise



# MAIN


def main():
    """Main execution flow."""
    try:
        print("=" * 60)
        print("GLUE JOB: Build Gold Analytics Layer (Silver → Gold)")
        print("=" * 60)

        # 1. Load transformed data from silver
        print("\n[1/5] Loading transformed data from silver bucket...")

        print("\nLoading loan applications...")
        loan_applications = get_latest_transformed_data(LOAN_APPLICATIONS_SILVER)

        print("\nLoading loan repayments...")
        loan_repayments = get_latest_transformed_data(LOAN_REPAYMENTS_SILVER)

        print("\nLoading credit bureau...")
        credit_bureau = get_latest_transformed_data(CREDIT_BUREAU_SILVER)

        # 2. Build dimensions
        print("\n[2/5] Building dimensions...")

        dim_customer = build_dim_customer(loan_applications, credit_bureau)
        dim_customer = apply_dim_customer_upsert(dim_customer)

        dim_loan = build_dim_loan(loan_repayments, loan_applications)
        dim_loan = apply_dim_loan_upsert(dim_loan)

        # 3. Build fact table
        print("\n[3/5] Building fact table...")

        fact_loan_performance = build_fact_loan_performance(loan_repayments, dim_loan)
        fact_loan_performance = apply_fact_loan_performance_upsert(fact_loan_performance)

        # 4. Calculate KPIs
        print("\n[4/5] Calculating KPIs...")

        kpi_portfolio = calculate_portfolio_kpis(fact_loan_performance, dim_customer, dim_loan)
        kpi_regional = calculate_regional_delinquency_kpis(fact_loan_performance, dim_customer, dim_loan)
        kpi_customer_drilldown = calculate_customer_drilldown_kpis(fact_loan_performance, dim_customer, dim_loan)

        # 5. Write to gold bucket
        print("\n[5/5] Writing to gold bucket...")

        print("\nWriting DimCustomer...")
        write_to_gold(dim_customer, DIM_CUSTOMER_OUTPUT)

        print("\nWriting DimLoan...")
        write_to_gold(dim_loan, DIM_LOAN_OUTPUT)

        print("\nWriting FactLoanPerformance...")
        write_to_gold(fact_loan_performance, FACT_LOAN_PERFORMANCE_OUTPUT)

        print("\nWriting KPI Portfolio...")
        write_to_gold(kpi_portfolio, KPI_PORTFOLIO_OUTPUT)

        print("\nWriting KPI Regional Delinquency...")
        write_to_gold(kpi_regional, KPI_REGIONAL_OUTPUT)

        print("\nWriting KPI Customer Drilldown...")
        write_to_gold(kpi_customer_drilldown, KPI_CUSTOMER_DRILLDOWN_OUTPUT)

        print("\n" + "=" * 60)
        print(" JOB COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"\nSummary:")
        print(f"  - DimCustomer: {len(dim_customer)} records")
        print(f"  - DimLoan: {len(dim_loan)} records")
        print(f"  - FactLoanPerformance: {len(fact_loan_performance)} records")
        print(f"  - KPI Portfolio: {len(kpi_portfolio)} record(s)")
        print(f"  - KPI Regional Delinquency: {len(kpi_regional)} record(s)")
        print(f"  - KPI Customer Drilldown: {len(kpi_customer_drilldown)} record(s)")

    except Exception as e:
        print(f"\n✗ JOB FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
