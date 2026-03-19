import yaml

data = {
    "metric_patterns": {
        "Revenue": ["us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax", "us-gaap_RevenueMineralSales"
                    , "scco_RevenueFromContractWithCustomerExcludingAssessedTax"],
        "GAAP_Cost_of_Sales": ["us-gaap_CostOfRevenue"],
        "GAAP_Operating_Income": ["us-gaap_OperatingIncomeLoss"],
        "Depreciation_And_Amortization": ["us-gaap_DepreciationDepletionAndAmortization"],
        "Interest_Expense": ["us-gaap_InterestCostsIncurred"],
        "Profit_Before_Tax": ["us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"],
        "Tax_Expense": ["scco_CurrentAndRoyaltyTaxes", "us-gaap_IncomeTaxExpenseBenefit"],
        "GAAP_Net_Profit": ["us-gaap_ProfitLoss"],
        "Net_Profit_Bottom_Line": ["us-gaap_NetIncomeLoss"],
        "Diluted_EPS": ["us-gaap_EarningsPerShareDiluted"],
        "Weighted_Average_Outstanding_Shares": ["us-gaap_WeightedAverageNumberOfShareOutstandingBasicAndDiluted", "us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding"],
        "Cash_And_Cash_Equivalents": ["us-gaap_CashAndCashEquivalentsAtCarryingValue"],
        "Trade_Accounts_Receivable": ["us-gaap_AccountsReceivableNetCurrent"],
        "Inventories": ["us-gaap_InventoryNet"],
        "Current_Assets": ["us-gaap_AssetsCurrent"],
        "Property_and_Mine_Development": ["us-gaap_PropertyPlantAndEquipmentNet"],
        "Ore_Stockpiles_on_Leach_Pads": ["us-gaap_InventoryOreStockpilesOnLeachPads", "scco_LeachableMaterial"],
        "Accounts_Payable": ["us-gaap_AccountsPayableCurrent"],
        "Current_Liabilities": ["us-gaap_LiabilitiesCurrent"],
        "Current_Portion_of_Long_Term_Debt": ["us-gaap_LongTermDebtCurrent"],
        "Long_Term_Debt": ["us-gaap_LongTermDebtNoncurrent"],
        "Lease_Liabilities": ["us-gaap_OperatingLeaseLiabilityNoncurrent"],
        "Additional_Paid_In_Capital": ["us-gaap_AdditionalPaidInCapitalCommonStock", "us-gaap_AdditionalPaidInCapital"],
        "Retained_Earnings": ["us-gaap_RetainedEarningsAccumulatedDeficit"],
        "Treasury_Stock": ["us-gaap_TreasuryStockValue"],
        "Shareholders_Equity": ["us-gaap_StockholdersEquity"],
        "Total_Equity": ["us-gaap_StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
        "Total_Assets": ["us-gaap_Assets"],
        "CFO": ["us-gaap_NetCashProvidedByUsedInOperatingActivities", "us-gaap_NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"],
        "Capex": ["us-gaap_PaymentsToAcquirePropertyPlantAndEquipment"],
        "Repayment_of_Debt": ["us-gaap_RepaymentsOfLongTermDebt"],
        "Cash_Dividends": ["us-gaap_PaymentsOfDividendsCommonStock"]
    },
    "fallback_patterns": {
        "Revenue": [],
        "GAAP_Cost_of_Sales": [],
        "GAAP_Operating_Income": [],
        "Depreciation_And_Amortization": [],
        "Interest_Expense": [],
        "Profit_Before_Tax": [],
        "Tax_Expense": [],
        "GAAP_Net_Profit": ["us-gaap_NetIncomeLoss", "us-gaap_NetIncomeLossAvailableToCommonStockholdersBasic"],
        "Net_Profit_Bottom_Line": ["us-gaap_NetIncomeLossAvailableToCommonStockholdersBasic"],
        "Diluted_EPS": ["us-gaap_EarningsPerShareBasicAndDiluted"],
        "Weighted_Average_Outstanding_Shares": [],
        "Cash_And_Cash_Equivalents": [],
        "Trade_Accounts_Receivable": [],
        "Inventories": [],
        "Current_Assets": [],
        "Property_and_Mine_Development": [],
        "Ore_Stockpiles_on_Leach_Pads": ["us-gaap_OtherInventoryNoncurrent"],
        "Accounts_Payable": [],
        "Current_Liabilities": [],
        "Current_Portion_of_Long_Term_Debt": [],
        "Long_Term_Debt": [],
        "Lease_Liabilities": [],
        "Additional_Paid_In_Capital": [],
        "Retained_Earnings": [],
        "Treasury_Stock": [],
        "Shareholders_Equity": [],
        "Total_Equity": [],
        "Total_Assets": [],
        "CFO": [],
        "Capex": [],
        "Repayment_of_Debt": [],
        "Cash_Dividends": []
    }
}

file_path = r"C:/Users/mithu/Documents/MEGA/Projects/Financial_Data_Analytics_Pipeline/Edgar_Data_Extractor/metrics_config.yaml"

# Save to a YAML file
with open(file_path, "w") as file:
    yaml.dump(data, file, default_flow_style=False, sort_keys=False)
