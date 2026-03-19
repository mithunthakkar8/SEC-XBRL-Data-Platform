CALL company.insert_concept_name_mappings('FCX', 'NYQ');

SELECT company.create_income_statement_view('FCX', 'NYQ');

-- Query the results and check if there are no 2 columns which report same numbers - 
-- one which was reported in earlier filing and one which was reported in new
-- these columns will need to be merged into one. We will do that using update statement below
SELECT * FROM company.fcx_nyq_income_statement;


-- Use Debug Mode
-- SELECT company.create_income_statement_view('FCX', 'NYQ', true);

-- use the results from the below query in the original_concept_name column and paste in chatgpt. Ask it 'which of these are are synonymous'
select * from company.IS_concept_name_mapping
where ticker_symbol='FCX'
and exchange_code = 'NYQ'
order by original_concept_name;


WITH concept_map (original, normalized) AS (
    VALUES
	-- Standard Concept Mapping
	    ('SalesRevenueNet', 'Net_Revenue'),
	    ('Revenues', 'Net_Revenue'),
	    ('CostOfGoodsSold', 'CostofRevenueOrGoodsSold'),
	    ('CostOfRevenue', 'CostofRevenueOrGoodsSold'),
	    ('OperatingIncomeLoss', 'OperatingProfitReported'),
	    ('DepreciationDepletionAndAmortization', 'DepreciationAndAmortization'),
		('CostDepreciationAmortizationAndDepletion', 'DepreciationAndAmortization'),
	    ('CostOfGoodsSoldDepreciationDepletionAndAmortization', 'DepreciationAndAmortization'),
	    ('InterestIncomeExpenseNet', 'InterestExpense'),
	    ('InterestExpenseIncludingAccretion', 'InterestExpense'),
	    ('IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments', 'NetProfitBeforeTax'),
		('IncomeLossFromContinuingOperationsAttributableToNoncontrollingEntity', 'MinorityInterestProfit'),
	    ('NetIncomeLossAttributableToNoncontrollingInterest', 'MinorityInterestProfit'),
	    ('NetIncomeLossAvailableToCommonStockholdersBasic', 'NetProfitBottomLine'),
	    ('IncomeLossFromContinuingOperationsPerDilutedShare', 'DilutedEPS'),
	    ('EarningsPerShareDiluted', 'DilutedEPS'),
	    ('WeightedAverageNumberOfDilutedSharesOutstanding', 'WeightedAverageDilutedSharesOutstanding'),


		-- Non-standard concept Name Mapping
        ('DiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTaxPerBasicShare', 'PerBasicShareDiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTax'),
        ('DiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTaxPerDilutedShare', 'PerDilutedShareDiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTax'),
        ('IncomeLossFromContinuingOperationsPerBasicShare', 'EarningsPerShareBasic'),
        ('EnvironmentalObligationsAndShutdownCosts', 'EnvironmentalRemediationExpense'),
        ('EnvironmentalRemediationExpense', 'EnvironmentalRemediationExpense'),
        ('ExplorationAndResearchExpenses', 'ExplorationExpense'),
        ('ExplorationExpense', 'ExplorationExpense'),
        ('CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization', 'CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization')
)
UPDATE company.IS_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'FCX'
  AND target.exchange_code = 'NYQ';


-- do same transformations as above for balance sheet

-- -- With debugging enabled
-- SELECT company.create_balance_sheet_view('FCX', 'NYQ', true);

-- Without debugging (default)
SELECT company.create_balance_sheet_view('FCX', 'NYQ');

-- Query the results
SELECT * FROM company.fcx_nyq_balance_sheet;

select * from company.BS_concept_name_mapping
order by original_concept_name;

WITH concept_map (original, normalized) AS (
    VALUES
		-- Standard Concept Mapping
		('CashAndCashEquivalentsAtCarryingValue', 'CashAndCashEquivalents'),
	    ('AccountsReceivableNetCurrent', 'TradeAccountsReceivable'),
	    ('Product', 'Inventories_Element_Product'),
		('InventoryRawMaterialsAndSuppliesNetOfReserves', 'Inventories_Element_InventoryRawMaterialsAndSuppliesNetOfReserves'),
		('InventoryMillandStockpilesonLeachPadsCurrent', 'Inventories_Element_InventoryMillandStockpilesonLeachPadsCurrent'),
	    ('AssetsCurrent', 'CurrentAssets'),
	    ('PropertyPlantAndEquipmentNet', 'PropertyPlantAndEquipmentNet'),
	    ('Assets', 'TotalAssets'),

		('LongTermDebtCurrent', 'CurrentPortionOfLongTermDebt'),
	    ('AccountsPayableAndAccruedLiabilitiesCurrent', 'AccountsPayable'),
	    ('LiabilitiesCurrent', 'CurrentLiabilities'),
	    ('LongTermDebtNoncurrent', 'LongTermDebt'),
	    ('Liabilities', 'TotalLiabilities'),
	    ('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'TotalEquity'),
	    ('RetainedEarningsAccumulatedDeficit', 'RetainedEarnings'),

		-- Non Standard Concept Mapping
        ('LiabilitiesOfDisposalGroupIncludingDiscontinuedOperationCurrent', 'LiabilitiesCurrentOfDisposalGroupIncludingDiscontinuedOperation'),
        ('LiabilitiesOfDisposalGroupIncludingDiscontinuedOperationNoncurrent', 'LiabilitiesNonCurrentOfDisposalGroupIncludingDiscontinuedOperation'),
        ('OilandNaturalGasPropertiesFullCostMethodSubjecttoAmortization', 'OilandNaturalGasPropertiesFullCostMethodSubjecttoAmortization'),
        ('OilandNaturalGasPropertiesFullCostMethodSubjecttoAmortizationLessAccumulatedAmortizationandImpairments', 'OilandNaturalGasPropertiesFullCostMethodSubjecttoAmortization'),
        ('InventoryRawMaterialsAndSuppliesNetOfReserves', 'InventorySuppliesNetOfReserves')
)
UPDATE company.BS_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'FCX'
  AND target.exchange_code = 'NYQ';


-- do same transformations as above for cash flow statement

-- SELECT company.create_cash_flow_statement_view('FCX', 'NYQ', TRUE);
SELECT company.create_cash_flow_statement_view('FCX', 'NYQ');
SELECT * FROM company.fcx_nyq_cash_flow_statement;

-- Cash flow
WITH concept_map (original, normalized) AS (
    VALUES
		-- Standard Concept Mapping
		('NetCashProvidedByUsedInOperatingActivities', 'NetCashFromOperatingActivities'),
        ('NetCashProvidedByUsedInInvestingActivities', 'NetCashFromInvestingActivities'),
        ('NetCashProvidedByUsedInFinancingActivities', 'NetCashFromFinancingActivities'),
        ('PaymentsToAcquireProductiveAssets', 'CapitalExpenditures'), 
        ('CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect', 'CashAndCashEquivalentsPeriodIncreaseDecrease'), -- ties into liquidity

		-- Non Standard Concept Mapping
        ('DeferredIncomeTax', 'DeferredIncomeTaxesAndTaxCredits'),
        ('DeferredIncomeTaxesAndTaxCredits', 'DeferredIncomeTaxesAndTaxCredits'),
        ('CostOfGoodsSoldDepreciationDepletionAndAmortization', 'DepreciationDepletionAndAmortization'),
        ('PaymentsforEnvironmentalLiabilitiesAndAssetRetirementObligations', 'PaymentsOfReclamationAndEnvironmentalObligations'),
        ('PaymentsOfReclamationAndEnvironmentalObligations', 'PaymentsOfReclamationAndEnvironmentalObligations'),
        ('ImpairmentOfOilAndGasProperties', 'ImpairmentofOilandGasPropertiesandGoodwill'),
        ('ImpairmentofOilandGasPropertiesandGoodwill', 'ImpairmentofOilandGasPropertiesandGoodwill'),
        ('RepaymentsOfDebt', 'RepaymentsOfDebtAndCapitalLeaseObligations'),
        ('RepaymentsOfDebtAndCapitalLeaseObligations', 'RepaymentsOfDebtAndCapitalLeaseObligations'),
        ('PTFISurfaceWaterTaxWithholdingTaxandEnvironmentalMattersExpense', 'PTFISurfaceWaterTaxWithholdingTaxandEnvironmentalMattersPayments')
)
UPDATE company.CF_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'FCX'
  AND target.exchange_code = 'NYQ';
;

