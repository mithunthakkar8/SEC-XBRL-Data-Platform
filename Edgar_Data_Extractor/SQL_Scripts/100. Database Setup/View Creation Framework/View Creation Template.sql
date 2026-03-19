CALL company.insert_concept_name_mappings('SCCO', 'NYQ');

SELECT company.create_income_statement_view('SCCO', 'NYQ');

-- use the results from the below query in the original_concept_name column and paste in chatgpt. Ask it 'which of these are are synonymous'
-- These are candidates for further processing
select * from company.IS_concept_name_mapping
where ticker_symbol='SCCO'
and exchange_code = 'NYQ';


-- Query the results and check if there are no 2 columns which report same numbers - 
-- one which was reported in earlier filing and one which was reported in new
-- these columns will need to be merged into one. We will do that using update statement below
SELECT * FROM company.scco_nyq_income_statement;

-- Use Debug Mode
-- SELECT company.create_income_statement_view('SCCO', 'NYQ', true);


-- Once found out which columns above need to be merged, assign them the same name in normalized_concept_name column below
WITH concept_map (original, normalized) AS (
    VALUES
	-- Standard Concept Mapping (edit as needed)
	    ('', 'NetRevenue'),
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
		('', '')
)
UPDATE company.IS_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'SCCO'
  AND target.exchange_code = 'NYQ';


-- do same transformations as above for balance sheet

-- -- With debugging enabled
-- SELECT company.create_balance_sheet_view('SCCO', 'NYQ', true);

-- Without debugging (default)
SELECT company.create_balance_sheet_view('SCCO', 'NYQ');

-- Query the results
SELECT * FROM company.scco_nyq_balance_sheet;


WITH concept_map (original, normalized) AS (
    VALUES
		-- Standard Concept Mapping
		-- Note: Whenever standard elements (like inventories) 
		-- are not given upfrontly in the report, we can create separate mapping with Inventories_Element_ prepended to the actual element
		-- the ratio analysis function will sum such elements to arrive at the standard element 'Inventories'
		('CashAndCashEquivalentsAtCarryingValue', 'CashAndCashEquivalents'),
	    ('AccountsReceivableNetCurrent', 'TradeAccountsReceivable'),
	    ('InventoryNet', 'Inventories'),
	    ('AssetsCurrent', 'CurrentAssets'),
	    ('PropertyPlantAndEquipmentNet', 'PropertyPlantAndEquipmentNet'),
	    ('Assets', 'TotalAssets'),
		('', 'PrepaidExpenses'),

		('LongTermDebtCurrent', 'CurrentPortionOfLongTermDebt'),
	    ('AccountsPayableCurrent', 'AccountsPayable'),
	    ('LiabilitiesCurrent', 'CurrentLiabilities'),
	    ('LongTermDebtNoncurrent', 'LongTermDebt'),
	    ('LiabilitiesCurrent', 'TotalLiabilities_Element_CurrentLiabilities'),
		('LiabilitiesNoncurrent', 'TotalLiabilities_Element_NonCurrentLiabilities'),
	    ('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'TotalEquity'),
	    ('RetainedEarningsAccumulatedDeficit', 'RetainedEarnings')
		('', 'Accrued_Expenses'),
		('', 'Deferred_Revenue'),
		
		-- Non Standard Concept Mapping
        ('', ''),
)
UPDATE company.BS_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'FCX'
  AND target.exchange_code = 'NYQ';


-- do same transformations as above for cash flow statement

-- SELECT company.create_cash_flow_statement_view('SCCO', 'NYQ', TRUE);
SELECT company.create_cash_flow_statement_view('SCCO', 'NYQ');
SELECT * FROM company.scco_nyq_cash_flow_statement;

WITH concept_map (original, normalized) AS (
    VALUES
        -- Standard Concept Mapping
		('NetCashProvidedByUsedInOperatingActivities', 'NetCashFromOperatingActivities'),
        ('NetCashProvidedByUsedInInvestingActivities', 'NetCashFromInvestingActivities'),
        ('NetCashProvidedByUsedInFinancingActivities', 'NetCashFromFinancingActivities'),
        ('PaymentsToAcquireProductiveAssets', 'CapitalExpenditures'), 
        ('CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect', 'CashAndCashEquivalentsPeriodIncreaseDecrease'), -- ties into liquidity

		-- Non Standard Concept Mapping
		('', '')
)
UPDATE company.CF_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'SCCO'
  AND target.exchange_code = 'NYQ';