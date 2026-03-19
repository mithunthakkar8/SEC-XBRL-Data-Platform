CALL company.insert_concept_name_mappings('SCCO', 'NYQ');

SELECT company.create_income_statement_view('SCCO', 'NYQ');

-- Query the results and check if there are no 2 columns which report same numbers - 
-- one which was reported in earlier filing and one which was reported in new
-- these columns will need to be merged into one. We will do that using update statement below
SELECT * FROM company.scco_nyq_income_statement;

-- Use Debug Mode
-- SELECT company.create_income_statement_view('SCCO', 'NYQ', true);

-- use the results from the below query in the original_concept_name column and paste in chatgpt. Ask it 'which of these are are synonymous'
select * from company.IS_concept_name_mapping
where ticker_symbol='SCCO'
and exchange_code = 'NYQ'
order by original_concept_name;

-- Once found out which columns above need to be merged, assign them the same name in normalized_concept_name column below
WITH concept_map (original, normalized) AS (
    VALUES
	-- Standard Concept Mapping
	    ('RevenueMineralSales', 'NetRevenue'),
		('Revenues', 'NetRevenue'),
		('RevenueFromContractWithCustomerExcludingAssessedTax', 'NetRevenue'),
	    ('CostOfRevenue', 'CostofRevenueOrGoodsSold'),
	    ('OperatingIncomeLoss', 'OperatingProfitReported'),
	    ('DepreciationDepletionAndAmortization', 'DepreciationAndAmortization'),
	    ('InterestCostsIncurred', 'InterestExpense'),
	    ('NetIncomeLossAttributableToNoncontrollingInterest', 'MinorityInterestProfit'),
	    ('CurrentAndRoyaltyTaxes', 'IncomeTaxExpense'),
	    ('NetIncomeLossAvailableToCommonStockholdersBasic', 'NetProfitBottomLine'),
	    ('EarningsPerShareDiluted', 'DilutedEPS'),
		('EarningsPerShareBasicAndDiluted', 'DilutedEPS'),
	    ('WeightedAverageNumberOfDilutedSharesOutstanding', 'WeightedAverageDilutedSharesOutstanding'),

		-- Non-standard concept Name Mapping
        ('InterestCostsCapitalized', 'InterestCostsCapitalizedAdjustment'),
		('ExplorationExpenseMining', 'ExplorationExpense')
)
UPDATE company.IS_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'SCCO'
  AND target.exchange_code = 'NYQ';

select * from company.IS_Concept_name_mapping
order by original_concept_name;
-- do same transformations as above for balance sheet

-- -- With debugging enabled
-- SELECT company.create_balance_sheet_view('SCCO', 'NYQ', true);

-- as we have proper concept mapping, we can now create flat normalized structure 
-- for usage in Power BI

SELECT company.create_income_statement_view_UP('SCCO', 'NYQ');

SELECT * FROM company.scco_nyq_income_statement_U;

CREATE OR REPLACE VIEW company.scco_nyq_income_statement_UP AS
SELECT * FROM company.scco_nyq_income_statement_U;


select current_user

-- Without debugging (default)
SELECT company.create_balance_sheet_view('SCCO', 'NYQ');

-- Query the results
SELECT * FROM company.scco_nyq_balance_sheet;


WITH concept_map (original, normalized) AS (
    VALUES
		-- Standard Concept Mapping
		('CashAndCashEquivalentsAtCarryingValue', 'CashAndCashEquivalents'),
	    ('AccountsReceivableNetCurrent', 'TradeAccountsReceivable'),
	    ('InventoryNet', 'Inventories'),
	    ('AssetsCurrent', 'CurrentAssets'),
	    ('PropertyPlantAndEquipmentNet', 'PropertyPlantAndEquipmentNet'),
	    ('Assets', 'TotalAssets'),
		('PrepaidTaxes', 'PrepaidExpensesElement_Taxes'),

		('LongTermDebtCurrent', 'CurrentPortionOfLongTermDebt'),
	    ('AccountsPayableCurrent', 'AccountsPayable'),
	    ('LiabilitiesCurrent', 'CurrentLiabilities'),
	    ('LongTermDebtNoncurrent', 'LongTermDebt'),
	    ('LiabilitiesCurrent', 'TotalLiabilities_Element_CurrentLiabilities'),
		('LiabilitiesNoncurrent', 'TotalLiabilities_Element_NonCurrentLiabilities'),
		('AccruedWorkersParticipation', 'Accrued_Expenses_Element_AccruedWorkersParticipation'),
		('InterestPayableCurrent', 'Accrued_Expenses_Element_InterestPayable'),

		('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'TotalEquity'),
	    ('RetainedEarningsAccumulatedDeficit', 'RetainedEarnings')
)
UPDATE company.BS_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'SCCO'
  AND target.exchange_code = 'NYQ';

SELECT company.create_balance_sheet_view_UP('SCCO', 'NYQ');
SELECT * FROM company.scco_nyq_balance_sheet_UP;

-- do same transformations as above for cash flow statement

-- SELECT company.create_cash_flow_statement_view('SCCO', 'NYQ', TRUE);
SELECT company.create_cash_flow_statement_view('SCCO', 'NYQ');
SELECT * FROM company.scco_nyq_cash_flow_statement;

select * from company.CF_concept_name_mapping

WITH concept_map (original, normalized) AS (
    VALUES
		-- Standard Concept Mapping
		('NetCashProvidedByUsedInOperatingActivities', 'NetCashFromOperatingActivities'),
		('NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'NetCashFromOperatingActivities'),
        ('NetCashProvidedByUsedInInvestingActivities', 'NetCashFromInvestingActivities'),
		('NetCashProvidedByUsedInInvestingActivitiesContinuingOperations', 'NetCashFromInvestingActivities'),
        ('NetCashProvidedByUsedInFinancingActivities', 'NetCashFromFinancingActivities'),
		('NetCashProvidedByUsedInFinancingActivitiesContinuingOperations', 'NetCashFromFinancingActivities'),
        ('PaymentsToAcquirePropertyPlantAndEquipment', 'CapitalExpenditures'), 
        ('CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect', 'CashAndCashEquivalentsPeriodIncreaseDecrease'), -- ties into liquidity

		-- Non-Standard Concept Mapping
        ('EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations', 'EffectOfExchangeRateOnCashAndCashEquivalents'),
		('EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents', 'EffectOfExchangeRateOnCashAndCashEquivalents')
)
UPDATE company.CF_concept_name_mapping AS target
SET normalized_concept_name = map.normalized
FROM concept_map AS map
WHERE target.original_concept_name = map.original
  AND target.ticker_symbol = 'SCCO'
  AND target.exchange_code = 'NYQ';


SELECT company.create_cash_flow_statement_view_UP('SCCO', 'NYQ');
SELECT * FROM company.scco_nyq_cash_flow_statement_UP;