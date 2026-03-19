
-- Financial Metric Types Lookup Table
CREATE TABLE IF NOT EXISTS alphascope.metric_types (
    metric_type TEXT PRIMARY KEY,
	financial_statement TEXT,
    description TEXT
);

INSERT INTO alphascope.metric_types (metric_type, financial_statement, description) VALUES
('Revenue', 'income_statement', 'Total income generated from sales of goods or services.'),
('GAAP_Cost_of_Sales', 'income_statement', 'Cost of goods sold calculated using GAAP accounting standards.'),
('GAAP_Operating_Income', 'income_statement', 'Operating income calculated using GAAP accounting standards.'),
('Depreciation_And_Amortization', 'income_statement', 'Reduction in value of assets over time.'),
('Interest_Expense', 'income_statement', 'Cost of borrowing funds, such as interest on loans.'),
('Profit_Before_Tax', 'income_statement', 'A measure that looks at a company’s profits before the company has to pay corporate income tax.'),
('Tax_Expense', 'income_statement', 'Taxes owed to government authorities.'),
('GAAP_Net_Profit', 'income_statement', 'Net profit calculated using GAAP accounting standards.'),
('Net_Profit_Bottom_Line', 'income_statement', 'Final profit after all expenses, taxes, and costs.'),
('Diluted_EPS', 'income_statement', 'Earnings per share assuming all convertible securities are exercised.'),
('Weighted_Average_Outstanding_Shares', 'income_statement', 'Average number of shares outstanding during the reporting period.'),

('Cash_And_Cash_Equivalents', 'balance_sheet', 'Liquid assets such as cash and short-term investments.'),
('Trade_Accounts_Receivable', 'balance_sheet', 'Amounts owed to the company by customers for goods or services sold on credit.'),
('Inventories', 'balance_sheet', 'Goods and materials held for sale or production.'),
('Current_Assets', 'balance_sheet', 'Assets expected to be converted to cash within one year.'),
('Property_and_Mine_Development', 'balance_sheet', 'Costs associated with developing property or mines.'),
('Ore_Stockpiles_on_Leach_Pads', 'balance_sheet', 'Ore stockpiles awaiting processing.'),
('Accounts_Payable', 'balance_sheet', 'Amounts owed by the company to suppliers.'),
('Current_Liabilities', 'balance_sheet', 'Liabilities due within one year.'),
('Current_Portion_of_Long_Term_Debt', 'balance_sheet', 'Portion of long-term debt due within one year.'),
('Long_Term_Debt', 'balance_sheet', 'Debt obligations due beyond one year.'),
('Lease_Liabilities', 'balance_sheet', 'Obligations related to lease agreements.'),
('Additional_Paid_In_Capital', 'balance_sheet', 'Amounts paid by investors above the par value of shares.'),
('Retained_Earnings', 'balance_sheet', 'Cumulative net income retained by the company.'),
('Treasury_Stock', 'balance_sheet', 'Shares repurchased by the company.'),
('Shareholders_Equity', 'balance_sheet', 'Owners’ equity in the company.'),
('Total_Equity', 'balance_sheet', 'Total shareholders’ equity.'),
('Total_Assets', 'balance_sheet', 'Sum of all assets owned by the company.'),

('CFO', 'cash_flow', 'Cash flow from operating activities.'),
('Capex', 'cash_flow', 'Capital expenditures for long-term assets.'),
('Repayment_of_Debt', 'cash_flow', 'Payments made to reduce outstanding debt.'),
('Cash_Dividends', 'cash_flow', 'Dividends paid to shareholders in cash.');

-- select * from alphascope.metric_types

-- Report Types Lookup Table
CREATE TABLE IF NOT EXISTS alphascope.report_frequencies (
    report_frequency TEXT PRIMARY KEY,
    description TEXT
);

INSERT INTO alphascope.report_frequencies (report_frequency, description) VALUES
('Quarterly', 'Financial report covering a three-month period.'),
('Annual', 'Financial report covering a full year.'),
('Semi-annual', 'Financial report covering a six-month period.');

-- select * from alphascope.report_frequencies

-- Filings table
CREATE TABLE IF NOT EXISTS alphascope.filings (
    filing_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    filing_date DATE NOT NULL,
    filing_url TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (filing_date, filing_url)
);

select * from alphascope.filings;

-- Company_Filings Junction Table (modelling many to many relationship between company and filing)
CREATE TABLE IF NOT EXISTS alphascope.company_filings (
    company_filing_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    company_id UUID REFERENCES alphascope.company(company_id) ON DELETE CASCADE,
    report_frequency TEXT NOT NULL REFERENCES alphascope.report_frequencies(report_frequency) ON UPDATE CASCADE,
    filing_id UUID REFERENCES alphascope.filings(filing_id) ON DELETE CASCADE,
    UNIQUE (company_id, report_frequency, filing_id)
);

select * from alphascope.company_filings;

-- Financial Metrics Table
CREATE TABLE IF NOT EXISTS alphascope.financial_metrics (
    metric_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    company_filing_id UUID REFERENCES alphascope.company_filings(company_filing_id) ON DELETE CASCADE,
    metric_type TEXT NOT NULL REFERENCES alphascope.metric_types(metric_type) ON UPDATE CASCADE,
    period_of_report DATE NOT NULL,
    value NUMERIC NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (company_filing_id, metric_type, period_of_report)
);


-- select * from alphascope.financial_metrics 
-- delete from alphascope.financial_metrics;
-- Drop Table alphascope.financial_metrics CASCADE;
-- delete from alphascope.company_filings;
-- drop table alphascope.company_filings CASCADE;
-- delete from alphascope.filings;
-- drop table alphascope.filings CASCADE;
-- delete from alphascope.report_frequencies;
-- drop table alphascope.report_frequencies CASCADE;
-- delete from alphascope.metric_types;
-- drop table alphascope.metric_types CASCADE;
-- Drop Table alphascope.company
-- Drop Schema alphascope


CREATE TABLE IF NOT EXISTS alphascope.test_reported_metrics (
    metric_type TEXT REFERENCES alphascope.metric_types(metric_type) ON DELETE CASCADE,
    reported_metric JSONB NOT NULL,
	period_of_report TEXT NOT NULL,
	UNIQUE(metric_type, reported_metric),
	created_at TIMESTAMP DEFAULT NOW()
);

-- select * from alphascope.test_reported_metrics where metric_type = 'Profit_Before_Tax' order by period_of_report
-- delete from alphascope.test_reported_metrics where metric_type = 'Accounts_Payable' or metric_type = 'Revenue'
-- delete from alphascope.test_reported_metrics 
-- drop table alphascope.test_reported_metrics

-- select * from alphascope.test_reported_metrics WHERE jsonb_array_length(reported_metric) = 2;
-- select * from alphascope.financial_metrics where period_of_report = '2012-03-31'

-- select * from alphascope.test_reported_metrics where metric_type = 'Net_Profit_Bottom_Line' or metric_type = 'GAAP_Net_Profit'
