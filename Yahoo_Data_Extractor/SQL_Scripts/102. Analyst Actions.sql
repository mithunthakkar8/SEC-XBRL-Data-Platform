-- Analyst Actions Table
CREATE TABLE IF NOT EXISTS alphascope.analyst_actions (
    analyst_action_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    company_id UUID REFERENCES alphascope.company(company_id) ON DELETE CASCADE,
    firm TEXT NOT NULL, 
    to_grade TEXT, 
    from_grade TEXT,
    action TEXT NOT NULL, 
    action_date DATE NOT NULL, 
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (company_id, firm, action_date) 
);