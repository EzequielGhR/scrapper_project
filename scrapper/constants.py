ACTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS actions (
	file_id TEXT PRIMARY KEY,
	date DATE,
	activity TEXT,
    action_id TEXT
);
"""
DOCUMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS documents (
	file_id TEXT PRIMARY KEY,
	name TEXT,
	url TEXT,
	date DATE,
	document_id TEXT
);
"""
SUMMARY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS summary (
	file_id TEXT PRIMARY KEY,
	url TEXT NOT NULL,
	date_received DATE,
	last_modified DATE,
	expiration DATE,
	reference TEXT,
	district TEXT,
	initiated_date DATE
);
"""
VOTE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vote (
    file_id TEXT PRIMARY KEY,
    meeting_date DATE,
    meeting_type TEXT,
    vote_action TEXT,
    vote_given TEXT
);
"""
MEMBERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS members (
    file_id TEXT PRIMARY KEY,
    member_name TEXT,
    cd INTEGER,
    vote TEXT
);
"""
ACTIONS_DOCUMENTS_SQL = """
CREATE TABLE IF NOT EXISTS actions_documents (
    action_id TEXT NOT NULL,
    document_id TEXT NOT NULL
);
"""