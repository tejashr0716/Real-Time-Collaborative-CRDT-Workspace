-- SyncCanvas Decentralized Collaborative CRDT Schema
DROP TABLE IF EXISTS collaborator_sessions CASCADE;
DROP TABLE IF EXISTS crdt_operations CASCADE;
DROP TABLE IF EXISTS documents CASCADE;
DROP TABLE IF EXISTS workspaces CASCADE;

CREATE TABLE workspaces (
    workspace_id VARCHAR(32) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    owner_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE documents (
    doc_id VARCHAR(32) PRIMARY KEY,
    workspace_id VARCHAR(32) NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    title VARCHAR(128) NOT NULL,
    current_text TEXT DEFAULT '',
    version BIGINT DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE crdt_operations (
    op_id BIGSERIAL PRIMARY KEY,
    doc_id VARCHAR(32) NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    client_id VARCHAR(64) NOT NULL,
    lamport_ts BIGINT NOT NULL,
    op_type VARCHAR(16) NOT NULL, -- INSERT, DELETE
    char_val VARCHAR(4) NOT NULL,
    pos_fractional DOUBLE PRECISION NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE collaborator_sessions (
    session_id VARCHAR(64) PRIMARY KEY,
    doc_id VARCHAR(32) NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    client_id VARCHAR(64) NOT NULL,
    cursor_pos INT DEFAULT 0,
    color_hex VARCHAR(16) NOT NULL,
    last_seen TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_crdt_doc_pos ON crdt_operations(doc_id, pos_fractional ASC);
CREATE INDEX idx_crdt_lamport ON crdt_operations(doc_id, lamport_ts ASC);
CREATE INDEX idx_sessions_doc ON collaborator_sessions(doc_id);

-- Seed Initial Workspace and Document
INSERT INTO workspaces (workspace_id, name, owner_id) VALUES
('WS_ENG_CORP', 'Distributed Systems Core Group', 'ENG_LEAD_01');

INSERT INTO documents (doc_id, workspace_id, title, current_text, version) VALUES
('DOC_DISTRIBUTED_SPECS', 'WS_ENG_CORP', 'System Architecture Specifications v1', 'Hello SyncCanvas!', 1),
('DOC_ROADMAP', 'WS_ENG_CORP', 'Q4 2026 Engineering Goals', 'Scale Raft and CRDT mesh', 1);

-- Seed Initial Character Operations for 'Hello'
INSERT INTO crdt_operations (doc_id, client_id, lamport_ts, op_type, char_val, pos_fractional, is_deleted) VALUES
('DOC_DISTRIBUTED_SPECS', 'ALICE', 1, 'INSERT', 'H', 0.1, FALSE),
('DOC_DISTRIBUTED_SPECS', 'ALICE', 2, 'INSERT', 'e', 0.2, FALSE),
('DOC_DISTRIBUTED_SPECS', 'ALICE', 3, 'INSERT', 'l', 0.3, FALSE),
('DOC_DISTRIBUTED_SPECS', 'ALICE', 4, 'INSERT', 'l', 0.4, FALSE),
('DOC_DISTRIBUTED_SPECS', 'ALICE', 5, 'INSERT', 'o', 0.5, FALSE);

SELECT setval('crdt_operations_op_id_seq', (SELECT MAX(op_id) FROM crdt_operations));
