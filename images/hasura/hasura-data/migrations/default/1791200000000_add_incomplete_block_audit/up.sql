CREATE TABLE IF NOT EXISTS incomplete_block_audit (
  node_internal_id integer NOT NULL,
  audited_through_block_internal_id bigint NOT NULL,
  completed_at timestamp without time zone DEFAULT now() NOT NULL,
  CONSTRAINT incomplete_block_audit_pkey PRIMARY KEY (node_internal_id),
  CONSTRAINT incomplete_block_audit_node_internal_id_fkey FOREIGN KEY (node_internal_id) REFERENCES node(internal_id) ON UPDATE RESTRICT ON DELETE CASCADE
);
COMMENT ON TABLE incomplete_block_audit IS 'Internal agent bookkeeping for the incomplete block repair scan (https://github.com/bitauth/chaingraph/issues/74). Each row records that every block accepted by the node with internal_id at or below audited_through_block_internal_id has been audited, so the agent skips those blocks on later startups. Delete rows to force a full re-audit.';
COMMENT ON COLUMN incomplete_block_audit.node_internal_id IS 'The internal_id of the node whose accepted blocks were audited.';
COMMENT ON COLUMN incomplete_block_audit.audited_through_block_internal_id IS 'The highest block internal_id which existed when the completed audit began.';
COMMENT ON COLUMN incomplete_block_audit.completed_at IS 'The time at which the audit completed.';
