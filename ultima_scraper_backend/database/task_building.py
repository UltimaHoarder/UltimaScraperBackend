"""Resolve ordinary job-wide builder rules inside the admission transaction."""

from sqlalchemy import text

from ..job_system.storage import TaskPayloadBudgetExceeded


def normalize_built_tasks(session, manifest_id: str, *, input_max_bytes: int, max_bytes: int):
    """Match the list builder using indexed, transaction-local task projections.

    Python never loads the graph. PostgreSQL uses its configured temp storage,
    work_mem and temp_buffers. The dependency array for any one row is capped
    before aggregation and the finished definition must fit its input budgets.
    """
    params = {"manifest": manifest_id, "dependency_limit": min(input_max_bytes, max_bytes) // 2 + 1}
    if session.execute(text("""
        SELECT 1 FROM management.mps_task_definitions
        WHERE manifest_id=:manifest AND (
            task_json::jsonb->>'task_type' IN ('scrape','download')
            OR cardinality(dependency_ids)>0
        ) LIMIT 1
    """), params).first() is None:
        return
    session.execute(text("""
        CREATE TEMP TABLE usr_ordinary_build_nodes ON COMMIT DROP AS
        SELECT ordinal, task_id, lower(task_json::jsonb->>'site_name') AS site,
               (task_json::jsonb->>'user_id')::bigint AS user_id,
               (task_json::jsonb->>'auth_id')::bigint AS auth_id,
               task_json::jsonb->>'content_type' AS content_type,
               task_json::jsonb->>'task_type' AS task_type
        FROM management.mps_task_definitions WHERE manifest_id=:manifest
    """), params)
    session.execute(text("CREATE UNIQUE INDEX ON usr_ordinary_build_nodes(task_id)"))
    session.execute(text("""
        CREATE INDEX ON usr_ordinary_build_nodes(site, user_id, ordinal DESC)
        INCLUDE (auth_id, content_type) WHERE task_type='scrape'
    """))
    session.execute(text("""
        CREATE INDEX ON usr_ordinary_build_nodes(site, auth_id)
        WHERE task_type='scrape'
    """))
    session.execute(text("ANALYZE usr_ordinary_build_nodes"))
    session.execute(text("""
        UPDATE usr_ordinary_build_nodes node SET auth_id=COALESCE(
            (SELECT CASE WHEN min(s.auth_id)=max(s.auth_id) THEN min(s.auth_id) END
             FROM usr_ordinary_build_nodes s
             WHERE s.task_type='scrape' AND s.site=node.site AND s.user_id=node.user_id
               AND s.content_type IS NOT DISTINCT FROM node.content_type),
            (SELECT CASE WHEN min(s.auth_id)=max(s.auth_id) THEN min(s.auth_id) END
             FROM usr_ordinary_build_nodes s
             WHERE s.task_type='scrape' AND s.site=node.site AND s.user_id=node.user_id)
        ) WHERE node.task_type='download' AND node.auth_id IS NULL
    """))
    # Preserve valid explicit dependency order, and the existing builder's rule
    # that a nonzero user_id selects the last matching scrape in input order.
    # A JSON string needs at least two bytes before separators; the LIMIT is
    # strictly larger than the number that can fit even with empty IDs. A
    # truncated array therefore necessarily fails the byte check below.
    session.execute(text("""
        CREATE TEMP TABLE usr_ordinary_build_resolved ON COMMIT DROP AS
        SELECT definition.ordinal, definition.task_id,
               resolved.ids AS dependency_ids,
               (jsonb_set(jsonb_set(definition.task_json::jsonb, '{auth_id}',
                    COALESCE(to_jsonb(node.auth_id), 'null'::jsonb)),
                    '{depends_on}', to_jsonb(resolved.ids)))::text AS task_json
        FROM management.mps_task_definitions definition
        JOIN usr_ordinary_build_nodes node USING(task_id)
        CROSS JOIN LATERAL (
            SELECT ARRAY(
                SELECT dependency.id FROM unnest(definition.dependency_ids)
                    WITH ORDINALITY AS dependency(id, position)
                WHERE EXISTS (SELECT 1 FROM usr_ordinary_build_nodes present
                              WHERE present.task_id=dependency.id OFFSET 0)
                ORDER BY dependency.position
            ) AS ids
        ) pruned
        LEFT JOIN LATERAL (
            SELECT scrape.task_id FROM usr_ordinary_build_nodes scrape
            WHERE node.task_type='download' AND node.user_id<>0
              AND scrape.task_type='scrape' AND scrape.site=node.site
              AND scrape.user_id=node.user_id
            ORDER BY scrape.ordinal DESC LIMIT 1
        ) user_match ON TRUE
        CROSS JOIN LATERAL (
            SELECT ARRAY(
                SELECT id FROM (
                    SELECT unnest(pruned.ids) AS id
                    UNION
                    SELECT user_match.task_id WHERE user_match.task_id IS NOT NULL
                    UNION
                    SELECT scrape.task_id FROM usr_ordinary_build_nodes scrape
                    WHERE node.task_type='download' AND user_match.task_id IS NULL
                      AND scrape.task_type='scrape' AND scrape.site=node.site
                      AND (scrape.auth_id IS NOT DISTINCT FROM node.auth_id OR scrape.auth_id IS NULL)
                ) candidates ORDER BY id COLLATE "C" LIMIT :dependency_limit
            ) AS ids
        ) inferred
        CROSS JOIN LATERAL (
            SELECT CASE WHEN node.task_type='download'
                             AND cardinality(inferred.ids)<>cardinality(pruned.ids)
                        THEN inferred.ids ELSE pruned.ids END AS ids
        ) resolved
        WHERE definition.manifest_id=:manifest
    """), params)
    size_sql = """octet_length(task_json) + octet_length(:manifest)
        + octet_length(ordinal::text) + octet_length(task_id)
        + octet_length(to_jsonb(dependency_ids)::text) + octet_length('pending')"""
    oversized = session.execute(text(f"""
        SELECT task_id, octet_length(task_json), {size_sql} AS insert_bytes
        FROM usr_ordinary_build_resolved
        WHERE octet_length(task_json)>:input_max_bytes OR {size_sql}>:max_bytes
        ORDER BY ordinal LIMIT 1
    """), {**params, "input_max_bytes": input_max_bytes, "max_bytes": max_bytes}).first()
    if oversized:
        task_id, read_bytes, insert_bytes = oversized
        if read_bytes > input_max_bytes:
            raise TaskPayloadBudgetExceeded(task_id, read_bytes, input_max_bytes, phase="SQL input read")
        raise TaskPayloadBudgetExceeded(task_id, insert_bytes, max_bytes, phase="SQL input insert")
    session.execute(text("""
        UPDATE management.mps_task_definitions definition
        SET task_json=resolved.task_json, dependency_ids=resolved.dependency_ids
        FROM usr_ordinary_build_resolved resolved
        WHERE definition.manifest_id=:manifest AND definition.ordinal=resolved.ordinal
          AND definition.task_json<>resolved.task_json
    """), params)
    session.execute(text("DROP TABLE usr_ordinary_build_resolved, usr_ordinary_build_nodes"))
