"""The shared archive favourites contract over ``management.favourites``."""
from __future__ import annotations

import base64
import json
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import exists, select, text
from sqlalchemy.exc import IntegrityError
from ultima_scraper_db_archive import UserModel
from ultima_scraper_db_archive.favourites_models import FavouriteModel, SITES, TARGETS


def validate_key(site: str, entity_type: str, entity_id: int | None = None) -> None:
    if site not in SITES or entity_type not in TARGETS or (entity_id is not None and not 0 < entity_id < 2**63):
        raise HTTPException(422, "Unsupported favourite entity key")


def ensure_active(session, site: str | None = None) -> None:
    if site is not None and site not in SITES:
        raise HTTPException(422, "Unsupported site")
    session.execute(text("SELECT pg_advisory_xact_lock_shared(hashtextextended('ultima:favourites:cutover',0))"))
    if not session.execute(text("SELECT to_regclass('management.favourites_migration')")).scalar():
        raise HTTPException(503, "Favourites schema has not been migrated")
    sites = [site] if site else list(SITES)
    active = session.execute(text("SELECT count(*) FROM management.favourites_migration WHERE site=ANY(:sites) AND active"), {"sites": sites}).scalar()
    if active != len(sites):
        raise HTTPException(503, "Favourites backfill/cutover is incomplete")


def user_expression(site: str):
    """SQL projection/filter over canonical favourites; the legacy column is never read."""
    return exists(select(1).where(FavouriteModel.site == site, FavouriteModel.entity_type == "user", FavouriteModel.entity_id == UserModel.id))


def user_states(session, site: str, users) -> set[int]:
    users = list(users)
    ensure_active(session, site)
    ids = [u.id for u in users]
    if not ids:
        return set()
    return set(session.execute(select(FavouriteModel.entity_id).where(FavouriteModel.site == site, FavouriteModel.entity_type == "user", FavouriteModel.entity_id.in_(ids))).scalars())


def user_state(session, site: str, user) -> bool:
    return user.id in user_states(session, site, [user])


def state(session, site: str, entity_type: str, entity_id: int) -> dict:
    validate_key(site, entity_type, entity_id)
    row = session.execute(text("SELECT id,created_at,created_at_source FROM management.favourites WHERE site=:site AND entity_type=:kind AND entity_id=:entity_id"), {"site": site, "kind": entity_type, "entity_id": entity_id}).mappings().first()
    return {"site": site, "entity_type": entity_type, "entity_id": entity_id,
            "favourited": row is not None, "favourite_id": row["id"] if row else None,
            "created_at": row["created_at"].isoformat() if row else None,
            "created_at_source": row["created_at_source"] if row else None}


def set_state(session, site: str, entity_type: str, entity_id: int, desired: bool, expected: bool | None = None) -> dict:
    validate_key(site, entity_type, entity_id)
    # Lock lasts until the caller commits. Both absent and present records use
    # the same key; duplicate retries cannot reset their creation timestamp.
    session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"), {"key": f"ultima:favourite:{site}:{entity_type}:{entity_id}"})
    previous = state(session, site, entity_type, entity_id)
    if expected is not None and previous["favourited"] != expected:
        raise HTTPException(409, {"message": "Favourite state changed", "state": previous})
    if desired and previous["favourited"]:
        return previous
    if desired:
        # A savepoint maps an FK race to 404 without corrupting a surrounding
        # legacy profile-edit transaction. Native constraints own integrity.
        try:
            with session.begin_nested():
                session.execute(text("INSERT INTO management.favourites(site,entity_type,entity_id) VALUES (:site,:kind,:entity_id) ON CONFLICT (site,entity_type,entity_id) DO NOTHING"), {"site": site, "kind": entity_type, "entity_id": entity_id})
        except IntegrityError as exc:
            if getattr(exc.orig, "sqlstate", None) == "23503":
                raise HTTPException(404, "Archived entity no longer exists") from exc
            raise
    else:
        session.execute(text("DELETE FROM management.favourites WHERE site=:site AND entity_type=:kind AND entity_id=:entity_id"), {"site": site, "kind": entity_type, "entity_id": entity_id})
    return state(session, site, entity_type, entity_id)


def set_user_state(session, site: str, user, desired: bool) -> None:
    ensure_active(session, site)
    set_state(session, site, "user", user.id, desired)


def batch_states(session, keys: list[dict]) -> list[dict]:
    if len(keys) > 200:
        raise HTTPException(422, "At most 200 favourite keys")
    for key in keys:
        validate_key(key["site"], key["entity_type"], key["entity_id"])
    if not keys:
        return []
    rows = session.execute(text("""SELECT f.* FROM management.favourites f JOIN
        jsonb_to_recordset(CAST(:keys AS jsonb)) AS k(site text,entity_type text,entity_id bigint)
        USING (site,entity_type,entity_id)"""), {"keys": json.dumps(keys)}).mappings().all()
    lookup = {(r["site"],r["entity_type"],r["entity_id"]): r for r in rows}
    result = []
    for key in keys:
        row = lookup.get((key["site"],key["entity_type"],key["entity_id"]))
        result.append({**key, "favourited": row is not None, "favourite_id": row["id"] if row else None,
                       "created_at": row["created_at"].isoformat() if row else None,
                       "created_at_source": row["created_at_source"] if row else None})
    return result


def filters(site: str | None, entity_type: str | None, search: str, advanced: dict | None = None) -> tuple[str, dict]:
    if site is not None and site not in SITES:
        raise HTTPException(422, "Unsupported site")
    if entity_type is not None and entity_type not in TARGETS:
        raise HTTPException(422, "Unsupported entity type")
    advanced = advanced or {}
    clauses, params = ["true"], dict(advanced)
    if site:
        clauses.append("f.site=:site"); params["site"] = site
    if entity_type:
        clauses.append("f.entity_type=:kind"); params["kind"] = entity_type
    if search or advanced:
        # OFFSET 0 keeps these indexed entity/author probes correlated instead
        # of allowing hashed scans of every user/content row in the archive.
        params["pattern"] = "%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        branches = []
        for s in ([site] if site else SITES):
            for kind in ([entity_type] if entity_type else TARGETS):
                table = TARGETS[kind]
                join = "" if kind == "user" else f"JOIN {s}.users u ON u.id=c.user_id"
                author = "c" if kind == "user" else "u"
                tests = ["c.id=f.entity_id"]
                if search:
                    content = "OR c.text ILIKE :pattern ESCAPE '\\'" if kind in {"post", "message"} else ""
                    tests.append(f"""(c.id::text ILIKE :pattern ESCAPE '\\'
                      OR {author}.username ILIKE :pattern ESCAPE '\\' OR i.name ILIKE :pattern ESCAPE '\\'
                      OR EXISTS (SELECT 1 FROM {s}.user_aliases a WHERE a.user_id={author}.id AND a.username ILIKE :pattern ESCAPE '\\' OFFSET 0) {content})""")
                if advanced.get("active_only"): tests.append(f"{author}.active IS TRUE")
                if advanced.get("performer"): tests.append(f"{author}.performer IS {'TRUE' if advanced['performer'] == 'include' else 'FALSE'}")
                if advanced.get("subscribed"):
                    negate = "NOT " if advanced["subscribed"] == "exclude" else ""
                    tests.append(f"{negate}EXISTS (SELECT 1 FROM {s}.subscriptions sub WHERE sub.user_id={author}.id AND sub.active IS TRUE OFFSET 0)")
                for name in ("sex", "gender", "profile_type", "sexual_content"):
                    values = advanced.get(name)
                    if not values: continue
                    choices = []
                    concrete = [value for value in values if value != -1]
                    if concrete:
                        params[name + "_values"] = concrete
                        choices.append(f"i.{name} = ANY(:{name}_values)")
                        if name in {"sex", "gender"}: choices.append(f"i.partner_{name} = ANY(:{name}_values)")
                    if -1 in values: choices.append(f"i.{name} IS NULL")
                    tests.append("(" + " OR ".join(choices) + ")")
                for name in ("price", "post_count", "image_count", "video_count"):
                    for bound, operator in (("min", ">="), ("max", "<=")):
                        key = f"{bound}_{name}"
                        if key in advanced: tests.append(f"coalesce(i.{name},0) {operator} :{key}")
                availability = advanced.get("availability")
                if availability:
                    if availability == "unavailable":
                        branches.append(f"(f.site='{s}' AND f.entity_type='{kind}' AND NOT EXISTS (SELECT 1 FROM {s}.{table} c WHERE c.id=f.entity_id OFFSET 0))" if len(advanced) == 1 and not search else "false")
                        continue
                    status = "CASE WHEN c.active IS NOT TRUE THEN 'inactive' ELSE 'available' END" if kind == "user" else (
                        "CASE " + ("WHEN c.deleted IS TRUE THEN 'deleted' " if kind in {"post", "message"} else "") +
                        "WHEN c.expires_at <= CURRENT_TIMESTAMP THEN 'expired' ELSE 'available' END")
                    tests.append(f"({status}) = :availability")
                branches.append(f"""(f.site='{s}' AND f.entity_type='{kind}' AND EXISTS (
                    SELECT 1 FROM {s}.{table} c {join} LEFT JOIN {s}.user_infos i ON i.user_id={author}.id
                    WHERE {' AND '.join(tests)} OFFSET 0))""")
        clauses.append("(" + " OR ".join(branches) + ")")
    return " AND ".join(clauses), params


def decode_cursor(cursor: str, signature: list) -> dict:
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        if payload["filters"] != signature:
            raise ValueError()
        at = datetime.fromisoformat(payload["created_at"])
        if at.tzinfo is None:
            raise ValueError()
        if type(payload["id"]) is not int or not 0 < payload["id"] < 2**63:
            raise ValueError()
        validate_key(payload["site"], payload["entity_type"])
        return {"cursor_at": at, "cursor_id": payload["id"], "cursor_site": payload["site"], "cursor_kind": payload["entity_type"]}
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise HTTPException(400, "Invalid favourites cursor") from exc


def list_favourites(session, site=None, entity_type=None, search="", order="desc", limit=50, cursor=None, advanced=None) -> dict:
    if order not in {"asc", "desc"} or not 1 <= limit <= 100:
        raise HTTPException(422, "Invalid pagination")
    search = search.strip()
    where, params = filters(site, entity_type, search, advanced)
    signature = [site,entity_type,search,order]
    if advanced: signature.append(advanced)
    if cursor:
        params.update(decode_cursor(cursor, signature))
        comparison = "<" if order == "desc" else ">"
        where += f" AND (f.created_at,f.id,f.site,f.entity_type) {comparison} (:cursor_at,CAST(:cursor_id AS bigint),:cursor_site,:cursor_kind)"
    params["limit"] = limit + 1
    rows = session.execute(text(f"SELECT f.* FROM management.favourites f WHERE {where} ORDER BY f.created_at {order},f.id {order},f.site {order},f.entity_type {order} LIMIT :limit"), params).mappings().all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    metadata = resolve_metadata(session, rows)
    items = []
    for row in rows:
        item = dict(row)
        item["created_at"] = item["created_at"].isoformat()
        entity = metadata.get((row["site"],row["entity_type"],row["entity_id"]))
        item["entity"] = entity
        item["availability"] = "unavailable" if entity is None else (
            "inactive" if row["entity_type"] == "user" and not entity["is_active"] else
            "deleted" if entity.get("deleted") else
            "expired" if entity.get("expires_at") and datetime.fromisoformat(entity["expires_at"]) <= datetime.now().astimezone() else "available")
        items.append(item)
    next_cursor = None
    if has_more:
        last = items[-1]
        value = {"filters": signature, **{k:last[k] for k in ("created_at","id","site","entity_type")}}
        next_cursor = base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
    return {"items": items, "has_more": has_more, "next_cursor": next_cursor}


def counts(session, site=None, entity_type=None, search="", advanced=None) -> dict:
    where, params = filters(site,entity_type,search.strip(),advanced)
    rows = session.execute(text(f"SELECT entity_type,count(*) AS count FROM management.favourites f WHERE {where} GROUP BY entity_type"), params).all()
    by_type = {kind:0 for kind in TARGETS}
    by_type.update(dict(rows))
    return {"total": sum(by_type.values()), "by_type": by_type}


def resolve_metadata(session, rows, *, favourite_users: bool = True) -> dict:
    """Three bounded SQL statements for any mix of sites/types/page size."""
    if not rows:
        return {}
    keys = [{"site":r["site"],"entity_type":r["entity_type"],"entity_id":r["entity_id"]} for r in rows]
    params = {"keys": json.dumps(keys)}
    cte = "WITH keys AS MATERIALIZED (SELECT * FROM jsonb_to_recordset(CAST(:keys AS jsonb)) AS k(site text,entity_type text,entity_id bigint)) "
    branches = []
    for site,kind in sorted({(r["site"],r["entity_type"]) for r in rows}):
        validate_key(site,kind)
        if kind == "user":
            favourite_sql = "true" if favourite_users else (
                f"EXISTS (SELECT 1 FROM management.favourites f WHERE f.site='{site}' AND f.entity_type='user' AND f.entity_id=u.id)")
            branches.append(f"""SELECT k.site,k.entity_type,k.entity_id,u.id AS user_id,
                jsonb_build_object('id',u.id,'user_id',u.id,'site',k.site,'username',u.username,
                  'name',i.name,'description',i.description,'sex',i.sex,'gender',i.gender,
                  'profile_type',i.profile_type,'sexual_content',i.sexual_content,'partner_sex',i.partner_sex,
                  'partner_gender',i.partner_gender,'ai_assisted',coalesce(i.ai_assisted,false),'verified',coalesce(i.verified,false),
                  'price',coalesce(i.price,0),'post_count',coalesce(i.post_count,0),'media_count',coalesce(i.media_count,0),
                  'image_count',coalesce(i.image_count,0),'video_count',coalesce(i.video_count,0),
                  'is_active',u.active,'favorite',{favourite_sql},'is_subscribed',false,'is_pending_order',false,
                  'avatar_url',pm.avatar_url,'header_url',pm.header_url,'downloaded_at',u.downloaded_at,'last_checked_at',u.last_checked_at) AS entity
                FROM keys k JOIN {site}.users u ON u.id=k.entity_id
                LEFT JOIN {site}.user_infos i ON i.user_id=u.id
                LEFT JOIN LATERAL (SELECT avatar_url,header_url FROM {site}.user_profile_media WHERE user_id=u.id ORDER BY created_at DESC,id DESC LIMIT 1) pm ON true
                WHERE k.site='{site}' AND k.entity_type='user'""")
        else:
            table = TARGETS[kind]
            txt,price,paid,deleted,archived,media_count = ("NULL","0","NULL","false","false","0") if kind == "story" else (
                "c.text","c.price","c.paid","c.deleted","c.archived" if kind == "post" else "false","c.media_count")
            branches.append(f"""SELECT k.site,k.entity_type,k.entity_id,c.user_id,
                jsonb_build_object('id',c.id,'user_id',c.user_id,'site',k.site,'username',u.username,'content_type',k.entity_type,
                  'text',{txt},'price',{price},'paid',{paid},'deleted',{deleted},'archived',{archived},
                  'media_count',{media_count},'expires_at',c.expires_at,'created_at',c.created_at,'favorite',true,'media','[]'::jsonb) AS entity
                FROM keys k JOIN {site}.{table} c ON c.id=k.entity_id JOIN {site}.users u ON u.id=c.user_id
                WHERE k.site='{site}' AND k.entity_type='{kind}'""")
    metadata_rows = session.execute(text(cte + " UNION ALL ".join(branches)),params).mappings().all()
    lookup = {(r["site"],r["entity_type"],r["entity_id"]):r["entity"] for r in metadata_rows}
    branches = []
    for site,kind in sorted({(r["site"],r["entity_type"]) for r in rows if r["entity_type"] != "user"}):
        branches.append(f"""SELECT k.site,k.entity_type,k.entity_id,m.* FROM keys k JOIN LATERAL (
            SELECT m.id,m.category,m.preview,m.duration FROM {site}.content_media_asso a
            JOIN {site}.media m ON m.id=a.media_id WHERE a.{kind}_id=k.entity_id ORDER BY m.id LIMIT 4
          ) m ON true WHERE k.site='{site}' AND k.entity_type='{kind}'""")
    if branches:
        from .content_previews import media_preview
        for media in session.execute(text(cte + " UNION ALL ".join(branches)),params).mappings():
            entity = lookup.get((media["site"],media["entity_type"],media["entity_id"]))
            if entity is not None:
                entity["media"].append(media_preview(media["site"],media["id"],media["category"],media["preview"],media["duration"]))
        for (site,kind,_),entity in lookup.items():
            if kind == "story": entity["media_count"] = len(entity["media"])
    return lookup
