import os
import time
import random
import requests
import json
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------
# ENV CONFIG (production)
# ---------------------------
BATCHES_URL = os.getenv("BATCHES_URL") 
API_BASE = os.getenv("API_BASE")

if not BATCHES_URL:
    raise RuntimeError("Missing BATCHES_URL secret")
if not API_BASE:
    raise RuntimeError("Missing API_BASE secret")

_keywords_env = os.getenv("KEYWORDS")
if not _keywords_env:
    raise RuntimeError("Missing KEYWORDS secret")
KEYWORDS = [k.strip().lower() for k in _keywords_env.split(",") if k.strip()]

REFERER = os.getenv("REFERER")
ORIGIN = os.getenv("ORIGIN")
if not REFERER or not ORIGIN:
    raise RuntimeError("Missing REFERER/ORIGIN secret")

HEADERS = {
    "Referer": REFERER,
    "Origin": ORIGIN,
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

THREADS = int(os.getenv("THREADS", "3"))
MASTER_JSON_FILE = "master_courses.json"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "0.8"))
BACKOFF_CAP = float(os.getenv("BACKOFF_CAP", "10"))
REQUEST_JITTER = float(os.getenv("REQUEST_JITTER", "0.15"))

session = requests.Session()


# ---------------------------
# Helpers
# ---------------------------
def ensure_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return [x]
    return []


def safe_get(url):
    """
    Robust GET with retries.
    Returns: (json_data_or_None, ok_bool)
    """
    for attempt in range(1, MAX_RETRIES + 1):
        if REQUEST_JITTER > 0:
            time.sleep(random.uniform(0, REQUEST_JITTER))
        try:
            r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            code = r.status_code

            if code in (500, 502, 503, 504):
                wait = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
                if attempt < MAX_RETRIES:
                    time.sleep(wait)
                    continue
                return None, False

            if code != 200:
                return None, False

            try:
                return r.json(), True
            except Exception:
                return None, False

        except requests.RequestException:
            wait = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
            if attempt < MAX_RETRIES:
                time.sleep(wait)
                continue
            return None, False


def load_master_json():
    try:
        with open(MASTER_JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("master_courses.json is not a list")
            for c in data:
                if isinstance(c, dict) and c.get("course_id") is not None:
                    c["course_id"] = str(c["course_id"])
            return data
    except FileNotFoundError:
        return []
    except Exception as e:
        raise RuntimeError(f"Failed to read {MASTER_JSON_FILE}: {e}")


def save_master_json(data):
    tmp = MASTER_JSON_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, MASTER_JSON_FILE)


def is_blank(x):
    return x in (None, "", [], {})


def merge_scalar_fill_only(existing_dict, k, new_val):
    if is_blank(existing_dict.get(k)) and not is_blank(new_val):
        existing_dict[k] = new_val


def merge_dict_fill_only(existing, new):
    for k, v in (new or {}).items():
        if isinstance(v, dict):
            if not isinstance(existing.get(k), dict):
                existing[k] = {}
            merge_dict_fill_only(existing[k], v)
        elif isinstance(v, list):
            if not isinstance(existing.get(k), list):
                existing[k] = []
        else:
            merge_scalar_fill_only(existing, k, v)


def merge_list_by_key(existing_list, new_list, key="id"):
    if not isinstance(existing_list, list):
        existing_list = []
    if not isinstance(new_list, list):
        return existing_list

    index = {}
    for idx, item in enumerate(existing_list):
        if isinstance(item, dict):
            item_id = item.get(key)
            if item_id not in (None, ""):
                index[str(item_id)] = idx

    for item in new_list:
        if not isinstance(item, dict):
            if item not in existing_list:
                existing_list.append(item)
            continue

        item_id = item.get(key)
        if item_id in (None, ""):
            if item not in existing_list:
                existing_list.append(item)
            continue

        sid = str(item_id)
        if sid in index:
            merge_dict_fill_only(existing_list[index[sid]], item)
        else:
            existing_list.append(item)
            index[sid] = len(existing_list) - 1

    return existing_list


def fingerprint(item):
    if not isinstance(item, dict):
        return str(item)
    for k in ("id", "notice_id", "_id", "update_id"):
        v = item.get(k)
        if v not in (None, "", [], {}):
            return f"{k}:{v}"
    ts = item.get("published_at") or item.get("publishedAt") or item.get("created_at") or item.get("createdAt") or ""
    body = item.get("content") or item.get("message") or item.get("description") or ""
    return f"{ts}|{hash(body)}"


def merge_list_by_fingerprint(existing_list, new_list):
    if not isinstance(existing_list, list):
        existing_list = []
    if not isinstance(new_list, list):
        return existing_list

    idx = {fingerprint(item): i for i, item in enumerate(existing_list)}

    for item in new_list:
        fp = fingerprint(item)
        if fp in idx:
            if isinstance(existing_list[idx[fp]], dict) and isinstance(item, dict):
                merge_dict_fill_only(existing_list[idx[fp]], item)
        else:
            existing_list.append(item)
            idx[fp] = len(existing_list) - 1

    return existing_list


def merge_course(existing_course, new_course):
    merge_dict_fill_only(existing_course, {k: v for k, v in new_course.items() if k != "_ok"})

    ok = new_course.get("_ok") or {}
    classroom_ok = bool(ok.get("classroom"))
    updates_ok = bool(ok.get("updates"))

    if classroom_ok:
        existing_course["classroom"] = merge_list_by_key(
            existing_course.get("classroom", []),
            new_course.get("classroom", []),
            key="id"
        )

        existing_lessons = existing_course.get("lessons", [])
        if not isinstance(existing_lessons, list):
            existing_lessons = []

        new_lessons = new_course.get("lessons", [])
        if not isinstance(new_lessons, list):
            new_lessons = []

        lesson_idx = {}
        for i, l in enumerate(existing_lessons):
            if isinstance(l, dict):
                lid = l.get("lesson_id")
                if lid not in (None, ""):
                    lesson_idx[str(lid)] = i

        for lesson in new_lessons:
            if not isinstance(lesson, dict):
                if lesson not in existing_lessons:
                    existing_lessons.append(lesson)
                continue

            lid = lesson.get("lesson_id")
            if lid in (None, ""):
                if lesson not in existing_lessons:
                    existing_lessons.append(lesson)
                continue

            lid = str(lid)
            if lid in lesson_idx:
                target = existing_lessons[lesson_idx[lid]]
                merge_dict_fill_only(target, lesson)

                target["videos"] = merge_list_by_key(target.get("videos", []), lesson.get("videos", []), key="id")
                target["notes"] = merge_list_by_key(target.get("notes", []), lesson.get("notes", []), key="id")

                target["video_count"] = len(target.get("videos", [])) if isinstance(target.get("videos"), list) else 0
                target["note_count"] = len(target.get("notes", [])) if isinstance(target.get("notes"), list) else 0
                target["lesson_count"] = target["video_count"] + target["note_count"]
            else:
                lesson["video_count"] = len(lesson.get("videos", [])) if isinstance(lesson.get("videos"), list) else 0
                lesson["note_count"] = len(lesson.get("notes", [])) if isinstance(lesson.get("notes"), list) else 0
                lesson["lesson_count"] = lesson["video_count"] + lesson["note_count"]

                existing_lessons.append(lesson)
                lesson_idx[lid] = len(existing_lessons) - 1

        existing_course["lessons"] = existing_lessons
        existing_course["lesson_count"] = sum(
            (l.get("lesson_count") or 0) for l in existing_lessons if isinstance(l, dict)
        )

    if updates_ok:
        existing_course["announcements"] = merge_list_by_fingerprint(
            existing_course.get("announcements", []),
            new_course.get("announcements", [])
        )


def upsert_course(master_json, course_id, new_course):
    course_id = str(course_id)
    for c in master_json:
        if str(c.get("course_id")) == course_id:
            merge_course(c, new_course)
            return
    master_json.append(new_course)


# ---------------------------
# Keyword filtering
# ---------------------------
keyword_patterns = []
for kw in KEYWORDS:
    # Supports patterns like: "72 bpsc" -> match 72 bpsc, 72nd bpsc, 72xx bpsc
    m = re.match(r"(\d+)\s+(.*)", kw)
    if m:
        n, w = m.groups()
        keyword_patterns.append(re.compile(rf"\b{n}(?:st|nd|rd|th)?\d*\s*{re.escape(w)}\b", re.I))
    else:
        keyword_patterns.append(re.compile(rf"\b{re.escape(kw)}\b", re.I))


# ---------------------------
# Fetch course details (new API)
# ---------------------------
def fetch_course_details(course, rank, total):
    cid = course.get("id")
    cname = course.get("title") or ""
    print(f"[{rank}/{total}] START course_id={cid}  {cname}", flush=True)

    out = {
        "ranking": rank,
        "course_id": str(cid) if cid is not None else None,
        "course_name": cname,
        "category_id": course.get("category_id"),
        "start_at": course.get("start_at"),
        "end_at": course.get("end_at"),
        "image_large": course.get("image_large"),
        "image_thumb": course.get("image_thumb"),

        "classroom": [],
        "lessons": [],
        "announcements": [],
        "lesson_count": 0,

        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "_ok": {"classroom": False, "updates": False},
    }

    # classroom endpoint returns {"classroom":[...]}
    classroom_data, classroom_ok = safe_get(f"{API_BASE}/classroom/{cid}")
    out["_ok"]["classroom"] = bool(classroom_ok)

    if classroom_ok and isinstance(classroom_data, dict):
        classroom = ensure_list(classroom_data.get("classroom"))
        out["classroom"] = classroom

        for cls in classroom:
            if not isinstance(cls, dict):
                continue
            lesson_id = cls.get("id")
            if not lesson_id:
                continue

            lesson_data, lesson_ok = safe_get(f"{API_BASE}/lesson/{lesson_id}")
            if not lesson_ok or not isinstance(lesson_data, dict):
                continue

            # videos
            videos_out = []
            for v in ensure_list(lesson_data.get("videos")):
                if not isinstance(v, dict):
                    continue
                vid = v.get("id")

                vd_data, vd_ok = (None, False)
                if vid:
                    vd_data, vd_ok = safe_get(f"{API_BASE}/video/{vid}")
                vd = vd_data if isinstance(vd_data, dict) else {}

                videos_out.append({
                    "id": str(vid) if vid is not None else None,
                    "name": v.get("name", ""),
                    "published_at": v.get("published_at", ""),
                    "thumb": v.get("thumb", ""),
                    "type": v.get("type", ""),
                    "pdfs": v.get("pdfs") if v.get("pdfs") is not None else [],
                    "m3u": vd.get("video_url", "") if vd_ok else "",
                    "yt": vd.get("hd_video_url", "") if vd_ok else "",
                    "video_pdfs": vd.get("pdfs", None) if vd_ok else None,
                })

            # notes/uploads/tests (also resolved through /video/{id})
            notes_out = []
            for n in ensure_list(lesson_data.get("notes")):
                if not isinstance(n, dict):
                    continue
                nid = n.get("id")

                nd_data, nd_ok = (None, False)
                if nid:
                    nd_data, nd_ok = safe_get(f"{API_BASE}/video/{nid}")
                nd = nd_data if isinstance(nd_data, dict) else {}

                notes_out.append({
                    "id": str(nid) if nid is not None else None,
                    "name": n.get("name", ""),
                    "published_at": n.get("published_at", ""),
                    "thumb": n.get("thumb", ""),
                    "type": n.get("type", "pdf"),
                    "resolved_url": nd.get("video_url", "") if nd_ok else "",
                    "yt": nd.get("hd_video_url", "") if nd_ok else "",
                    "pdfs": nd.get("pdfs", None) if nd_ok else None,
                })

            out["lessons"].append({
                "lesson_id": str(lesson_data.get("id")) if lesson_data.get("id") is not None else str(lesson_id),
                "lesson_name": lesson_data.get("name", "") or cls.get("name", ""),
                "teacher": lesson_data.get("teacher", {}),
                "video_count": len(videos_out),
                "note_count": len(notes_out),
                "lesson_count": len(videos_out) + len(notes_out),
                "videos": videos_out,
                "notes": notes_out,
            })

    # updates endpoint returns list
    updates_data, updates_ok = safe_get(f"{API_BASE}/updates/{cid}")
    out["_ok"]["updates"] = bool(updates_ok)
    if updates_ok:
        out["announcements"] = ensure_list(updates_data)

    out["lesson_count"] = sum((l.get("lesson_count") or 0) for l in out.get("lessons", []) if isinstance(l, dict))

    print(
        f"[{rank}/{total}] DONE  course_id={cid} lessons={len(out.get('lessons', []))} "
        f"announcements={len(out.get('announcements', []))} lesson_count={out.get('lesson_count', 0)}",
        flush=True
    )
    return out


def main():
    print("Fetching batches list...", flush=True)
    batches_data, batches_ok = safe_get(BATCHES_URL)
    batches = ensure_list(batches_data)

    if not batches_ok or not batches:
        with open("SKIP_PUSH", "w", encoding="utf-8") as f:
            f.write("1")
        print("batches_fetch_failed_skip", flush=True)
        raise SystemExit(0)

    # Filter courses
    filtered_courses = []
    for item in batches:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or "").lower()
        if not any(p.search(title) for p in keyword_patterns):
            continue
        filtered_courses.append(item)

    print(f"Matched courses: {len(filtered_courses)}", flush=True)
    if not filtered_courses:
        # not an outage: just no matches
        save_master_json(load_master_json())
        print("no_matches_done", flush=True)
        return

    master_json = load_master_json()
    results = []

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = [
            ex.submit(fetch_course_details, c, i + 1, len(filtered_courses))
            for i, c in enumerate(filtered_courses)
        ]
        for f in as_completed(futures):
            results.append(f.result())

    # Global outage logic (skip saving/pushing if EVERYTHING failed)
    any_ok_anywhere = any(
        r.get("_ok", {}).get("classroom") or r.get("_ok", {}).get("updates")
        for r in results
    )
    if not any_ok_anywhere:
        with open("SKIP_PUSH", "w", encoding="utf-8") as f:
            f.write("1")
        print("global_outage_skip_save", flush=True)
        raise SystemExit(0)

    # Merge results
    for r in results:
        cid = r.get("course_id")
        if cid:
            upsert_course(master_json, cid, r)

    # Cleanup internal keys
    for c in master_json:
        if isinstance(c, dict):
            c.pop("_ok", None)

    save_master_json(master_json)
    print("done", flush=True)


if __name__ == "__main__":
    main()
