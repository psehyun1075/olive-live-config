#!/usr/bin/env python3
import os, re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from google.oauth2 import service_account
from googleapiclient.discovery import build

KST = ZoneInfo("Asia/Seoul")

CALENDAR_ID = os.environ["GCAL_CALENDAR_ID"]
SA_JSON_PATH = os.environ["GCAL_SA_JSON_PATH"]

# 프리워밍: 이벤트 시작보다 몇 분 먼저 스케일 업할지
PREWARM_MINUTES = int(os.getenv("PREWARM_MINUTES", "10"))
# 이벤트 끝난 뒤 몇 분 더 유지할지(스파이크 잔여 처리용)
POSTWARM_MINUTES = int(os.getenv("POSTWARM_MINUTES", "10"))

# 5단계 레벨(과제 시나리오용)
# - order-api는 최대 20까지
# - worker는 최대 60까지
LEVELS = {
    "L0": {"api": 2,  "worker": 1,  "overprov": 0},
    "L1": {"api": 4,  "worker": 6,  "overprov": 1},
    "L2": {"api": 8,  "worker": 18, "overprov": 2},
    "L3": {"api": 12, "worker": 30, "overprov": 3},
    "L4": {"api": 18, "worker": 45, "overprov": 4},
}

ORDER_API_YAML = "envs/dev/keda/order-api-scaleobject.yaml"
WORKER_YAML    = "envs/dev/keda/order-worker-scaleobject.yaml"

# 캘린더 이벤트 제목: "SCALE:L0" ~ "SCALE:L4"
TITLE_RE = re.compile(r"SCALE:(L[0-4])\b", re.IGNORECASE)

def iso_now():
    return datetime.now(tz=KST)

def to_cron(dt: datetime) -> str:
    # KEDA cron format: "m h d M *"
    return f"{dt.minute} {dt.hour} {dt.day} {dt.month} *"

def level_rank(lvl: str) -> int:
    # "L0" -> 0, "L4" -> 4
    return int(lvl[1:])

def read_events(time_min: datetime, time_max: datetime):
    creds = service_account.Credentials.from_service_account_file(
        SA_JSON_PATH,
        scopes=["https://www.googleapis.com/auth/calendar.readonly"],
    )
    svc = build("calendar", "v3", credentials=creds, cache_discovery=False)

    events = []
    page_token = None
    while True:
        resp = svc.events().list(
            calendarId=CALENDAR_ID,
            timeMin=time_min.astimezone(timezone.utc).isoformat(),
            timeMax=time_max.astimezone(timezone.utc).isoformat(),
            singleEvents=True,
            orderBy="startTime",
            pageToken=page_token,
        ).execute()

        for e in resp.get("items", []):
            summary = (e.get("summary", "") or "").strip()
            m = TITLE_RE.search(summary)
            if not m:
                continue
            level = m.group(1).upper()

            start = e["start"].get("dateTime")
            end   = e["end"].get("dateTime")
            if not start or not end:
                continue

            sdt = datetime.fromisoformat(start.replace("Z","+00:00")).astimezone(KST)
            edt = datetime.fromisoformat(end.replace("Z","+00:00")).astimezone(KST)
            if edt <= sdt:
                continue

            # 프리워밍/포스트워밍 적용
            sdt = sdt - timedelta(minutes=PREWARM_MINUTES)
            edt = edt + timedelta(minutes=POSTWARM_MINUTES)

            events.append((sdt, edt, level, summary))

        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return events

def build_time_slices(events, horizon_minutes=24*60):
    """
    겹치는 이벤트가 있으면 같은 구간에서 '가장 높은 레벨'만 적용.
    """
    now = iso_now()
    end = now + timedelta(minutes=horizon_minutes)

    points = {now, end}
    for s,e,_,_ in events:
        points.add(max(s, now))
        points.add(min(e, end))

    pts = sorted(points)
    slices = []
    for i in range(len(pts)-1):
        a, b = pts[i], pts[i+1]
        if b <= a:
            continue

        active_levels = []
        for s,e,lvl,_ in events:
            if s < b and e > a:
                active_levels.append(lvl)

        if not active_levels:
            continue

        top = max(active_levels, key=level_rank)
        slices.append((a, b, top))
    return slices

def make_cron_triggers(slices, target: str, default_desired: int):
    out = []
    for a,b,lvl in slices:
        # start/end 동일 금지 + 최소 1분 차이 필요
        if (b - a) < timedelta(minutes=1):
            continue
        desired = LEVELS[lvl][target]
        out.append(
f"""    - type: cron
      metadata:
        timezone: "Asia/Seoul"
        start: "{to_cron(a)}"
        end:   "{to_cron(b)}"
        desiredReplicas: "{desired}"
"""
        )

    if not out:
        out.append(
f"""    - type: cron
      metadata:
        timezone: "Asia/Seoul"
        start: "0 0 1 1 *"
        end:   "1 0 1 1 *"
        desiredReplicas: "{default_desired}"
"""
        )
    return "".join(out)

def replace_block(text: str, new_block: str):
    begin = "# BEGIN: calendar-managed-cron"
    end   = "# END: calendar-managed-cron"
    if begin not in text or end not in text:
        raise RuntimeError("marker not found in yaml (BEGIN/END)")

    pre, rest = text.split(begin, 1)
    _, post = rest.split(end, 1)

    # 들여쓰기 안정적으로 유지(마커 라인 다음부터 cron 리스트가 오도록)
    return pre + begin + "\n" + new_block.rstrip() + "\n    " + end + post

def update_yaml(path: str, cron_block: str):
    with open(path, "r", encoding="utf-8") as f:
        t = f.read()
    t2 = replace_block(t, cron_block)
    if t2 != t:
        with open(path, "w", encoding="utf-8") as f:
            f.write(t2)
        return True
    return False

def main():
    now = iso_now()
    horizon = now + timedelta(hours=24)

    events = read_events(now - timedelta(minutes=5), horizon)
    slices = build_time_slices(events, horizon_minutes=24*60)

    api_cron = make_cron_triggers(slices, "api", default_desired=2)
    worker_cron = make_cron_triggers(slices, "worker", default_desired=1)

    changed1 = update_yaml(ORDER_API_YAML, api_cron)
    changed2 = update_yaml(WORKER_YAML, worker_cron)

    print("updated:", ORDER_API_YAML, "changed=" + str(changed1))
    print("updated:", WORKER_YAML, "changed=" + str(changed2))
    print(f"PREWARM_MINUTES={PREWARM_MINUTES}, POSTWARM_MINUTES={POSTWARM_MINUTES}")
    print("events:")
    for s,e,lvl,summary in events:
        print(" -", lvl, summary, s, "~", e)

if __name__ == "__main__":
    main()
