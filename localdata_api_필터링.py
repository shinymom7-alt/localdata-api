#!/usr/bin/env python3
"""
공공데이터포털 지방행정 인허가정보 API 통합 추출 스크립트 v9.0
대상: 인천 검단구 전용

【사용법】
python localdata_api_필터링.py            ← 최근 7일 자동 적용
python localdata_api_필터링.py --start 20260701 --end 20260716
python localdata_api_필터링.py --debug    ← 첫 번째 업종 응답 원문 출력
"""

import requests
import pandas as pd
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import argparse
import os
import sys

# =====================================================================
# ★ 설정 부분 - 여기만 수정하세요 ★
# =====================================================================

API_LIST = [
    {
        "name": "병원",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/hospitals",
    },
    {
        "name": "약국",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/pharmacies",
    },
    {
        "name": "안경업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/optical_shops",
    },
    {
        "name": "동물미용업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/pet_grooming",
    },
    {
        "name": "동물병원",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/animal_hospitals",
    },
    {
        "name": "미용업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/beauty_salons",
    },
    {
        "name": "이용업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/barber_shops",
    },
    {
        "name": "세탁업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/laundries",
    },
    {
        "name": "종합체육시설업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/comprehensive_sports_facilities",
    },
    {
        "name": "체력단련장업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/fitness_centers",
    },
    {
        "name": "체육도장업",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/martial_arts_dojo",
    },
    {
        "name": "일반음식점",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/general_restaurants",
    },
    {
        "name": "휴게음식점",
        "api_key": "c2543a063d5f7373a685b89891e5846878960c416d2a14f5cd3ae0eda4c20779",
        "endpoint": "https://apis.data.go.kr/1741000/rest_cafes",
    },
]

TARGET_SIGUN_CD = "3565000"  # 인천 검단구

# 제외할 영업상태
EXCLUDE_STATUSES = [
    '폐업', '폐업처리', '폐쇄', '영업정지', '폐지', '휴업처리', '지정취소',
]

OUTPUT_DIR = "./"
NUM_OF_ROWS = 100

# =====================================================================
# 필드명 매핑
# =====================================================================

FIELD_MAP = {
    "LCPMT_YMD":    "인허가일자",
    "DTL_SALS_STTS_NM": "상세영업상태명",
    "TELNO":        "소재지전화",
    "LOTNO_ADDR":   "소재지전체주소",
    "ROAD_NM_ADDR": "도로명전체주소",
    "BPLC_NM":      "사업장명",
    "SALS_STTS_NM": "영업상태명",
    "MNG_NO":       "관리번호",
}

OUTPUT_COLUMNS = [
    "개방서비스명",
    "인허가일자",
    "상세영업상태명",
    "소재지전화",
    "소재지전체주소",
    "도로명전체주소",
    "사업장명",
]

# =====================================================================
# 함수
# =====================================================================

def get_date_range():
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=6)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

def to_datetime_str(date_str):
    return date_str + "000000"

def fetch_page(api_info, start_datetime, page_no, num_of_rows=NUM_OF_ROWS):
    url = api_info["endpoint"].rstrip("/") + "/info"
    params = {
        "serviceKey": api_info["api_key"],
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "cond[DAT_UPDT_PNT::GTE]": start_datetime,
        "cond[OPN_ATMY_GRP_CD::EQ]": TARGET_SIGUN_CD,
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        try:
            return "json", resp.json()
        except Exception:
            return "xml", resp.text
    except requests.exceptions.RequestException as e:
        print(f"  ✗ 요청 오류 (페이지 {page_no}): {e}")
        return None, None

def extract_items_from_json(data):
    total_count = 0
    item_list = []

    if not isinstance(data, dict):
        return item_list, total_count

    node = data
    for key in ["response", "Response"]:
        if key in node:
            node = node[key]
            break
    for key in ["body", "Body"]:
        if key in node:
            node = node[key]
            break
    for key in ["totalCount", "TotalCount", "total_count"]:
        if key in node:
            try:
                total_count = int(node[key])
            except Exception:
                pass
            break

    items_node = node.get("items") or node.get("Items") or node.get("data") or {}
    if isinstance(items_node, list):
        item_list = items_node
    elif isinstance(items_node, dict):
        raw = items_node.get("item") or items_node.get("Item") or []
        item_list = [raw] if isinstance(raw, dict) else (raw or [])
    elif isinstance(node, list):
        item_list = node

    return item_list, total_count

def parse_response(fmt, data, api_name):
    records = []
    total_count = 0

    if fmt == "json":
        item_list, total_count = extract_items_from_json(data)
        for item in (item_list or []):
            record = {"_api": api_name}
            record.update({k: (str(v) if v is not None else "") for k, v in item.items()})
            records.append(record)

    elif fmt == "xml":
        try:
            root = ET.fromstring(data)
            for key in ["totalCount", "total_count"]:
                el = root.find(f".//{key}")
                if el is not None:
                    try:
                        total_count = int(el.text or 0)
                    except Exception:
                        pass
                    break
            for row in root.findall(".//item") + root.findall(".//row"):
                record = {"_api": api_name}
                for child in row:
                    record[child.tag] = child.text.strip() if child.text else ""
                if len(record) > 1:
                    records.append(record)
        except ET.ParseError as e:
            print(f"  ✗ XML 파싱 오류: {e}")

    return records, total_count

def fetch_all_for_api(api_info, start_datetime, debug=False):
    name = api_info["name"]
    print(f"\n  ▶ [{name}] 수집 시작")

    all_records = []
    page_no = 1
    total_count = None
    num_rows = 3 if debug else NUM_OF_ROWS

    while True:
        fmt, data = fetch_page(api_info, start_datetime, page_no, num_rows)

        if data is None:
            print(f"  재시도 중...")
            time.sleep(3)
            fmt, data = fetch_page(api_info, start_datetime, page_no, num_rows)
            if data is None:
                print(f"  재시도 실패 → [{name}] 수집 중단")
                break

        if debug:
            print(f"\n  ===== [{name}] 실제 응답 원문 (디버그) =====")
            if fmt == "json":
                print(json.dumps(data, ensure_ascii=False, indent=2)[:3000])
            else:
                print(str(data)[:3000])
            print("  ===========================================\n")
            break

        records, total = parse_response(fmt, data, name)
        if total_count is None:
            total_count = total
            total_pages = max(1, -(-total_count // NUM_OF_ROWS))
            print(f"  전체 {total_count:,}건 ({total_pages}페이지)")

        if not records:
            break

        all_records.extend(records)
        print(f"  페이지 {page_no} 완료 → 누적 {len(all_records):,}건")

        if len(all_records) >= total_count:
            break

        page_no += 1
        time.sleep(0.3)

    if not debug:
        print(f"  ◀ [{name}] 완료: {len(all_records):,}건")

    return all_records

def map_fields(df):
    rename_map = {}
    for col in df.columns:
        if col in FIELD_MAP and FIELD_MAP[col] not in df.columns:
            rename_map[col] = FIELD_MAP[col]
    df = df.rename(columns=rename_map)

    if rename_map:
        print(f"  필드 매핑: {rename_map}")
    else:
        non_internal = [c for c in df.columns if not c.startswith("_")]
        print(f"  ⚠ 매핑된 필드 없음. 실제 수신 필드: {non_internal[:20]}")
        print(f"  → --debug 옵션으로 응답 원문을 확인하세요.")

    if "_api" in df.columns:
        df["개방서비스명"] = df["_api"]

    return df

def apply_filters(df):
    original = len(df)
    sts_col = next(
        (c for c in ["상세영업상태명", "DTL_SALS_STTS_NM"] if c in df.columns),
        None
    )
    if sts_col:
        df = df[~df[sts_col].isin(EXCLUDE_STATUSES)]
        print(f"  영업상태 필터: {original:,} → {len(df):,}건 (제외 {original - len(df):,}건)")

    return df

def build_output(df):
    df = df.copy()

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[OUTPUT_COLUMNS]

    if "개방서비스명" in df.columns:
        print("  업종별:")
        for svc, cnt in df["개방서비스명"].value_counts().items():
            print(f"    - {svc}: {cnt:,}건")

    return df

def save_excel(df, start_date, end_date):
    filename = f"검단구_신규가맹점_{start_date}_{end_date}.xlsx"
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_excel(path, index=False, engine="openpyxl")
    print(f"\n  ✓ 저장 완료: {filename}")
    print(f"  경로: {os.path.abspath(path)}")

# =====================================================================
# 메인
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="지방행정 인허가정보 API - 인천 검단구 전용")
    parser.add_argument("--start", type=str, help="조회 시작일 (YYYYMMDD)")
    parser.add_argument("--end",   type=str, help="조회 종료일 (YYYYMMDD)")
    parser.add_argument("--debug", action="store_true", help="첫 번째 업종 응답 원문 출력")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  지방행정 인허가정보 API - 인천 검단구 전용 v9.0")
    print("="*60)

    if not API_LIST:
        print("\n⚠️ API_LIST가 비어 있습니다.\n")
        sys.exit(1)

    if args.start:
        start_date = args.start
        end_date = args.end or datetime.now().strftime("%Y%m%d")
        print(f"\n  조회 기준일: {start_date} 이후 변동분 (직접 지정)")
    else:
        start_date, end_date = get_date_range()
        print(f"\n  조회 기준일: {start_date} 이후 변동분 (최근 7일 자동)")

    start_datetime = to_datetime_str(start_date)
    print(f"  API 파라미터: cond[DAT_UPDT_PNT::GTE]={start_datetime}")
    print(f"  자치단체코드: {TARGET_SIGUN_CD} (인천 검단구)")
    print(f"  처리 업종: {len(API_LIST)}개 ({', '.join(a['name'] for a in API_LIST)})")

    if args.debug:
        print(f"\n  ★ 디버그 모드: [{API_LIST[0]['name']}] 응답 원문 출력")
        fetch_all_for_api(API_LIST[0], start_datetime, debug=True)
        return

    # 1단계: 수집
    print("\n" + "─"*60)
    print("[1단계] 업종별 데이터 수집")
    print("─"*60)

    all_records, success_list, fail_list = [], [], []
    for api_info in API_LIST:
        try:
            records = fetch_all_for_api(api_info, start_datetime)
            (success_list if records else fail_list).append(api_info["name"])
            all_records.extend(records)
        except Exception as e:
            print(f"  [{api_info['name']}] 예외: {e}")
            fail_list.append(api_info["name"])

    print(f"\n  수집: 성공 {len(success_list)}개 업종 / 총 {len(all_records):,}건")
    if fail_list:
        print(f"  데이터 없음 또는 실패: {', '.join(fail_list)}")

    if not all_records:
        print("\n  수집된 데이터 없음. API 설정 및 조회 기간을 확인해주세요.")
        sys.exit(0)

    # 2단계: 통합 + 필드 매핑
    print("\n" + "─"*60)
    print("[2단계] 데이터 통합 및 필드명 매핑")
    print("─"*60)
    df = pd.DataFrame(all_records)
    print(f"  통합: {len(df):,}건")
    df = map_fields(df)

    # 3단계: 영업상태 필터
    print("\n" + "─"*60)
    print("[3단계] 영업상태 필터링")
    print("─"*60)
    df = apply_filters(df)
    if df.empty:
        print("  필터링 후 데이터 없음.")
        sys.exit(0)

    # 4단계: 열 정리
    print("\n" + "─"*60)
    print("[4단계] 최종 정리")
    print("─"*60)
    df = build_output(df)

    # 5단계: 저장
    print("\n" + "─"*60)
    print("[5단계] 엑셀 저장")
    print("─"*60)
    save_excel(df, start_date, end_date)

    print("\n" + "="*60)
    print(f"  ✓ 완료! 최종 {len(df):,}건 추출 (인천 검단구)")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
