#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
간단한 테스트 코드 - 함수 변경사항 확인
"""

import pandas as pd
import os

def test_normalize_function():
    """normalize_center_name 함수 테스트"""
    print("🧪 normalize_center_name 함수 테스트")
    print("=" * 50)
    
    # community_center_crawler에서 함수 임포트
    try:
        from community_center_crawler import normalize_center_name
        
        # 테스트 케이스
        test_cases = [
            ("서울", "청운효자동주민센터"),
            ("세종", "조치원읍행정복지센터"),
            ("경기", "수원시청"),
            ("부산", "해운대구청"),
        ]
        
        print("📋 테스트 케이스:")
        for sido, name in test_cases:
            result = normalize_center_name(sido, name)
            print(f"  입력: {sido}, {name}")
            print(f"  결과: {result}")
            print()
        
        print("✅ 함수 테스트 완료")
        
    except Exception as e:
        print(f"❌ 함수 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

def test_csv_data():
    """CSV 데이터 확인"""
    print("\n🧪 CSV 데이터 확인")
    print("=" * 50)
    
    csv_path = "행정안전부_읍면동 하부행정기관 현황_20240731.csv"
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    try:
        # 데이터 로드
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(csv_path, encoding='cp949')
            except UnicodeDecodeError:
                df = pd.read_csv(csv_path, encoding='euc-kr')
        
        print(f"✅ 데이터 로드 완료: {len(df)}개")
        
        # 첫 5개 데이터 확인
        print("\n📋 첫 5개 데이터:")
        for idx, row in df.head(5).iterrows():
            sido = row.get('시도', '')
            sigungu = row.get('시군구', '')
            name = row.get('읍면동', '')
            print(f"  {idx+1}. 시도: {sido} | 시군구: {sigungu} | 읍면동: {name}")
        
        # 세종시 데이터 확인
        print("\n📋 세종시 데이터 (첫 3개):")
        sejong_data = df[df['시도'] == '세종'].head(3)
        for idx, row in sejong_data.iterrows():
            sido = row.get('시도', '')
            sigungu = row.get('시군구', '')
            name = row.get('읍면동', '')
            print(f"  {idx+1}. 시도: {sido} | 시군구: {sigungu} | 읍면동: {name}")
        
        print("✅ CSV 데이터 확인 완료")
        
    except Exception as e:
        print(f"❌ CSV 데이터 확인 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_normalize_function()
    test_csv_data() 