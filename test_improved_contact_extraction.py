#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
개선된 연락처 추출 로직 테스트
- 전화번호와 팩스번호를 별도 검색
- 중복 번호 처리 로직 테스트
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime

# 현재 디렉토리에서 모듈 임포트
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from community_center_crawler import (
    ImprovedCommunityCenterCrawler,
    process_improved_contact_extraction,
    KOREAN_AREA_CODES
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_improved_extraction.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def test_improved_contact_extraction():
    """개선된 연락처 추출 로직 테스트"""
    try:
        print("🧪 개선된 연락처 추출 로직 테스트 시작")
        print("=" * 60)
        
        # CSV 파일 경로
        csv_path = "행정안전부_읍면동 하부행정기관 현황_20240731.csv"
        
        if not os.path.exists(csv_path):
            print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
            return
        
        # 데이터 로드
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(csv_path, encoding='cp949')
            except UnicodeDecodeError:
                df = pd.read_csv(csv_path, encoding='euc-kr')
        
        print(f"📊 전체 데이터 로드: {len(df)}개")
        
        # 테스트용 10개 데이터 선택
        test_df = df.head(10).copy()
        print(f"🔬 테스트 데이터: {len(test_df)}개")
        
        # 테스트 데이터 정보 출력
        print("\n📋 테스트 대상 주민센터:")
        for idx, row in test_df.iterrows():
            sido = row.get('시도', '')
            name = row.get('읍면동', '')
            print(f"  {idx+1}. {sido} {name}")
        
        # 봇 초기화
        bot = ImprovedCommunityCenterCrawler(csv_path, use_ai=False)
        
        # 테스트 실행
        print(f"\n🚀 개선된 연락처 추출 테스트 시작")
        print(f"📞 전화번호와 팩스번호를 별도로 검색합니다...")
        
        start_time = datetime.now()
        
        # 워커 1개로 테스트 (디버깅 용이)
        results = process_improved_contact_extraction(
            test_df, 
            worker_id=0,
            phone_patterns=bot.phone_patterns,
            fax_patterns=bot.fax_patterns,
            area_codes=KOREAN_AREA_CODES
        )
        
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        
        print(f"\n⏱️ 테스트 완료 시간: {elapsed_time:.2f}초")
        print(f"📊 처리 결과: {len(results)}개")
        
        # 결과 분석
        print("\n📈 결과 분석:")
        phone_count = sum(1 for r in results if r.get('phone'))
        fax_count = sum(1 for r in results if r.get('fax'))
        both_count = sum(1 for r in results if r.get('phone') and r.get('fax'))
        duplicate_count = sum(1 for r in results if r.get('phone') and r.get('fax') and r.get('phone') == r.get('fax'))
        
        print(f"  - 전화번호 발견: {phone_count}개 ({phone_count/len(results)*100:.1f}%)")
        print(f"  - 팩스번호 발견: {fax_count}개 ({fax_count/len(results)*100:.1f}%)")
        print(f"  - 둘 다 발견: {both_count}개 ({both_count/len(results)*100:.1f}%)")
        print(f"  - 중복 번호: {duplicate_count}개 ({duplicate_count/len(results)*100:.1f}%)")
        
        # 상세 결과 출력
        print("\n📋 상세 결과:")
        for i, result in enumerate(results, 1):
            name = result.get('name', '알 수 없음')
            phone = result.get('phone', '없음')
            fax = result.get('fax', '없음')
            
            status = "✅" if phone != '없음' or fax != '없음' else "❌"
            duplicate_warning = " ⚠️ 중복" if phone != '없음' and fax != '없음' and phone == fax else ""
            
            print(f"  {status} {i:2d}. {name}")
            print(f"      전화: {phone}")
            print(f"      팩스: {fax}{duplicate_warning}")
        
        # 결과 저장
        result_df = pd.DataFrame(results)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_path = f"test_improved_extraction_result_{timestamp}.xlsx"
        result_df.to_excel(result_path, index=False)
        
        print(f"\n💾 결과 저장: {result_path}")
        print("🎉 테스트 완료!")
        
        # 봇 정리
        bot._cleanup()
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"❌ 테스트 오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_improved_contact_extraction() 