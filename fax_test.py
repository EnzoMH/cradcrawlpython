#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
팩스번호 추출 집중 테스트

작성자: AI Assistant  
작성일: 2025-01-15
"""

import time
import logging
from institution_name_extractor import WebDriverManager, GoogleSearchEngine

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_fax_extraction():
    """팩스번호 추출 집중 테스트"""
    print("📠 팩스번호 추출 집중 테스트 시작")
    print("=" * 50)
    
    # 테스트할 팩스번호들 (실제 데이터에서 선별)
    test_fax_numbers = [
        {"fax": "02-730-5479", "expected": "청운효자동주민센터", "location": "서울 종로구"},
        {"fax": "02-2148-5951", "expected": "사직동주민센터", "location": "서울 종로구"},
        {"fax": "064-760-4509", "expected": "송산동주민센터", "location": "제주 서귀포시"},
    ]
    
    # WebDriver 및 검색엔진 초기화
    driver_manager = WebDriverManager(headless=True)
    search_engine = GoogleSearchEngine(driver_manager)
    
    success_count = 0
    total_count = len(test_fax_numbers)
    
    for i, test_case in enumerate(test_fax_numbers, 1):
        print(f"\n--- 테스트 {i}/{total_count}: {test_case['expected']} ---")
        print(f"팩스번호: {test_case['fax']}")
        print(f"위치: {test_case['location']}")
        
        try:
            # 팩스번호로 검색 실행
            result = search_engine.search_institution_name(
                test_case['fax'], 
                "팩스번호", 
                worker_id=999
            )
            
            if result.search_successful:
                print(f"✅ 검색 성공!")
                print(f"   발견된 기관명: {result.institution_name}")
                print(f"   신뢰도: {result.confidence}")
                print(f"   검색시간: {result.search_time:.2f}초")
                
                # 예상 기관명과 비교
                if (test_case['expected'] in result.institution_name or 
                    result.institution_name in test_case['expected']):
                    print(f"🎯 예상 기관명과 일치!")
                    success_count += 1
                else:
                    print(f"⚠️ 예상과 다름 (예상: {test_case['expected']})")
            else:
                print(f"❌ 검색 실패: {result.error_message}")
                print(f"   검색시간: {result.search_time:.2f}초")
            
        except Exception as e:
            print(f"💥 테스트 중 오류: {e}")
        
        # 다음 테스트까지 잠시 대기
        if i < total_count:
            time.sleep(2)
    
    # 결과 요약
    print("\n" + "=" * 50)
    print(f"📊 테스트 결과 요약")
    print(f"성공: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
    
    if success_count == total_count:
        print("🎉 모든 테스트 성공! 팩스번호 추출이 정상 작동합니다.")
    elif success_count > 0:
        print("⚠️ 일부 성공. 추가 개선이 필요합니다.")
    else:
        print("❌ 모든 테스트 실패. 근본적인 문제가 있습니다.")
    
    return success_count == total_count

if __name__ == "__main__":
    try:
        success = test_fax_extraction()
        print(f"\n테스트 완료: {'성공' if success else '개선 필요'}")
    except KeyboardInterrupt:
        print("\n테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n테스트 실행 중 오류: {e}") 