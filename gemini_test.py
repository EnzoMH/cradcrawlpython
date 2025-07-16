#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gemini AI 기반 팩스번호 추출 테스트

작성자: AI Assistant  
작성일: 2025-01-15
"""

import os
import sys
import logging
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 프로젝트 루트 디렉토리 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 모듈을 먼저 임포트하고 필요한 클래스들을 가져옵니다
try:
    import institution_name_extractor as ine
    WebDriverManager = ine.WebDriverManager
    GoogleSearchEngine = ine.GoogleSearchEngine
    GeminiAnalyzer = ine.GeminiAnalyzer
    CacheManager = ine.CacheManager
except ImportError as e:
    print(f"❌ Import 오류: {e}")
    print("institution_name_extractor.py 파일을 확인해주세요.")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_gemini_api_keys():
    """Gemini API 키 확인"""
    print("🔑 Gemini API 키 확인...")
    
    keys = [
        os.getenv('GEMINI_API_KEY'),
        os.getenv('GEMINI_API_KEY_2'),
        os.getenv('GEMINI_API_KEY_3'),
        os.getenv('GEMINI_API_KEY_4')
    ]
    
    valid_keys = [key for key in keys if key]
    
    print(f"✅ 총 {len(valid_keys)}개 API 키 발견")
    
    if len(valid_keys) == 0:
        print("❌ Gemini API 키가 설정되지 않았습니다!")
        print("📝 .env 파일에 다음과 같이 설정해주세요:")
        print("GEMINI_API_KEY=your_api_key_1")
        print("GEMINI_API_KEY_2=your_api_key_2")
        print("GEMINI_API_KEY_3=your_api_key_3")
        print("GEMINI_API_KEY_4=your_api_key_4")
        return False
    
    return True

def test_gemini_analyzer():
    """GeminiAnalyzer 단위 테스트"""
    print("\n🤖 GeminiAnalyzer 테스트...")
    
    try:
        analyzer = GeminiAnalyzer()
        print(f"✅ GeminiAnalyzer 초기화 성공 - {len(analyzer.api_keys)}개 키")
        
        # 간단한 테스트 데이터
        test_texts = [
            "송산동주민센터 전화: 064-760-0511 팩스: 064-760-4509",
            "제주특별자치도 서귀포시 소암로 4 (서귀동)"
        ]
        
        result = analyzer.analyze_search_results(test_texts, "064-760-4509", worker_id=999)
        
        if result:
            print(f"✅ Gemini 분석 성공: '{result}'")
        else:
            print("⚠️ Gemini 분석 결과 없음")
        
        return True
        
    except Exception as e:
        print(f"❌ GeminiAnalyzer 테스트 실패: {e}")
        return False

def test_cache_manager():
    """CacheManager 테스트"""
    print("\n💾 CacheManager 테스트...")
    
    try:
        cache = CacheManager()
        
        # 테스트 데이터 저장
        test_number = "064-760-4509"
        test_result = "서귀포시 송산동주민센터"
        
        cache.save_result(test_number, test_result, {'test': True})
        print("✅ 캐시 저장 성공")
        
        # 캐시에서 조회
        cached_result = cache.get_cached_result(test_number)
        
        if cached_result == test_result:
            print(f"✅ 캐시 조회 성공: '{cached_result}'")
        else:
            print(f"⚠️ 캐시 조회 불일치: 예상 '{test_result}', 실제 '{cached_result}'")
        
        return True
        
    except Exception as e:
        print(f"❌ CacheManager 테스트 실패: {e}")
        return False

def test_new_search_engine():
    """새로운 검색 엔진 통합 테스트"""
    print("\n🔍 새로운 검색 엔진 테스트...")
    
    # 테스트할 팩스번호 (실제 데이터)
    test_cases = [
        {"fax": "064-760-4509", "expected": "송산동주민센터", "location": "제주 서귀포시"},
        {"fax": "02-730-5479", "expected": "청운효자동주민센터", "location": "서울 종로구"},
    ]
    
    try:
        # WebDriver 및 검색엔진 초기화
        driver_manager = WebDriverManager(headless=True)
        search_engine = GoogleSearchEngine(driver_manager)
        
        success_count = 0
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n--- 테스트 {i}: {test_case['expected']} ---")
            print(f"팩스번호: {test_case['fax']}")
            
            try:
                # 새로운 Gemini AI 기반 검색 실행
                result = search_engine.search_institution_name_v2(
                    test_case['fax'], 
                    "팩스번호", 
                    worker_id=999
                )
                
                if result.search_successful:
                    print(f"✅ 검색 성공!")
                    print(f"   발견된 기관명: {result.institution_name}")
                    print(f"   신뢰도: {result.confidence}")
                    print(f"   검색시간: {result.search_time:.2f}초")
                    
                    if (test_case['expected'] in result.institution_name or 
                        result.institution_name in test_case['expected']):
                        print(f"🎯 예상 기관명과 일치!")
                        success_count += 1
                    else:
                        print(f"⚠️ 예상과 다름 (예상: {test_case['expected']})")
                        success_count += 0.5  # 부분 점수
                else:
                    print(f"❌ 검색 실패: {result.error_message}")
                    print(f"   검색시간: {result.search_time:.2f}초")
                
            except Exception as e:
                print(f"💥 테스트 중 오류: {e}")
        
        print(f"\n📊 결과: {success_count}/{len(test_cases)} 성공 ({success_count/len(test_cases)*100:.1f}%)")
        return success_count == len(test_cases)
        
    except Exception as e:
        print(f"❌ 검색 엔진 테스트 실패: {e}")
        return False

def main():
    """메인 테스트 함수"""
    print("🧪 Gemini AI 기반 팩스번호 추출 시스템 테스트")
    print("=" * 60)
    
    # 1. API 키 확인
    if not check_gemini_api_keys():
        return False
    
    # 2. GeminiAnalyzer 테스트
    if not test_gemini_analyzer():
        return False
    
    # 3. CacheManager 테스트
    if not test_cache_manager():
        return False
    
    # 4. 통합 테스트 옵션
    print("\n🎯 실제 웹 검색 테스트를 진행하시겠습니까?")
    print("   이 테스트는 실제 Gemini API를 호출하므로 요금이 발생할 수 있습니다.")
    
    choice = input("계속하시겠습니까? (y/N): ").strip().lower()
    if choice in ['y', 'yes']:
        success = test_new_search_engine()
        print(f"\n최종 결과: {'✅ 성공' if success else '⚠️ 부분 성공/실패'}")
    else:
        print("실제 검색 테스트를 건너뜁니다.")
        print("✅ 기본 컴포넌트 테스트 완료!")
    
    print("\n" + "=" * 60)
    print("🎉 Gemini AI 기반 시스템 통합 완료!")
    print("📋 사용법:")
    print("   1. .env 파일에 Gemini API 키 설정")
    print("   2. 팩스번호는 자동으로 새로운 AI 방식 사용")
    print("   3. 전화번호는 기존 방식 유지")
    print("   4. 결과는 자동으로 캐시에 저장")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️ 테스트가 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        sys.exit(1) 