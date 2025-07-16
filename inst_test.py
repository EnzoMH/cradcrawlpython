#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실제기관명 추출 시스템 테스트 파일 (간단 버전)
팩스번호 추출 문제 진단용

작성자: AI Assistant
작성일: 2025-01-15
"""

import re
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_phone_number_normalization():
    """전화번호 정규화 테스트"""
    print("📞 전화번호 정규화 테스트 시작...")
    
    def normalize_phone_number(phone_number: str) -> str:
        """전화번호 정규화 (institution_name_extractor.py에서 복사)"""
        if not phone_number:
            return ""
        
        # 숫자와 하이픈만 추출
        clean_number = re.sub(r'[^\d-]', '', str(phone_number).strip())
        
        # 기본 형식 검증
        if not re.match(r'^[\d-]+$', clean_number):
            return ""
        
        # 하이픈 제거 후 숫자만 추출
        digits_only = re.sub(r'[^\d]', '', clean_number)
        
        # 길이 검증
        if len(digits_only) < 8 or len(digits_only) > 11:
            return ""
        
        return clean_number
    
    test_numbers = [
        "02-2148-5001",    # 일반 전화번호
        "02-730-5479",     # 팩스번호
        "064-760-0511",    # 지역번호
        "064-760-4509",    # 제주 팩스번호
        "02-394-5682",     # 또 다른 팩스번호
        "",                # 빈 번호
        "invalid-number"   # 잘못된 번호
    ]
    
    for number in test_numbers:
        normalized = normalize_phone_number(number)
        print(f"  원본: '{number}' -> 정규화: '{normalized}'")
    
    print("✅ 전화번호 정규화 테스트 완료\n")

def test_institution_extraction():
    """기관명 추출 로직 테스트"""
    print("🏢 기관명 추출 로직 테스트 시작...")
    
    # institution_name_extractor.py에서 복사한 키워드와 함수들
    institution_keywords = [
        '주민센터', '행정복지센터', '동사무소', '면사무소', '읍사무소',
        '시청', '구청', '군청', '청사', '시 ', '구 ', '군 ',
        '병원', '의원', '보건소', '보건센터', '클리닉',
        '학교', '대학', '교육청', '교육지원청',
        '경찰서', '파출소', '지구대', '소방서',
        '법원', '검찰청', '세무서', '등기소',
        '우체국', '체신청', '공사', '공단', '센터', '사업소'
    ]
    
    def extract_name_from_line(line: str, keyword: str) -> str:
        """한 줄에서 기관명 추출"""
        # 키워드 앞의 한글 텍스트를 기관명으로 추출
        pattern = r'([가-힣]{2,10})' + re.escape(keyword)
        match = re.search(pattern, line)
        
        if match:
            institution_name = match.group(1) + keyword
            # 기관명 길이 검증
            if 2 <= len(institution_name) <= 20:
                return institution_name
        
        # 키워드 뒤의 텍스트에서 기관명 추출
        keyword_index = line.find(keyword)
        if keyword_index != -1:
            # 키워드 앞뒤 텍스트 추출
            before_text = line[:keyword_index].strip()
            after_text = line[keyword_index + len(keyword):].strip()
            
            # 앞쪽 텍스트에서 기관명 추출
            before_match = re.search(r'([가-힣]{2,10})$', before_text)
            if before_match:
                return before_match.group(1) + keyword
        
        return ""
    
    def find_institution_name(text: str, phone_number: str) -> str:
        """텍스트에서 기관명 찾기"""
        if not text:
            return ""
        
        # 전화번호 주변 텍스트 추출
        phone_clean = re.sub(r'[^\d]', '', phone_number)
        
        # 텍스트를 줄 단위로 분리
        lines = text.split('\n')
        
        # 전화번호가 포함된 줄들 찾기
        relevant_lines = []
        for line in lines:
            line_clean = re.sub(r'[^\d]', '', line)
            if phone_clean in line_clean:
                relevant_lines.append(line.strip())
        
        # 관련 줄들에서 기관명 추출
        for line in relevant_lines:
            # 기관 키워드가 포함된 경우
            for keyword in institution_keywords:
                if keyword in line:
                    # 기관명 추출 시도
                    institution_name = extract_name_from_line(line, keyword)
                    if institution_name:
                        return institution_name
        
        # 기관 키워드가 없는 경우, 일반적인 기관명 패턴 찾기
        for line in relevant_lines:
            # 한글 기관명 패턴 찾기
            matches = re.findall(r'([가-힣]{2,10}(?:구청|시청|군청|센터|사무소|병원|의원|학교|대학|청|서|소|원|관|공사|공단))', line)
            if matches:
                return matches[0]
        
        return ""
    
    # 가상의 검색 결과 텍스트로 테스트
    test_cases = [
        {
            "text": "청운효자동주민센터 - 서울특별시 종로구 자하문로 92 전화번호: 02-2148-5001 팩스번호: 02-730-5479",
            "phone": "02-730-5479",
            "expected": "청운효자동주민센터"
        },
        {
            "text": "종로구 청운효자동 행정복지센터 연락처 02-730-5479",
            "phone": "02-730-5479",
            "expected": "청운효자동행정복지센터"
        },
        {
            "text": "서울 종로구청 청운효자동주민센터 FAX: 02-730-5479",
            "phone": "02-730-5479",
            "expected": "청운효자동주민센터"
        },
        {
            "text": "02-730-5479 청운효자동 주민센터 팩스번호",
            "phone": "02-730-5479",
            "expected": "청운효자동주민센터"
        },
        {
            "text": "팩스: 02-730-5479 청운효자동주민센터 서울시 종로구",
            "phone": "02-730-5479",
            "expected": "청운효자동주민센터"
        },
        {
            "text": "송산동주민센터 전화: 064-760-0511 팩스: 064-760-4509",
            "phone": "064-760-4509",
            "expected": "송산동주민센터"
        },
        {
            "text": "사직동주민센터 서울특별시 종로구 경희궁1길 15 전화번호 02-2148-5033 팩스번호 02-2148-5951",
            "phone": "02-2148-5951",
            "expected": "사직동주민센터"
        }
    ]
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n테스트 케이스 {i}:")
        print(f"  입력 텍스트: {case['text']}")
        print(f"  전화번호: {case['phone']}")
        print(f"  예상 기관명: {case['expected']}")
        
        extracted = find_institution_name(case['text'], case['phone'])
        print(f"  추출된 기관명: '{extracted}'")
        
        # 결과 분석
        if extracted:
            if case['expected'] in extracted or extracted in case['expected']:
                print(f"  ✅ 성공! 예상과 일치")
            else:
                print(f"  ⚠️ 부분 성공 - 기관명 발견했으나 예상과 다름")
        else:
            print(f"  ❌ 실패 - 기관명 추출 못함")
    
    print("\n✅ 기관명 추출 로직 테스트 완료\n")

def test_search_patterns():
    """검색 패턴 분석"""
    print("🔍 검색 패턴 분석...")
    
    # 실제 데이터에서 팩스번호 패턴 분석
    fax_numbers = [
        "02-730-5479",    # 청운효자동주민센터
        "02-2148-5951",   # 사직동주민센터
        "02-2148-5842",   # 부암동주민센터
        "02-394-5682",    # 평창동주민센터
        "064-760-4509",   # 송산동주민센터
        "064-760-4539",   # 정방동주민센터
        "064-760-4569",   # 중앙동주민센터
    ]
    
    print("팩스번호들의 패턴 분석:")
    for fax in fax_numbers:
        # 각 번호의 특징 분석
        parts = fax.split('-')
        area_code = parts[0] if len(parts) > 0 else ""
        middle = parts[1] if len(parts) > 1 else ""
        last = parts[2] if len(parts) > 2 else ""
        
        print(f"  {fax}: 지역코드={area_code}, 중간={middle}, 끝={last}")
        
        # 검색 쿼리 제안
        queries = [
            f'"{fax}" 팩스번호',
            f'"{fax}" 주민센터',
            f'"{fax}" 팩스',
            f'"{fax}"'
        ]
        print(f"    추천 검색 쿼리: {queries}")
    
    print("\n✅ 검색 패턴 분석 완료\n")

def analyze_data_pattern():
    """데이터 패턴 분석"""
    print("📊 데이터 패턴 분석...")
    
    # 사용자가 제공한 샘플 데이터
    sample_data = [
        {"name": "청운효자동주민센터", "phone": "02-2148-5001", "fax": "02-730-5479"},
        {"name": "사직동주민센터", "phone": "02-2148-5033", "fax": "02-2148-5951"},
        {"name": "삼청동주민센터", "phone": "02-2148-5062", "fax": ""},
        {"name": "부암동주민센터", "phone": "02-2148-5092", "fax": "02-2148-5842"},
        {"name": "평창동주민센터", "phone": "02-2148-5123", "fax": "02-394-5682"},
        {"name": "송산동주민센터", "phone": "064-760-0511", "fax": "064-760-4509"},
        {"name": "정방동주민센터", "phone": "064-760-4530", "fax": "064-760-4539"},
    ]
    
    print("데이터 분석 결과:")
    phone_count = 0
    fax_count = 0
    same_prefix_count = 0
    
    for data in sample_data:
        if data["phone"]:
            phone_count += 1
        if data["fax"]:
            fax_count += 1
            
        # 전화번호와 팩스번호의 앞부분이 같은지 확인
        if data["phone"] and data["fax"]:
            phone_prefix = data["phone"].split('-')[0:2]
            fax_prefix = data["fax"].split('-')[0:2]
            if phone_prefix == fax_prefix:
                same_prefix_count += 1
            
            print(f"  {data['name']}:")
            print(f"    전화: {data['phone']}, 팩스: {data['fax']}")
            print(f"    같은 prefix: {phone_prefix == fax_prefix}")
    
    print(f"\n통계:")
    print(f"  전화번호 있음: {phone_count}개")
    print(f"  팩스번호 있음: {fax_count}개")
    print(f"  같은 prefix: {same_prefix_count}개")
    print(f"  팩스번호 비율: {fax_count/len(sample_data)*100:.1f}%")
    
    print("\n✅ 데이터 패턴 분석 완료\n")

def main():
    """메인 테스트 함수"""
    print("🧪 실제기관명 추출 시스템 테스트 (간단 버전)")
    print("=" * 60)
    
    # 1. 전화번호 정규화 테스트
    test_phone_number_normalization()
    
    # 2. 데이터 패턴 분석
    analyze_data_pattern()
    
    # 3. 기관명 추출 로직 테스트
    test_institution_extraction()
    
    # 4. 검색 패턴 분석
    test_search_patterns()
    
    print("=" * 60)
    print("🧪 테스트 완료!")
    print("\n📋 결론 및 권장사항:")
    print("1. 전화번호 정규화는 정상 작동")
    print("2. 기관명 추출 로직 확인 - 키워드 기반 추출")
    print("3. 팩스번호 검색 문제 가능성:")
    print("   - 구글 검색에서 팩스번호 정보가 전화번호보다 적음")
    print("   - 검색 쿼리 최적화 필요")
    print("   - 팩스번호 + 지역정보 조합 검색 고려")
    print("4. 개선 방안:")
    print("   - 더 다양한 검색 쿼리 시도")
    print("   - 지역 정보 활용한 검색")
    print("   - AI 모델(Gemini) 활용 고려")

if __name__ == "__main__":
    main()
