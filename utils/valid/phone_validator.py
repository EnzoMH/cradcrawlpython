#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전화번호/팩스번호 유효성 검증 클래스
"""

import re
import logging
import pandas as pd
from typing import Dict, Optional, Tuple

# 한국 지역번호 매핑
KOREAN_AREA_CODES = {
    "02": "서울", 
    "031": "경기", 
    "032": "인천", 
    "033": "강원",
    "041": "충남", 
    "042": "대전", 
    "043": "충북", 
    "044": "세종",
    "051": "부산", 
    "052": "울산", 
    "053": "대구", 
    "054": "경북", 
    "055": "경남",
    "061": "전남", 
    "062": "광주", 
    "063": "전북", 
    "064": "제주",
    "070": "인터넷전화", 
    "010": "핸드폰", 
    "017": "핸드폰"
}

class PhoneValidator:
    """전화번호/팩스번호 유효성 검증 클래스"""
    
    def __init__(self, logger=None):
        """
        전화번호 검증기 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.area_codes = KOREAN_AREA_CODES
        
        # 지역별 세부 매핑
        self.area_mapping = {
            '02': ['서울', '서울특별시', '서울시'],
            '031': ['경기', '경기도', '인천', '인천광역시'],
            '032': ['인천', '인천광역시', '경기', '경기도'],
            '033': ['강원', '강원도', '강원특별자치도'],
            '041': ['충남', '충청남도', '세종', '세종특별자치시'],
            '042': ['대전', '대전광역시', '충남', '충청남도'],
            '043': ['충북', '충청북도', '충북도', '청주', '제천', '충주', '음성', '진천', '괴산', '증평', '영동', '옥천', '보은', '단양'],
            '044': ['세종', '세종특별자치시', '충남', '충청남도'],
            '051': ['부산', '부산광역시'],
            '052': ['울산', '울산광역시'],
            '053': ['대구', '대구광역시'],
            '054': ['경북', '경상북도', '대구', '대구광역시'],
            '055': ['경남', '경상남도', '부산', '부산광역시'],
            '061': ['전남', '전라남도', '광주', '광주광역시'],
            '062': ['광주', '광주광역시', '전남', '전라남도'],
            '063': ['전북', '전라북도', '전북도'],
            '064': ['제주', '제주도', '제주특별자치도'],
            '070': ['인터넷전화'],
        }
    
    def normalize_phone_number(self, phone: str) -> str:
        """
        전화번호 정규화
        
        Args:
            phone: 원본 전화번호
            
        Returns:
            str: 정규화된 전화번호 (예: 02-1234-5678)
        """
        if not phone or pd.isna(phone):
            return ""
        
        try:
            # 숫자만 추출
            numbers = re.findall(r'\d+', str(phone))
            if not numbers:
                return ""
            
            # 숫자 조합에 따라 포맷팅
            if len(numbers) >= 3:
                return f"{numbers[0]}-{numbers[1]}-{numbers[2]}"
            elif len(numbers) == 2:
                return f"{numbers[0]}-{numbers[1]}"
            else:
                return numbers[0]
                
        except Exception as e:
            self.logger.error(f"❌ 전화번호 정규화 오류: {phone} - {e}")
            return ""
    
    def is_valid_phone_format(self, phone: str) -> bool:
        """
        전화번호 형식 유효성 검사
        
        Args:
            phone: 전화번호
            
        Returns:
            bool: 유효한 형식인지 여부
        """
        try:
            if not phone or pd.isna(phone):
                return False
            
            # 숫자만 추출
            digits = re.sub(r'[^\d]', '', str(phone))
            if len(digits) < 8 or len(digits) > 11:
                return False
            
            # 유효한 지역번호 패턴
            valid_patterns = [
                r'^02\d{7,8}$',      # 서울
                r'^0[3-6]\d{7,8}$',  # 지역번호
                r'^070\d{7,8}$',     # 인터넷전화
                r'^1[5-9]\d{6,7}$',  # 특수번호
                r'^080\d{7,8}$',     # 무료전화
                r'^010\d{7,8}$',     # 휴대폰
                r'^01[1679]\d{7,8}$' # 기타 휴대폰
            ]
            
            for pattern in valid_patterns:
                if re.match(pattern, digits):
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 전화번호 형식 검증 오류: {phone} - {e}")
            return False
    
    def extract_area_code(self, phone: str) -> str:
        """
        전화번호에서 지역번호 추출
        
        Args:
            phone: 전화번호
            
        Returns:
            str: 지역번호
        """
        try:
            if not phone:
                return ""
            
            digits = re.sub(r'[^\d]', '', str(phone))
            
            if len(digits) >= 10:
                if digits.startswith('02'):
                    return '02'
                else:
                    return digits[:3]
            elif len(digits) >= 9:
                if digits.startswith('02'):
                    return '02'
                else:
                    return digits[:3]
            else:
                return digits[:2] if len(digits) >= 2 else ""
                
        except Exception as e:
            self.logger.error(f"❌ 지역번호 추출 오류: {phone} - {e}")
            return ""
    
    def get_region_from_phone(self, phone: str) -> str:
        """
        전화번호에서 지역명 추출
        
        Args:
            phone: 전화번호
            
        Returns:
            str: 지역명
        """
        try:
            area_code = self.extract_area_code(phone)
            return self.area_codes.get(area_code, "")
        except Exception as e:
            self.logger.error(f"❌ 지역명 추출 오류: {phone} - {e}")
            return ""
    
    def is_same_area_code(self, phone1: str, phone2: str) -> bool:
        """
        두 전화번호의 지역번호 일치성 검사
        
        Args:
            phone1: 첫 번째 전화번호
            phone2: 두 번째 전화번호
            
        Returns:
            bool: 지역번호가 같은지 여부
        """
        try:
            area1 = self.extract_area_code(phone1)
            area2 = self.extract_area_code(phone2)
            return area1 == area2 and area1 != ""
        except Exception as e:
            self.logger.error(f"❌ 지역번호 비교 오류: {phone1}, {phone2} - {e}")
            return False
    
    def are_numbers_too_similar(self, phone1: str, phone2: str) -> bool:
        """
        두 전화번호의 유사성 검사 (연속된 번호인지 확인)
        
        Args:
            phone1: 첫 번째 전화번호
            phone2: 두 번째 전화번호
            
        Returns:
            bool: 너무 유사한지 여부
        """
        try:
            digits1 = re.sub(r'[^\d]', '', str(phone1))
            digits2 = re.sub(r'[^\d]', '', str(phone2))
            
            if len(digits1) != len(digits2) or len(digits1) < 8:
                return False
            
            # 지역번호가 다르면 유사하지 않음
            area1 = self.extract_area_code(digits1)
            area2 = self.extract_area_code(digits2)
            if area1 != area2:
                return False
            
            # 지역번호 이후 부분 비교
            suffix1 = digits1[len(area1):]
            suffix2 = digits2[len(area2):]
            
            # 차이나는 자리수 계산
            diff_count = sum(1 for i, (d1, d2) in enumerate(zip(suffix1, suffix2)) if d1 != d2)
            
            # 1자리 이하 차이면 유사한 것으로 판단
            return diff_count <= 1
            
        except Exception as e:
            self.logger.error(f"❌ 번호 유사성 검사 오류: {phone1}, {phone2} - {e}")
            return False
    
    def is_regional_match(self, phone: str, address: str, org_name: str = "") -> bool:
        """
        전화번호와 주소의 지역 일치성 검사
        
        Args:
            phone: 전화번호
            address: 주소
            org_name: 기관명 (로깅용)
            
        Returns:
            bool: 지역이 일치하는지 여부
        """
        try:
            if not phone or not address or pd.isna(phone) or pd.isna(address):
                return True  # 정보가 없으면 통과
            
            area_code = self.extract_area_code(phone)
            
            # 인터넷전화는 지역 제한 없음
            if area_code == '070':
                return True
            
            expected_regions = self.area_mapping.get(area_code, [])
            if not expected_regions:
                return True  # 알 수 없는 지역번호는 통과
            
            # 주소에서 지역명 찾기
            for region in expected_regions:
                if region in str(address):
                    return True
            
            # 충북 지역 특별 검사 (시/군/구 단위)
            if area_code == '043':
                chungbuk_cities = ['청주시', '청주', '충주시', '충주', '제천시', '제천', 
                                 '음성군', '음성', '진천군', '진천', '괴산군', '괴산', 
                                 '증평군', '증평', '영동군', '영동', '옥천군', '옥천', 
                                 '보은군', '보은', '단양군', '단양']
                for city in chungbuk_cities:
                    if city in str(address):
                        return True
            
            if org_name:
                self.logger.debug(f"🚫 지역 불일치: {org_name} - 전화:{area_code}({expected_regions}) vs 주소:{address}")
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 지역 일치성 검사 오류: {phone}, {address} - {e}")
            return True  # 오류 시 통과
    
    def validate_fax_number(self, fax_number: str, phone_number: str, address: str, org_name: str = "", strict: bool = False) -> Tuple[bool, str]:
        """
        팩스번호 종합 유효성 검증
        
        Args:
            fax_number: 팩스번호
            phone_number: 전화번호 (비교용)
            address: 주소
            org_name: 기관명
            strict: 엄격한 검증 여부
            
        Returns:
            Tuple[bool, str]: (유효성 여부, 검증 결과 메시지)
        """
        try:
            if not fax_number or pd.isna(fax_number):
                return False, "팩스번호 없음"
            
            normalized_fax = self.normalize_phone_number(fax_number)
            
            # 1. 형식 검증
            if not self.is_valid_phone_format(normalized_fax):
                return False, f"형식 검증 실패: {normalized_fax}"
            
            # 2. 전화번호와 비교 (있는 경우)
            if phone_number and not pd.isna(phone_number) and str(phone_number).strip():
                normalized_phone = self.normalize_phone_number(str(phone_number))
                
                if self.is_valid_phone_format(normalized_phone):
                    # 완전히 동일한 경우
                    if normalized_fax == normalized_phone:
                        if strict:
                            return False, f"전화번호와 동일: {normalized_fax}"
                        else:
                            return True, f"전화번호와 동일하지만 허용: {normalized_fax}"
                    
                    # 지역번호 일치성 검사
                    if not self.is_same_area_code(normalized_fax, normalized_phone):
                        if strict:
                            return False, f"지역번호 불일치: 팩스={self.extract_area_code(normalized_fax)}, 전화={self.extract_area_code(normalized_phone)}"
                    
                    # 유사성 검사
                    if self.are_numbers_too_similar(normalized_fax, normalized_phone):
                        if strict:
                            return False, f"번호 유사성: {normalized_fax} vs {normalized_phone}"
            
            # 3. 주소와 지역 일치성 검사
            if not self.is_regional_match(normalized_fax, address, org_name):
                if strict:
                    return False, f"주소-지역 불일치: {self.extract_area_code(normalized_fax)} vs {address}"
            
            return True, f"유효한 팩스번호: {normalized_fax}"
            
        except Exception as e:
            self.logger.error(f"❌ 팩스번호 검증 오류: {org_name} - {e}")
            return False, f"검증 오류: {str(e)}"
    
    def get_validation_summary(self, phone_list: list) -> Dict:
        """
        전화번호 목록 검증 요약
        
        Args:
            phone_list: 전화번호 목록
            
        Returns:
            Dict: 검증 요약 통계
        """
        try:
            total = len(phone_list)
            valid = 0
            invalid = 0
            area_distribution = {}
            
            for phone in phone_list:
                if self.is_valid_phone_format(phone):
                    valid += 1
                    area_code = self.extract_area_code(phone)
                    region = self.area_codes.get(area_code, "기타")
                    area_distribution[region] = area_distribution.get(region, 0) + 1
                else:
                    invalid += 1
            
            return {
                'total': total,
                'valid': valid,
                'invalid': invalid,
                'valid_rate': (valid / total * 100) if total > 0 else 0,
                'area_distribution': area_distribution
            }
            
        except Exception as e:
            self.logger.error(f"❌ 검증 요약 생성 오류: {e}")
            return {}


# 전역 함수들 (기존 코드와의 호환성을 위해)
def normalize_phone_simple(phone: str) -> str:
    """간단한 전화번호 정규화 (호환성 함수)"""
    validator = PhoneValidator()
    return validator.normalize_phone_number(phone)

def is_valid_phone_format_simple(phone: str) -> bool:
    """간단한 전화번호 형식 검사 (호환성 함수)"""
    validator = PhoneValidator()
    return validator.is_valid_phone_format(phone)

def is_regional_match_simple(phone: str, region: str) -> bool:
    """간단한 지역 일치성 검사 (호환성 함수)"""
    validator = PhoneValidator()
    return validator.is_regional_match(phone, region) 