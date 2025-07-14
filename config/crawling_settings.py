#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
크롤링 설정 통합 모듈
"""

import re
from typing import Dict, List

# 팩스번호 정규식 패턴
FAX_PATTERNS = [
    r'팩스[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
    r'fax[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
    r'F[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
    r'전송[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
    r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*팩스',
    r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*fax',
    r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*전송',
]

# 전화번호 정규식 패턴
PHONE_PATTERNS = [
    r'전화[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
    r'tel[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
    r'연락처[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
    r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
]

# 이메일 정규식 패턴
EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'

# 주소 정규식 패턴
ADDRESS_PATTERNS = [
    r'[서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충북|충남|전북|전남|경북|경남|제주].*?[시|군|구].*?[동|읍|면]',
    r'\d{5}.*?[시|군|구].*?[동|읍|면]',
]

# 제외할 번호 패턴
EXCLUDE_NUMBER_PATTERNS = [
    r'^\d{4}$',  # 4자리 숫자
    r'^\d{1,3}$',  # 1-3자리 숫자
    r'^\d{4}-\d{2}-\d{2}$',  # 날짜 형식
    r'^\d{6}$',  # 6자리 숫자
    r'^1[0-9]{3}$',  # 1000번대
    r'^[0-9]{1,4}$',  # 너무 짧은 번호
]

# 기본 Chrome 옵션
BASE_CHROME_OPTIONS = [
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--disable-blink-features=AutomationControlled',
    '--disable-extensions',
    '--mute-audio',
    '--no-first-run',
    '--disable-infobars',
    '--disable-notifications',
    '--disable-web-security',
    '--disable-features=VizDisplayCompositor',
    '--disable-ipc-flooding-protection',
    '--disable-background-timer-throttling',
    '--disable-backgrounding-occluded-windows',
    '--disable-renderer-backgrounding',
    '--disable-features=TranslateUI',
    '--disable-default-apps',
    '--disable-sync',
    '--memory-pressure-off',
    '--aggressive-cache-discard',
    '--max-unused-resource-memory-usage-percentage=5'
]

# 저사양 환경용 Chrome 옵션
LOW_SPEC_CHROME_OPTIONS = BASE_CHROME_OPTIONS + [
    '--disable-images',
    '--disable-plugins',
    '--disable-javascript',
    '--disable-application-cache',
    '--disk-cache-size=1',
    '--media-cache-size=1',
    '--window-size=800,600',
    '--max_old_space_size=128'
]

# 고성능 환경용 Chrome 옵션
HIGH_SPEC_CHROME_OPTIONS = BASE_CHROME_OPTIONS + [
    '--window-size=1920,1080',
    '--max_old_space_size=512',
    '--disk-cache-size=67108864',  # 64MB
    '--media-cache-size=67108864'
]

# User-Agent 목록
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

# 크롤링 지연 설정
CRAWLING_DELAYS = {
    'low_spec': {'min': 3.0, 'max': 5.0},
    'medium_spec': {'min': 2.0, 'max': 3.0},
    'high_spec': {'min': 1.0, 'max': 2.0},
    'ultra_spec': {'min': 0.5, 'max': 1.0}
}

# 타임아웃 설정
TIMEOUT_SETTINGS = {
    'page_load': 30,
    'implicit_wait': 10,
    'explicit_wait': 15,
    'request_timeout': 10
}

# 재시도 설정
RETRY_SETTINGS = {
    'max_retries': 3,
    'retry_delay': 2.0,
    'backoff_factor': 1.5
}

# 배치 크기 설정
BATCH_SIZES = {
    'low_spec': 2,
    'medium_spec': 5,
    'high_spec': 8,
    'ultra_spec': 15
}

# 메모리 정리 간격 설정
MEMORY_CLEANUP_INTERVALS = {
    'low_spec': 10,
    'medium_spec': 20,
    'high_spec': 50,
    'ultra_spec': 100
}

# 구글 검색 설정
GOOGLE_SEARCH_SETTINGS = {
    'base_url': 'https://www.google.com/search',
    'max_results': 5,
    'search_delay': {'min': 2.0, 'max': 4.0},
    'result_selectors': [
        'div.g',
        'div[data-ved]',
        '.rc',
        '.tF2Cxc'
    ],
    'link_selectors': [
        'h3 a',
        'a[href*="http"]',
        '.yuRUbf a'
    ]
}

# 기관 유형별 설정
INSTITUTION_TYPES = {
    'academy': {
        'name': '아동센터',
        'search_keywords': ['아동센터', '지역아동센터', '방과후', '돌봄'],
        'exclude_keywords': ['폐원', '폐쇄', '중단']
    },
    'community_center': {
        'name': '주민센터',
        'search_keywords': ['주민센터', '행정복지센터', '동사무소'],
        'exclude_keywords': ['폐쇄', '통합']
    },
    'church': {
        'name': '교회',
        'search_keywords': ['교회', '성당', '절', '교단'],
        'exclude_keywords': ['폐쇄', '해산']
    }
}

class CrawlingSettings:
    """크롤링 설정 관리 클래스"""
    
    def __init__(self):
        """크롤링 설정 초기화"""
        self.fax_patterns = FAX_PATTERNS
        self.phone_patterns = PHONE_PATTERNS
        self.email_pattern = EMAIL_PATTERN
        self.address_patterns = ADDRESS_PATTERNS
        self.exclude_patterns = EXCLUDE_NUMBER_PATTERNS
        self.user_agents = USER_AGENTS
        self.crawling_delays = CRAWLING_DELAYS
        self.timeout_settings = TIMEOUT_SETTINGS
        self.retry_settings = RETRY_SETTINGS
        self.batch_sizes = BATCH_SIZES
        self.memory_cleanup_intervals = MEMORY_CLEANUP_INTERVALS
        self.google_search_settings = GOOGLE_SEARCH_SETTINGS
        self.institution_types = INSTITUTION_TYPES
    
    def get_chrome_options(self, spec_level: str = 'medium_spec') -> List[str]:
        """
        사양별 Chrome 옵션 반환
        
        Args:
            spec_level: 사양 레벨 ('low_spec', 'medium_spec', 'high_spec', 'ultra_spec')
            
        Returns:
            List[str]: Chrome 옵션 목록
        """
        if spec_level == 'low_spec':
            return LOW_SPEC_CHROME_OPTIONS.copy()
        elif spec_level in ['high_spec', 'ultra_spec']:
            return HIGH_SPEC_CHROME_OPTIONS.copy()
        else:
            return BASE_CHROME_OPTIONS.copy()
    
    def get_crawling_delay(self, spec_level: str = 'medium_spec') -> Dict[str, float]:
        """
        사양별 크롤링 지연 시간 반환
        
        Args:
            spec_level: 사양 레벨
            
        Returns:
            Dict[str, float]: 최소/최대 지연 시간
        """
        return self.crawling_delays.get(spec_level, self.crawling_delays['medium_spec'])
    
    def get_batch_size(self, spec_level: str = 'medium_spec') -> int:
        """
        사양별 배치 크기 반환
        
        Args:
            spec_level: 사양 레벨
            
        Returns:
            int: 배치 크기
        """
        return self.batch_sizes.get(spec_level, self.batch_sizes['medium_spec'])
    
    def get_memory_cleanup_interval(self, spec_level: str = 'medium_spec') -> int:
        """
        사양별 메모리 정리 간격 반환
        
        Args:
            spec_level: 사양 레벨
            
        Returns:
            int: 메모리 정리 간격 (초)
        """
        return self.memory_cleanup_intervals.get(spec_level, self.memory_cleanup_intervals['medium_spec'])
    
    def get_institution_settings(self, institution_type: str) -> Dict:
        """
        기관 유형별 설정 반환
        
        Args:
            institution_type: 기관 유형
            
        Returns:
            Dict: 기관 설정 정보
        """
        return self.institution_types.get(institution_type, {})
    
    def validate_phone_number(self, phone: str) -> bool:
        """
        전화번호 유효성 검사
        
        Args:
            phone: 전화번호
            
        Returns:
            bool: 유효성 여부
        """
        if not phone:
            return False
        
        # 숫자만 추출
        digits = re.sub(r'[^\d]', '', phone)
        
        # 길이 체크
        if len(digits) < 8 or len(digits) > 11:
            return False
        
        # 제외 패턴 체크
        for pattern in self.exclude_patterns:
            if re.match(pattern, digits):
                return False
        
        return True
    
    def extract_numbers_from_text(self, text: str, pattern_type: str = 'phone') -> List[str]:
        """
        텍스트에서 번호 추출
        
        Args:
            text: 텍스트
            pattern_type: 패턴 유형 ('phone', 'fax')
            
        Returns:
            List[str]: 추출된 번호 목록
        """
        if not text:
            return []
        
        patterns = self.fax_patterns if pattern_type == 'fax' else self.phone_patterns
        numbers = []
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    number = match[0] if match[0] else match[1] if len(match) > 1 else ""
                else:
                    number = match
                
                if number and self.validate_phone_number(number):
                    numbers.append(number)
        
        return list(set(numbers))
    
    def get_search_query(self, org_name: str, institution_type: str, address: str = "") -> str:
        """
        검색 쿼리 생성
        
        Args:
            org_name: 기관명
            institution_type: 기관 유형
            address: 주소
            
        Returns:
            str: 검색 쿼리
        """
        query_parts = [org_name]
        
        # 기관 유형별 키워드 추가
        institution_settings = self.get_institution_settings(institution_type)
        if institution_settings and 'search_keywords' in institution_settings:
            query_parts.extend(institution_settings['search_keywords'][:2])  # 최대 2개
        
        # 주소 추가 (첫 번째 부분만)
        if address:
            address_parts = address.split()
            if address_parts:
                query_parts.append(address_parts[0])
        
        return " ".join(query_parts)
    
    def should_exclude_result(self, text: str, institution_type: str) -> bool:
        """
        검색 결과 제외 여부 판단
        
        Args:
            text: 검색 결과 텍스트
            institution_type: 기관 유형
            
        Returns:
            bool: 제외 여부
        """
        if not text:
            return False
        
        institution_settings = self.get_institution_settings(institution_type)
        if institution_settings and 'exclude_keywords' in institution_settings:
            exclude_keywords = institution_settings['exclude_keywords']
            
            for keyword in exclude_keywords:
                if keyword in text:
                    return True
        
        return False


# 전역 설정 인스턴스
settings = CrawlingSettings()

# 편의 함수들
def get_fax_patterns():
    """팩스번호 패턴 반환"""
    return FAX_PATTERNS

def get_phone_patterns():
    """전화번호 패턴 반환"""
    return PHONE_PATTERNS

def get_chrome_options(spec_level='medium_spec'):
    """Chrome 옵션 반환"""
    return settings.get_chrome_options(spec_level)

def get_user_agents():
    """User-Agent 목록 반환"""
    return USER_AGENTS 