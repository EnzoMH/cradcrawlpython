#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Institution Finder v2 - 검색어 로직 개선 버전
failed_data_250715.xlsx의 H열(전화번호 기관명)과 J열(팩스번호 기관명) 채우기

개선사항:
- 자연스러운 검색어 형태로 수정 (따옴표 제거)
- 더욱 효과적인 검색 패턴 적용
- 기존 utils/config 모듈 완전 활용

작성자: AI Assistant
작성일: 2025-01-16
업데이트: 검색어 로직 개선
"""

import pandas as pd
import numpy as np
import time
import random
import re
import os
import sys
import logging
import gc
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
import threading
from dataclasses import dataclass, field
import traceback

# 외부 라이브러리 imports
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

# 기존 모듈들 import
from utils.web_driver_manager import WebDriverManager
from utils.google_search_engine import GoogleSearchEngine
from utils.phone_validator import PhoneValidator
from utils.worker_manager import WorkerManager
from utils.excel_processor import ExcelProcessor
from utils.data_mapper import DataMapper
from utils.verification_engine import VerificationEngine
from config.performance_profiles import PerformanceManager
from config.crawling_settings import CrawlingSettings

# 로깅 설정
def setup_logging():
    """로깅 시스템 설정"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
    
    # 파일 핸들러
    file_handler = logging.FileHandler(f'enhanced_finder_v2_{timestamp}.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

@dataclass
class SearchResult:
    """개별 검색 결과"""
    row_index: int
    phone_number: str = ""
    fax_number: str = ""
    found_phone_institution: str = ""
    found_fax_institution: str = ""
    phone_success: bool = False
    fax_success: bool = False
    processing_time: float = 0.0
    error_message: str = ""
    search_queries_used: List[str] = field(default_factory=list)

class ImprovedSearchEngine:
    """개선된 검색 엔진 - 자연스러운 검색어 적용"""
    
    def __init__(self, logger=None):
        """
        개선된 검색 엔진 초기화
        
        Args:
            logger: 로깅 객체
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # 기존 GoogleSearchEngine 활용
        self.google_search_engine = GoogleSearchEngine(self.logger)
        
        # 기관명 추출 패턴
        self.institution_patterns = [
            r'([\w\s]*(?:센터|기관|청|구청|시청|군청|면사무소|읍사무소|동|주민센터|행정복지센터)[\w\s]*)',
            r'([\w\s]*(?:복지관|보건소|보건지소|병원|의원|클리닉|한의원)[\w\s]*)',
            r'([\w\s]*(?:학교|대학교|대학|학원|교육원|교육청)[\w\s]*)',
            r'([\w\s]*(?:협회|단체|재단|법인|공단|공사|회|조합)[\w\s]*)',
            r'([\w\s]*(?:교회|성당|절|사찰|종교시설)[\w\s]*)',
        ]
        
        # 제외 키워드
        self.exclude_keywords = [
            '광고', '배너', '클릭', '링크', '바로가기', '사이트맵',
            '검색결과', '네이버', '다음', '구글', '야후', '카카오',
            'COM', 'co.kr', 'www', 'http', 'https', '.com', '.kr',
            '옥션원모바일', '스팸', '홍보', '마케팅', '업체'
        ]
        
        self.logger.info("🔍 개선된 검색 엔진 초기화 완료")
    
    def create_natural_queries(self, number: str, number_type: str = "전화") -> List[str]:
        """
        자연스러운 검색 쿼리 생성 (따옴표 제거)
        
        Args:
            number: 전화번호 또는 팩스번호
            number_type: "전화" 또는 "팩스"
            
        Returns:
            List[str]: 우선순위별 검색 쿼리 목록
        """
        queries = []
        
        # 🎯 핵심 자연어 검색 쿼리 (최우선 - 따옴표 제거)
        if number_type == "전화":
            priority_queries = [
                f'{number} 은 어디전화번호',
                f'{number} 어디전화번호',
                f'{number} 은 어디 전화번호',
                f'{number} 어디 전화번호',
                f'{number} 전화번호 어디',
                f'{number} 는 어디전화번호',
                f'{number} 전화 어디',
            ]
        else:  # 팩스
            priority_queries = [
                f'{number} 은 어디팩스번호',
                f'{number} 어디팩스번호',
                f'{number} 은 어디 팩스번호',
                f'{number} 어디 팩스번호',
                f'{number} 팩스번호 어디',
                f'{number} 는 어디팩스번호',
                f'{number} 팩스 어디',
            ]
        
        # 우선순위 쿼리 먼저 추가
        queries.extend(priority_queries)
        
        # 🔍 정확한 매칭 검색어 (따옴표 사용)
        if number_type == "전화":
            exact_queries = [
                f'"{number}" 전화번호 기관',
                f'"{number}" 연락처 어디',
                f'"{number}" 기관명',
                f'"{number}" 전화 기관',
                f'전화번호 "{number}" 어디',
            ]
        else:  # 팩스
            exact_queries = [
                f'"{number}" 팩스번호 기관',
                f'"{number}" fax 어디',
                f'"{number}" 기관명',
                f'"{number}" 팩스 기관',
                f'팩스번호 "{number}" 어디',
            ]
        
        queries.extend(exact_queries)
        
        # 🏢 지역별 검색 강화
        area_code = number.split('-')[0] if '-' in number else number[:3]
        area_names = self._get_area_names(area_code)
        
        for area in area_names[:2]:  # 상위 2개 지역만
            if number_type == "전화":
                queries.extend([
                    f'{area} {number} 전화번호',
                    f'{number} {area} 기관',
                    f'{area} {number} 연락처',
                ])
            else:
                queries.extend([
                    f'{area} {number} 팩스번호',
                    f'{number} {area} 기관',
                    f'{area} {number} 팩스',
                ])
        
        # 🏛️ 공식 사이트 우선 검색
        official_queries = [
            f'"{number}" site:go.kr',
            f'"{number}" site:or.kr',
            f'{number} 공식 홈페이지',
            f'{number} 관공서',
        ]
        
        queries.extend(official_queries)
        
        # 📞 기관 유형별 검색
        institution_types = ['주민센터', '구청', '보건소', '복지관', '센터', '기관']
        for inst_type in institution_types[:3]:  # 상위 3개만
            if number_type == "전화":
                queries.append(f'{number} {inst_type} 전화')
            else:
                queries.append(f'{number} {inst_type} 팩스')
        
        return queries[:20]  # 상위 20개만 반환
    
    def _get_area_names(self, area_code: str) -> List[str]:
        """지역번호 기반 지역명 반환"""
        area_mapping = {
            "02": ["서울", "서울특별시", "서울시"],
            "031": ["경기", "경기도", "수원", "성남", "안양"],
            "032": ["인천", "인천광역시", "인천시"],
            "033": ["강원", "강원도", "춘천", "원주"],
            "041": ["충남", "충청남도", "천안", "아산"],
            "042": ["대전", "대전광역시", "대전시"],
            "043": ["충북", "충청북도", "청주", "충주"],
            "044": ["세종", "세종특별자치시", "세종시"],
            "051": ["부산", "부산광역시", "부산시"],
            "052": ["울산", "울산광역시", "울산시"],
            "053": ["대구", "대구광역시", "대구시"],
            "054": ["경북", "경상북도", "포항", "구미"],
            "055": ["경남", "경상남도", "창원", "마산"],
            "061": ["전남", "전라남도", "목포", "여수"],
            "062": ["광주", "광주광역시", "광주시"],
            "063": ["전북", "전라북도", "전주", "익산"],
            "064": ["제주", "제주특별자치도", "제주시"],
        }
        
        return area_mapping.get(area_code, [])
    
    def search_institution_by_number(self, driver, number: str, number_type: str = "전화") -> Optional[str]:
        """
        번호로 기관명 검색 (개선된 로직)
        
        Args:
            driver: WebDriver 인스턴스
            number: 전화번호 또는 팩스번호
            number_type: "전화" 또는 "팩스"
            
        Returns:
            Optional[str]: 발견된 기관명 또는 None
        """
        try:
            self.logger.info(f"🔍 {number_type}번호 기관명 검색 시작: {number}")
            
            # 자연스러운 검색 쿼리 생성
            search_queries = self.create_natural_queries(number, number_type)
            
            # 사용된 쿼리 로깅
            self.logger.debug(f"📝 생성된 검색어 예시: {search_queries[:3]}")
            
            # 각 쿼리별로 검색 시도 (우선순위 순)
            for query_idx, query in enumerate(search_queries[:12]):  # 상위 12개만 시도
                try:
                    self.logger.debug(f"🔎 쿼리 {query_idx + 1}: {query}")
                    
                    # 구글 검색 실행
                    page_source = self._perform_search(driver, query)
                    
                    if page_source:
                        # 기관명 추출
                        institution_name = self._extract_institution_name(page_source, number)
                        if institution_name:
                            self.logger.info(f"✅ 기관명 발견: {institution_name} (쿼리: {query})")
                            return institution_name
                    
                    # 검색 간 지연 (봇 감지 방지)
                    delay = random.uniform(1.5, 3.0)
                    # 우선순위 쿼리는 짧은 지연, 나머지는 긴 지연
                    if query_idx >= 7:  # 7번째 쿼리부터 더 긴 지연
                        delay = random.uniform(2.5, 4.0)
                    
                    time.sleep(delay)
                    
                except Exception as e:
                    self.logger.debug(f"⚠️ 쿼리 검색 실패: {query} - {e}")
                    continue
            
            self.logger.warning(f"❌ 모든 쿼리 실패: {number} ({number_type})")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ {number_type}번호 검색 오류: {number} - {e}")
            return None
    
    def _perform_search(self, driver, query: str) -> Optional[str]:
        """구글 검색 수행 (개선된 로직)"""
        try:
            # 구글 검색 페이지로 이동
            driver.get('https://www.google.com')
            time.sleep(random.uniform(1.0, 2.0))
            
            # 검색창 찾기
            search_box = WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            
            # 검색어 입력 (더 자연스럽게)
            search_box.clear()
            time.sleep(random.uniform(0.3, 0.7))
            
            # 한글자씩 입력하는 것처럼 (봇 감지 회피)
            for char in query:
                search_box.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
            
            time.sleep(random.uniform(0.5, 1.0))
            
            # 검색 실행
            search_box.send_keys(Keys.RETURN)
            
            # 검색 결과 로딩 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            
            time.sleep(random.uniform(2.0, 3.5))
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            
            # 검색 결과 확인
            if any(phrase in page_source for phrase in ["검색결과가 없습니다", "검색 결과 없음", "관련 검색결과가 없습니다"]):
                self.logger.debug(f"검색 결과 없음: {query}")
                return None
            
            # 봇 감지 확인
            if any(phrase in page_source.lower() for phrase in ["unusual traffic", "recaptcha", "suspicious activity"]):
                self.logger.warning(f"⚠️ 봇 감지 가능성 - 대기: {query}")
                time.sleep(random.uniform(8.0, 15.0))
                return None
            
            # 실제 검색이 수행되었는지 확인
            if len(page_source) < 5000:  # 너무 짧은 응답은 오류 가능성
                self.logger.debug(f"페이지 응답이 너무 짧음: {len(page_source)} bytes")
                return None
            
            return page_source
            
        except Exception as e:
            self.logger.debug(f"구글 검색 실패: {query} - {e}")
            return None
    
    def _extract_institution_name(self, page_source: str, number: str) -> Optional[str]:
        """페이지에서 기관명 추출 (강화된 로직)"""
        try:
            soup = BeautifulSoup(page_source, 'html.parser')
            text_content = soup.get_text()
            
            self.logger.debug(f"📄 페이지 텍스트 길이: {len(text_content)}자")
            
            # 1. 번호 주변 텍스트에서 기관명 찾기 (최우선)
            lines = text_content.split('\n')
            for i, line in enumerate(lines):
                if number in line:
                    self.logger.debug(f"🎯 번호 발견 라인: {line.strip()[:100]}...")
                    
                    # 주변 라인들과 함께 분석 (더 넓은 범위)
                    context_lines = lines[max(0, i-5):i+6]
                    context_text = ' '.join(context_lines)
                    
                    # 컨텍스트에서 기관명 찾기
                    institution_name = self._find_institution_in_context(context_text, number)
                    if institution_name:
                        return institution_name
            
            # 2. 제목이나 헤더에서 기관명 우선 찾기
            for tag in ['title', 'h1', 'h2', 'h3']:
                elements = soup.find_all(tag)
                for element in elements:
                    text = element.get_text().strip()
                    cleaned_name = self._clean_institution_name(text)
                    if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                        self.logger.debug(f"🏢 헤더에서 기관명 발견: {cleaned_name}")
                        return cleaned_name
            
            # 3. 전체 텍스트에서 기관명 패턴 검색
            for pattern in self.institution_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                for match in matches[:5]:  # 상위 5개만 확인
                    cleaned_name = self._clean_institution_name(match)
                    if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                        self.logger.debug(f"🏢 패턴 매칭 기관명: {cleaned_name}")
                        return cleaned_name
            
            return None
            
        except Exception as e:
            self.logger.debug(f"기관명 추출 실패: {e}")
            return None
    
    def _find_institution_in_context(self, context_text: str, number: str) -> Optional[str]:
        """컨텍스트 텍스트에서 기관명 찾기 (개선된 로직)"""
        try:
            # 주요 기관명 키워드들 (우선순위별)
            priority_keywords = ['주민센터', '행정복지센터', '구청', '시청', '군청']
            secondary_keywords = ['센터', '기관', '청', '동', '복지관', '보건소', '보건지소', '병원', '의원']
            
            # 단어 단위로 분리
            words = context_text.split()
            
            # 우선순위 키워드부터 찾기
            for keyword_list in [priority_keywords, secondary_keywords]:
                for i, word in enumerate(words):
                    for keyword in keyword_list:
                        if keyword in word:
                            # 주변 단어들과 결합하여 완전한 기관명 구성
                            start_idx = max(0, i-4)  # 더 넓은 범위
                            end_idx = min(len(words), i+5)
                            candidate = ' '.join(words[start_idx:end_idx])
                            
                            # 기관명 정제
                            cleaned_name = self._clean_institution_name(candidate)
                            if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                                return cleaned_name
            
            return None
            
        except Exception as e:
            self.logger.debug(f"컨텍스트 기관명 찾기 실패: {e}")
            return None
    
    def _clean_institution_name(self, candidate: str) -> Optional[str]:
        """기관명 후보 정제 (개선된 로직)"""
        try:
            if not candidate:
                return None
            
            # 불필요한 문자 제거 (더 정교하게)
            cleaned = re.sub(r'[^\w\s가-힣()]', ' ', candidate)
            cleaned = re.sub(r'\s+', ' ', cleaned)  # 연속 공백 제거
            cleaned = cleaned.strip()
            
            # 길이 검증
            if len(cleaned) < 3 or len(cleaned) > 60:
                return None
            
            # 숫자로만 이루어진 경우 제외
            if cleaned.isdigit():
                return None
            
            # 기관명 키워드 포함 여부 확인 (더 포괄적)
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '군청', '동', '주민센터',
                '행정복지센터', '복지관', '보건소', '보건지소', '병원', '의원', 
                '학교', '대학', '협회', '단체', '재단', '법인', '공단', '공사',
                '교회', '성당', '절', '사찰'
            ]
            
            has_keyword = any(keyword in cleaned for keyword in institution_keywords)
            if not has_keyword:
                return None
            
            # 기관명에서 핵심 부분만 추출 (키워드 중심으로)
            for keyword in institution_keywords:
                if keyword in cleaned:
                    # 키워드를 포함한 적절한 길이의 기관명 추출
                    parts = cleaned.split()
                    keyword_idx = -1
                    
                    for idx, part in enumerate(parts):
                        if keyword in part:
                            keyword_idx = idx
                            break
                    
                    if keyword_idx >= 0:
                        # 키워드 앞뒤 적절한 범위의 단어들 결합
                        start = max(0, keyword_idx - 2)
                        end = min(len(parts), keyword_idx + 3)
                        final_name = ' '.join(parts[start:end])
                        
                        if len(final_name) >= 3:
                            return final_name
            
            return cleaned if len(cleaned) >= 3 else None
            
        except Exception as e:
            self.logger.debug(f"기관명 정제 실패: {e}")
            return None
    
    def _is_valid_institution_name(self, name: str, number: str) -> bool:
        """유효한 기관명인지 검증 (강화된 로직)"""
        try:
            if not name or len(name.strip()) < 3:
                return False
            
            name = name.strip()
            
            # 제외 키워드 체크 (더 엄격하게)
            for exclude in self.exclude_keywords:
                if exclude.lower() in name.lower():
                    return False
            
            # 숫자만 있는 경우 제외
            if name.isdigit() or re.match(r'^\d+$', name):
                return False
            
            # 번호 자체가 포함된 경우 제외
            if number in name or name in number:
                return False
            
            # 너무 긴 경우 제외
            if len(name) > 60:
                return False
            
            # URL이나 이메일 형태 제외
            if any(pattern in name.lower() for pattern in ['http', 'www', '.com', '.kr', '@']):
                return False
            
            # 기관명 패턴 포함 여부 확인
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '군청', '동', '주민센터', 
                '행정복지센터', '복지관', '보건소', '보건지소', '병원', '의원',
                '학교', '대학', '협회', '단체', '재단', '법인'
            ]
            
            has_institution_keyword = any(keyword in name for keyword in institution_keywords)
            
            # 키워드가 있고, 적절한 길이인 경우만 유효
            return has_institution_keyword and 3 <= len(name) <= 60
            
        except Exception as e:
            self.logger.debug(f"기관명 검증 실패: {e}")
            return False

class EnhancedInstitutionProcessor:
    """강화된 기관명 추출 메인 처리기 v2"""
    
    def __init__(self, max_workers: int = 10, batch_size: int = 350):
        """
        메인 처리기 초기화
        
        Args:
            max_workers: 최대 워커 수 (기본값: 10)
            batch_size: 배치 크기 (기본값: 350)
        """
        self.logger = logging.getLogger(__name__)
        self.max_workers = max_workers
        self.batch_size = batch_size
        
        # 기존 모듈들 초기화
        self.performance_manager = PerformanceManager(self.logger)
        self.crawling_settings = CrawlingSettings()
        self.web_driver_manager = WebDriverManager(self.logger)
        self.phone_validator = PhoneValidator(self.logger)
        self.excel_processor = ExcelProcessor(self.logger)
        self.data_mapper = DataMapper(self.logger)
        self.verification_engine = VerificationEngine()
        
        # 개선된 검색 엔진
        self.search_engine = ImprovedSearchEngine(self.logger)
        
        # 워커별 드라이버 관리
        self.worker_drivers = {}
        self.lock = threading.Lock()
        
        # 통계
        self.total_rows = 0
        self.processed_count = 0
        self.phone_success = 0
        self.fax_success = 0
        
        self.logger.info(f"🚀 개선된 기관명 추출 프로세서 v2 초기화 완료")
        self.logger.info(f"⚙️  설정: 워커 {max_workers}개, 배치 {batch_size}개")
        self.logger.info(f"🔍 검색어 개선: 자연스러운 형태 적용")
    
    def load_data(self, filepath: str) -> pd.DataFrame:
        """Excel 파일 로드 및 전처리"""
        try:
            # ExcelProcessor 활용
            success = self.excel_processor.load_excel_file(filepath)
            if not success:
                raise ValueError(f"파일 로드 실패: {filepath}")
            
            df = self.excel_processor.df
            self.logger.info(f"📊 데이터 로드 완료: {len(df)}행 × {len(df.columns)}열")
            self.logger.info(f"📋 컬럼: {list(df.columns)}")
            
            # 컬럼 확인 및 정보 출력
            if len(df.columns) >= 10:
                phone_col = df.columns[6]      # G열 (전화번호)
                phone_result_col = df.columns[7]  # H열 (전화번호 기관명)
                fax_col = df.columns[8]        # I열 (팩스번호)
                fax_result_col = df.columns[9]   # J열 (팩스번호 기관명)
                
                self.logger.info(f"🎯 처리 대상:")
                self.logger.info(f"   - {phone_col} (G열) → {phone_result_col} (H열)")
                self.logger.info(f"   - {fax_col} (I열) → {fax_result_col} (J열)")
                
                # 빈 값 통계
                phone_empty = df.iloc[:, 7].isna().sum()
                fax_empty = df.iloc[:, 9].isna().sum()
                total_empty = phone_empty + fax_empty
                self.logger.info(f"📈 빈 값 현황: H열 {phone_empty}개, J열 {fax_empty}개 (총 {total_empty}개)")
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def process_single_row(self, row_data: Tuple[int, pd.Series], worker_id: int) -> SearchResult:
        """개별 행 처리 (전화번호와 팩스번호 모두)"""
        row_idx, row = row_data
        result = SearchResult(row_index=row_idx)
        start_time = time.time()
        
        try:
            # 컬럼 인덱스로 접근
            phone_number = str(row.iloc[6]).strip() if len(row) > 6 else ""  # G열
            fax_number = str(row.iloc[8]).strip() if len(row) > 8 else ""    # I열
            
            # 기존 결과 확인 (이미 채워진 경우 스킵)
            existing_phone_result = str(row.iloc[7]).strip() if len(row) > 7 else ""  # H열
            existing_fax_result = str(row.iloc[9]).strip() if len(row) > 9 else ""    # J열
            
            result.phone_number = phone_number
            result.fax_number = fax_number
            
            self.logger.info(f"📋 워커 {worker_id}: 행 {row_idx+1} 처리 시작")
            
            # 드라이버 가져오기
            driver = self._get_worker_driver(worker_id)
            if not driver:
                result.error_message = "드라이버 생성 실패"
                return result
            
            processed_items = []
            
            # 전화번호 처리 (H열이 비어있는 경우만)
            if (phone_number and phone_number != 'nan' and 
                (not existing_phone_result or existing_phone_result == 'nan') and
                self.phone_validator.is_valid_phone_format(phone_number)):
                
                self.logger.info(f"📞 워커 {worker_id}: 전화번호 {phone_number} 검색 시작")
                institution = self.search_engine.search_institution_by_number(driver, phone_number, "전화")
                
                if institution:
                    result.found_phone_institution = institution
                    result.phone_success = True
                    processed_items.append(f"전화({institution})")
                    self.logger.info(f"✅ 전화번호 성공: {institution}")
                else:
                    processed_items.append("전화(실패)")
                    self.logger.warning(f"⚠️ 전화번호 결과 없음")
            else:
                if existing_phone_result and existing_phone_result != 'nan':
                    processed_items.append("전화(기존)")
                else:
                    processed_items.append("전화(스킵)")
            
            # 팩스번호 처리 (J열이 비어있는 경우만)
            if (fax_number and fax_number != 'nan' and 
                (not existing_fax_result or existing_fax_result == 'nan') and
                self.phone_validator.is_valid_phone_format(fax_number)):
                
                self.logger.info(f"📠 워커 {worker_id}: 팩스번호 {fax_number} 검색 시작")
                institution = self.search_engine.search_institution_by_number(driver, fax_number, "팩스")
                
                if institution:
                    result.found_fax_institution = institution
                    result.fax_success = True
                    processed_items.append(f"팩스({institution})")
                    self.logger.info(f"✅ 팩스번호 성공: {institution}")
                else:
                    processed_items.append("팩스(실패)")
                    self.logger.warning(f"⚠️ 팩스번호 결과 없음")
            else:
                if existing_fax_result and existing_fax_result != 'nan':
                    processed_items.append("팩스(기존)")
                else:
                    processed_items.append("팩스(스킵)")
            
            result.processing_time = time.time() - start_time
            
            # 처리 결과 로깅
            self.logger.info(f"🎯 워커 {worker_id}: 행 {row_idx+1} 완료 - {', '.join(processed_items)} ({result.processing_time:.1f}초)")
            
            return result
            
        except Exception as e:
            result.error_message = str(e)
            result.processing_time = time.time() - start_time
            self.logger.error(f"❌ 워커 {worker_id}: 행 {row_idx+1} 처리 오류 - {e}")
            return result
    
    def _get_worker_driver(self, worker_id: int):
        """워커별 드라이버 가져오기 (기존 WebDriverManager 활용)"""
        # 기존 드라이버 상태 확인
        if worker_id in self.worker_drivers:
            try:
                driver = self.worker_drivers[worker_id]
                driver.current_url  # 상태 확인
                return driver
            except Exception as e:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 기존 드라이버 비정상 - {e}")
                # 비정상 드라이버 정리
                try:
                    self.worker_drivers[worker_id].quit()
                except:
                    pass
                del self.worker_drivers[worker_id]
        
        # 새 드라이버 생성 (WebDriverManager 활용)
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                driver = self.web_driver_manager.create_bot_evasion_driver(worker_id)
                if driver:
                    self.worker_drivers[worker_id] = driver
                    self.logger.info(f"✅ 워커 {worker_id}: 새 드라이버 할당 성공 ({attempt+1}차)")
                    return driver
                else:
                    self.logger.warning(f"⚠️ 워커 {worker_id}: 드라이버 생성 실패 ({attempt+1}차)")
                    
            except Exception as e:
                self.logger.error(f"❌ 워커 {worker_id}: 드라이버 생성 오류 ({attempt+1}차) - {e}")
            
            if attempt < max_attempts - 1:
                wait_time = (attempt + 1) * 3
                self.logger.info(f"⏱️ 워커 {worker_id}: {wait_time}초 대기 후 재시도")
                time.sleep(wait_time)
        
        self.logger.error(f"❌ 워커 {worker_id}: 모든 드라이버 생성 시도 실패")
        return None
    
    def process_file(self, input_filepath: str) -> str:
        """파일 전체 처리 (배치별 병렬 처리)"""
        try:
            # 시스템 정보 출력
            self.performance_manager.display_performance_info()
            
            # 데이터 로드
            df = self.load_data(input_filepath)
            self.total_rows = len(df)
            
            self.logger.info(f"🚀 처리 시작: {len(df)}행")
            self.logger.info(f"⚙️  설정: 워커 {self.max_workers}개, 배치 {self.batch_size}개")
            
            # 모든 결과 저장
            all_results = {}
            
            # 배치별로 처리
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            
            for batch_start in range(0, len(df), self.batch_size):
                batch_end = min(batch_start + self.batch_size, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                
                batch_num = (batch_start // self.batch_size) + 1
                
                self.logger.info(f"📦 배치 {batch_num}/{total_batches} 처리: {batch_start+1}~{batch_end} ({len(batch_df)}개)")
                
                # 배치 내 병렬 처리
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    
                    # 워커에 작업 할당
                    for idx, (original_idx, row) in enumerate(batch_df.iterrows()):
                        worker_id = idx % self.max_workers
                        future = executor.submit(self.process_single_row, (original_idx, row), worker_id)
                        futures.append((future, original_idx))
                    
                    # 결과 수집
                    for future, row_idx in futures:
                        try:
                            result = future.result(timeout=300)  # 5분 타임아웃
                            all_results[row_idx] = result
                            
                            # 통계 업데이트
                            with self.lock:
                                self.processed_count += 1
                                if result.phone_success:
                                    self.phone_success += 1
                                if result.fax_success:
                                    self.fax_success += 1
                            
                            # 진행률 출력
                            if self.processed_count % 50 == 0:
                                progress = (self.processed_count / self.total_rows) * 100
                                self.logger.info(f"📊 진행률: {self.processed_count}/{self.total_rows} ({progress:.1f}%) - 전화:{self.phone_success}, 팩스:{self.fax_success}")
                            
                        except Exception as e:
                            self.logger.error(f"❌ 행 {row_idx+1} 결과 처리 오류: {e}")
                
                # 배치 완료 후 시스템 리소스 체크 및 조정
                current_resources = self.performance_manager.get_current_resources()
                adjustment = self.performance_manager.adjust_performance_dynamically(current_resources)
                if adjustment.get('adjusted'):
                    self.logger.info(f"⚙️  시스템 조정: {adjustment.get('reason')}")
                
                # 배치 간 휴식
                if batch_end < len(df):
                    rest_time = random.uniform(4.0, 8.0)
                    self.logger.info(f"⏱️ 배치 {batch_num} 완료 - {rest_time:.1f}초 휴식")
                    time.sleep(rest_time)
            
            # 결과를 DataFrame에 반영
            updated_count = 0
            for row_idx, result in all_results.items():
                if result.phone_success and len(df.columns) > 7:
                    df.iloc[row_idx, 7] = result.found_phone_institution  # H열
                    updated_count += 1
                if result.fax_success and len(df.columns) > 9:
                    df.iloc[row_idx, 9] = result.found_fax_institution    # J열
                    updated_count += 1
            
            self.logger.info(f"📝 총 {updated_count}개 셀 업데이트 완료")
            
            # 결과 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"enhanced_failed_data_v2_{timestamp}.xlsx"
            
            # ExcelProcessor로 저장
            save_success = self.excel_processor.save_excel(df, output_file)
            if not save_success:
                # 백업 저장 방법
                df.to_excel(output_file, index=False)
                self.logger.info(f"📁 백업 방법으로 저장 완료: {output_file}")
            
            # 최종 통계 출력
            self._print_final_statistics()
            
            self.logger.info(f"🎉 모든 처리 완료! 결과 파일: {output_file}")
            return output_file
            
        except Exception as e:
            self.logger.error(f"❌ 파일 처리 실패: {e}")
            self.logger.error(traceback.format_exc())
            raise
        finally:
            # 모든 드라이버 정리
            self._cleanup_drivers()
    
    def _cleanup_drivers(self):
        """모든 드라이버 정리"""
        try:
            self.logger.info("🧹 드라이버 정리 시작")
            for worker_id, driver in self.worker_drivers.items():
                try:
                    driver.quit()
                    self.logger.info(f"✅ 워커 {worker_id} 드라이버 정리 완료")
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 드라이버 정리 실패: {e}")
            
            self.worker_drivers.clear()
            gc.collect()
            self.logger.info("🧹 드라이버 정리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 드라이버 정리 오류: {e}")
    
    def _print_final_statistics(self):
        """최종 통계 출력"""
        self.logger.info("=" * 70)
        self.logger.info("📊 최종 처리 통계 (개선된 검색어 적용)")
        self.logger.info("=" * 70)
        self.logger.info(f"전체 행 수: {self.total_rows:,}")
        self.logger.info(f"처리 완료: {self.processed_count:,}")
        self.logger.info(f"전화번호 성공: {self.phone_success:,}")
        self.logger.info(f"팩스번호 성공: {self.fax_success:,}")
        
        if self.processed_count > 0:
            phone_rate = (self.phone_success / self.processed_count) * 100
            fax_rate = (self.fax_success / self.processed_count) * 100
            total_success = self.phone_success + self.fax_success
            total_attempts = self.processed_count * 2  # 전화+팩스
            overall_rate = (total_success / total_attempts) * 100
            
            self.logger.info(f"전화번호 성공률: {phone_rate:.1f}%")
            self.logger.info(f"팩스번호 성공률: {fax_rate:.1f}%") 
            self.logger.info(f"전체 성공률: {overall_rate:.1f}%")
        
        self.logger.info("🔍 주요 개선사항:")
        self.logger.info("   - 자연스러운 검색어 적용 (따옴표 제거)")
        self.logger.info("   - 더 효과적인 검색 패턴 순서")
        self.logger.info("   - 강화된 기관명 추출 로직")
        self.logger.info("=" * 70)

def main():
    """메인 실행 함수"""
    # 로깅 설정
    logger = setup_logging()
    
    try:
        logger.info("🎯 개선된 기관명 추출 시스템 v2 시작")
        logger.info("🔍 검색어 개선: 자연스러운 형태 (예: '02-1234-5678 은 어디전화번호')")
        
        # 입력 파일 설정
        input_file = 'rawdatafile/failed_data_250715.xlsx'
        
        # 파일 존재 확인
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_file}")
        
        # 프로세서 초기화 및 실행
        processor = EnhancedInstitutionProcessor(max_workers=10, batch_size=350)
        result_file = processor.process_file(input_file)
        
        logger.info(f"🎉 시스템 완료! 결과 파일: {result_file}")
        print(f"\n🎊 처리 완료! 개선된 결과를 확인하세요: {result_file}")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ 사용자에 의해 중단됨")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 시스템 오류: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 