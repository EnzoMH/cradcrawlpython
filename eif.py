#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Institution Finder - 강화된 기관명 추출 시스템
failed_data_250715.xlsx의 H열(전화번호 기관명)과 J열(팩스번호 기관명) 채우기

특징:
- 강화된 검색어 로직: "{번호} 은 어디전화번호?", "{번호} 은 어디팩스번호?" 
- 기존 utils/config 모듈 완전 활용
- max_workers: 10개, batch_size: 350개
- 안정적인 봇 우회 및 병렬 처리

작성자: AI Assistant
작성일: 2025-01-16
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
    file_handler = logging.FileHandler(f'enhanced_finder_{timestamp}.log', encoding='utf-8')
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

class EnhancedSearchEngine:
    """강화된 검색 엔진 - 특별한 검색어 패턴 적용"""
    
    def __init__(self, logger=None):
        """
        강화된 검색 엔진 초기화
        
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
            '옥션원모바일', '스팸', '홍보', '마케팅'
        ]
        
        self.logger.info("🔍 강화된 검색 엔진 초기화 완료")
    
    def create_enhanced_queries(self, number: str, number_type: str = "전화") -> List[str]:
        """
        강화된 검색 쿼리 생성 - 특별한 패턴 우선 적용
        
        Args:
            number: 전화번호 또는 팩스번호
            number_type: "전화" 또는 "팩스"
            
        Returns:
            List[str]: 우선순위별 검색 쿼리 목록
        """
        queries = []
        
        # 🎯 핵심 강화 검색어 (최우선)
        if number_type == "전화":
            priority_queries = [
                f'"{number} 은 어디전화번호?"',
                f'"{number} 은 어디전화번호"',
                f'"{number} 어디전화번호"',
                f'"{number}" 은 어디전화번호',
                f'"{number}" 어디 전화번호',
            ]
        else:  # 팩스
            priority_queries = [
                f'"{number} 은 어디팩스번호?"',
                f'"{number} 은 어디팩스번호"',
                f'"{number} 어디팩스번호"',
                f'"{number}" 은 어디팩스번호',
                f'"{number}" 어디 팩스번호',
            ]
        
        # 우선순위 쿼리 먼저 추가
        queries.extend(priority_queries)
        
        # 🔍 보조 검색어 (기존 효과적인 패턴들)
        if number_type == "전화":
            secondary_queries = [
                f'"{number}" 전화번호 기관',
                f'"{number}" 연락처 어디',
                f'"{number}" 기관명',
                f'전화번호 "{number}" 어디',
                f'"{number}" 전화 어느기관',
            ]
        else:  # 팩스
            secondary_queries = [
                f'"{number}" 팩스번호 기관',
                f'"{number}" fax 어디',
                f'"{number}" 기관명',
                f'팩스번호 "{number}" 어디',
                f'"{number}" 팩스 어느기관',
            ]
        
        queries.extend(secondary_queries)
        
        # 🏢 지역별 검색 강화
        area_code = number.split('-')[0] if '-' in number else number[:3]
        area_names = self._get_area_names(area_code)
        
        for area in area_names[:2]:  # 상위 2개 지역만
            if number_type == "전화":
                queries.extend([
                    f'{area} "{number}" 전화번호',
                    f'"{number}" {area} 기관',
                ])
            else:
                queries.extend([
                    f'{area} "{number}" 팩스번호',
                    f'"{number}" {area} 기관',
                ])
        
        # 🏛️ 공식 사이트 우선 검색
        official_queries = [
            f'"{number}" site:go.kr',
            f'"{number}" site:or.kr',
            f'"{number}" 공식 홈페이지',
        ]
        
        queries.extend(official_queries)
        
        return queries[:18]  # 상위 18개만 반환
    
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
        번호로 기관명 검색 (강화된 로직)
        
        Args:
            driver: WebDriver 인스턴스
            number: 전화번호 또는 팩스번호
            number_type: "전화" 또는 "팩스"
            
        Returns:
            Optional[str]: 발견된 기관명 또는 None
        """
        try:
            self.logger.info(f"🔍 {number_type}번호 기관명 검색 시작: {number}")
            
            # 강화된 검색 쿼리 생성
            search_queries = self.create_enhanced_queries(number, number_type)
            
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
                    time.sleep(random.uniform(1.5, 3.0))
                    
                except Exception as e:
                    self.logger.debug(f"⚠️ 쿼리 검색 실패: {query} - {e}")
                    continue
            
            self.logger.warning(f"❌ 모든 쿼리 실패: {number} ({number_type})")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ {number_type}번호 검색 오류: {number} - {e}")
            return None
    
    def _perform_search(self, driver, query: str) -> Optional[str]:
        """구글 검색 수행"""
        try:
            # 구글 검색 페이지로 이동
            driver.get('https://www.google.com')
            time.sleep(random.uniform(1.0, 2.0))
            
            # 검색창 찾기
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            
            # 검색어 입력
            search_box.clear()
            time.sleep(random.uniform(0.3, 0.7))
            search_box.send_keys(query)
            time.sleep(random.uniform(0.5, 1.0))
            
            # 검색 실행
            search_box.send_keys(Keys.RETURN)
            
            # 검색 결과 로딩 대기
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            
            time.sleep(random.uniform(2.0, 3.5))
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            
            # 검색 결과 확인
            if "검색결과가 없습니다" in page_source or "검색 결과 없음" in page_source:
                self.logger.debug(f"검색 결과 없음: {query}")
                return None
            
            # 봇 감지 확인
            if "unusual traffic" in page_source.lower() or "recaptcha" in page_source.lower():
                self.logger.warning(f"⚠️ 봇 감지 가능성 - 대기: {query}")
                time.sleep(random.uniform(5.0, 10.0))
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
                    self.logger.debug(f"🎯 번호 발견 라인: {line.strip()}")
                    
                    # 주변 라인들과 함께 분석
                    context_lines = lines[max(0, i-3):i+4]
                    context_text = ' '.join(context_lines)
                    
                    # 컨텍스트에서 기관명 찾기
                    institution_name = self._find_institution_in_context(context_text, number)
                    if institution_name:
                        return institution_name
            
            # 2. 전체 텍스트에서 기관명 패턴 검색
            for pattern in self.institution_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    cleaned_name = self._clean_institution_name(match)
                    if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                        self.logger.debug(f"🏢 패턴 매칭 기관명: {cleaned_name}")
                        return cleaned_name
            
            # 3. HTML 구조에서 기관명 찾기
            institution_name = self._extract_from_html_structure(soup, number)
            if institution_name:
                return institution_name
            
            return None
            
        except Exception as e:
            self.logger.debug(f"기관명 추출 실패: {e}")
            return None
    
    def _find_institution_in_context(self, context_text: str, number: str) -> Optional[str]:
        """컨텍스트 텍스트에서 기관명 찾기"""
        try:
            # 주요 기관명 키워드들
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '군청', '동', '주민센터', 
                '행정복지센터', '복지관', '보건소', '보건지소', '병원', '의원', 
                '학교', '대학', '협회', '단체', '재단', '법인', '교회', '성당'
            ]
            
            # 단어 단위로 분리
            words = context_text.split()
            
            # 기관명 키워드를 포함한 구문 찾기
            for i, word in enumerate(words):
                for keyword in institution_keywords:
                    if keyword in word:
                        # 주변 단어들과 결합하여 완전한 기관명 구성
                        start_idx = max(0, i-3)
                        end_idx = min(len(words), i+4)
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
        """기관명 후보 정제"""
        try:
            if not candidate:
                return None
            
            # 불필요한 문자 제거
            cleaned = re.sub(r'[^\w\s가-힣]', ' ', candidate)
            cleaned = ' '.join(cleaned.split())  # 공백 정리
            cleaned = cleaned.strip()
            
            # 길이 검증
            if len(cleaned) < 3 or len(cleaned) > 50:
                return None
            
            # 기관명 키워드 포함 여부 확인
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '군청', '동', '주민센터',
                '행정복지센터', '복지관', '보건소', '병원', '의원', '학교', 
                '대학', '협회', '단체', '재단', '법인'
            ]
            
            has_keyword = any(keyword in cleaned for keyword in institution_keywords)
            if not has_keyword:
                return None
            
            return cleaned
            
        except Exception as e:
            self.logger.debug(f"기관명 정제 실패: {e}")
            return None
    
    def _extract_from_html_structure(self, soup: BeautifulSoup, number: str) -> Optional[str]:
        """HTML 구조에서 기관명 추출"""
        try:
            # 제목이나 헤더에서 기관명 찾기
            for tag in ['h1', 'h2', 'h3', 'title']:
                elements = soup.find_all(tag)
                for element in elements:
                    text = element.get_text().strip()
                    cleaned_name = self._clean_institution_name(text)
                    if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                        return cleaned_name
            
            return None
            
        except Exception as e:
            self.logger.debug(f"HTML 구조 추출 실패: {e}")
            return None
    
    def _is_valid_institution_name(self, name: str, number: str) -> bool:
        """유효한 기관명인지 검증"""
        try:
            if not name or len(name.strip()) < 3:
                return False
            
            name = name.strip()
            
            # 제외 키워드 체크
            for exclude in self.exclude_keywords:
                if exclude.lower() in name.lower():
                    return False
            
            # 숫자만 있는 경우 제외
            if name.isdigit():
                return False
            
            # 번호 자체인 경우 제외
            if number in name:
                return False
            
            # 너무 긴 경우 제외
            if len(name) > 50:
                return False
            
            # 기관명 패턴 포함 여부 확인
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '동', '주민센터', 
                '복지관', '보건소', '병원', '의원', '학교', '협회', '단체'
            ]
            
            return any(keyword in name for keyword in institution_keywords)
            
        except Exception as e:
            self.logger.debug(f"기관명 검증 실패: {e}")
            return False

class EnhancedInstitutionProcessor:
    """강화된 기관명 추출 메인 처리기"""
    
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
        
        # 강화된 검색 엔진
        self.search_engine = EnhancedSearchEngine(self.logger)
        
        # 워커별 드라이버 관리
        self.worker_drivers = {}
        self.lock = threading.Lock()
        
        # 통계
        self.total_rows = 0
        self.processed_count = 0
        self.phone_success = 0
        self.fax_success = 0
        
        self.logger.info(f"🚀 강화된 기관명 추출 프로세서 초기화 완료")
        self.logger.info(f"⚙️  설정: 워커 {max_workers}개, 배치 {batch_size}개")
    
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
                self.logger.info(f"📈 빈 값 현황: H열 {phone_empty}개, J열 {fax_empty}개")
            
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
            
            self.logger.info(f"📋 워커 {worker_id}: 행 {row_idx} 처리 시작")
            self.logger.debug(f"   전화: {phone_number}, 팩스: {fax_number}")
            
            # 드라이버 가져오기
            driver = self._get_worker_driver(worker_id)
            if not driver:
                result.error_message = "드라이버 생성 실패"
                return result
            
            # 전화번호 처리 (H열이 비어있는 경우만)
            if (phone_number and phone_number != 'nan' and 
                (not existing_phone_result or existing_phone_result == 'nan') and
                self.phone_validator.is_valid_phone_format(phone_number)):
                
                self.logger.info(f"📞 워커 {worker_id}: 전화번호 {phone_number} 검색 시작")
                institution = self.search_engine.search_institution_by_number(driver, phone_number, "전화")
                
                if institution:
                    result.found_phone_institution = institution
                    result.phone_success = True
                    self.logger.info(f"✅ 전화번호 성공: {institution}")
                else:
                    self.logger.warning(f"⚠️ 전화번호 결과 없음: {phone_number}")
            
            # 팩스번호 처리 (J열이 비어있는 경우만)
            if (fax_number and fax_number != 'nan' and 
                (not existing_fax_result or existing_fax_result == 'nan') and
                self.phone_validator.is_valid_phone_format(fax_number)):
                
                self.logger.info(f"📠 워커 {worker_id}: 팩스번호 {fax_number} 검색 시작")
                institution = self.search_engine.search_institution_by_number(driver, fax_number, "팩스")
                
                if institution:
                    result.found_fax_institution = institution
                    result.fax_success = True
                    self.logger.info(f"✅ 팩스번호 성공: {institution}")
                else:
                    self.logger.warning(f"⚠️ 팩스번호 결과 없음: {fax_number}")
            
            result.processing_time = time.time() - start_time
            
            # 성공 여부 로깅
            success_msg = []
            if result.phone_success:
                success_msg.append("전화번호")
            if result.fax_success:
                success_msg.append("팩스번호")
            
            if success_msg:
                self.logger.info(f"🎉 워커 {worker_id}: 행 {row_idx} 완료 - {'/'.join(success_msg)} 성공")
            
            return result
            
        except Exception as e:
            result.error_message = str(e)
            result.processing_time = time.time() - start_time
            self.logger.error(f"❌ 워커 {worker_id}: 행 {row_idx} 처리 오류 - {e}")
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
        try:
            driver = self.web_driver_manager.create_bot_evasion_driver(worker_id)
            if driver:
                self.worker_drivers[worker_id] = driver
                self.logger.info(f"✅ 워커 {worker_id}: 새 드라이버 할당 성공")
                return driver
            else:
                self.logger.error(f"❌ 워커 {worker_id}: 드라이버 생성 실패")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 드라이버 생성 오류 - {e}")
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
            for batch_start in range(0, len(df), self.batch_size):
                batch_end = min(batch_start + self.batch_size, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                
                batch_num = (batch_start // self.batch_size) + 1
                total_batches = (len(df) + self.batch_size - 1) // self.batch_size
                
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
                            if self.processed_count % 25 == 0:
                                progress = (self.processed_count / self.total_rows) * 100
                                self.logger.info(f"📊 진행률: {self.processed_count}/{self.total_rows} ({progress:.1f}%) - 전화:{self.phone_success}, 팩스:{self.fax_success}")
                            
                        except Exception as e:
                            self.logger.error(f"❌ 행 {row_idx} 결과 처리 오류: {e}")
                
                # 배치 완료 후 시스템 리소스 체크 및 조정
                current_resources = self.performance_manager.get_current_resources()
                adjustment = self.performance_manager.adjust_performance_dynamically(current_resources)
                if adjustment.get('adjusted'):
                    self.logger.info(f"⚙️  시스템 조정: {adjustment.get('reason')}")
                
                # 배치 간 휴식
                if batch_end < len(df):
                    rest_time = random.uniform(3.0, 7.0)
                    self.logger.info(f"⏱️ 배치 완료 - {rest_time:.1f}초 휴식")
                    time.sleep(rest_time)
            
            # 결과를 DataFrame에 반영
            for row_idx, result in all_results.items():
                if result.phone_success and len(df.columns) > 7:
                    df.iloc[row_idx, 7] = result.found_phone_institution  # H열
                if result.fax_success and len(df.columns) > 9:
                    df.iloc[row_idx, 9] = result.found_fax_institution    # J열
            
            # 결과 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"enhanced_failed_data_processed_{timestamp}.xlsx"
            
            # ExcelProcessor로 저장
            save_success = self.excel_processor.save_excel(df, output_file)
            if not save_success:
                # 백업 저장 방법
                df.to_excel(output_file, index=False)
            
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
        self.logger.info("📊 최종 처리 통계")
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
        
        self.logger.info("=" * 70)

def main():
    """메인 실행 함수"""
    # 로깅 설정
    logger = setup_logging()
    
    try:
        logger.info("🎯 강화된 기관명 추출 시스템 시작")
        logger.info("🔍 특별한 검색어: '{번호} 은 어디전화번호?', '{번호} 은 어디팩스번호?'")
        
        # 입력 파일 설정
        input_file = 'rawdatafile/failed_data_250715.xlsx'
        
        # 파일 존재 확인
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_file}")
        
        # 프로세서 초기화 및 실행
        processor = EnhancedInstitutionProcessor(max_workers=10, batch_size=350)
        result_file = processor.process_file(input_file)
        
        logger.info(f"🎉 시스템 완료! 결과 파일: {result_file}")
        print(f"\n🎊 처리 완료! 결과를 확인하세요: {result_file}")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ 사용자에 의해 중단됨")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 시스템 오류: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 