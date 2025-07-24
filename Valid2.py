#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valid2.py - 단순화된 5단계 팩스번호 검증 시스템
기존 Valid.py의 핵심 로직 + utils.WebDriverManager 기반 안정성

핵심 데이터: E열(읍면동) = I열(팩스번호) [전화번호와 팩스번호는 엄밀히 다름]

5단계 팩스번호 검증 프로세스:
1차 검증: 팩스번호 지역번호 vs E열 읍면동 매칭
2차 검증: Google 검색으로 팩스번호의 진짜 기관명 확인  
3차 검증: 검색결과 링크 크롤링 + 기관명 추출
4차 검증: AI를 통한 팩스번호 실제 소유 기관명 도출
5차 검증: 모든 단계 결과 종합 → 데이터 정확성 최종 판단

특징:
- utils.WebDriverManager 100% 활용으로 안정성 확보
- 복잡한 ProxyRotator, AdvancedPortManager 제거
- 상세한 로깅으로 문제 지점 정확한 파악 가능
- eif4.py의 단순함 + Valid.py의 정교한 검증 로직

작성자: AI Assistant
작성일: 2025-07-24
버전: 2.0 - Simplified & Stable
"""

import os
import sys
import time
import random
import logging
import pandas as pd
import json
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import threading
import re

# 웹 크롤링
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

# 환경변수 및 AI
from dotenv import load_dotenv
import google.generativeai as genai

# utils 모듈 활용 (검증된 안정성)
from utils.web_driver_manager import WebDriverManager
from utils.ai_model_manager import AIModelManager
from utils.phone_validator import PhoneValidator

# 환경변수 로드
load_dotenv()

# ================================
# 전역 설정
# ================================

# 입력/출력 파일 경로
INPUT_FILE = "rawdatafile/failed_data_250724.xlsx"
OUTPUT_FILE_PREFIX = "Valid2_검증결과"

# 검증 설정 (안정성 우선)
MAX_WORKERS = 2  # 안정성을 위해 2개 워커
BATCH_SIZE = 50  # 배치 크기
SEARCH_RESULTS_LIMIT = 3  # 검색 결과 링크 수 (속도 vs 정확도)
CONFIDENCE_THRESHOLD = 80  # 신뢰도 임계값 (%)

# 타임아웃 설정 (속도 우선으로 단축)
GOOGLE_SEARCH_TIMEOUT = 8   # Google 검색 타임아웃 (20→8초)
PAGE_LOAD_TIMEOUT = 6       # 페이지 로드 타임아웃 (15→6초)
CRAWLING_TIMEOUT = 5        # 개별 크롤링 타임아웃 (10→5초)

# ================================
# 상세 로깅 시스템
# ================================

def setup_detailed_logger(name: str = "Valid2") -> logging.Logger:
    """상세한 디버깅이 가능한 로깅 시스템 설정"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'valid2_{timestamp}.log'
    
    # 상세한 포맷 (문제 지점 파악 용이)
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - [워커%(thread)d] - %(message)s'
    )
    
    # 파일 핸들러 (모든 로그)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # 디버그 레벨까지 모든 로그
    file_handler.setFormatter(detailed_formatter)
    
    # 콘솔 핸들러 (중요 로그만)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(detailed_formatter)
    
    # 로거 설정
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 시스템 정보 로그
    logger.info("=" * 80)
    logger.info("🚀 Valid2.py - 단순화된 5단계 검증 시스템 시작")
    logger.info("=" * 80)
    logger.info(f"📁 로그 파일: {log_filename}")
    logger.info(f"⚙️ 워커 수: {MAX_WORKERS}")
    logger.info(f"🎯 신뢰도 임계값: {CONFIDENCE_THRESHOLD}%")
    logger.info(f"🔗 검색 결과 한도: {SEARCH_RESULTS_LIMIT}개")
    logger.info(f"⏱️ 타임아웃: Google({GOOGLE_SEARCH_TIMEOUT}s), 페이지({PAGE_LOAD_TIMEOUT}s), 크롤링({CRAWLING_TIMEOUT}s) - 속도 최적화됨")
    
    return logger

# ================================
# 데이터 클래스
# ================================

@dataclass
class ValidationResult:
    """5단계 검증 결과 (Valid.py와 동일 구조 유지)"""
    row_index: int
    fax_number: str
    institution_name: str  # 원본 기관명 (읍면동) - 핵심 데이터
    region: str           # 지역 (시도)
    phone_number: str = ""  # 전화번호 (H열)
    address: str = ""     # 주소 (G열)
    
    # 1차 검증 결과
    stage1_passed: bool = False
    stage1_message: str = ""
    area_code_match: bool = False
    
    # 2차 검증 결과  
    stage2_passed: bool = False
    stage2_message: str = ""
    google_search_result: str = ""
    
    # 3차 검증 결과
    stage3_passed: bool = False
    stage3_message: str = ""
    extracted_links: List[str] = None
    crawled_data: List[Dict] = None
    confidence_score: float = 0.0
    # 3차에서 추출된 기관명들 (4-5차 검증용)
    discovered_institutions: List[str] = None
    
    # 4차 검증 결과
    stage4_passed: bool = False
    stage4_message: str = ""
    ai_extracted_institution: str = ""
    
    # 5차 검증 결과 (최종)
    stage5_passed: bool = False
    stage5_message: str = ""
    final_verification: str = ""
    
    # 전체 결과
    overall_result: str = "검증 실패"  # "데이터 올바름", "데이터 오류", "직접 확인 요망"
    final_confidence: float = 0.0
    processing_time: float = 0.0
    error_message: str = ""

# ================================
# 단순화된 검증 관리자
# ================================

class SimpleValidationManager:
    """단순화된 5단계 검증 관리자 (utils 기반)"""
    
    def __init__(self):
        """초기화 - utils 모듈들로 간소화"""
        self.logger = setup_detailed_logger("SimpleValidationManager")
        
        try:
            self.logger.info("🔧 SimpleValidationManager 초기화 시작")
            
            # utils 모듈들 초기화 (검증된 안정성)
            self.logger.debug("📱 PhoneValidator 초기화 중...")
            self.phone_validator = PhoneValidator(self.logger)
            self.logger.debug("✅ PhoneValidator 초기화 완료")
            
            self.logger.debug("🤖 AIModelManager 초기화 중...")
            self.ai_manager = AIModelManager(self.logger)
            self.logger.debug("✅ AIModelManager 초기화 완료")
            
            # WebDriverManager는 워커별로 생성 (메모리 효율성)
            self.web_driver_managers = {}  # 워커별 관리
            self.driver_lock = threading.Lock()
            
            # 데이터
            self.input_data = None
            self.validation_results = []
            
            self.logger.info("✅ SimpleValidationManager 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ SimpleValidationManager 초기화 실패: {e}")
            self.logger.error(traceback.format_exc())
            raise
    
    def load_data(self) -> bool:
        """Excel 데이터 로드 (Valid.py와 동일한 로직 유지)"""
        try:
            self.logger.info(f"📊 데이터 로드 시작: {INPUT_FILE}")
            self.logger.debug(f"파일 존재 확인: {os.path.exists(INPUT_FILE)}")
            
            if not os.path.exists(INPUT_FILE):
                self.logger.error(f"❌ 입력 파일 없음: {INPUT_FILE}")
                return False
            
            # Excel 파일 로드 (시트 자동 선택 - Valid.py 로직 그대로)
            self.logger.debug("Excel 파일 시트 분석 중...")
            excel_file = pd.ExcelFile(INPUT_FILE)
            sheet_names = excel_file.sheet_names
            self.logger.debug(f"발견된 시트들: {sheet_names}")
            
            # 가장 큰 시트 선택 (Valid.py와 동일)
            if len(sheet_names) > 1:
                sheet_sizes = {}
                for sheet in sheet_names:
                    temp_df = pd.read_excel(INPUT_FILE, sheet_name=sheet)
                    sheet_sizes[sheet] = len(temp_df)
                    self.logger.debug(f"시트 '{sheet}': {len(temp_df)}행")
                
                # 가장 큰 시트 선택
                selected_sheet = max(sheet_sizes, key=sheet_sizes.get)
                self.logger.info(f"📋 선택된 시트: '{selected_sheet}' ({sheet_sizes[selected_sheet]}행)")
            else:
                selected_sheet = sheet_names[0]
                self.logger.info(f"📋 기본 시트 사용: '{selected_sheet}'")
            
            # 데이터 로드
            self.input_data = pd.read_excel(INPUT_FILE, sheet_name=selected_sheet)
            self.logger.info(f"📊 로드 완료: {len(self.input_data)}행 × {len(self.input_data.columns)}열")
            
            # 컬럼 정보 로그 (디버깅용)
            self.logger.debug("컬럼 정보:")
            for i, col in enumerate(self.input_data.columns):
                self.logger.debug(f"  {i}: {col}")
            
            # 필요 컬럼 확인 (Valid.py와 동일한 매핑)
            required_columns = ['C', 'E', 'G', 'H', 'I']  # 시도, 읍면동, 주소, 전화번호, 팩스번호
            if len(self.input_data.columns) >= 9:  # I열까지 있어야 함
                self.logger.info("✅ 필요 컬럼 확인 완료")
                return True
            else:
                self.logger.error(f"❌ 필요 컬럼 부족: {len(self.input_data.columns)}개 (최소 9개 필요)")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def save_results(self) -> str:
        """검증 결과 저장 (Valid.py 형식 유지)"""
        try:
            if not self.validation_results:
                self.logger.warning("⚠️ 저장할 결과가 없습니다")
                return ""
            
            self.logger.info(f"💾 검증 결과 저장 시작: {len(self.validation_results)}개")
            
            # 결과 DataFrame 생성 (Valid.py와 동일한 형식)
            results_data = []
            
            for result in self.validation_results:
                results_data.append({
                    '행번호': result.row_index + 1,
                    '팩스번호': result.fax_number,
                    '기관명(읍면동)': result.institution_name,
                    '지역(시도)': result.region,
                    '전화번호': result.phone_number,
                    '주소': result.address,
                    
                    # 단계별 결과
                    '1차_통과여부': result.stage1_passed,
                    '1차_메시지': result.stage1_message,
                    '2차_통과여부': result.stage2_passed,
                    '2차_메시지': result.stage2_message,
                    '2차_검색결과': result.google_search_result,
                    '3차_통과여부': result.stage3_passed,
                    '3차_메시지': result.stage3_message,
                    '3차_신뢰도점수': result.confidence_score,
                    '3차_발견기관명': ', '.join(result.discovered_institutions or []),
                    '4차_통과여부': result.stage4_passed,
                    '4차_메시지': result.stage4_message,
                    '4차_AI추출기관': result.ai_extracted_institution,
                    '5차_통과여부': result.stage5_passed,
                    '5차_메시지': result.stage5_message,
                    '5차_최종검증': result.final_verification,
                    
                    # 최종 결과
                    '전체결과': result.overall_result,
                    '최종신뢰도': result.final_confidence,
                    '처리시간(초)': result.processing_time,
                    '오류메시지': result.error_message
                })
            
            # DataFrame 생성
            results_df = pd.DataFrame(results_data)
            
            # 파일명 생성 (타임스탬프 포함)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{OUTPUT_FILE_PREFIX}_{timestamp}.xlsx"
            
            # Excel 파일로 저장
            self.logger.debug(f"Excel 파일 저장 중: {filename}")
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                results_df.to_excel(writer, index=False, sheet_name='검증결과')
            
            # 저장 완료 로그
            file_size = os.path.getsize(filename)
            self.logger.info(f"✅ 결과 저장 완료: {filename}")
            self.logger.info(f"📁 파일 크기: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            
            # 요약 통계
            success_count = sum(1 for r in self.validation_results if r.overall_result == "데이터 올바름")
            error_count = sum(1 for r in self.validation_results if r.overall_result == "데이터 오류")
            check_count = sum(1 for r in self.validation_results if r.overall_result == "직접 확인 요망")
            
            self.logger.info(f"📊 검증 결과 요약:")
            self.logger.info(f"   - 데이터 올바름: {success_count}개")
            self.logger.info(f"   - 데이터 오류: {error_count}개")
            self.logger.info(f"   - 직접 확인 요망: {check_count}개")
            self.logger.info(f"   - 총 처리: {len(self.validation_results)}개")
            
            return filename
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            self.logger.error(traceback.format_exc())
            return ""
    
    def validate_stage1(self, fax_number: str, institution_name: str, region: str, address: str) -> Tuple[bool, str]:
        """1차 검증: 팩스번호 지역번호 매칭 (Valid.py 로직 그대로 활용)"""
        try:
            self.logger.debug(f"📍 1차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}, 지역:{region}")
            self.logger.debug(f"주소: {address}")
            
            # 1. 팩스번호 유효성 검사 (phone_validator.py 활용)
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                message = "검증 불가 (팩스번호 없음)"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
            
            # 팩스번호 형식 검증 및 정규화
            if not self.phone_validator.is_valid_phone_format(fax_number):
                message = f"검증 불가 (팩스번호 형식 오류: {fax_number})"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
            
            # 팩스번호 정규화 (verification_engine 방식)
            normalized_fax = self._normalize_phone_number(fax_number)
            if normalized_fax and normalized_fax != fax_number:
                self.logger.debug(f"📞 팩스번호 정규화: {fax_number} → {normalized_fax}")
            
            # 2. 지역번호 추출 (phone_validator.py 활용)
            area_code = self.phone_validator.extract_area_code(fax_number)
            if not area_code:
                message = f"검증 불가 (지역번호 추출 실패: {fax_number})"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
            
            self.logger.debug(f"추출된 지역번호: {area_code}")
            
            # 3. 지역 매칭 검사 (phone_validator.py 활용)
            is_match = self.phone_validator.is_regional_match(area_code, address)
            
            if is_match:
                message = f"팩스번호 지역번호 일치: {area_code} ↔ {address} (기관: {institution_name})"
                self.logger.info(f"✅ 1차 검증 통과: {message}")
                return True, message
            else:
                # 지역 불일치 상세 정보
                from utils.phone_validator import KOREAN_AREA_CODES
                area_name = KOREAN_AREA_CODES.get(area_code, "알 수 없음")
                message = f"팩스번호 지역번호 불일치: {area_code}({area_name}) ↔ {address} (기관: {institution_name})"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
                
        except Exception as e:
            error_msg = f"1차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            return False, error_msg
    
    def get_driver_for_worker(self, worker_id: int):
        """워커별 WebDriver 인스턴스 획득 (thread-safe)"""
        with self.driver_lock:
            if worker_id not in self.web_driver_managers:
                self.logger.debug(f"🔧 워커 {worker_id} WebDriverManager 생성 중...")
                self.web_driver_managers[worker_id] = WebDriverManager(logger=self.logger)
                self.logger.debug(f"✅ 워커 {worker_id} WebDriverManager 생성 완료")
            
            return self.web_driver_managers[worker_id]
    
    def validate_stage2(self, fax_number: str, institution_name: str, worker_id: int = 0) -> Tuple[bool, str, str]:
        """2차 검증: Google 검색으로 팩스번호의 진짜 기관명 확인 (단순화된 접근)"""
        try:
            self.logger.debug(f"🔍 2차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}")
            
            # 1차 검증을 통과한 경우만 진행
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                message = "1차 검증 실패로 인한 2차 검증 건너뛰기"
                self.logger.info(f"⏭️ {message}")
                return False, message, ""
            
            # WebDriverManager 획득
            web_manager = self.get_driver_for_worker(worker_id)
            
            # Google 검색 쿼리 생성 (사람처럼 자연스럽게, 따옴표 제거)
            search_query = f'{fax_number} 팩스번호 어느기관'
            self.logger.debug(f"🔍 검색 쿼리: {search_query}")
            
            # 드라이버 생성 및 검색 실행
            driver = None
            try:
                self.logger.debug(f"🛡️ 워커 {worker_id} 드라이버 생성 중...")
                
                # 워커별 포트 할당
                port = web_manager.get_available_port(worker_id)
                self.logger.debug(f"🔌 워커 {worker_id} 할당 포트: {port}")
                
                driver = web_manager.create_bot_evasion_driver()
                
                if not driver:
                    message = "드라이버 생성 실패"
                    self.logger.error(f"❌ {message}")
                    return False, message, ""
                
                self.logger.debug(f"✅ 워커 {worker_id} 드라이버 생성 완료 (포트: {port})")
                
                # Google 검색 페이지 접속
                self.logger.debug("🌐 Google 검색 페이지 접속 중...")
                driver.get("https://www.google.com")
                
                # 페이지 로드 대기
                wait = WebDriverWait(driver, GOOGLE_SEARCH_TIMEOUT)
                
                # 검색창 찾기 (최적화된 순서, 빠른 타임아웃)
                search_box = None
                # 가장 자주 성공하는 순서로 재배치
                selectors = ['textarea[name="q"]', '#APjFqb', 'input[name="q"]']
                
                for selector in selectors:
                    try:
                        # 개별 선택자당 빠른 타임아웃 (3초)
                        quick_wait = WebDriverWait(driver, 3)
                        search_box = quick_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                        self.logger.debug(f"✅ 검색창 발견: {selector}")
                        break
                    except TimeoutException:
                        self.logger.debug(f"⚠️ 검색창 선택자 실패: {selector}")
                        continue
                
                if not search_box:
                    message = "Google 검색창을 찾을 수 없음"
                    self.logger.error(f"❌ {message}")
                    return False, message, ""
                
                # 검색어 입력 (속도 최적화된 타이핑)
                self.logger.debug("⌨️ 검색어 입력 중...")
                search_box.clear()
                
                # 속도 우선: 딜레이 단축
                for char in search_query:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.02, 0.05))  # 타이핑 속도 3배 향상
                
                # 검색 실행
                search_box.send_keys(Keys.RETURN)
                self.logger.debug("🔍 검색 실행됨")
                
                # 검색 결과 대기 (빠른 타임아웃)
                try:
                    # 검색 결과 대기 시간 단축 (3초)
                    quick_wait = WebDriverWait(driver, 3)
                    quick_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#search')))
                    self.logger.debug("✅ 검색 결과 로드 완료")
                except TimeoutException:
                    self.logger.warning("⚠️ 검색 결과 로드 타임아웃 (3초)")
                    # 타임아웃이어도 계속 진행
                
                # 검색 결과 텍스트 추출 (간단한 접근)
                try:
                    # 첫 번째 결과 몇 개의 제목과 설명 추출
                    results = driver.find_elements(By.CSS_SELECTOR, 'h3')[:3]  # 상위 3개 결과
                    snippets = driver.find_elements(By.CSS_SELECTOR, '.VwiC3b')[:3]  # 설명 텍스트
                    
                    search_results = []
                    for i, result in enumerate(results):
                        title = result.text.strip()
                        snippet = snippets[i].text.strip() if i < len(snippets) else ""
                        search_results.append(f"{title}: {snippet}")
                    
                    search_result_text = " | ".join(search_results)
                    
                    if search_result_text:
                        # 기관명이 검색 결과에 포함되어 있는지 확인
                        if institution_name in search_result_text:
                            message = f"Google 검색에서 기관명 확인됨: {institution_name}"
                            self.logger.info(f"✅ 2차 검증 통과: {message}")
                            return True, message, search_result_text
                        else:
                            message = f"Google 검색에서 기관명 불일치 (검색: {search_result_text[:100]}...)"
                            self.logger.warning(f"⚠️ 2차 검증 실패: {message}")
                            return False, message, search_result_text
                    else:
                        message = "Google 검색 결과 없음"
                        self.logger.warning(f"⚠️ 2차 검증 실패: {message}")
                        return False, message, ""
                
                except Exception as e:
                    message = f"검색 결과 추출 오류: {e}"
                    self.logger.error(f"❌ {message}")
                    return False, message, ""
                
            finally:
                # 드라이버 정리
                if driver:
                    try:
                        driver.quit()
                        self.logger.debug(f"🧹 워커 {worker_id} 드라이버 정리 완료")
                    except:
                        pass
                        
        except Exception as e:
            error_msg = f"2차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            return False, error_msg, ""
    
    def validate_stage3(self, fax_number: str, institution_name: str, google_search_result: str, worker_id: int = 0) -> Tuple[bool, str, List[str], List[Dict], float]:
        """3차 검증: 검색결과 링크 크롤링 + 기관명 추출 (병렬 처리)"""
        try:
            self.logger.debug(f"🔗 3차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}")
            
            # 2차 검증 결과가 없으면 건너뛰기
            if not google_search_result:
                message = "2차 검증 결과 없음으로 3차 검증 건너뛰기"
                self.logger.info(f"⏭️ {message}")
                return False, message, [], [], 0.0
            
            # WebDriverManager 획득
            web_manager = self.get_driver_for_worker(worker_id)
            
            # 드라이버 생성 및 링크 추출
            driver = None
            extracted_links = []
            crawled_data = []
            
            try:
                self.logger.debug(f"🛡️ 워커 {worker_id} 3차 검증용 드라이버 생성 중...")
                
                # 포트 할당
                port = web_manager.get_available_port(worker_id)
                self.logger.debug(f"🔌 워커 {worker_id} 3차 검증 포트: {port}")
                
                driver = web_manager.create_bot_evasion_driver()
                
                if not driver:
                    message = "3차 검증용 드라이버 생성 실패"
                    self.logger.error(f"❌ {message}")
                    return False, message, [], [], 0.0
                
                self.logger.debug(f"✅ 워커 {worker_id} 3차 검증용 드라이버 생성 완료")
                
                # Google 검색 재실행하여 링크 추출
                search_query = f'{fax_number} 팩스번호 어느기관'
                self.logger.debug("🌐 Google 검색 페이지 재접속 (링크 추출용)...")
                driver.get("https://www.google.com")
                
                # 페이지 로드 대기
                wait = WebDriverWait(driver, GOOGLE_SEARCH_TIMEOUT)
                
                # 검색창 찾기 (3차 검증용 최적화)
                search_box = None
                selectors = ['textarea[name="q"]', '#APjFqb', 'input[name="q"]']
                
                for selector in selectors:
                    try:
                        quick_wait = WebDriverWait(driver, 3)
                        search_box = quick_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                        break
                    except TimeoutException:
                        continue
                
                if not search_box:
                    message = "3차 검증: Google 검색창을 찾을 수 없음"
                    self.logger.error(f"❌ {message}")
                    return False, message, [], [], 0.0
                
                # 검색어 입력 (3차 검증용 최적화)
                search_box.clear()
                for char in search_query:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.02, 0.05))
                
                search_box.send_keys(Keys.RETURN)
                self.logger.debug("🔍 3차 검증용 검색 실행됨")
                
                # 검색 결과 대기 (3차 검증용 최적화)
                try:
                    quick_wait = WebDriverWait(driver, 3)
                    quick_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#search')))
                except TimeoutException:
                    self.logger.warning("⚠️ 3차 검증: 검색 결과 로드 타임아웃 (3초)")
                
                # 검색 결과 링크 추출 (SEARCH_RESULTS_LIMIT개까지)
                try:
                    # 실제 검색 결과 링크 추출 (광고 제외)
                    link_elements = driver.find_elements(By.CSS_SELECTOR, '#search a[href]')
                    
                    for element in link_elements[:SEARCH_RESULTS_LIMIT]:
                        href = element.get_attribute('href')
                        if href and href.startswith('http') and 'google.com' not in href:
                            extracted_links.append(href)
                            self.logger.debug(f"🔗 추출된 링크: {href}")
                    
                    self.logger.info(f"📎 총 {len(extracted_links)}개 링크 추출 완료")
                    
                except Exception as e:
                    self.logger.error(f"❌ 링크 추출 오류: {e}")
                
                # 추출된 링크들을 병렬로 크롤링
                if extracted_links:
                    crawled_data = self._crawl_links_parallel(extracted_links, fax_number, institution_name, worker_id)
                
                # 신뢰도 점수 계산
                confidence_score = self._calculate_confidence_score(crawled_data, fax_number, institution_name)
                
                # 3차 검증 결과 판정
                if confidence_score >= CONFIDENCE_THRESHOLD:
                    message = f"3차 검증 통과: 신뢰도 {confidence_score:.1f}% (임계값: {CONFIDENCE_THRESHOLD}%)"
                    self.logger.info(f"✅ {message}")
                    return True, message, extracted_links, crawled_data, confidence_score
                else:
                    message = f"3차 검증 실패: 신뢰도 {confidence_score:.1f}% (임계값: {CONFIDENCE_THRESHOLD}%)"
                    self.logger.warning(f"⚠️ {message}")
                    return False, message, extracted_links, crawled_data, confidence_score
                
            finally:
                # 드라이버 정리
                if driver:
                    try:
                        driver.quit()
                        self.logger.debug(f"🧹 워커 {worker_id} 3차 검증용 드라이버 정리 완료")
                    except:
                        pass
                        
        except Exception as e:
            error_msg = f"3차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            return False, error_msg, [], [], 0.0
    
    def _parse_link_with_verification_engine(self, url: str, fax_number: str, institution_name: str) -> Dict:
        """verification_engine.py 방식으로 링크 직접 파싱"""
        try:
            self.logger.debug(f"🔍 verification_engine 방식 파싱: {url[:50]}...")
            
            # URL 정규화
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url
            
            # requests + BeautifulSoup으로 상세 파싱
            try:
                response = requests.get(url, timeout=CRAWLING_TIMEOUT, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 제목 추출
                    title = soup.find('title')
                    title_text = title.get_text(strip=True) if title else ""
                    
                    # 전체 텍스트 추출
                    full_text = soup.get_text()
                    
                    # 정교한 연락처 추출
                    contact_numbers = self._extract_phone_fax_numbers(full_text)
                    
                    # 팩스번호 정확도 검사
                    target_fax = self._normalize_phone_number(fax_number)
                    fax_exact_match = target_fax in contact_numbers.get('faxes', [])
                    
                    # 기관명 관련 정보 추출
                    institution_keywords = ['주민센터', '구청', '시청', '동사무소', '행정복지센터', '기관', '센터']
                    found_keywords = [kw for kw in institution_keywords if kw in full_text]
                    
                    # 팩스번호 주변 맥락 추출 (더 넓은 범위)
                    fax_context = ""
                    if target_fax in full_text:
                        fax_index = full_text.find(target_fax)
                        start = max(0, fax_index - 200)
                        end = min(len(full_text), fax_index + len(target_fax) + 200)
                        fax_context = full_text[start:end].strip()
                    elif fax_number in full_text:
                        fax_index = full_text.find(fax_number)
                        start = max(0, fax_index - 200)
                        end = min(len(full_text), fax_index + len(fax_number) + 200)
                        fax_context = full_text[start:end].strip()
                    
                    # 기관명 주변 맥락 추출
                    institution_context = ""
                    if institution_name in full_text:
                        inst_index = full_text.find(institution_name)
                        start = max(0, inst_index - 200)
                        end = min(len(full_text), inst_index + len(institution_name) + 200)
                        institution_context = full_text[start:end].strip()
                    
                    return {
                        'url': url,
                        'title': title_text,
                        'full_text': full_text[:1000],  # AI용 텍스트 (1000자 제한)
                        'extracted_phones': contact_numbers.get('phones', []),
                        'extracted_faxes': contact_numbers.get('faxes', []),
                        'fax_exact_match': fax_exact_match,
                        'found_keywords': found_keywords,
                        'fax_context': fax_context,
                        'institution_context': institution_context,
                        'has_fax_number': fax_number in full_text or target_fax in full_text,
                        'has_institution_name': institution_name in full_text,
                        'parsing_method': 'bs4_enhanced',
                        'success': True
                    }
                
            except Exception as e:
                self.logger.debug(f"BS4 파싱 실패: {e}")
            
            return {
                'url': url,
                'success': False,
                'error': 'parsing_failed'
            }
            
        except Exception as e:
            self.logger.error(f"❌ verification_engine 파싱 오류: {e}")
            return {
                'url': url,
                'success': False,
                'error': str(e)
            }
    
    def validate_stage4(self, fax_number: str, institution_name: str, extracted_links: List[str], 
                       discovered_institutions: List[str], worker_id: int = 0) -> Tuple[bool, str, str]:
        """4차 검증: 링크 직접 파싱 + AI 기관명 도출 (verification_engine.py 활용)"""
        try:
            self.logger.debug(f"🤖 4차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}")
            
            # 3차 검증 결과가 없으면 건너뛰기
            if not extracted_links:
                message = "3차 검증 링크 없음으로 4차 검증 건너뛰기"
                self.logger.info(f"⏭️ {message}")
                return False, message, ""
            
            # 링크들을 직접 파싱하여 상세 정보 수집
            detailed_parsing_results = []
            
            self.logger.info(f"🔍 4차 검증: {len(extracted_links)}개 링크 직접 파싱 시작")
            
            for i, link in enumerate(extracted_links):
                self.logger.debug(f"🌐 링크 {i+1}/{len(extracted_links)} 직접 파싱: {link[:50]}...")
                
                # verification_engine 방식으로 상세 파싱
                detailed_result = self._parse_link_with_verification_engine(link, fax_number, institution_name)
                
                if detailed_result.get('success', False):
                    detailed_parsing_results.append(detailed_result)
                    self.logger.debug(f"✅ 링크 {i+1} 상세 파싱 성공")
                else:
                    self.logger.debug(f"⚠️ 링크 {i+1} 상세 파싱 실패")
            
            if not detailed_parsing_results:
                message = "모든 링크 파싱 실패로 4차 검증 실패"
                self.logger.warning(f"⚠️ {message}")
                return False, message, ""
            
            # AI 모델에 전달할 종합 정보 구성
            ai_context = self._prepare_ai_context_for_stage4(
                fax_number, institution_name, detailed_parsing_results, discovered_institutions
            )
            
            # AI 모델을 통한 기관명 도출
            ai_extracted_institution = self._extract_institution_with_ai(ai_context, fax_number, institution_name)
            
            if ai_extracted_institution:
                # AI가 추출한 기관명과 원본 기관명 비교
                similarity_score = self._calculate_institution_similarity(institution_name, ai_extracted_institution)
                
                if similarity_score >= 0.7:  # 70% 이상 유사성
                    message = f"4차 검증 통과: AI 추출 기관명 일치 ({ai_extracted_institution}, 유사도: {similarity_score:.2f})"
                    self.logger.info(f"✅ {message}")
                    return True, message, ai_extracted_institution
                else:
                    message = f"4차 검증 실패: AI 추출 기관명 불일치 ({ai_extracted_institution}, 유사도: {similarity_score:.2f})"
                    self.logger.warning(f"⚠️ {message}")
                    return False, message, ai_extracted_institution
            else:
                message = "4차 검증 실패: AI 기관명 추출 실패"
                self.logger.warning(f"⚠️ {message}")
                return False, message, ""
                
        except Exception as e:
            error_msg = f"4차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            return False, error_msg, ""
    
    def _prepare_ai_context_for_stage4(self, fax_number: str, institution_name: str, 
                                      detailed_results: List[Dict], discovered_institutions: List[str]) -> str:
        """4차 검증용 AI 컨텍스트 준비"""
        try:
            context_parts = [
                f"검증 대상 팩스번호: {fax_number}",
                f"예상 기관명: {institution_name}",
                ""
            ]
            
            # 발견된 기관명들 추가
            if discovered_institutions:
                context_parts.append(f"3차 검증에서 발견된 기관명들: {', '.join(discovered_institutions)}")
                context_parts.append("")
            
            # 상세 파싱 결과들 추가
            for i, result in enumerate(detailed_results):
                context_parts.append(f"=== 웹사이트 {i+1}: {result['url'][:50]}... ===")
                context_parts.append(f"제목: {result.get('title', 'N/A')}")
                
                # 추출된 팩스번호들
                if result.get('extracted_faxes'):
                    context_parts.append(f"추출된 팩스번호: {', '.join(result['extracted_faxes'])}")
                
                # 팩스번호 정확 일치 여부
                if result.get('fax_exact_match'):
                    context_parts.append("✅ 팩스번호 정확 일치 확인")
                
                # 팩스번호 맥락
                if result.get('fax_context'):
                    context_parts.append(f"팩스번호 주변 맥락: {result['fax_context'][:200]}...")
                
                # 기관명 맥락
                if result.get('institution_context'):
                    context_parts.append(f"기관명 주변 맥락: {result['institution_context'][:200]}...")
                
                # 발견된 키워드들
                if result.get('found_keywords'):
                    context_parts.append(f"발견된 기관 키워드: {', '.join(result['found_keywords'])}")
                
                context_parts.append("")
            
            return "\n".join(context_parts)
            
        except Exception as e:
            self.logger.error(f"❌ AI 컨텍스트 준비 실패: {e}")
            return f"검증 대상: {fax_number} - {institution_name}"
    
    def _extract_institution_with_ai(self, context: str, fax_number: str, expected_institution: str) -> str:
        """AI 모델을 통한 기관명 추출"""
        try:
            prompt = f"""다음 정보를 바탕으로 팩스번호 {fax_number}의 실제 소유 기관명을 정확히 추출해주세요.

{context}

분석 요청:
1. 위 웹사이트들에서 팩스번호 {fax_number}이 실제로 어느 기관에 속하는지 확인
2. 예상 기관명 '{expected_institution}'과 실제 기관명이 일치하는지 판단
3. 가장 신뢰할 수 있는 기관명을 하나만 추출

응답 형식: 기관명만 정확히 답변 (예: "종로구청", "청운효자동주민센터")
기관명을 확실히 알 수 없는 경우: "확인불가"
"""
            
            # AI 모델 호출 (ai_model_manager 활용)
            response = self.ai_manager.generate_content(prompt)
            
            if response and response.strip():
                extracted_institution = response.strip()
                
                # "확인불가" 응답 처리
                if "확인불가" in extracted_institution:
                    self.logger.debug("🤖 AI: 기관명 확인불가 응답")
                    return ""
                
                self.logger.info(f"🤖 AI 추출 기관명: {extracted_institution}")
                return extracted_institution
            
            return ""
            
        except Exception as e:
            self.logger.error(f"❌ AI 기관명 추출 실패: {e}")
            return ""
    
    def _calculate_institution_similarity(self, original: str, extracted: str) -> float:
        """기관명 유사도 계산"""
        try:
            # 단순 문자열 유사도 계산
            if original == extracted:
                return 1.0
            
            # 부분 일치 확인
            if original in extracted or extracted in original:
                return 0.8
            
            # 주요 키워드 일치 확인
            original_keywords = set(original.replace('주민센터', '').replace('구청', '').replace('시청', '').split())
            extracted_keywords = set(extracted.replace('주민센터', '').replace('구청', '').replace('시청', '').split())
            
            if original_keywords & extracted_keywords:  # 교집합이 있으면
                return 0.6
            
            return 0.0
            
        except Exception as e:
            self.logger.error(f"❌ 기관명 유사도 계산 실패: {e}")
            return 0.0
    
    def _normalize_phone_number(self, phone: str) -> str:
        """전화번호 정규화 (verification_engine.py 기반)"""
        try:
            # 숫자만 추출
            digits = re.sub(r'[^\d]', '', phone)
            
            # 길이 검증
            if len(digits) < 9 or len(digits) > 11:
                return ''
            
            # 형식 통일 (02-1234-5678)
            if len(digits) == 9:
                return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
            elif len(digits) == 10:
                if digits.startswith('02'):
                    return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
                else:
                    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11:
                return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
            
            return ''
            
        except Exception as e:
            self.logger.error(f"❌ 전화번호 정규화 실패: {e}")
            return ''
    
    def _extract_phone_fax_numbers(self, text: str) -> Dict[str, List[str]]:
        """텍스트에서 전화번호/팩스번호 정확 추출 (verification_engine.py 기반)"""
        try:
            # 기본 전화번호 패턴들
            phone_patterns = [
                r'(\d{2,3}-\d{3,4}-\d{4})',  # 02-1234-5678, 031-123-4567
                r'(\d{2,3}\.\d{3,4}\.\d{4})',  # 02.1234.5678
                r'(\d{2,3}\s\d{3,4}\s\d{4})',  # 02 1234 5678
                r'(\(\d{2,3}\)\s*\d{3,4}-\d{4})',  # (02) 1234-5678
            ]
            
            # 팩스번호 패턴들 (팩스 키워드 포함)
            fax_patterns = [
                r'팩스[:：]\s*(\d{2,3}[-\.\s]\d{3,4}[-\.\s]\d{4})',
                r'fax[:：]\s*(\d{2,3}[-\.\s]\d{3,4}[-\.\s]\d{4})',
                r'F[:：]\s*(\d{2,3}[-\.\s]\d{3,4}[-\.\s]\d{4})',
                r'(?:팩스|fax|F).*?(\d{2,3}[-\.\s]\d{3,4}[-\.\s]\d{4})',
            ]
            
            phones = []
            faxes = []
            
            # 전화번호 추출
            for pattern in phone_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    normalized = self._normalize_phone_number(match)
                    if normalized and normalized not in phones:
                        phones.append(normalized)
            
            # 팩스번호 추출
            for pattern in fax_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    normalized = self._normalize_phone_number(match)
                    if normalized and normalized not in faxes:
                        faxes.append(normalized)
            
            return {
                'phones': phones,
                'faxes': faxes
            }
            
        except Exception as e:
            self.logger.error(f"❌ 전화번호/팩스번호 추출 실패: {e}")
            return {'phones': [], 'faxes': []}
    
    def _crawl_links_parallel(self, links: List[str], fax_number: str, institution_name: str, worker_id: int) -> List[Dict]:
        """링크들을 병렬로 크롤링하여 정보 추출"""
        crawled_data = []
        
        self.logger.debug(f"🕷️ 병렬 링크 크롤링 시작: {len(links)}개 링크")
        
        # 간단한 HTTP 요청으로 빠르게 크롤링 (JavaScript 비활성화)
        for i, link in enumerate(links):
            try:
                self.logger.debug(f"🌐 링크 {i+1}/{len(links)} 크롤링: {link[:50]}...")
                
                # 타임아웃을 짧게 설정하여 속도 우선
                response = requests.get(link, timeout=CRAWLING_TIMEOUT, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
                if response.status_code == 200:
                    # BeautifulSoup으로 텍스트 추출
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 제목 추출
                    title = soup.find('title')
                    title_text = title.get_text(strip=True) if title else ""
                    
                    # 본문 텍스트 추출 (팩스 관련 정보 위주)
                    body_text = soup.get_text()
                    
                    # 정교한 전화번호/팩스번호 추출 (verification_engine.py 방식)
                    contact_numbers = self._extract_phone_fax_numbers(body_text)
                    extracted_phones = contact_numbers.get('phones', [])
                    extracted_faxes = contact_numbers.get('faxes', [])
                    
                    # 팩스번호가 포함된 텍스트 부분 추출
                    fax_related_text = ""
                    target_fax = self._normalize_phone_number(fax_number)
                    
                    # 1. 정확히 일치하는 팩스번호 찾기
                    if target_fax in extracted_faxes:
                        fax_related_text = f"정확한 팩스번호 발견: {target_fax}"
                    # 2. 원본 팩스번호로 텍스트 검색
                    elif fax_number in body_text:
                        fax_index = body_text.find(fax_number)
                        start = max(0, fax_index - 100)
                        end = min(len(body_text), fax_index + len(fax_number) + 100)
                        fax_related_text = body_text[start:end].strip()
                    # 3. 추출된 팩스번호들과 비교
                    elif extracted_faxes:
                        fax_related_text = f"발견된 팩스번호들: {', '.join(extracted_faxes)}"
                    
                    # 기관명 관련 키워드 검색
                    institution_keywords = ['주민센터', '구청', '시청', '동사무소', '행정복지센터', '기관', '센터']
                    found_institutions = []
                    for keyword in institution_keywords:
                        if keyword in body_text:
                            found_institutions.append(keyword)
                    
                    # 추출된 기관명들 수집 (4-5차 검증용)
                    extracted_institution_names = []
                    
                    # 제목에서 기관명 추출
                    for keyword in ['주민센터', '구청', '시청', '동사무소', '행정복지센터']:
                        if keyword in title_text:
                            # 키워드 앞의 지역명 포함하여 추출
                            words = title_text.split()
                            for i, word in enumerate(words):
                                if keyword in word:
                                    # 앞의 1-2개 단어와 함께 기관명 구성
                                    start = max(0, i-2)
                                    extracted_name = ' '.join(words[start:i+1])
                                    if extracted_name not in extracted_institution_names:
                                        extracted_institution_names.append(extracted_name)
                    
                    # 본문에서 기관명 추출 (팩스번호 주변)
                    if fax_related_text:
                        for keyword in ['주민센터', '구청', '시청', '동사무소', '행정복지센터']:
                            if keyword in fax_related_text:
                                # 팩스번호 주변에서 기관명 패턴 찾기
                                import re
                                pattern = r'([가-힣\s]+' + keyword + ')'
                                matches = re.findall(pattern, fax_related_text)
                                for match in matches:
                                    clean_name = match.strip()
                                    if len(clean_name) > 2 and clean_name not in extracted_institution_names:
                                        extracted_institution_names.append(clean_name)
                    
                    # 팩스번호 정확도 검사
                    target_fax = self._normalize_phone_number(fax_number)
                    fax_exact_match = target_fax in extracted_faxes
                    fax_contains = fax_number in body_text
                    
                    crawled_info = {
                        'url': link,
                        'title': title_text,
                        'fax_related_text': fax_related_text,
                        'found_institutions': found_institutions,
                        'extracted_institution_names': extracted_institution_names,  # 4-5차용 추가
                        'extracted_phones': extracted_phones,  # verification_engine 방식
                        'extracted_faxes': extracted_faxes,    # verification_engine 방식
                        'fax_exact_match': fax_exact_match,    # 정확한 팩스번호 일치
                        'has_fax_number': fax_contains,        # 원본 검색 결과
                        'has_institution_name': institution_name in body_text,
                        'text_length': len(body_text)
                    }
                    
                    crawled_data.append(crawled_info)
                    self.logger.debug(f"✅ 링크 {i+1} 크롤링 완료: 팩스번호 포함={crawled_info['has_fax_number']}, 기관명 포함={crawled_info['has_institution_name']}")
                
                else:
                    self.logger.debug(f"⚠️ 링크 {i+1} HTTP 오류: {response.status_code}")
                    
            except Exception as e:
                self.logger.debug(f"❌ 링크 {i+1} 크롤링 실패: {e}")
                continue
        
        self.logger.info(f"🕷️ 병렬 크롤링 완료: {len(crawled_data)}개 성공")
        return crawled_data
    
    def _calculate_confidence_score(self, crawled_data: List[Dict], fax_number: str, institution_name: str) -> float:
        """크롤링 결과를 바탕으로 신뢰도 점수 계산"""
        if not crawled_data:
            return 0.0
        
        total_score = 0.0
        max_possible_score = len(crawled_data) * 100
        
        for data in crawled_data:
            page_score = 0.0
            
            # 팩스번호 정확도 (30점)
            if data.get('fax_exact_match', False):
                page_score += 30  # 정확한 팩스번호 일치: 만점
                self.logger.debug(f"🎯 정확한 팩스번호 일치 발견: +30점")
            elif data['has_fax_number']:
                page_score += 15  # 텍스트에서 발견: 절반 점수
                self.logger.debug(f"📝 팩스번호 텍스트 발견: +15점")
            
            # 기관명 포함 여부 (40점)
            if data['has_institution_name']:
                page_score += 40
            
            # 관련 기관 키워드 발견 (15점)
            if data['found_institutions']:
                page_score += 15
            
            # 추출된 기관명 정보 (15점) - 4-5차 검증용 핵심 데이터
            if data.get('extracted_institution_names'):
                page_score += 15
                # 추출된 기관명이 원본과 유사한 경우 추가 점수
                for extracted_name in data['extracted_institution_names']:
                    if any(word in extracted_name for word in institution_name.split()):
                        page_score += 5  # 최대 5점 추가
                        break
            
            # 팩스 관련 텍스트 품질 (10점)
            if data['fax_related_text'] and len(data['fax_related_text']) > 50:
                page_score += 10
            
            total_score += page_score
            
            self.logger.debug(f"📊 페이지 점수: {page_score}/100 - {data['url'][:30]}...")
        
        # 전체 신뢰도 계산
        confidence = (total_score / max_possible_score) * 100 if max_possible_score > 0 else 0.0
        self.logger.debug(f"📊 전체 신뢰도 점수: {confidence:.1f}% ({total_score}/{max_possible_score})")
        
        return confidence
    
    def validate_single_row(self, row_data: Tuple[int, pd.Series]) -> ValidationResult:
        """개별 행 검증 (Valid.py 로직 기반)"""
        row_idx, row = row_data
        start_time = time.time()
        
        try:
            self.logger.info(f"🔄 행 {row_idx + 1} 검증 시작")
            
            # 데이터 추출 (Valid.py와 동일한 매핑)
            institution_name = str(row.iloc[4]).strip()  # E열 읍면동
            region = str(row.iloc[2]).strip()           # C열 시도  
            address = str(row.iloc[6]).strip()          # G열 주소
            phone_number = str(row.iloc[7]).strip()     # H열 전화번호
            fax_number = str(row.iloc[8]).strip()       # I열 팩스번호
            
            self.logger.debug(f"추출된 데이터:")
            self.logger.debug(f"  기관명: {institution_name}")
            self.logger.debug(f"  지역: {region}")
            self.logger.debug(f"  주소: {address}")
            self.logger.debug(f"  전화번호: {phone_number}")
            self.logger.debug(f"  팩스번호: {fax_number}")
            
            # ValidationResult 초기화
            result = ValidationResult(
                row_index=row_idx,
                fax_number=fax_number,
                institution_name=institution_name,
                region=region,
                phone_number=phone_number,
                address=address
            )
            
            # 1차 검증 실행
            stage1_passed, stage1_message = self.validate_stage1(
                fax_number, institution_name, region, address
            )
            
            result.stage1_passed = stage1_passed
            result.stage1_message = stage1_message
            result.area_code_match = stage1_passed
            
            # 2차 검증 실행 (1차 통과 여부와 무관하게 실행)
            stage2_passed, stage2_message, google_search_result = self.validate_stage2(
                fax_number, institution_name, worker_id=0  # 단일 스레드 테스트용
            )
            
            result.stage2_passed = stage2_passed
            result.stage2_message = stage2_message
            result.google_search_result = google_search_result
            
            # 3차 검증 실행 (2차 결과와 무관하게 실행)
            stage3_passed, stage3_message, extracted_links, crawled_data, confidence_score = self.validate_stage3(
                fax_number, institution_name, google_search_result, worker_id=0  # 단일 스레드 테스트용
            )
            
            result.stage3_passed = stage3_passed
            result.stage3_message = stage3_message
            result.extracted_links = extracted_links or []
            result.crawled_data = crawled_data or []
            result.confidence_score = confidence_score
            
            # 3차 검증에서 발견된 기관명들 수집 (4-5차용)
            all_discovered_institutions = []
            if crawled_data:
                for data in crawled_data:
                    if data.get('extracted_institution_names'):
                        all_discovered_institutions.extend(data['extracted_institution_names'])
            
            # 중복 제거 및 정리
            result.discovered_institutions = list(set(all_discovered_institutions)) if all_discovered_institutions else []
            
            # 발견된 기관명 로깅
            if result.discovered_institutions:
                self.logger.info(f"🏢 3차 검증에서 발견된 기관명들: {', '.join(result.discovered_institutions)}")
            else:
                self.logger.debug("🔍 3차 검증에서 기관명 추출되지 않음")
            
            # 4차 검증 실행 (링크 직접 파싱 + AI 분석)
            stage4_passed, stage4_message, ai_extracted_institution = self.validate_stage4(
                fax_number, institution_name, result.extracted_links, result.discovered_institutions, worker_id=0
            )
            
            result.stage4_passed = stage4_passed
            result.stage4_message = stage4_message
            result.ai_extracted_institution = ai_extracted_institution
            
            # TODO: 5차 검증은 다음 단계에서 구현
            
            # 현재는 1-4차 검증 결과로 전체 결과 설정
            passed_stages = sum([stage1_passed, stage2_passed, stage3_passed, stage4_passed])
            
            if passed_stages == 4:
                result.overall_result = "1-4차 모두 통과"
            elif passed_stages == 3:
                result.overall_result = "4단계 중 3단계 통과"
            elif passed_stages == 2:
                result.overall_result = "4단계 중 2단계 통과"
            elif passed_stages == 1:
                result.overall_result = "4단계 중 1단계 통과"
            else:
                if "팩스번호 없음" in stage1_message or "형식 오류" in stage1_message:
                    result.overall_result = "검증 불가"
                else:
                    result.overall_result = "1-4차 모두 실패"
            
            # 처리 시간 계산
            result.processing_time = time.time() - start_time
            
            self.logger.info(f"✅ 행 {row_idx + 1} 검증 완료: {result.overall_result} ({result.processing_time:.2f}초)")
            
            return result
            
        except Exception as e:
            error_msg = f"행 {row_idx + 1} 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            
            # 오류 결과 반환
            result = ValidationResult(
                row_index=row_idx,
                fax_number="오류",
                institution_name="오류",
                region="오류",
                error_message=error_msg,
                overall_result="검증 오류",
                processing_time=time.time() - start_time
            )
            return result

# ================================
# 메인 함수
# ================================

def main():
    """메인 실행 함수"""
    try:
        # 검증 관리자 초기화
        manager = SimpleValidationManager()
        
        print("=" * 60)
        print("🚀 Valid2.py - 단순화된 5단계 검증 시스템")
        print("=" * 60)
        print("📊 핵심 데이터: E열(읍면동) = I열(팩스번호)")
        print("⚠️ 중요: 전화번호와 팩스번호는 엄밀히 다름")
        print(f"⚙️ 워커 수: {MAX_WORKERS}개")
        print(f"🎯 신뢰도 임계값: {CONFIDENCE_THRESHOLD}%")
        print()
        print("검증 단계 (팩스번호 필수):")
        print("1차: 팩스번호 지역번호 매칭")
        print("2차: Google 검색 - 팩스번호의 진짜 기관명 확인")
        print("3차: 검색결과 링크 크롤링 + 기관명 추출")
        print("4차: AI 기관명 도출 및 매칭")
        print("5차: 최종 종합 판정")
        print()
        
        # 사용자 확인
        choice = input("검증을 시작하시겠습니까? (y/n): ").lower().strip()
        if choice != 'y':
            print("검증을 취소했습니다.")
            return
        
        # Data I/O 테스트
        manager.logger.info("🔄 Data I/O 테스트 시작")
        
        # 1. 데이터 로드 테스트
        print("📊 데이터 로드 중...")
        if not manager.load_data():
            print("❌ 데이터 로드 실패")
            return
        
        print(f"✅ 데이터 로드 성공: {len(manager.input_data)}행")
        
        # 2. 샘플 데이터 정보 출력
        print("\n📋 데이터 샘플 (첫 3행):")
        for i in range(min(3, len(manager.input_data))):
            row = manager.input_data.iloc[i]
            print(f"  행 {i+1}:")
            print(f"    C열(시도): {row.iloc[2] if len(row) > 2 else 'N/A'}")
            print(f"    E열(읍면동): {row.iloc[4] if len(row) > 4 else 'N/A'}")
            print(f"    G열(주소): {row.iloc[6] if len(row) > 6 else 'N/A'}")
            print(f"    H열(전화번호): {row.iloc[7] if len(row) > 7 else 'N/A'}")
            print(f"    I열(팩스번호): {row.iloc[8] if len(row) > 8 else 'N/A'}")
        
        # 3. 테스트용 ValidationResult 생성
        print("\n🧪 결과 저장 테스트...")
        test_result = ValidationResult(
            row_index=0,
            fax_number="02-730-5479",
            institution_name="청운효자동주민센터",
            region="서울",
            phone_number="02-2148-5001",
            address="서울특별시 종로구 자하문로 92",
            stage1_passed=True,
            stage1_message="지역번호 매칭 성공",
            overall_result="테스트 완료",
            processing_time=1.5
        )
        
        manager.validation_results = [test_result]
        
        # 4. 결과 저장 테스트
        saved_file = manager.save_results()
        if saved_file:
            print(f"✅ 결과 저장 테스트 성공: {saved_file}")
        else:
            print("❌ 결과 저장 테스트 실패")
        
        manager.logger.info("🎯 Data I/O 테스트 완료")
        
        # 5. 1-4차 검증 테스트
        print("\n📍 1-4차 검증 테스트...")
        manager.logger.info("🔄 1-4차 검증 테스트 시작")
        
        # 첫 3행으로 1-4차 검증 테스트
        test_results = []
        for i in range(min(3, len(manager.input_data))):
            row_data = (i, manager.input_data.iloc[i])
            result = manager.validate_single_row(row_data)
            test_results.append(result)
            
            print(f"  행 {i+1}: {result.overall_result}")
            print(f"    팩스번호: {result.fax_number}")
            print(f"    기관명: {result.institution_name}")
            print(f"    1차 검증: {'통과' if result.stage1_passed else '실패'} - {result.stage1_message}")
            print(f"    2차 검증: {'통과' if result.stage2_passed else '실패'} - {result.stage2_message}")
            print(f"    3차 검증: {'통과' if result.stage3_passed else '실패'} - {result.stage3_message}")
            print(f"    4차 검증: {'통과' if result.stage4_passed else '실패'} - {result.stage4_message}")
            if result.google_search_result:
                print(f"    Google 검색: {result.google_search_result[:80]}...")
            if result.extracted_links:
                print(f"    추출 링크: {len(result.extracted_links)}개")
            if result.crawled_data:
                print(f"    크롤링 데이터: {len(result.crawled_data)}개, 신뢰도: {result.confidence_score:.1f}%")
            if result.discovered_institutions:
                print(f"    발견된 기관명: {', '.join(result.discovered_institutions)}")
            if result.ai_extracted_institution:
                print(f"    AI 추출 기관명: {result.ai_extracted_institution}")
            print(f"    처리시간: {result.processing_time:.2f}초")
            print()
        
        # 1-4차 검증 결과 저장 테스트
        manager.validation_results = test_results
        saved_file = manager.save_results()
        if saved_file:
            print(f"✅ 1-4차 검증 결과 저장 성공: {saved_file}")
        
        manager.logger.info("🎯 1-4차 검증 테스트 완료")
        print("\n✅ Valid2.py 4차 검증 (링크 직접 파싱 + AI 분석) 로직 구현 완료!")
        print("📋 다음 단계: 5차 검증 (최종 종합 판정) 로직 구현")
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 