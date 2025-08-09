#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Institution Finder v4 - 단순화된 안정성 우선 크롤링 시스템
전화번호/팩스번호로 기관명을 찾는 단순하고 안정적인 시스템

주요 개선사항:
- 복잡한 스니펫 수집 제거 → 기본 Google 검색으로 단순화  
- Chrome 138 안정화 (headless + minimal 방식만 사용)
- acrawl_i5.py 방식의 단계별 저장 도입
- utils 모듈 적극 활용으로 검증된 기능 재사용
- 저장 과정에서 AI 개입으로 효율성 증대

작성자: AI Assistant  
작성일: 2025-01-18
버전: 4.0 - Simplified & Stable
"""

import pandas as pd
import time
import random
import re
import os
import sys
import logging
import gc
import json
import tempfile
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
import threading
from dataclasses import dataclass, field
import traceback

# 환경변수 로드
from dotenv import load_dotenv
load_dotenv()

# 외부 라이브러리 imports
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.keys import Keys

# Gemini AI
import google.generativeai as genai

# 기존 utils 모듈들 활용
from utils.valid.phone_validator import PhoneValidator
from utils.data.excel_processor import ExcelProcessor
from utils.data.data_processor import DataProcessor
from utils.ai_model_manager import AIModelManager
from config.performance_profiles import PerformanceManager

# ================================
# 데이터 클래스
# ================================

@dataclass
class SimpleSearchResult:
    """단순화된 검색 결과"""
    row_index: int
    phone_number: str = ""
    fax_number: str = ""
    found_phone_institution: str = ""
    found_fax_institution: str = ""
    phone_success: bool = False
    fax_success: bool = False
    processing_time: float = 0.0
    error_message: str = ""
    search_text: str = ""  # 추출된 텍스트
    worker_id: int = 0

@dataclass
class SimpleConfig:
    """단순화된 설정"""
    max_workers: int = 2  # 안정성을 위해 줄임
    batch_size: int = 10
    save_interval: int = 10
    execution_mode: str = "full"  # "test" or "full"
    test_sample_size: int = 30
    save_directory: str = ""

# ================================
# 로깅 설정
# ================================

def setup_logging():
    """로깅 시스템 설정"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
    
    # 파일 핸들러
    file_handler = logging.FileHandler(f'eif4_simple_{timestamp}.log', encoding='utf-8')
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

# ================================
# 단순화된 검색 엔진
# ================================

class SimpleSearchEngine:
    """단순화된 검색 엔진 - 안정성 우선"""
    
    def __init__(self, logger=None):
        """단순 검색 엔진 초기화"""
        self.logger = logger or logging.getLogger(__name__)
        self.request_delay = (2.0, 4.0)  # 요청 간 지연
        
        self.logger.info("🔍 단순화된 검색 엔진 초기화 완료")
    
    def create_simple_driver(self, worker_id: int) -> Optional[object]:
        """Chrome 138 안정화 드라이버 생성 (headless + minimal만)"""
        try:
            self.logger.info(f"🚀 워커 {worker_id}: 안정화 드라이버 생성 시작")
            
            # 성공한 방식만 사용: headless 우선, 실패시 minimal
            strategies = [
                self._create_headless_driver,
                self._create_minimal_driver
            ]
            
            for strategy_idx, strategy in enumerate(strategies):
                try:
                    self.logger.info(f"🔧 워커 {worker_id} 전략 {strategy_idx + 1}: {strategy.__name__}")
                    driver = strategy(worker_id)
                    
                    if driver and self._test_driver(driver):
                        self.logger.info(f"✅ 워커 {worker_id}: {strategy.__name__} 성공")
                        return driver
                    else:
                        if driver:
                            driver.quit()
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 전략 {strategy_idx + 1} 실패: {e}")
                    continue
            
            self.logger.error(f"❌ 워커 {worker_id}: 모든 드라이버 생성 전략 실패")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 드라이버 생성 오류 - {e}")
            return None
    
    def _create_headless_driver(self, worker_id: int):
        """헤드리스 Chrome 드라이버 생성 (안정화)"""
        chrome_options = uc.ChromeOptions()
        
        # 헤드리스 모드 (안정화된 옵션만)
        headless_options = [
            '--headless',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1366,768',
            '--disable-logging',
            '--log-level=3',
            '--disable-extensions',
            '--disable-images',  # 속도 향상
            '--disable-plugins'
        ]
        
        for option in headless_options:
            chrome_options.add_argument(option)
        
        # 안전한 포트
        port = 9222 + worker_id + 1000
        chrome_options.add_argument(f'--remote-debugging-port={port}')
        
        # Chrome 138 핵심: version_main=None
        driver = uc.Chrome(options=chrome_options, version_main=None)
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(30)
        
        return driver
    
    def _create_minimal_driver(self, worker_id: int):
        """최소 옵션 Chrome 드라이버 생성 (안정화)"""
        chrome_options = uc.ChromeOptions()
        
        # 최소 옵션만
        minimal_options = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=800,600',
            '--disable-logging',
            '--log-level=3'
        ]
        
        for option in minimal_options:
            chrome_options.add_argument(option)
        
        # 안전한 포트
        port = 9222 + worker_id + 2000
        chrome_options.add_argument(f'--remote-debugging-port={port}')
        
        # Chrome 138 핵심: version_main=None
        driver = uc.Chrome(options=chrome_options, version_main=None)
        driver.implicitly_wait(15)
        driver.set_page_load_timeout(40)
        
        return driver
    
    def _test_driver(self, driver) -> bool:
        """드라이버 테스트"""
        try:
            driver.get("https://www.google.com")
            time.sleep(2)
            title = driver.title.lower()
            return "google" in title or "구글" in title
        except:
            return False
    
    def simple_google_search(self, driver, number: str, number_type: str = "전화") -> str:
        """단순한 Google 검색 (텍스트만 추출)"""
        try:
            # 검색 쿼리 생성
            query = f'"{number}" 기관 {number_type}번호'
            
            self.logger.debug(f"🔍 검색: {query}")
            
            # Google 검색 수행
            driver.get("https://www.google.com")
            time.sleep(random.uniform(1.0, 2.0))
            
            # 검색창 찾기
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            
            # 검색어 입력
            search_box.clear()
            search_box.send_keys(query)
            search_box.send_keys(Keys.RETURN)
            
            # 결과 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "search"))
            )
            time.sleep(random.uniform(2.0, 3.0))
            
            # 페이지 전체 텍스트 추출 (단순화)
            page_text = driver.find_element(By.TAG_NAME, "body").text
            
            # 검색 간 지연
            time.sleep(random.uniform(*self.request_delay))
            
            return page_text[:5000]  # 처음 5000자만
            
        except Exception as e:
            self.logger.warning(f"⚠️ Google 검색 실패: {e}")
            return ""

# ================================
# AI 후처리 엔진
# ================================

class SimpleAIProcessor:
    """단순화된 AI 후처리기"""
    
    def __init__(self, logger=None):
        """AI 후처리기 초기화"""
        self.logger = logger or logging.getLogger(__name__)
        self.ai_manager = AIModelManager(self.logger)
        
        self.logger.info("🤖 단순화된 AI 후처리기 초기화 완료")
    
    def extract_institution_name(self, search_text: str, number: str, number_type: str = "전화") -> Optional[str]:
        """AI를 통한 기관명 추출 (단순화)"""
        try:
            if not search_text or len(search_text) < 50:
                return None
            
            # 단순한 프롬프트
            prompt = f"""
다음 Google 검색 결과에서 '{number}' {number_type}번호를 사용하는 기관명을 찾아주세요.

검색 결과:
{search_text[:3000]}

요청사항:
1. {number}번호와 연관된 정확한 기관명만 추출
2. 정식 기관명을 우선 (예: XX구청, XX주민센터, XX병원 등)
3. 확실하지 않으면 "찾을 수 없음" 응답

응답 형식: 기관명만 간단히 (설명 없이)
"""
            
            # AI 호출
            result = self.ai_manager.extract_with_gemini(search_text, prompt)
            
            if result and "찾을 수 없음" not in result and len(result) > 3:
                # 기관명 정제
                cleaned_name = self._clean_institution_name(result)
                if cleaned_name:
                    self.logger.info(f"🎯 AI 추출 성공: {cleaned_name}")
                    return cleaned_name
            
            return None
            
        except Exception as e:
            self.logger.warning(f"⚠️ AI 추출 실패: {e}")
            return None
    
    def _clean_institution_name(self, raw_name: str) -> Optional[str]:
        """기관명 정제"""
        try:
            # 기본 정제
            cleaned = raw_name.strip().replace('\n', ' ')
            cleaned = re.sub(r'\s+', ' ', cleaned)
            
            # 불필요한 문구 제거
            remove_patterns = [
                r'^(답변:|기관명:|결과:)',
                r'입니다$',
                r'것으로.*',
                r'같습니다$'
            ]
            
            for pattern in remove_patterns:
                cleaned = re.sub(pattern, '', cleaned).strip()
            
            # 길이 체크
            if 3 <= len(cleaned) <= 50:
                return cleaned
            
            return None
            
        except:
            return None

# ================================
# 메인 프로세서
# ================================

class SimpleInstitutionProcessor:
    """단순화된 기관명 추출 프로세서"""
    
    def __init__(self, config: SimpleConfig):
        """메인 프로세서 초기화"""
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # 기존 utils 모듈들 활용
        self.phone_validator = PhoneValidator(self.logger)
        self.excel_processor = ExcelProcessor(self.logger)
        self.data_processor = DataProcessor(self.logger)
        self.performance_manager = PerformanceManager(self.logger)
        
        # 새로운 단순화 엔진들
        self.search_engine = SimpleSearchEngine(self.logger)
        self.ai_processor = SimpleAIProcessor(self.logger)
        
        # 드라이버 관리
        self.worker_drivers = {}
        self.lock = threading.Lock()
        
        # 통계
        self.total_rows = 0
        self.processed_count = 0
        self.phone_success = 0
        self.fax_success = 0
        
        # Desktop 경로 설정 (기존 utils 방식)
        self.config.save_directory = self._setup_desktop_path()
        
        self.logger.info("🚀 단순화된 기관명 추출 프로세서 초기화 완료")
        self.logger.info(f"⚙️ 설정: {self.config.execution_mode} 모드, 워커 {self.config.max_workers}개")
    
    def _setup_desktop_path(self) -> str:
        """Desktop 경로 자동 설정 (기존 방식)"""
        try:
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            if os.path.exists(desktop_path):
                return desktop_path
            else:
                return os.path.expanduser("~")
        except:
            return os.getcwd()
    
    def load_and_prepare_data(self, filepath: str) -> pd.DataFrame:
        """데이터 로드 및 준비 (기존 방식 활용)"""
        try:
            # Excel 파일 로드
            success = self.excel_processor.load_excel_file(filepath)
            if not success:
                raise ValueError(f"파일 로드 실패: {filepath}")
            
            df = self.excel_processor.df
            self.logger.info(f"📊 데이터 로드 완료: {len(df)}행 × {len(df.columns)}열")
            self.logger.info(f"📋 컬럼: {list(df.columns)}")
            
            # 테스트 모드 처리
            if self.config.execution_mode == "test":
                df = self._create_test_sample(df)
                self.logger.info(f"🧪 테스트 샘플: {len(df)}행")
            
            self.total_rows = len(df)
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def _create_test_sample(self, df: pd.DataFrame) -> pd.DataFrame:
        """테스트용 샘플 생성"""
        try:
            sample_size = min(self.config.test_sample_size, len(df))
            return df.sample(n=sample_size, random_state=42).sort_index()
        except Exception as e:
            self.logger.warning(f"⚠️ 테스트 샘플 생성 실패: {e}")
            return df.head(self.config.test_sample_size)
    
    def process_single_row(self, row_data: Tuple[int, pd.Series], worker_id: int) -> SimpleSearchResult:
        """개별 행 처리 (단순화)"""
        row_idx, row = row_data
        result = SimpleSearchResult(row_index=row_idx, worker_id=worker_id)
        start_time = time.time()
        
        try:
            # 데이터 추출
            phone_number = str(row.iloc[6]).strip() if len(row) > 6 else ""
            fax_number = str(row.iloc[8]).strip() if len(row) > 8 else ""
            
            # 기존 결과 확인
            existing_phone = str(row.iloc[7]).strip() if len(row) > 7 else ""
            existing_fax = str(row.iloc[9]).strip() if len(row) > 9 else ""
            
            result.phone_number = phone_number
            result.fax_number = fax_number
            
            self.logger.info(f"🔄 워커 {worker_id}: 행 {row_idx+1} 처리 시작")
            
            # 드라이버 가져오기
            driver = self._get_worker_driver(worker_id)
            if not driver:
                result.error_message = "드라이버 없음"
                return result
            
            processed_items = []
            
            # 전화번호 처리
            if (phone_number and phone_number not in ['nan', 'None', ''] and 
                (not existing_phone or existing_phone in ['nan', 'None', '']) and
                self.phone_validator.is_valid_phone_format(phone_number)):
                
                self.logger.info(f"📞 워커 {worker_id}: 전화번호 {phone_number} 검색")
                
                try:
                    search_text = self.search_engine.simple_google_search(driver, phone_number, "전화")
                    if search_text:
                        institution_name = self.ai_processor.extract_institution_name(search_text, phone_number, "전화")
                        if institution_name:
                            result.found_phone_institution = institution_name
                            result.phone_success = True
                            processed_items.append(f"전화({institution_name})")
                        else:
                            processed_items.append("전화(AI실패)")
                    else:
                        processed_items.append("전화(검색실패)")
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 전화번호 처리 오류: {e}")
                    processed_items.append("전화(오류)")
            else:
                processed_items.append("전화(스킵)")
            
            # 팩스번호 처리
            if (fax_number and fax_number not in ['nan', 'None', ''] and 
                (not existing_fax or existing_fax in ['nan', 'None', '']) and
                self.phone_validator.is_valid_phone_format(fax_number)):
                
                self.logger.info(f"📠 워커 {worker_id}: 팩스번호 {fax_number} 검색")
                
                try:
                    search_text = self.search_engine.simple_google_search(driver, fax_number, "팩스")
                    if search_text:
                        institution_name = self.ai_processor.extract_institution_name(search_text, fax_number, "팩스")
                        if institution_name:
                            result.found_fax_institution = institution_name
                            result.fax_success = True
                            processed_items.append(f"팩스({institution_name})")
                        else:
                            processed_items.append("팩스(AI실패)")
                    else:
                        processed_items.append("팩스(검색실패)")
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 팩스번호 처리 오류: {e}")
                    processed_items.append("팩스(오류)")
            else:
                processed_items.append("팩스(스킵)")
            
            result.processing_time = time.time() - start_time
            
            # 통계 업데이트
            with self.lock:
                self.processed_count += 1
                if result.phone_success:
                    self.phone_success += 1
                if result.fax_success:
                    self.fax_success += 1
            
            self.logger.info(f"✅ 워커 {worker_id}: 행 {row_idx+1} 완료 - {', '.join(processed_items)} ({result.processing_time:.1f}초)")
            
            return result
            
        except Exception as e:
            result.error_message = str(e)
            result.processing_time = time.time() - start_time
            self.logger.error(f"❌ 워커 {worker_id}: 행 {row_idx+1} 처리 오류 - {e}")
            return result
    
    def _get_worker_driver(self, worker_id: int):
        """워커별 드라이버 가져오기"""
        # 기존 드라이버 확인
        if worker_id in self.worker_drivers:
            try:
                driver = self.worker_drivers[worker_id]
                # 간단한 상태 확인
                driver.current_url
                return driver
            except:
                # 드라이버 문제시 제거
                try:
                    self.worker_drivers[worker_id].quit()
                except:
                    pass
                del self.worker_drivers[worker_id]
        
        # 새 드라이버 생성
        try:
            self.logger.info(f"🔧 워커 {worker_id}: 새 드라이버 생성")
            driver = self.search_engine.create_simple_driver(worker_id)
            if driver:
                with self.lock:
                    self.worker_drivers[worker_id] = driver
                self.logger.info(f"✅ 워커 {worker_id}: 드라이버 생성 성공")
                return driver
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 드라이버 생성 실패 - {e}")
        
        return None
    
    def save_intermediate_results(self, results: List[Dict], batch_idx: int) -> bool:
        """중간 결과 저장 (acrawl_i5.py 방식)"""
        try:
            if not results:
                return True
                
            self.logger.info(f"💾 중간 결과 저장: {len(results)}개 (배치 {batch_idx})")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.config.save_directory, 
                f"eif4_중간결과_배치{batch_idx:03d}_{timestamp}.xlsx")
            
            df_result = pd.DataFrame(results)
            
            # acrawl_i5.py 방식: ExcelWriter 사용
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False)
                
            self.logger.info(f"✅ 중간 저장 완료: {filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 중간 저장 실패: {e}")
            return False
    
    def save_final_results(self, all_results: List[Dict]) -> str:
        """최종 결과 저장 (acrawl_i5.py 방식)"""
        try:
            if not all_results:
                self.logger.warning("⚠️ 저장할 결과가 없습니다")
                return ""
                
            self.logger.info(f"💾 최종 결과 저장: {len(all_results)}개")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.config.save_directory, 
                f"eif4_최종결과_{self.config.execution_mode}_{timestamp}.xlsx")
            
            df_result = pd.DataFrame(all_results)
            
            # 컬럼 순서 정리
            priority_columns = [
                'row_index', 'phone_number', 'fax_number', 
                'found_phone_institution', 'found_fax_institution',
                'phone_success', 'fax_success', 'processing_time', 'worker_id'
            ]
            
            existing_priority = [col for col in priority_columns if col in df_result.columns]
            other_columns = [col for col in df_result.columns if col not in existing_priority]
            final_columns = existing_priority + other_columns
            
            df_result = df_result[final_columns]
            
            # acrawl_i5.py 방식: ExcelWriter 사용
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False)
            
            # 통계 로그
            success_count = len([r for r in all_results if r.get('phone_success') or r.get('fax_success')])
            success_rate = (success_count / len(all_results)) * 100 if all_results else 0
            
            self.logger.info(f"✅ 최종 저장 완료: {filename}")
            self.logger.info(f"📊 성공률: {success_count}/{len(all_results)} ({success_rate:.1f}%)")
            
            return filename
            
        except Exception as e:
            self.logger.error(f"❌ 최종 저장 실패: {e}")
            return ""
    
    def process_file(self, input_filepath: str) -> str:
        """파일 전체 처리 (단순화 + 안정성)"""
        try:
            # 시스템 정보 출력
            self.performance_manager.display_performance_info()
            
            # 데이터 로드
            df = self.load_and_prepare_data(input_filepath)
            
            self.logger.info(f"🚀 단순화 처리 시작: {len(df)}행")
            self.logger.info(f"⚙️ 워커 {self.config.max_workers}개, 배치 {self.config.batch_size}개")
            
            all_results = []
            
            # 배치별 처리
            total_batches = (len(df) + self.config.batch_size - 1) // self.config.batch_size
            
            for batch_start in range(0, len(df), self.config.batch_size):
                batch_end = min(batch_start + self.config.batch_size, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                
                batch_num = (batch_start // self.config.batch_size) + 1
                
                self.logger.info(f"📦 배치 {batch_num}/{total_batches}: {batch_start+1}~{batch_end}")
                
                batch_results = []
                
                try:
                    # 배치 내 병렬 처리
                    with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                        futures = []
                        
                        for idx, (original_idx, row) in enumerate(batch_df.iterrows()):
                            worker_id = idx % self.config.max_workers
                            future = executor.submit(self.process_single_row, (original_idx, row), worker_id)
                            futures.append((future, original_idx))
                        
                        # 결과 수집
                        for future, row_idx in futures:
                            try:
                                result = future.result(timeout=300)  # 5분 타임아웃
                                
                                # 결과를 딕셔너리로 변환
                                result_dict = {
                                    'row_index': row_idx + 1,
                                    'phone_number': result.phone_number,
                                    'fax_number': result.fax_number,
                                    'found_phone_institution': result.found_phone_institution,
                                    'found_fax_institution': result.found_fax_institution,
                                    'phone_success': result.phone_success,
                                    'fax_success': result.fax_success,
                                    'processing_time': result.processing_time,
                                    'error_message': result.error_message,
                                    'worker_id': result.worker_id,
                                    'batch_number': batch_num,
                                    'processed_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                
                                batch_results.append(result_dict)
                                all_results.append(result_dict)
                                
                            except Exception as e:
                                self.logger.error(f"❌ 행 {row_idx+1} 결과 처리 오류: {e}")
                    
                    # 배치 완료 후 중간 저장
                    if len(batch_results) > 0:
                        self.save_intermediate_results(batch_results, batch_num)
                    
                    # 진행률 출력
                    progress = (batch_end / len(df)) * 100
                    self.logger.info(f"📊 진행률: {batch_end}/{len(df)} ({progress:.1f}%) - 성공(전화:{self.phone_success}, 팩스:{self.fax_success})")
                    
                except Exception as batch_error:
                    self.logger.error(f"❌ 배치 {batch_num} 처리 실패: {batch_error}")
                
                # 배치 간 휴식
                if batch_end < len(df):
                    rest_time = random.uniform(3.0, 6.0)
                    self.logger.info(f"⏱️ 배치 휴식: {rest_time:.1f}초")
                    time.sleep(rest_time)
            
            # 최종 저장
            final_file = self.save_final_results(all_results)
            
            # 최종 통계
            self._print_final_statistics()
            
            return final_file
            
        except Exception as e:
            self.logger.error(f"❌ 파일 처리 실패: {e}")
            self.logger.error(traceback.format_exc())
            raise
        finally:
            self._cleanup_drivers()
    
    def _cleanup_drivers(self):
        """모든 드라이버 정리"""
        try:
            self.logger.info("🧹 드라이버 정리 시작")
            
            for worker_id, driver in list(self.worker_drivers.items()):
                try:
                    driver.quit()
                    self.logger.info(f"✅ 워커 {worker_id} 드라이버 정리")
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 드라이버 정리 실패: {e}")
            
            self.worker_drivers.clear()
            gc.collect()
            
            self.logger.info("🧹 드라이버 정리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 드라이버 정리 오류: {e}")
    
    def _print_final_statistics(self):
        """최종 통계 출력"""
        self.logger.info("=" * 60)
        self.logger.info("📊 Enhanced Institution Finder v4 - 최종 통계")
        self.logger.info("=" * 60)
        self.logger.info(f"실행 모드: {self.config.execution_mode}")
        self.logger.info(f"전체 행 수: {self.total_rows:,}")
        self.logger.info(f"처리 완료: {self.processed_count:,}")
        self.logger.info(f"전화번호 성공: {self.phone_success:,}")
        self.logger.info(f"팩스번호 성공: {self.fax_success:,}")
        
        if self.processed_count > 0:
            phone_rate = (self.phone_success / self.processed_count) * 100
            fax_rate = (self.fax_success / self.processed_count) * 100
            
            self.logger.info(f"전화번호 성공률: {phone_rate:.1f}%")
            self.logger.info(f"팩스번호 성공률: {fax_rate:.1f}%")
        
        self.logger.info("🆕 v4 핵심 특징:")
        self.logger.info("   - 🔧 단순화된 안정성 우선 구조")
        self.logger.info("   - 🤖 저장 과정 AI 후처리")
        self.logger.info("   - 📊 기존 utils 모듈 적극 활용")
        self.logger.info("   - ✅ Chrome 138 완전 호환")
        self.logger.info("=" * 60)

# ================================
# 설정 관리자
# ================================

def setup_config():
    """설정 관리"""
    print("=" * 60)
    print("🚀 Enhanced Institution Finder v4 - 단순화된 안정성 우선")
    print("=" * 60)
    print("🆕 v4 주요 개선사항:")
    print("   - 복잡한 스니펫 수집 제거 → 기본 Google 검색")
    print("   - Chrome 138 안정화 (headless + minimal만)")
    print("   - acrawl_i5.py 방식 단계별 저장 도입")
    print("   - 저장 과정에서 AI 개입으로 효율성 증대")
    print()
    
    config = SimpleConfig()
    
    print("📋 실행 모드 선택:")
    print("1. 🧪 테스트 모드 (30개 샘플)")
    print("2. 🔄 전체 처리")
    
    while True:
        try:
            choice = input("선택 (1-2): ").strip()
            if choice == "1":
                config.execution_mode = "test"
                config.max_workers = 1  # 테스트는 단일 워커
                config.batch_size = 5
                print("✅ 테스트 모드 설정 완료")
                break
            elif choice == "2":
                config.execution_mode = "full"
                print("✅ 전체 처리 모드 설정 완료")
                break
            else:
                print("❌ 1-2 중에서 선택해주세요")
        except KeyboardInterrupt:
            print("\n🚫 사용자 취소")
            sys.exit(0)
    
    print(f"\n📊 최종 설정:")
    print(f"   - 모드: {config.execution_mode}")
    print(f"   - 워커: {config.max_workers}개")
    print(f"   - 배치: {config.batch_size}개")
    print(f"   - 저장위치: Desktop")
    
    return config

# ================================
# 메인 함수
# ================================

def main():
    """메인 실행 함수"""
    # 로깅 설정
    logger = setup_logging()
    
    try:
        logger.info("🚀 Enhanced Institution Finder v4 시작")
        logger.info("🎯 단순화된 안정성 우선 크롤링 시스템")
        
        # 설정
        config = setup_config()
        
        # 입력 파일 확인
        input_file = 'rawdatafile/failed_data_250715.xlsx'
        
        if not os.path.exists(input_file):
            logger.error(f"❌ 입력 파일 없음: {input_file}")
            
            # 사용 가능한 파일 찾기
            rawdata_dir = 'rawdatafile'
            if os.path.exists(rawdata_dir):
                files = [f for f in os.listdir(rawdata_dir) if f.endswith(('.xlsx', '.csv'))]
                if files:
                    print(f"\n📁 사용 가능한 파일들:")
                    for i, file in enumerate(files, 1):
                        print(f"   {i}. {file}")
                    
                    try:
                        choice = input(f"파일 선택 (1-{len(files)}): ").strip()
                        if choice.isdigit() and 1 <= int(choice) <= len(files):
                            input_file = os.path.join(rawdata_dir, files[int(choice)-1])
                            logger.info(f"📄 선택된 파일: {input_file}")
                        else:
                            raise ValueError("잘못된 선택")
                    except:
                        logger.error("❌ 파일 선택 실패")
                        sys.exit(1)
            else:
                sys.exit(1)
        
        # 메인 프로세서 초기화 및 실행
        processor = SimpleInstitutionProcessor(config)
        result_file = processor.process_file(input_file)
        
        if result_file:
            logger.info(f"🎉 처리 완료! 결과: {result_file}")
            print(f"\n🎊 Enhanced Institution Finder v4 완료!")
            print(f"📁 결과 파일: {result_file}")
            print(f"🎯 단순화된 안정성 우선 처리 완료")
        else:
            logger.error("❌ 처리 실패")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ 사용자 중단")
        print("\n⚠️ 작업이 중단되었습니다")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 시스템 오류: {e}")
        logger.error(traceback.format_exc())
        print(f"\n❌ 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 