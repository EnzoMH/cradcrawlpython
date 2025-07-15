#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실제기관명 추출 시스템 (Institution Name Extractor)

전화번호와 팩스번호를 기반으로 구글 검색을 통해 실제 기관명을 추출하는 시스템
AMD Ryzen 5 3600 환경에 최적화된 성능 설정 적용

작성자: AI Assistant
작성일: 2025-01-15
"""

import pandas as pd
import numpy as np
import time
import random
import re
import os
import sys
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
import threading
from dataclasses import dataclass, field
from collections import defaultdict
import queue
import json
import traceback

# 셀레니움 관련 imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, NoSuchElementException,
    ElementNotInteractableException, StaleElementReferenceException
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('institution_name_extractor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 전역 설정
HEADLESS_MODE = True  # Headless 모드 고정
MAX_WORKERS = 12  # AMD Ryzen 5 3600 기준 최적화 (6코어 * 2)
MIN_WORKERS = 9   # 최소 워커 수
CURRENT_WORKERS = MAX_WORKERS  # 현재 워커 수 (동적 조정)

@dataclass
class SearchResult:
    """검색 결과 데이터 클래스"""
    phone_number: str
    institution_name: str = ""
    confidence: float = 0.0
    search_successful: bool = False
    error_message: str = ""
    search_time: float = 0.0

@dataclass
class ExtractionStats:
    """추출 통계 데이터 클래스"""
    total_processed: int = 0
    phone_extractions: int = 0
    fax_extractions: int = 0
    successful_extractions: int = 0
    failed_extractions: int = 0
    empty_numbers: int = 0
    search_times: List[float] = field(default_factory=list)
    error_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    def add_search_time(self, time_taken: float):
        """검색 시간 추가"""
        self.search_times.append(time_taken)
    
    def get_average_search_time(self) -> float:
        """평균 검색 시간 계산"""
        return sum(self.search_times) / len(self.search_times) if self.search_times else 0.0
    
    def get_success_rate(self) -> float:
        """성공률 계산"""
        if self.total_processed == 0:
            return 0.0
        return (self.successful_extractions / self.total_processed) * 100

class SystemMonitor:
    """시스템 성능 모니터링 클래스"""
    
    def __init__(self):
        self.start_time = time.time()
        self.worker_performance = defaultdict(list)
        self.lock = threading.Lock()
    
    def record_worker_performance(self, worker_id: str, processing_time: float, success: bool):
        """워커 성능 기록"""
        with self.lock:
            self.worker_performance[worker_id].append({
                'time': processing_time,
                'success': success,
                'timestamp': time.time()
            })
    
    def get_worker_stats(self) -> Dict[str, Dict]:
        """워커별 통계 반환"""
        with self.lock:
            stats = {}
            for worker_id, performances in self.worker_performance.items():
                if performances:
                    times = [p['time'] for p in performances]
                    successes = [p['success'] for p in performances]
                    stats[worker_id] = {
                        'total_tasks': len(performances),
                        'success_rate': sum(successes) / len(successes) * 100,
                        'avg_time': sum(times) / len(times),
                        'min_time': min(times),
                        'max_time': max(times)
                    }
            return stats
    
    def should_adjust_workers(self) -> Tuple[bool, str]:
        """워커 수 조정 필요 여부 판단"""
        stats = self.get_worker_stats()
        if not stats:
            return False, "no_data"
        
        # 평균 처리 시간이 너무 긴 경우 워커 수 감소
        avg_times = [s['avg_time'] for s in stats.values()]
        if avg_times and sum(avg_times) / len(avg_times) > 15.0:  # 15초 이상
            return True, "decrease"
        
        # 성공률이 너무 낮은 경우 워커 수 감소
        success_rates = [s['success_rate'] for s in stats.values()]
        if success_rates and sum(success_rates) / len(success_rates) < 50.0:  # 50% 미만
            return True, "decrease"
        
        return False, "maintain"

class WebDriverManager:
    """웹드라이버 관리 클래스"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver_options = self._setup_chrome_options()
    
    def _setup_chrome_options(self) -> Options:
        """크롬 옵션 설정"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # 메모리 최적화 설정 (8GB 환경 고려)
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('--disable-javascript')
        options.add_argument('--memory-pressure-off')
        options.add_argument('--max_old_space_size=512')
        
        # 성능 최적화
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-sync')
        
        # 사용자 에이전트 설정
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        return options
    
    def create_driver(self) -> webdriver.Chrome:
        """새로운 웹드라이버 생성"""
        try:
            return webdriver.Chrome(options=self.driver_options)
        except Exception as e:
            logger.error(f"웹드라이버 생성 실패: {e}")
            raise

class GoogleSearchEngine:
    """구글 검색 엔진 클래스"""
    
    def __init__(self, driver_manager: WebDriverManager):
        self.driver_manager = driver_manager
        self.search_patterns = [
            r'([가-힣]+(?:구청|시청|군청|동사무소|주민센터|행정복지센터|면사무소|읍사무소))',
            r'([가-힣]+(?:대학교|대학|학교|교육청|교육지원청))',
            r'([가-힣]+(?:병원|의원|클리닉|보건소|보건센터))',
            r'([가-힣]+(?:경찰서|파출소|지구대|소방서|소방관서))',
            r'([가-힣]+(?:법원|검찰청|등기소|세무서))',
            r'([가-힣]+(?:우체국|체신청|통신사업소))',
            r'([가-힣]+(?:공사|공단|공기업|기관|센터|사업소))',
            r'([가-힣\s]+)(?:\s|$)'
        ]
    
    def search_institution_name(self, phone_number: str, number_type: str = "전화번호") -> SearchResult:
        """전화번호로 기관명 검색"""
        if not phone_number or phone_number.strip() == "":
            return SearchResult(
                phone_number=phone_number,
                search_successful=False,
                error_message="빈 번호"
            )
        
        # 전화번호 정규화
        clean_number = self._normalize_phone_number(phone_number)
        if not clean_number:
            return SearchResult(
                phone_number=phone_number,
                search_successful=False,
                error_message="잘못된 번호 형식"
            )
        
        driver = None
        start_time = time.time()
        
        try:
            driver = self.driver_manager.create_driver()
            
            # 검색 쿼리 생성
            search_query = f'"{clean_number}" {number_type}'
            
            # 구글 검색 실행
            driver.get('https://www.google.com')
            
            # 검색창 찾기 및 검색
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            
            search_box.clear()
            search_box.send_keys(search_query)
            search_box.send_keys(Keys.RETURN)
            
            # 검색 결과 대기
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "search"))
            )
            
            # 기관명 추출
            institution_name = self._extract_institution_name(driver, clean_number)
            
            search_time = time.time() - start_time
            
            return SearchResult(
                phone_number=phone_number,
                institution_name=institution_name,
                confidence=0.8 if institution_name else 0.0,
                search_successful=bool(institution_name),
                search_time=search_time
            )
            
        except TimeoutException:
            return SearchResult(
                phone_number=phone_number,
                search_successful=False,
                error_message="검색 타임아웃",
                search_time=time.time() - start_time
            )
        except Exception as e:
            return SearchResult(
                phone_number=phone_number,
                search_successful=False,
                error_message=f"검색 오류: {str(e)}",
                search_time=time.time() - start_time
            )
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def _normalize_phone_number(self, phone_number: str) -> str:
        """전화번호 정규화"""
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
    
    def _extract_institution_name(self, driver: webdriver.Chrome, phone_number: str) -> str:
        """검색 결과에서 기관명 추출"""
        try:
            # 검색 결과 요소들 찾기
            search_results = driver.find_elements(By.CSS_SELECTOR, "div.g")
            
            for result in search_results[:5]:  # 상위 5개 결과만 확인
                try:
                    # 제목과 설명 텍스트 추출
                    title_element = result.find_element(By.CSS_SELECTOR, "h3")
                    title_text = title_element.text if title_element else ""
                    
                    snippet_element = result.find_element(By.CSS_SELECTOR, "div[data-sncf]")
                    snippet_text = snippet_element.text if snippet_element else ""
                    
                    # 전체 텍스트에서 기관명 추출
                    full_text = f"{title_text} {snippet_text}"
                    institution_name = self._parse_institution_name(full_text, phone_number)
                    
                    if institution_name:
                        return institution_name
                        
                except Exception as e:
                    continue
            
            return ""
            
        except Exception as e:
            logger.error(f"기관명 추출 오류: {e}")
            return ""
    
    def _parse_institution_name(self, text: str, phone_number: str) -> str:
        """텍스트에서 기관명 파싱"""
        if not text:
            return ""
        
        # 패턴 매칭으로 기관명 추출
        for pattern in self.search_patterns:
            matches = re.findall(pattern, text)
            if matches:
                # 가장 적절한 매치 선택
                for match in matches:
                    institution_name = match.strip()
                    if len(institution_name) >= 2 and len(institution_name) <= 30:
                        # 전화번호와 연관성 확인
                        if self._is_relevant_institution(institution_name, text, phone_number):
                            return institution_name
        
        return ""
    
    def _is_relevant_institution(self, institution_name: str, full_text: str, phone_number: str) -> bool:
        """기관명의 연관성 확인"""
        # 기본 필터링
        if not institution_name or len(institution_name) < 2:
            return False
        
        # 불필요한 단어 필터링
        exclude_words = ['전화번호', '팩스번호', '연락처', '문의', '상담', '예약', '신청']
        if any(word in institution_name for word in exclude_words):
            return False
        
        # 전화번호가 텍스트에 포함되어 있는지 확인
        clean_phone = re.sub(r'[^\d]', '', phone_number)
        if clean_phone in re.sub(r'[^\d]', '', full_text):
            return True
        
        return False

class InstitutionNameExtractor:
    """실제기관명 추출 메인 클래스"""
    
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        self.stats = ExtractionStats()
        self.system_monitor = SystemMonitor()
        self.driver_manager = WebDriverManager(headless=HEADLESS_MODE)
        self.search_engine = GoogleSearchEngine(self.driver_manager)
        self.lock = threading.Lock()
        
        # 동적 워커 수 조정
        self.current_workers = MAX_WORKERS
        self.worker_adjustment_interval = 50  # 50개 처리마다 워커 수 조정 검토
    
    def load_data(self) -> pd.DataFrame:
        """Excel 데이터 로드"""
        try:
            logger.info(f"데이터 로드 시작: {self.input_file}")
            
            # Excel 파일 읽기
            df = pd.read_excel(self.input_file)
            logger.info(f"총 {len(df)}개 행 로드 완료")
            
            # 실제 컬럼 구조 확인
            logger.info(f"원본 컬럼: {list(df.columns)}")
            
            # 컬럼명 정리 - 실제 파일 구조에 맞게 수정
            if len(df.columns) == 10:
                # 예상 구조: 연번, 시도, 시군구, 읍면동, 우편번호, 주소, 전화번호, 실제기관명(전화), 팩스번호, 실제기관명(팩스)
                new_columns = ['연번', '시도', '시군구', '읍면동', '우편번호', '주    소', '전화번호', '전화번호_실제기관명', '팩스번호', '팩스번호_실제기관명']
                df.columns = new_columns
            else:
                # 기존 컬럼명 유지하되 실제기관명 컬럼 구분
                columns = list(df.columns)
                for i, col in enumerate(columns):
                    if '실제기관명' in str(col):
                        if i == 7:  # 전화번호 다음
                            columns[i] = '전화번호_실제기관명'
                        elif i == 9:  # 팩스번호 다음
                            columns[i] = '팩스번호_실제기관명'
                df.columns = columns
            
            # 빈 값 처리
            df = df.fillna('')
            
            # 전화번호_실제기관명 컬럼이 없으면 생성
            if '전화번호_실제기관명' not in df.columns:
                df['전화번호_실제기관명'] = ''
            
            # 팩스번호_실제기관명 컬럼이 없으면 생성
            if '팩스번호_실제기관명' not in df.columns:
                df['팩스번호_실제기관명'] = ''
            
            logger.info(f"데이터 로드 완료 - 컬럼: {list(df.columns)}")
            return df
            
        except Exception as e:
            logger.error(f"데이터 로드 실패: {e}")
            raise
    
    def process_single_row(self, row_data: Tuple[int, pd.Series]) -> Dict[str, Any]:
        """단일 행 처리"""
        idx, row = row_data
        worker_id = f"worker_{threading.current_thread().ident}"
        start_time = time.time()
        
        try:
            results = {
                'index': idx,
                'phone_institution': '',
                'fax_institution': '',
                'phone_success': False,
                'fax_success': False
            }
            
            # 전화번호 처리
            phone_number = str(row.get('전화번호', '')).strip()
            if phone_number and phone_number != '':
                # 기존에 실제기관명이 있는지 확인
                existing_phone_institution = str(row.get('전화번호_실제기관명', '')).strip()
                if not existing_phone_institution:
                    phone_result = self.search_engine.search_institution_name(phone_number, "전화번호")
                    results['phone_institution'] = phone_result.institution_name
                    results['phone_success'] = phone_result.search_successful
                    
                    with self.lock:
                        self.stats.phone_extractions += 1
                        if phone_result.search_successful:
                            self.stats.successful_extractions += 1
                        else:
                            self.stats.failed_extractions += 1
                            self.stats.error_counts[phone_result.error_message] += 1
                        self.stats.add_search_time(phone_result.search_time)
                else:
                    results['phone_institution'] = existing_phone_institution
                    results['phone_success'] = True
            
            # 팩스번호 처리
            fax_number = str(row.get('팩스번호', '')).strip()
            if fax_number and fax_number != '':
                # 기존에 실제기관명이 있는지 확인
                existing_fax_institution = str(row.get('팩스번호_실제기관명', '')).strip()
                if not existing_fax_institution:
                    fax_result = self.search_engine.search_institution_name(fax_number, "팩스번호")
                    results['fax_institution'] = fax_result.institution_name
                    results['fax_success'] = fax_result.search_successful
                    
                    with self.lock:
                        self.stats.fax_extractions += 1
                        if fax_result.search_successful:
                            self.stats.successful_extractions += 1
                        else:
                            self.stats.failed_extractions += 1
                            self.stats.error_counts[fax_result.error_message] += 1
                        self.stats.add_search_time(fax_result.search_time)
                else:
                    results['fax_institution'] = existing_fax_institution
                    results['fax_success'] = True
            
            # 빈 번호 처리
            if not phone_number and not fax_number:
                with self.lock:
                    self.stats.empty_numbers += 1
            
            processing_time = time.time() - start_time
            success = results['phone_success'] or results['fax_success']
            
            self.system_monitor.record_worker_performance(worker_id, processing_time, success)
            
            with self.lock:
                self.stats.total_processed += 1
            
            return results
            
        except Exception as e:
            logger.error(f"행 처리 오류 (인덱스 {idx}): {e}")
            with self.lock:
                self.stats.total_processed += 1
                self.stats.failed_extractions += 1
                self.stats.error_counts[f"처리 오류: {str(e)}"] += 1
            
            return {
                'index': idx,
                'phone_institution': '',
                'fax_institution': '',
                'phone_success': False,
                'fax_success': False
            }
    
    def adjust_worker_count(self):
        """워커 수 동적 조정"""
        should_adjust, action = self.system_monitor.should_adjust_workers()
        
        if should_adjust:
            if action == "decrease" and self.current_workers > MIN_WORKERS:
                self.current_workers = max(MIN_WORKERS, self.current_workers - 2)
                logger.info(f"워커 수 감소: {self.current_workers}개")
            elif action == "increase" and self.current_workers < MAX_WORKERS:
                self.current_workers = min(MAX_WORKERS, self.current_workers + 1)
                logger.info(f"워커 수 증가: {self.current_workers}개")
    
    def extract_institution_names(self) -> bool:
        """실제기관명 추출 실행"""
        try:
            logger.info("실제기관명 추출 시작")
            
            # 데이터 로드
            df = self.load_data()
            
            if df.empty:
                logger.warning("처리할 데이터가 없습니다.")
                return False
            
            # 처리 대상 필터링 (빈 실제기관명 컬럼이 있는 행만)
            mask = (
                (df['전화번호'].notna() & (df['전화번호'] != '') & 
                 (df['전화번호_실제기관명'].isna() | (df['전화번호_실제기관명'] == ''))) |
                (df['팩스번호'].notna() & (df['팩스번호'] != '') & 
                 (df['팩스번호_실제기관명'].isna() | (df['팩스번호_실제기관명'] == '')))
            )
            
            target_rows = df[mask]
            logger.info(f"처리 대상: {len(target_rows)}개 행")
            
            if target_rows.empty:
                logger.info("처리할 대상이 없습니다 (모든 실제기관명이 이미 존재)")
                return True
            
            # 결과 저장용 딕셔너리
            results = {}
            
            # 멀티스레딩으로 처리
            with ThreadPoolExecutor(max_workers=self.current_workers) as executor:
                # 작업 제출
                future_to_idx = {
                    executor.submit(self.process_single_row, (idx, row)): idx 
                    for idx, row in target_rows.iterrows()
                }
                
                # 결과 수집
                processed_count = 0
                for future in as_completed(future_to_idx):
                    try:
                        result = future.result()
                        results[result['index']] = result
                        processed_count += 1
                        
                        # 진행률 출력
                        if processed_count % 10 == 0:
                            progress = (processed_count / len(target_rows)) * 100
                            logger.info(f"진행률: {progress:.1f}% ({processed_count}/{len(target_rows)})")
                        
                        # 워커 수 조정 검토
                        if processed_count % self.worker_adjustment_interval == 0:
                            self.adjust_worker_count()
                            
                    except Exception as e:
                        logger.error(f"Future 처리 오류: {e}")
            
            # 결과를 DataFrame에 적용
            for idx, result in results.items():
                if result['phone_institution']:
                    df.at[idx, '전화번호_실제기관명'] = result['phone_institution']
                if result['fax_institution']:
                    df.at[idx, '팩스번호_실제기관명'] = result['fax_institution']
            
            # 결과 저장
            self.save_results(df)
            
            # 통계 출력
            self.print_statistics()
            
            logger.info("실제기관명 추출 완료")
            return True
            
        except Exception as e:
            logger.error(f"실제기관명 추출 실패: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def save_results(self, df: pd.DataFrame):
        """결과 저장"""
        try:
            # 타임스탬프 추가
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"institution_names_extracted_{timestamp}.xlsx"
            output_path = os.path.join("rawdatafile", output_filename)
            
            # 디렉토리 생성
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Excel 저장
            df.to_excel(output_path, index=False, engine='openpyxl')
            logger.info(f"결과 저장 완료: {output_path}")
            
            # 통계 파일 저장
            stats_filename = f"extraction_stats_{timestamp}.json"
            stats_path = os.path.join("rawdatafile", stats_filename)
            
            stats_data = {
                'timestamp': timestamp,
                'total_processed': self.stats.total_processed,
                'phone_extractions': self.stats.phone_extractions,
                'fax_extractions': self.stats.fax_extractions,
                'successful_extractions': self.stats.successful_extractions,
                'failed_extractions': self.stats.failed_extractions,
                'empty_numbers': self.stats.empty_numbers,
                'success_rate': self.stats.get_success_rate(),
                'average_search_time': self.stats.get_average_search_time(),
                'error_counts': dict(self.stats.error_counts),
                'worker_stats': self.system_monitor.get_worker_stats()
            }
            
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"통계 저장 완료: {stats_path}")
            
        except Exception as e:
            logger.error(f"결과 저장 실패: {e}")
            raise
    
    def print_statistics(self):
        """통계 출력"""
        logger.info("=" * 60)
        logger.info("실제기관명 추출 통계")
        logger.info("=" * 60)
        logger.info(f"총 처리 건수: {self.stats.total_processed:,}")
        logger.info(f"전화번호 추출: {self.stats.phone_extractions:,}")
        logger.info(f"팩스번호 추출: {self.stats.fax_extractions:,}")
        logger.info(f"성공 건수: {self.stats.successful_extractions:,}")
        logger.info(f"실패 건수: {self.stats.failed_extractions:,}")
        logger.info(f"빈 번호: {self.stats.empty_numbers:,}")
        logger.info(f"성공률: {self.stats.get_success_rate():.1f}%")
        logger.info(f"평균 검색 시간: {self.stats.get_average_search_time():.2f}초")
        
        if self.stats.error_counts:
            logger.info("\n주요 오류 유형:")
            for error, count in sorted(self.stats.error_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                logger.info(f"  {error}: {count}건")
        
        # 워커 성능 통계
        worker_stats = self.system_monitor.get_worker_stats()
        if worker_stats:
            logger.info(f"\n워커 성능 통계 (총 {len(worker_stats)}개 워커):")
            for worker_id, stats in list(worker_stats.items())[:5]:
                logger.info(f"  {worker_id}: {stats['total_tasks']}건, "
                           f"성공률 {stats['success_rate']:.1f}%, "
                           f"평균시간 {stats['avg_time']:.2f}초")
        
        logger.info("=" * 60)

def main():
    """메인 함수"""
    try:
        print("=" * 60)
        print("실제기관명 추출 시스템")
        print("=" * 60)
        print(f"시스템 설정:")
        print(f"  - Headless 모드: {HEADLESS_MODE}")
        print(f"  - 워커 수 범위: {MIN_WORKERS}-{MAX_WORKERS}개")
        print(f"  - 시작 워커 수: {MAX_WORKERS}개")
        print("=" * 60)
        
        # 입력 파일 경로
        input_file = r"rawdatafile\failed_data_250715.xlsx"
        
        # 파일 존재 확인
        if not os.path.exists(input_file):
            print(f"오류: 입력 파일을 찾을 수 없습니다: {input_file}")
            return False
        
        # 추출기 생성 및 실행
        extractor = InstitutionNameExtractor(
            input_file=input_file,
            output_file="institution_names_extracted.xlsx"
        )
        
        success = extractor.extract_institution_names()
        
        if success:
            print("\n실제기관명 추출이 완료되었습니다!")
            print("결과 파일을 확인해주세요.")
        else:
            print("\n실제기관명 추출 중 오류가 발생했습니다.")
            print("로그 파일을 확인해주세요.")
        
        return success
        
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
        return False
    except Exception as e:
        print(f"\n예상치 못한 오류가 발생했습니다: {e}")
        logger.error(f"메인 함수 오류: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    main() 