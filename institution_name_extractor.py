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
# from selenium.webdriver.chrome.options import Options  # undetected_chromedriver 사용
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, NoSuchElementException,
    ElementNotInteractableException, StaleElementReferenceException
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import undetected_chromedriver as uc

# BeautifulSoup 관련 imports
from bs4 import BeautifulSoup

# Gemini API import 추가
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

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

class GeminiAnalyzer:
    """Gemini AI 기반 기관명 분석 클래스"""
    
    def __init__(self):
        # 환경변수에서 API 키들 로드
        self.api_keys = [
            os.getenv('GEMINI_API_KEY'),
            os.getenv('GEMINI_API_KEY_2'), 
            os.getenv('GEMINI_API_KEY_3'),
            os.getenv('GEMINI_API_KEY_4')
        ]
        
        # 유효한 키만 필터링
        self.api_keys = [key for key in self.api_keys if key]
        
        if not self.api_keys:
            raise ValueError("Gemini API 키가 설정되지 않았습니다.")
        
        logger.info(f"🤖 GeminiAnalyzer 초기화 완료 - {len(self.api_keys)}개 API 키 로드")
        
        # 레이트 리밋 관리 (키별 분당 요청 수 추적)
        self.rate_limits = {i: {'requests': 0, 'last_reset': time.time()} for i in range(len(self.api_keys))}
        self.rpm_limit = 1800  # 분당 1800회 제한 (여유분 200 보존)
        
        # 프롬프트 템플릿
        self.prompt_template = """다음은 "{phone_number}" 번호에 대한 구글 검색 결과입니다.
이 번호가 속한 정확한 기관명을 추출해주세요.

검색 결과:
{search_results}

답변은 기관명만 간단히 답해주세요. 예: "서귀포시 송산동주민센터"
기관명을 찾을 수 없다면 "없음"이라고 답해주세요."""

    def analyze_search_results(self, texts: List[str], phone_number: str, worker_id: int = 0) -> str:
        """검색 결과 텍스트들을 Gemini AI로 분석하여 기관명 추출"""
        try:
            # API 키 선택 (워커별 할당)
            key_index = worker_id % len(self.api_keys)
            
            # 레이트 리밋 체크
            if not self._check_rate_limit(key_index):
                # 다른 키 시도
                key_index = self._get_available_key()
                if key_index is None:
                    logger.warning(f"⚠️ 워커 {worker_id}: 모든 API 키가 레이트 리밋 초과")
                    return ""
            
            # API 키 설정
            genai.configure(api_key=self.api_keys[key_index])
            
            # 프롬프트 생성
            prompt = self._create_prompt(texts, phone_number)
            
            # Gemini 모델 생성
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            # 안전 설정 (제한 완화)
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }
            
            logger.info(f"🤖 워커 {worker_id}: Gemini API 호출 중 (키: {key_index+1})")
            
            # API 호출
            response = model.generate_content(
                prompt,
                safety_settings=safety_settings
            )
            
            # 요청 수 증가
            self._record_request(key_index)
            
            # 응답 처리
            if response.text:
                result = response.text.strip()
                logger.info(f"✅ 워커 {worker_id}: Gemini 분석 완료 - '{result}'")
                
                # 응답 검증
                if self._validate_response(result):
                    return result
                else:
                    logger.warning(f"⚠️ 워커 {worker_id}: Gemini 응답 검증 실패 - '{result}'")
                    return ""
            else:
                logger.warning(f"⚠️ 워커 {worker_id}: Gemini 응답이 비어있음")
                return ""
                
        except Exception as e:
            logger.error(f"❌ 워커 {worker_id}: Gemini API 오류 - {e}")
            return ""
    
    def _create_prompt(self, texts: List[str], phone_number: str) -> str:
        """프롬프트 생성"""
        # 텍스트들을 번호순으로 정리
        search_results = ""
        for i, text in enumerate(texts, 1):
            if text.strip():
                search_results += f"{i}. {text.strip()}\n"
        
        return self.prompt_template.format(
            phone_number=phone_number,
            search_results=search_results
        )
    
    def _validate_response(self, response: str) -> bool:
        """Gemini 응답 검증"""
        if not response or response.strip() == "":
            return False
            
        response = response.strip()
        
        # "없음" 응답 체크
        if response in ["없음", "정보없음", "찾을 수 없음"]:
            return False
            
        # 너무 긴 응답 체크
        if len(response) > 50:
            return False
            
        # 한글 기관명 패턴 체크 (2-30자)
        if not re.match(r'^[가-힣0-9\s]{2,30}$', response):
            return False
            
        # 금지된 단어 체크
        forbidden_words = ["검색결과", "정보없음", "확인불가", "ERROR", "error"]
        if any(word in response for word in forbidden_words):
            return False
            
        return True
    
    def _check_rate_limit(self, key_index: int) -> bool:
        """레이트 리밋 체크"""
        current_time = time.time()
        rate_info = self.rate_limits[key_index]
        
        # 1분이 지났으면 리셋
        if current_time - rate_info['last_reset'] >= 60:
            rate_info['requests'] = 0
            rate_info['last_reset'] = current_time
        
        return rate_info['requests'] < self.rpm_limit
    
    def _get_available_key(self) -> Optional[int]:
        """사용 가능한 API 키 인덱스 반환"""
        for i in range(len(self.api_keys)):
            if self._check_rate_limit(i):
                return i
        return None
    
    def _record_request(self, key_index: int):
        """API 요청 기록"""
        self.rate_limits[key_index]['requests'] += 1

class CacheManager:
    """파일 기반 캐싱 시스템"""
    
    def __init__(self, cache_file: str = "rawdatafile/search_cache.json"):
        self.cache_file = cache_file
        self.cache_data = self._load_cache()
        logger.info(f"💾 CacheManager 초기화 - {len(self.cache_data)}개 캐시 항목 로드")
    
    def get_cached_result(self, phone_number: str) -> Optional[str]:
        """캐시에서 결과 조회"""
        return self.cache_data.get(phone_number, {}).get('institution_name')
    
    def save_result(self, phone_number: str, result: str, metadata: dict = None):
        """결과를 캐시에 저장"""
        self.cache_data[phone_number] = {
            'institution_name': result,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        self._save_cache()
    
    def _load_cache(self) -> dict:
        """캐시 파일 로드"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ 캐시 로드 실패: {e}")
        return {}
    
    def _save_cache(self):
        """캐시 파일 저장"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ 캐시 저장 실패: {e}")

# LinkCrawler는 WebDriverManager 정의 후에 정의됩니다

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
    
    def create_driver(self, worker_id: int = 0) -> uc.Chrome:
        """새로운 undetected-chromedriver 생성 (워커별 개별 설정)"""
        try:
            # 워커 간 시차 두기
            startup_delay = random.uniform(0.5, 1.5) * worker_id
            time.sleep(startup_delay)
            
            chrome_options = uc.ChromeOptions()
            
            # 기본 옵션
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1366,768')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--mute-audio')
            chrome_options.add_argument('--no-first-run')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-notifications')
            
            # Headless 모드 설정
            if self.headless:
                chrome_options.add_argument('--headless')
            
            # 리소스 절약 옵션
            chrome_options.add_argument('--disable-images')
            chrome_options.add_argument('--disable-plugins')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-ipc-flooding-protection')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-features=TranslateUI')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--disable-sync')
            
            # 메모리 최적화
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=256')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
            
            # 워커별 별도 사용자 데이터 디렉토리 설정 (충돌 방지)
            import tempfile
            temp_dir = tempfile.mkdtemp(prefix=f'chrome_worker_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={temp_dir}')
            
            # 안전한 포트 설정
            debug_port = 9222 + (worker_id * 10)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # User-Agent 랜덤화
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
            ]
            chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # 드라이버 생성 (version_main=None으로 Chrome 138 호환성 확보)
            driver = uc.Chrome(options=chrome_options, version_main=None)
            
            # 타임아웃 설정
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(20)
            
            # 웹드라이버 감지 방지
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info(f"🔧 워커 {worker_id}: undetected-chromedriver 생성 완료 (포트: {debug_port})")
            return driver
            
        except Exception as e:
            logger.error(f"워커 {worker_id} 웹드라이버 생성 실패: {e}")
            raise

class LinkCrawler:
    """링크 추출 및 페이지 크롤링 클래스"""
    
    def __init__(self, driver_manager: WebDriverManager):
        self.driver_manager = driver_manager
        logger.info("🔗 LinkCrawler 초기화 완료")
    
    def extract_links_from_search(self, driver) -> List[str]:
        """구글 검색 결과에서 링크 추출"""
        try:
            # BeautifulSoup로 페이지 파싱
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            links = []
            # 검색 결과 링크 선택자
            search_results = soup.select('div.g h3 a')
            
            for result in search_results[:5]:  # 상위 5개만
                href = result.get('href')
                if href and href.startswith('http'):
                    links.append(href)
            
            logger.info(f"🔗 검색 결과에서 {len(links)}개 링크 추출")
            return links
            
        except Exception as e:
            logger.error(f"❌ 링크 추출 오류: {e}")
            return []
    
    def crawl_page_content(self, url: str, worker_id: int) -> str:
        """개별 페이지 크롤링"""
        driver = None
        try:
            logger.info(f"🌐 워커 {worker_id}: 페이지 크롤링 시작 - {url}")
            
            driver = self.driver_manager.create_driver(worker_id + 1000)  # 별도 워커 ID 범위
            driver.set_page_load_timeout(10)  # 타임아웃 단축
            
            # 페이지 접속
            driver.get(url)
            time.sleep(3)  # JS 렌더링 대기
            
            # HTML 콘텐츠 추출 및 전처리
            content = self._preprocess_html_content(driver.page_source)
            
            logger.info(f"✅ 워커 {worker_id}: 페이지 크롤링 완료 - {len(content)}자")
            return content
            
        except Exception as e:
            logger.warning(f"⚠️ 워커 {worker_id}: 페이지 크롤링 실패 - {url}: {e}")
            return ""
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def _preprocess_html_content(self, html: str) -> str:
        """HTML 콘텐츠 전처리"""
        try:
            # BeautifulSoup로 파싱
            soup = BeautifulSoup(html, 'html.parser')
            
            # 불필요한 태그 제거
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                tag.decompose()
            
            # 텍스트만 추출
            text = soup.get_text()
            
            # 공백 정리
            lines = [line.strip() for line in text.splitlines()]
            text = '\n'.join([line for line in lines if line])
            
            # 최대 길이 제한 (2000자)
            if len(text) > 2000:
                text = text[:2000]
            
            return text
            
        except Exception as e:
            logger.error(f"❌ HTML 전처리 오류: {e}")
            return ""

class GoogleSearchEngine:
    """구글 검색 엔진 클래스 - Gemini AI 통합 버전"""
    
    def __init__(self, driver_manager: WebDriverManager):
        self.driver_manager = driver_manager
        
        # 새로운 구성 요소들 초기화
        self.gemini_analyzer = GeminiAnalyzer()
        self.cache_manager = CacheManager()
        self.link_crawler = LinkCrawler(driver_manager)
        # 기관명 패턴 단순화
        self.institution_keywords = [
            '주민센터', '행정복지센터', '동사무소', '면사무소', '읍사무소',
            '시청', '구청', '군청', '청사', '시 ', '구 ', '군 ',
            '병원', '의원', '보건소', '보건센터', '클리닉',
            '학교', '대학', '교육청', '교육지원청',
            '경찰서', '파출소', '지구대', '소방서',
            '법원', '검찰청', '세무서', '등기소',
            '우체국', '체신청', '공사', '공단', '센터', '사업소'
        ]
    
    def search_institution_name(self, phone_number: str, number_type: str = "전화번호", worker_id: int = 0) -> SearchResult:
        """전화번호로 기관명 검색 - 단순화된 버전"""
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
            logger.info(f"🔍 워커 {worker_id}: {number_type} 검색 시작 - {clean_number}")
            
            driver = self.driver_manager.create_driver(worker_id)
            
            # 팩스번호의 경우 다양한 검색 쿼리 시도
            if number_type == "팩스번호":
                search_queries = [
                    f'"{clean_number}" 팩스번호',
                    f'"{clean_number}" 팩스',
                    f'"{clean_number}" FAX',
                    f'"{clean_number}" 주민센터',
                    f'"{clean_number}"'
                ]
                search_query = search_queries[0]  # 첫 번째부터 시도
                logger.info(f"🔍 워커 {worker_id}: 팩스번호 검색 - 다양한 쿼리 시도 예정")
            else:
                search_query = f'"{clean_number}" {number_type}'
            
            logger.info(f"🔍 워커 {worker_id}: 구글 검색 쿼리 - {search_query}")
            
            # 안전한 랜덤 지연
            delay = random.uniform(0.5, 1.5)
            time.sleep(delay)
            
            # 구글 검색 실행
            logger.info(f"🌐 워커 {worker_id}: 구글 검색 페이지 접속 중...")
            driver.get('https://www.google.com')
            time.sleep(random.uniform(1.0, 2.0))
            
            # 검색창 찾기 및 검색
            logger.info(f"⌨️ 워커 {worker_id}: 검색어 입력 중...")
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            
            search_box.clear()
            search_box.send_keys(search_query)
            search_box.send_keys(Keys.RETURN)
            
            # 검색 결과 대기
            logger.info(f"⏳ 워커 {worker_id}: 검색 결과 대기 중...")
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "search"))
            )
            time.sleep(random.uniform(1.0, 2.0))
            
            # 단순화된 기관명 추출
            logger.info(f"🔎 워커 {worker_id}: 기관명 추출 중...")
            institution_name = self._extract_institution_name_simple(driver, clean_number)
            
            search_time = time.time() - start_time
            
            if institution_name:
                logger.info(f"✅ 워커 {worker_id}: 기관명 발견! {clean_number} -> {institution_name} ({search_time:.2f}초)")
            else:
                logger.info(f"❌ 워커 {worker_id}: 기관명 찾기 실패 - {clean_number} ({search_time:.2f}초)")
            
            return SearchResult(
                phone_number=phone_number,
                institution_name=institution_name,
                confidence=0.8 if institution_name else 0.0,
                search_successful=bool(institution_name),
                search_time=search_time
            )
            
        except TimeoutException:
            logger.warning(f"⏰ 워커 {worker_id}: 검색 타임아웃 - {clean_number}")
            return SearchResult(
                phone_number=phone_number,
                search_successful=False,
                error_message="검색 타임아웃",
                search_time=time.time() - start_time
            )
        except Exception as e:
            logger.error(f"❌ 워커 {worker_id}: 검색 오류 - {clean_number}: {e}")
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
    
    def _extract_search_results_with_links(self, driver: uc.Chrome, phone_number: str) -> Tuple[List[str], List[str]]:
        """검색 결과 텍스트 5개 + 링크 5개 동시 추출"""
        try:
            logger.info(f"📄 검색 결과 및 링크 추출 중...")
            
            # BeautifulSoup를 사용하여 페이지 소스 파싱
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            search_texts = []
            search_links = []
            
            # 검색 결과 컨테이너 찾기
            search_results = soup.select('div.g')[:5]  # 상위 5개만
            
            for result in search_results:
                # 텍스트 추출 (제목 + 스니펫)
                title_elem = result.select_one('h3')
                snippet_elem = result.select_one('span')
                
                text_parts = []
                if title_elem:
                    text_parts.append(title_elem.get_text().strip())
                if snippet_elem:
                    text_parts.append(snippet_elem.get_text().strip())
                
                combined_text = ' '.join(text_parts)
                if combined_text:
                    search_texts.append(combined_text)
                
                # 링크 추출
                link_elem = result.select_one('h3 a')
                if link_elem:
                    href = link_elem.get('href')
                    if href and href.startswith('http'):
                        search_links.append(href)
            
            logger.info(f"🔍 추출 완료 - 텍스트: {len(search_texts)}개, 링크: {len(search_links)}개")
            return search_texts, search_links
            
        except Exception as e:
            logger.error(f"❌ 검색 결과 추출 오류: {e}")
            return [], []
    
    def _extract_institution_name_simple(self, driver: uc.Chrome, phone_number: str) -> str:
        """단순화된 기관명 추출 (기존 방식 - 전화번호용)"""
        try:
            logger.info(f"📄 페이지 소스 파싱 중...")
            
            # BeautifulSoup를 사용하여 페이지 소스 파싱
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 검색 결과 영역만 추출 (더 정확한 결과를 위해)
            search_results = soup.find('div', {'id': 'search'})
            if search_results:
                search_text = search_results.get_text()
                logger.info(f"🔍 검색 결과 텍스트 길이: {len(search_text)} 문자")
            else:
                search_text = soup.get_text()
                logger.info(f"🔍 전체 페이지 텍스트 길이: {len(search_text)} 문자")
            
            # 기관명 추출
            logger.info(f"🏢 기관명 추출 시작...")
            institution_name = self._find_institution_name(search_text, phone_number)
            
            if institution_name:
                logger.info(f"✅ 기관명 추출 성공: {institution_name}")
            else:
                logger.info(f"❌ 기관명 추출 실패")
            
            return institution_name
            
        except Exception as e:
            logger.error(f"❌ 기관명 추출 오류: {e}")
            return ""
    
    def _find_institution_name(self, text: str, phone_number: str) -> str:
        """텍스트에서 기관명 찾기 - 단순화된 버전"""
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
            for keyword in self.institution_keywords:
                if keyword in line:
                    # 기관명 추출 시도
                    institution_name = self._extract_name_from_line(line, keyword)
                    if institution_name:
                        return institution_name
        
        # 기관 키워드가 없는 경우, 일반적인 기관명 패턴 찾기
        for line in relevant_lines:
            # 한글 기관명 패턴 찾기
            matches = re.findall(r'([가-힣]{2,10}(?:구청|시청|군청|센터|사무소|병원|의원|학교|대학|청|서|소|원|관|공사|공단))', line)
            if matches:
                return matches[0]
        
        return ""
    
    def _extract_name_from_line(self, line: str, keyword: str) -> str:
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
    
    def search_institution_name_v2(self, phone_number: str, number_type: str = "팩스번호", worker_id: int = 0) -> SearchResult:
        """Gemini AI 기반 기관명 검색 - 개선된 버전"""
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
        
        # 캐시 확인
        cached_result = self.cache_manager.get_cached_result(clean_number)
        if cached_result:
            logger.info(f"💾 워커 {worker_id}: 캐시에서 결과 발견 - {clean_number} -> {cached_result}")
            return SearchResult(
                phone_number=phone_number,
                institution_name=cached_result,
                confidence=0.95,
                search_successful=True,
                search_time=0.1
            )
        
        driver = None
        start_time = time.time()
        
        try:
            logger.info(f"🔍 워커 {worker_id}: {number_type} 검색 시작 (Gemini AI) - {clean_number}")
            
            driver = self.driver_manager.create_driver(worker_id)
            
            # 팩스번호의 경우 다양한 검색 쿼리 시도
            if number_type == "팩스번호":
                search_queries = [
                    f'"{clean_number}" 팩스번호',
                    f'"{clean_number}" 팩스',
                    f'"{clean_number}" FAX',
                    f'"{clean_number}" 주민센터',
                    f'"{clean_number}"'
                ]
            else:
                search_queries = [f'"{clean_number}" {number_type}']
            
            # 1차: 검색 결과 텍스트로 Gemini 분석
            for query in search_queries:
                logger.info(f"🔍 워커 {worker_id}: 검색 쿼리 시도 - {query}")
                
                try:
                    # 구글 검색 실행
                    driver.get('https://www.google.com')
                    time.sleep(random.uniform(1.0, 2.0))
                    
                    search_box = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.NAME, "q"))
                    )
                    
                    search_box.clear()
                    search_box.send_keys(query)
                    search_box.send_keys(Keys.RETURN)
                    
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "search"))
                    )
                    time.sleep(random.uniform(1.0, 2.0))
                    
                    # 검색 결과 + 링크 추출
                    search_texts, search_links = self._extract_search_results_with_links(driver, clean_number)
                    
                    if search_texts:
                        # Gemini AI 분석
                        logger.info(f"🤖 워커 {worker_id}: Gemini AI 분석 시작")
                        institution_name = self.gemini_analyzer.analyze_search_results(
                            search_texts, clean_number, worker_id
                        )
                        
                        if institution_name:
                            search_time = time.time() - start_time
                            logger.info(f"✅ 워커 {worker_id}: 1차 성공! {clean_number} -> {institution_name}")
                            
                            # 캐시에 저장
                            self.cache_manager.save_result(clean_number, institution_name, {
                                'method': '1차_검색결과_Gemini',
                                'query': query,
                                'search_time': search_time
                            })
                            
                            return SearchResult(
                                phone_number=phone_number,
                                institution_name=institution_name,
                                confidence=0.9,
                                search_successful=True,
                                search_time=search_time
                            )
                    
                    # 2차: 링크 크롤링 + Gemini 분석
                    if search_links:
                        logger.info(f"🔗 워커 {worker_id}: 2차 시도 - 링크 크롤링")
                        
                        crawled_texts = []
                        for i, link in enumerate(search_links[:3], 1):  # 상위 3개 링크만
                            logger.info(f"🌐 워커 {worker_id}: 링크 {i} 크롤링 - {link}")
                            content = self.link_crawler.crawl_page_content(link, worker_id)
                            if content:
                                crawled_texts.append(content)
                        
                        if crawled_texts:
                            # Gemini AI 분석
                            institution_name = self.gemini_analyzer.analyze_search_results(
                                crawled_texts, clean_number, worker_id
                            )
                            
                            if institution_name:
                                search_time = time.time() - start_time
                                logger.info(f"✅ 워커 {worker_id}: 2차 성공! {clean_number} -> {institution_name}")
                                
                                # 캐시에 저장
                                self.cache_manager.save_result(clean_number, institution_name, {
                                    'method': '2차_링크크롤링_Gemini',
                                    'query': query,
                                    'links_count': len(crawled_texts),
                                    'search_time': search_time
                                })
                                
                                return SearchResult(
                                    phone_number=phone_number,
                                    institution_name=institution_name,
                                    confidence=0.8,
                                    search_successful=True,
                                    search_time=search_time
                                )
                
                except Exception as e:
                    logger.warning(f"⚠️ 워커 {worker_id}: 쿼리 실패 - {query}: {e}")
                    continue
            
            # 3차: 기존 키워드 매칭 방식 시도
            logger.info(f"🔄 워커 {worker_id}: 3차 시도 - 기존 키워드 방식")
            institution_name = self._extract_institution_name_simple(driver, clean_number)
            
            if institution_name:
                search_time = time.time() - start_time
                logger.info(f"✅ 워커 {worker_id}: 3차 성공! {clean_number} -> {institution_name}")
                
                # 캐시에 저장
                self.cache_manager.save_result(clean_number, institution_name, {
                    'method': '3차_키워드매칭',
                    'search_time': search_time
                })
                
                return SearchResult(
                    phone_number=phone_number,
                    institution_name=institution_name,
                    confidence=0.6,
                    search_successful=True,
                    search_time=search_time
                )
            
            # 4차: 전체 실패
            search_time = time.time() - start_time
            logger.info(f"❌ 워커 {worker_id}: 모든 시도 실패 - {clean_number} ({search_time:.2f}초)")
            
            return SearchResult(
                phone_number=phone_number,
                search_successful=False,
                error_message="모든 방법 실패",
                search_time=search_time
            )
            
        except Exception as e:
            logger.error(f"❌ 워커 {worker_id}: 전체 검색 오류 - {clean_number}: {e}")
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

# 첫 번째 InstitutionNameExtractor 클래스는 두 번째와 통합됩니다

class InstitutionNameExtractor:
    """실제기관명 추출 메인 클래스"""
    
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        self.stats = ExtractionStats()
        self.system_monitor = SystemMonitor()
        
        # Headless 모드 설정
        self.headless_mode = globals().get('HEADLESS_MODE', True)
        
        # WebDriver 관리자 초기화
        self.driver_manager = WebDriverManager(headless=self.headless_mode)
        self.search_engine = GoogleSearchEngine(self.driver_manager)
        
        # 스레드 동기화
        self.lock = threading.Lock()
        
        # 중간저장 관련 변수
        self.intermediate_save_counter = 0
        self.intermediate_save_interval = 100  # 100개 단위로 중간저장
        self.processed_count = 0
        
        # 워커 수 동적 조정
        headless_status = "Headless" if self.headless_mode else "GUI"
        if self.headless_mode:
            self.current_workers = MAX_WORKERS  # Headless 모드: 12개 워커
        else:
            self.current_workers = max(MIN_WORKERS, MAX_WORKERS - 3)  # GUI 모드: 9개 워커
        
        self.worker_adjustment_interval = 50  # 50개 처리마다 워커 수 조정 검토
        
        logger.info(f"🚀 InstitutionNameExtractor 초기화 완료")
        logger.info(f"🔧 {headless_status} 모드 - 워커: {self.current_workers}개")
        logger.info(f"🔧 워커 수 동적 조정 활성화 (범위: {MIN_WORKERS}-{MAX_WORKERS}개)")
        logger.info(f"🔧 중간저장 간격: {self.intermediate_save_interval}개 단위")
        logger.info(f"🔧 AMD Ryzen 5 3600 (6코어 12스레드) 환경에 최적화된 설정 적용")
    
    def load_data(self) -> pd.DataFrame:
        """Excel 데이터 로드"""
        try:
            logger.info(f"데이터 로드 시작: {self.input_file}")
            
            # Excel 파일 읽기
            df = pd.read_excel(self.input_file)
            logger.info(f"총 {len(df)}개 행 로드 완료")
            
            # 실제 컬럼 구조 확인
            logger.info(f"원본 컬럼: {list(df.columns)}")
            
            # 실제 파일 구조에 맞게 컬럼명 처리
            if len(df.columns) == 10:
                # 사용자가 보여준 구조: 연번, 시도, 시군구, 읍면동, 우편번호, 주소, 전화번호, 실제기관명, 팩스번호, 실제기관명
                expected_columns = ['연번', '시도', '시군구', '읍면동', '우편번호', '주    소', '전화번호', '전화번호_실제기관명', '팩스번호', '팩스번호_실제기관명']
                df.columns = expected_columns
                logger.info("컬럼명을 표준 구조로 변경 완료")
            else:
                # 기존 컬럼명 유지하되 실제기관명 컬럼 구분
                columns = list(df.columns)
                phone_col_idx = -1
                fax_col_idx = -1
                
                # 전화번호와 팩스번호 컬럼 위치 찾기
                for i, col in enumerate(columns):
                    if '전화번호' in str(col) and '실제기관명' not in str(col):
                        phone_col_idx = i
                    elif '팩스번호' in str(col) and '실제기관명' not in str(col):
                        fax_col_idx = i
                
                # 실제기관명 컬럼 이름 변경
                for i, col in enumerate(columns):
                    if '실제기관명' in str(col) or col == '':
                        if i == phone_col_idx + 1:  # 전화번호 다음
                            columns[i] = '전화번호_실제기관명'
                        elif i == fax_col_idx + 1:  # 팩스번호 다음
                            columns[i] = '팩스번호_실제기관명'
                
                df.columns = columns
            
            # 빈 값 처리
            df = df.fillna('')
            
            # 실제기관명 컬럼이 없으면 생성
            if '전화번호_실제기관명' not in df.columns:
                df['전화번호_실제기관명'] = ''
            if '팩스번호_실제기관명' not in df.columns:
                df['팩스번호_실제기관명'] = ''
            
            logger.info(f"최종 컬럼 구조: {list(df.columns)}")
            return df
            
        except Exception as e:
            logger.error(f"데이터 로드 실패: {e}")
            raise
    
    def process_single_row(self, row_data: Tuple[int, pd.Series]) -> Dict[str, Any]:
        """단일 행 처리"""
        idx, row = row_data
        
        # 워커 ID 생성 (스레드 ID 기반)
        thread_id = threading.current_thread().ident
        worker_id = abs(hash(thread_id)) % 100  # 0-99 범위의 워커 ID
        
        start_time = time.time()
        
        try:
            logger.info(f"🔄 워커 {worker_id}: 행 {idx} 처리 시작")
            
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
                logger.info(f"📞 워커 {worker_id}: 전화번호 처리 - {phone_number}")
                # 기존에 실제기관명이 있는지 확인
                existing_phone_institution = str(row.get('전화번호_실제기관명', '')).strip()
                if not existing_phone_institution:
                    phone_result = self.search_engine.search_institution_name(phone_number, "전화번호", worker_id)
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
                    logger.info(f"⏭️ 워커 {worker_id}: 전화번호 기관명 이미 존재 - {existing_phone_institution}")
                    results['phone_institution'] = existing_phone_institution
                    results['phone_success'] = True
            
            # 팩스번호 처리
            fax_number = str(row.get('팩스번호', '')).strip()
            if fax_number and fax_number != '':
                logger.info(f"📠 워커 {worker_id}: 팩스번호 처리 - {fax_number}")
                # 기존에 실제기관명이 있는지 확인
                existing_fax_institution = str(row.get('팩스번호_실제기관명', '')).strip()
                if not existing_fax_institution:
                    # 팩스번호는 새로운 Gemini AI 기반 방식 사용
                    fax_result = self.search_engine.search_institution_name_v2(fax_number, "팩스번호", worker_id)
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
                    logger.info(f"⏭️ 워커 {worker_id}: 팩스번호 기관명 이미 존재 - {existing_fax_institution}")
                    results['fax_institution'] = existing_fax_institution
                    results['fax_success'] = True
            
            # 빈 번호 처리
            if not phone_number and not fax_number:
                logger.info(f"⚠️ 워커 {worker_id}: 전화번호와 팩스번호 모두 없음")
                with self.lock:
                    self.stats.empty_numbers += 1
            
            processing_time = time.time() - start_time
            success = results['phone_success'] or results['fax_success']
            
            worker_id_str = f"worker_{worker_id}"
            self.system_monitor.record_worker_performance(worker_id_str, processing_time, success)
            
            with self.lock:
                self.stats.total_processed += 1
            
            logger.info(f"✅ 워커 {worker_id}: 행 {idx} 처리 완료 ({processing_time:.2f}초) - 성공: {success}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 워커 {worker_id}: 행 처리 오류 (인덱스 {idx}): {e}")
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
            
            # 중간저장 카운터 초기화
            self.intermediate_save_counter = 0
            self.processed_count = 0
            
            # 멀티스레딩으로 처리
            with ThreadPoolExecutor(max_workers=self.current_workers) as executor:
                # 작업 제출
                future_to_idx = {
                    executor.submit(self.process_single_row, (idx, row)): idx 
                    for idx, row in target_rows.iterrows()
                }
                
                # 결과 수집
                for future in as_completed(future_to_idx):
                    try:
                        result = future.result()
                        results[result['index']] = result
                        self.processed_count += 1
                        self.intermediate_save_counter += 1
                        
                        # 진행률 출력
                        if self.processed_count % 10 == 0:
                            progress = (self.processed_count / len(target_rows)) * 100
                            logger.info(f"진행률: {progress:.1f}% ({self.processed_count}/{len(target_rows)})")
                        
                        # 중간저장 (100개 단위)
                        if self.intermediate_save_counter >= self.intermediate_save_interval:
                            # 현재까지의 결과를 DataFrame에 적용
                            for idx, res in results.items():
                                if res['phone_institution']:
                                    df.at[idx, '전화번호_실제기관명'] = res['phone_institution']
                                if res['fax_institution']:
                                    df.at[idx, '팩스번호_실제기관명'] = res['fax_institution']
                            
                            # 중간저장 수행
                            self._save_intermediate_results(df, f"중간저장_{self.processed_count}개처리")
                            self.intermediate_save_counter = 0
                            logger.info(f"💾 중간 저장 완료: {self.processed_count}개 처리됨")
                        
                        # 워커 수 조정 검토
                        if self.processed_count % self.worker_adjustment_interval == 0:
                            self.adjust_worker_count()
                            
                    except Exception as e:
                        logger.error(f"Future 처리 오류: {e}")
                        self.processed_count += 1  # 오류 발생 시에도 카운터 증가
            
            # 최종 결과를 DataFrame에 적용
            for idx, result in results.items():
                if result['phone_institution']:
                    df.at[idx, '전화번호_실제기관명'] = result['phone_institution']
                if result['fax_institution']:
                    df.at[idx, '팩스번호_실제기관명'] = result['fax_institution']
            
            # 최종 결과 저장
            self.save_results(df)
            
            # 통계 출력
            self.print_statistics()
            
            logger.info("실제기관명 추출 완료")
            return True
            
        except KeyboardInterrupt:
            logger.info("⚠️ 사용자 중단 요청 감지")
            # 중단 시 현재까지의 결과를 DataFrame에 적용
            for idx, result in results.items():
                if result['phone_institution']:
                    df.at[idx, '전화번호_실제기관명'] = result['phone_institution']
                if result['fax_institution']:
                    df.at[idx, '팩스번호_실제기관명'] = result['fax_institution']
            
            # 사용자 중단 시 중간저장
            self._save_intermediate_results(df, "사용자중단저장")
            raise
        except Exception as e:
            logger.error(f"실제기관명 추출 실패: {e}")
            logger.error(traceback.format_exc())
            
            # 오류 발생 시 현재까지의 결과를 DataFrame에 적용
            try:
                for idx, result in results.items():
                    if result['phone_institution']:
                        df.at[idx, '전화번호_실제기관명'] = result['phone_institution']
                    if result['fax_institution']:
                        df.at[idx, '팩스번호_실제기관명'] = result['fax_institution']
                
                # 오류 발생 시 중간저장
                self._save_intermediate_results(df, "오류발생저장")
            except:
                pass
            
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
    
    def _save_intermediate_results(self, df: pd.DataFrame, suffix: str = "중간저장"):
        """중간 결과 저장 (Excel 형식)"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(self.input_file))[0]
            intermediate_filename = f"{base_name}_기관명추출_{suffix}_{timestamp}.xlsx"
            intermediate_path = os.path.join("rawdatafile", intermediate_filename)
            
            # 디렉토리 생성
            os.makedirs(os.path.dirname(intermediate_path), exist_ok=True)
            
            # Excel 저장
            df.to_excel(intermediate_path, index=False, engine='openpyxl')
            
            # 통계 정보
            total_count = len(df)
            phone_filled = len(df[df['전화번호_실제기관명'].notna() & (df['전화번호_실제기관명'] != '')])
            fax_filled = len(df[df['팩스번호_실제기관명'].notna() & (df['팩스번호_실제기관명'] != '')])
            
            logger.info(f"💾 중간 저장 완료: {intermediate_path}")
            logger.info(f"📊 현재 통계 - 전체: {total_count}, 전화기관명: {phone_filled}, 팩스기관명: {fax_filled}")
            logger.info(f"📊 처리 진행률: {self.processed_count}개 처리 완료")
            
            return intermediate_path
            
        except Exception as e:
            logger.error(f"❌ 중간 저장 오류: {e}")
            return None
    
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
        print("🚀 실제기관명 추출 시스템 시작")
        print("=" * 60)
        
        # Headless 모드 선택
        print("\n🔧 브라우저 모드 선택:")
        print("1. Headless 모드 (권장) - CPU/메모리 사용량 낮음, 브라우저 창 안 보임")
        print("2. GUI 모드 - 브라우저 창 보임, CPU/메모리 사용량 높음")
        
        while True:
            choice = input("\n선택하세요 (1 또는 2, 기본값: 1): ").strip()
            if choice == "" or choice == "1":
                globals()['HEADLESS_MODE'] = True
                print("✅ Headless 모드로 실행합니다 (CPU/메모리 최적화)")
                break
            elif choice == "2":
                globals()['HEADLESS_MODE'] = False
                print("✅ GUI 모드로 실행합니다 (브라우저 창 표시)")
                break
            else:
                print("❌ 잘못된 선택입니다. 1 또는 2를 입력하세요.")
        
        # 워커 수 조정 (Headless 모드에 따라)
        if globals()['HEADLESS_MODE']:
            print(f"🔧 Headless 모드: {MAX_WORKERS}개 워커로 최적화")
        else:
            print(f"🔧 GUI 모드: {max(MIN_WORKERS, MAX_WORKERS - 3)}개 워커로 안정화")
        
        print("=" * 60)
        print(f"시스템 설정:")
        print(f"  - Headless 모드: {globals()['HEADLESS_MODE']}")
        print(f"  - 워커 수 범위: {MIN_WORKERS}-{MAX_WORKERS}개")
        print(f"  - 동적 워커 수 조정: 활성화")
        print(f"  - 중간저장 간격: 100개 단위")
        print(f"  - AMD Ryzen 5 3600 최적화: 적용")
        print("=" * 60)
        
        # 입력 파일 경로
        input_file = r"rawdatafile\failed_data_250715.xlsx"
        
        # 파일 존재 확인
        if not os.path.exists(input_file):
            print(f"❌ 입력 파일을 찾을 수 없습니다: {input_file}")
            return False
        
        print(f"📁 입력 파일 경로: {input_file}")
        
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
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        return False
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류가 발생했습니다: {e}")
        logger.error(f"메인 함수 오류: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    main() 