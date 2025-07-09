#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Intel Core i5-4210M 환경 최적화된 교회 크롤러
- Intel Core i5-4210M (2코어 4스레드) 환경 최적화
- 멀티프로세싱 처리 (4개 워커)
- 메모리 사용량 관리
- Chrome 브라우저 최적화
"""

import os
import re
import time
import json
import logging
import pandas as pd
import traceback
import psutil
import threading
import multiprocessing
from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from queue import Empty
from concurrent.futures import ThreadPoolExecutor, as_completed

# Selenium 관련
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import undetected_chromedriver as uc
import random

# AI 관련
import google.generativeai as genai
from dotenv import load_dotenv

# 한국 지역번호 매핑
KOREAN_AREA_CODES = {
    "02": "서울", "031": "경기", "032": "인천", "033": "강원",
    "041": "충남", "042": "대전", "043": "충북", "044": "세종",
    "051": "부산", "052": "울산", "053": "대구", "054": "경북", 
    "055": "경남", "061": "전남", "062": "광주", "063": "전북", 
    "064": "제주", "070": "인터넷전화", "010": "핸드폰"
}

# 로깅 설정
def setup_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'church_crawler_{name}.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(name)

class I5ChurchCrawler:
    """Intel i5-4210M 환경 최적화된 교회 크롤러"""
    
    def __init__(self, excel_path: str, worker_id: int = 0):
        self.excel_path = excel_path
        self.worker_id = worker_id
        self.logger = setup_logger(f"worker_{worker_id}")
        
        # 환경 변수 로드
        load_dotenv()
        
        # Gemini API 설정
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        if self.gemini_api_key:
            genai.configure(api_key=self.gemini_api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash')
            self.use_ai = True
            self.logger.info("🤖 Gemini AI 모델 초기화 성공")
        else:
            self.use_ai = False
            self.logger.warning("⚠️ Gemini API 키가 없어 AI 기능 비활성화")
        
        # WebDriver 초기화
        self.driver = None
        self._initialize_webdriver()
        
        # 데이터 로드
        self.df = None
        self._load_data()
        
        # 결과 저장용
        self.results = []
        self.processed_count = 0
        self.success_count = 0
        self.start_time = datetime.now()
        
        # i5-4210M 환경 설정
        self.request_delay_min = 2.0  # 최소 2초 (성능 향상)
        self.request_delay_max = 4.0  # 최대 4초
        self.memory_cleanup_interval = 30  # 30개마다 메모리 정리 (성능 향상)
        self.max_threads = 2  # 워커당 2개 스레드 사용
        
        # 팩스번호 정규식 패턴
        self.fax_patterns = [
            r'팩스[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'F[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'전송[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*팩스',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*fax',
        ]
        
        # 시스템 모니터링 시작
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(target=self._monitor_system, daemon=True)
        self.monitoring_thread.start()
        
        self.logger.info(f"🚀 Worker {worker_id} 초기화 완료")
    
    def _initialize_webdriver(self):
        """i5-4210M 환경 최적화된 WebDriver 초기화"""
        try:
            chrome_options = uc.ChromeOptions()
            
            # 기본 옵션
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=800,600')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            
            # 🛡️ i5-4210M 환경 메모리/CPU 최적화
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-images')
            chrome_options.add_argument('--disable-javascript')
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
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--mute-audio')
            chrome_options.add_argument('--no-first-run')
            
            # 메모리 최적화 (i5-4210M은 더 많은 메모리 사용 가능)
            chrome_options.add_argument('--disk-cache-size=32768')  # 32MB 캐시
            chrome_options.add_argument('--media-cache-size=32768')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--disable-application-cache')
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=256')  # JS 힙 크기 증가
            
            self.driver = uc.Chrome(options=chrome_options)
            self.driver.implicitly_wait(5)  # 응답성 향상
            self.driver.set_page_load_timeout(10)  # 타임아웃 단축
            
            # 메모리 관리
            import gc
            gc.collect()
            
            self.logger.info("🌐 WebDriver 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 초기화 실패: {e}")
            raise

    def _load_data(self):
        """Excel 데이터 로드"""
        try:
            if not os.path.exists(self.excel_path):
                self.excel_path = 'academy.xlsx'
            
            self.df = pd.read_excel(self.excel_path)
            self.logger.info(f"📊 데이터 로드 완료: {len(self.df)}개 교회")
            
            # 컬럼명 정규화
            if '기관명' in self.df.columns:
                self.df = self.df.rename(columns={
                    '기관명': 'name',
                    '주소': 'address', 
                    '전화번호': 'phone',
                    '팩스번호': 'fax',
                    '홈페이지': 'homepage'
                })
            
            # 누락된 컬럼 추가
            for col in ['name', 'address', 'phone', 'fax', 'homepage']:
                if col not in self.df.columns:
                    self.df[col] = ''
            
            # NaN 값 처리
            self.df = self.df.fillna('')
            
            self.logger.info("✅ 데이터 전처리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise

    def process_chunk(self, chunk):
        """청크 단위 처리 (멀티프로세싱용)"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = []
            
            for idx, row in chunk.iterrows():
                if not row['name'].strip():
                    continue
                    
                future = executor.submit(self._process_single_church, row)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    self.logger.error(f"처리 중 오류 발생: {e}")
        
        return results

    def _process_single_church(self, row):
        """단일 교회 처리"""
        try:
            name = str(row['name']).strip()
            address = str(row['address']).strip()
            phone = str(row['phone']).strip()
            homepage = str(row['homepage']).strip()
            
            result = {
                'name': name,
                'address': address,
                'phone': phone,
                'fax': '',
                'homepage': homepage
            }
            
            # 홈페이지가 있는 경우
            if homepage:
                page_data = self._crawl_homepage(homepage)
                if page_data:
                    fax_numbers = self._extract_fax_from_html(page_data['html'])
                    if fax_numbers:
                        for fax in fax_numbers:
                            if self._validate_fax_number(fax, phone, address, name):
                                result['fax'] = fax
                                break
                    
                    # AI 추출 시도
                    if not result['fax'] and self.use_ai:
                        ai_fax = self._extract_fax_with_ai(name, page_data)
                        if ai_fax and self._validate_fax_number(ai_fax, phone, address, name):
                            result['fax'] = ai_fax
            
            # 홈페이지가 없거나 팩스를 찾지 못한 경우
            if not result['fax']:
                google_fax = self._search_google_for_fax(name, address)
                if google_fax and self._validate_fax_number(google_fax, phone, address, name):
                    result['fax'] = google_fax
            
            self.processed_count += 1
            if result['fax']:
                self.success_count += 1
            
            # 메모리 정리
            if self.processed_count % self.memory_cleanup_interval == 0:
                self._cleanup_memory()
            
            return result
            
        except Exception as e:
            self.logger.error(f"교회 처리 중 오류 발생 ({name}): {e}")
            return None

    def _search_google_for_fax(self, name: str, address: str) -> Optional[str]:
        """구글 검색으로 팩스번호 찾기"""
        try:
            search_query = f"{name} {address} 팩스"
            self.driver.get(f"https://www.google.com/search?q={search_query}")
            
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 검색 결과에서 팩스번호 패턴 찾기
            text_content = soup.get_text()
            for pattern in self.fax_patterns:
                matches = re.finditer(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    fax_number = match.group(1)
                    if self._is_valid_phone_format(fax_number):
                        return self._normalize_phone_number(fax_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"구글 검색 중 오류 발생: {e}")
            return None

    def _crawl_homepage(self, url: str) -> Optional[Dict[str, Any]]:
        """홈페이지 크롤링"""
        try:
            self.driver.get(url)
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            return {
                'url': url,
                'html': self.driver.page_source,
                'text': BeautifulSoup(self.driver.page_source, 'html.parser').get_text()
            }
            
        except Exception as e:
            self.logger.error(f"홈페이지 크롤링 중 오류 발생 ({url}): {e}")
            return None

    def _extract_fax_from_html(self, html_content: str) -> List[str]:
        """HTML에서 팩스번호 추출"""
        fax_numbers = []
        for pattern in self.fax_patterns:
            matches = re.finditer(pattern, html_content, re.IGNORECASE)
            for match in matches:
                fax_number = match.group(1)
                if self._is_valid_phone_format(fax_number):
                    normalized = self._normalize_phone_number(fax_number)
                    if normalized not in fax_numbers:
                        fax_numbers.append(normalized)
        return fax_numbers

    def _extract_fax_with_ai(self, name: str, page_data: Dict[str, Any]) -> Optional[str]:
        """AI를 사용하여 팩스번호 추출"""
        if not self.use_ai:
            return None
            
        try:
            prompt = f"""
            다음 텍스트에서 '{name}' 교회의 팩스번호를 찾아주세요.
            형식: 지역번호-국번-번호 (예: 02-1234-5678)
            응답은 팩스번호만 작성해주세요.
            
            텍스트:
            {page_data['text'][:3000]}  # 텍스트 길이 제한
            """
            
            response = self.model.generate_content(prompt)
            if response and response.text:
                fax_number = response.text.strip()
                if self._is_valid_phone_format(fax_number):
                    return self._normalize_phone_number(fax_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"AI 추출 중 오류 발생: {e}")
            return None

    def _validate_fax_number(self, fax_number: str, phone_number: str, address: str, name: str) -> bool:
        """팩스번호 유효성 검증"""
        try:
            if not self._is_valid_phone_format(fax_number):
                return False
            
            # 전화번호와 동일한지 확인
            if fax_number == phone_number:
                return False
            
            # 지역 일치 여부 확인
            fax_region = self._get_region_from_phone(fax_number)
            phone_region = self._get_region_from_phone(phone_number)
            address_region = self._get_region_from_address(address)
            
            # 1. 팩스번호와 전화번호의 지역이 같은 경우
            if fax_region and phone_region and fax_region == phone_region:
                return True
            
            # 2. 팩스번호와 주소의 지역이 같은 경우
            if fax_region and address_region and fax_region == address_region:
                return True
            
            # 3. 인터넷 팩스(070)는 허용
            if fax_number.startswith('070'):
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"팩스번호 검증 중 오류 발생: {e}")
            return False

    def _normalize_phone_number(self, phone: str) -> str:
        """전화번호 정규화"""
        # 숫자만 추출
        numbers = re.sub(r'[^0-9]', '', phone)
        
        # 길이별 처리
        if len(numbers) == 7:  # 지역번호 없는 경우
            return f"02-{numbers[:3]}-{numbers[3:]}"
        elif len(numbers) == 8:  # 지역번호 없는 경우 (4-4)
            return f"02-{numbers[:4]}-{numbers[4:]}"
        elif len(numbers) in [9, 10, 11]:  # 지역번호 있는 경우
            if numbers.startswith('02'):
                return f"02-{numbers[2:-4]}-{numbers[-4:]}"
            else:
                return f"{numbers[:3]}-{numbers[3:-4]}-{numbers[-4:]}"
        
        return phone

    def _is_valid_phone_format(self, phone: str) -> bool:
        """전화번호 형식 검증"""
        # 기본 정규식 패턴
        patterns = [
            r'^\d{2,3}-\d{3,4}-\d{4}$',  # 02-123-4567 or 031-123-4567
            r'^\d{2,3}\d{3,4}\d{4}$',    # 0212345678 or 02123456789
            r'^\d{2,3} \d{3,4} \d{4}$',  # 02 123 4567 or 031 123 4567
        ]
        
        # 숫자만 추출
        numbers = re.sub(r'[^0-9]', '', phone)
        
        # 길이 체크
        if not (7 <= len(numbers) <= 11):
            return False
            
        # 지역번호 체크
        area_code = numbers[:2] if numbers.startswith('02') else numbers[:3]
        if area_code not in KOREAN_AREA_CODES:
            return False
        
        # 패턴 매칭
        normalized = self._normalize_phone_number(phone)
        return any(re.match(pattern, normalized) for pattern in patterns)

    def _get_region_from_phone(self, phone: str) -> str:
        """전화번호에서 지역 추출"""
        if not phone:
            return ""
            
        numbers = re.sub(r'[^0-9]', '', phone)
        if numbers.startswith('02'):
            return KOREAN_AREA_CODES.get('02', '')
        else:
            area_code = numbers[:3]
            return KOREAN_AREA_CODES.get(area_code, '')

    def _get_region_from_address(self, address: str) -> str:
        """주소에서 지역 추출"""
        if not address:
            return ""
            
        # 시도 단위 매칭
        regions = {
            '서울': '서울', '경기': '경기', '인천': '인천', '강원': '강원',
            '충남': '충남', '대전': '대전', '충북': '충북', '세종': '세종',
            '부산': '부산', '울산': '울산', '대구': '대구', '경북': '경북',
            '경남': '경남', '전남': '전남', '광주': '광주', '전북': '전북',
            '제주': '제주'
        }
        
        for region, value in regions.items():
            if region in address:
                return value
        
        return ""

    def _cleanup_memory(self):
        """메모리 정리"""
        try:
            # 브라우저 캐시 정리
            self.driver.execute_script("window.localStorage.clear();")
            self.driver.execute_script("window.sessionStorage.clear();")
            
            # 파이썬 가비지 컬렉션
            import gc
            gc.collect()
            
            # 시스템 캐시 정리 (Linux)
            if os.name == 'posix':
                os.system('sync')
            
        except Exception as e:
            self.logger.error(f"메모리 정리 중 오류 발생: {e}")

    def _monitor_system(self):
        """시스템 리소스 모니터링"""
        while self.monitoring_active:
            try:
                process = psutil.Process(os.getpid())
                memory_usage = process.memory_info().rss / 1024 / 1024  # MB
                cpu_percent = process.cpu_percent()
                
                self.logger.info(f"시스템 상태 - CPU: {cpu_percent:.1f}%, "
                               f"메모리: {memory_usage:.1f}MB, "
                               f"처리: {self.processed_count}개, "
                               f"성공: {self.success_count}개")
                
                time.sleep(30)  # 30초마다 갱신
                
            except Exception as e:
                self.logger.error(f"모니터링 중 오류 발생: {e}")
                time.sleep(60)

    def _save_results(self) -> str:
        """결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"church_results_worker{self.worker_id}_{timestamp}.xlsx"
            
            df_result = pd.DataFrame(self.results)
            df_result.to_excel(filename, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"✅ 결과 저장 완료: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"결과 저장 중 오류 발생: {e}")
            return self._save_intermediate_results("오류")

    def _save_intermediate_results(self, suffix: str):
        """중간 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"church_results_worker{self.worker_id}_{suffix}_{timestamp}.xlsx"
            
            df_result = pd.DataFrame(self.results)
            df_result.to_excel(filename, index=False, encoding='utf-8-sig')
            
            return filename
            
        except Exception as e:
            self.logger.error(f"중간 결과 저장 중 오류 발생: {e}")
            return None

    def _cleanup(self):
        """리소스 정리"""
        try:
            self.monitoring_active = False
            if self.monitoring_thread.is_alive():
                self.monitoring_thread.join(timeout=3)
            
            if self.driver:
                self.driver.quit()
            
            self.logger.info("🧹 리소스 정리 완료")
            
        except Exception as e:
            self.logger.error(f"리소스 정리 중 오류 발생: {e}")

def split_dataframe(df, n_chunks):
    """데이터프레임을 n개의 청크로 분할"""
    chunk_size = len(df) // n_chunks
    chunks = []
    for i in range(n_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < n_chunks - 1 else len(df)
        chunks.append(df.iloc[start_idx:end_idx].copy())
    return chunks

def worker_process(excel_path: str, chunk_df: pd.DataFrame, worker_id: int):
    """워커 프로세스"""
    crawler = I5ChurchCrawler(excel_path, worker_id)
    try:
        results = crawler.process_chunk(chunk_df)
        return results
    finally:
        crawler._cleanup()

def main():
    """메인 함수"""
    try:
        # 프로세스 수 설정 (i5-4210M은 4개 스레드 지원)
        n_processes = 4
        
        # 엑셀 파일 경로
        excel_path = 'academy.xlsx'
        
        # 전체 데이터 로드
        df = pd.read_excel(excel_path)
        
        # 데이터 분할
        chunks = split_dataframe(df, n_processes)
        
        # 멀티프로세싱 실행
        with multiprocessing.Pool(processes=n_processes) as pool:
            worker_args = [(excel_path, chunk, i) for i, chunk in enumerate(chunks)]
            all_results = pool.starmap(worker_process, worker_args)
        
        # 결과 병합
        merged_results = []
        for results in all_results:
            merged_results.extend(results)
        
        # 최종 결과 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_filename = f"church_results_final_{timestamp}.xlsx"
        pd.DataFrame(merged_results).to_excel(final_filename, index=False, encoding='utf-8-sig')
        
        print(f"✨ 크롤링 완료! 결과 파일: {final_filename}")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 