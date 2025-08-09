#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
병렬 크롤링 엔진 - ProcessPoolExecutor 기반 안정적인 병렬 처리
"""

import time
import logging
import multiprocessing
import random
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
import pandas as pd
from utils.system.system_analyzer import SystemAnalyzer

# ===== 독립적인 워커 함수들 =====

def create_worker_driver(worker_id: int):
    """
    워커용 WebDriver 생성 (프로세스 분리 방식)
    
    Args:
        worker_id: 워커 ID
        
    Returns:
        WebDriver 인스턴스 또는 None
    """
    try:
        import undetected_chromedriver as uc
        import random
        import time
        
        # 워커 간 시차 두기 (파일 접근 충돌 방지)
        startup_delay = random.uniform(1.0, 3.0) * (worker_id + 1)
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
        
        # 🛡️ 리소스 절약 옵션
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
        
        # 메모리 제한
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--max_old_space_size=256')
        chrome_options.add_argument('--aggressive-cache-discard')
        chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
        
        # 안전한 포트 설정 (충돌 방지)
        debug_port = 9222 + (worker_id * 10)
        chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
        
        # User-Agent 랜덤화
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
        ]
        chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
        
        # 드라이버 생성
        driver = uc.Chrome(options=chrome_options, version_main=None)
        
        # 타임아웃 설정
        driver.implicitly_wait(10)
        driver.set_page_load_timeout(30)
        
        # 웹드라이버 감지 방지
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print(f"🔧 워커 {worker_id}: WebDriver 생성 완료 (포트: {debug_port})")
        return driver
        
    except Exception as e:
        print(f"❌ 워커 {worker_id} WebDriver 생성 오류: {e}")
        return None

def process_institution_worker(institution_data: Dict, worker_id: int) -> Dict:
    """
    독립적인 워커 프로세스에서 기관 처리
    
    Args:
        institution_data: 기관 정보
        worker_id: 워커 ID
        
    Returns:
        Dict: 처리 결과
    """
    import os
    import sys
    import logging
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s - Worker{worker_id} - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(f'worker_{worker_id}')
    
    driver = None
    try:
        institution_name = institution_data.get('institution_name', '')
        region = institution_data.get('region', '')
        address = institution_data.get('address', '')
        
        logger.info(f"🏢 [{worker_id}] {institution_name} 처리 시작")
        
        # WebDriver 초기화
        driver = create_worker_driver(worker_id)
        if not driver:
            logger.error(f"❌ [{worker_id}] WebDriver 초기화 실패")
            return institution_data
        
        # 여기서 실제 크롤링 로직 구현
        # 1. 전화번호 추출
        logger.info(f"📞 [{worker_id}] {institution_name} 전화번호 추출")
        phone = search_google_for_phone(driver, institution_name, region, address)
        
        # 2. 팩스번호 추출
        logger.info(f"📠 [{worker_id}] {institution_name} 팩스번호 추출")
        fax = search_google_for_fax(driver, institution_name, region, address)
        
        # 3. 홈페이지 추출
        logger.info(f"🌐 [{worker_id}] {institution_name} 홈페이지 추출")
        homepage = search_google_for_homepage(driver, institution_name, region, address)
        
        # 결과 구성
        result = institution_data.copy()
        result.update({
            'phone': phone or '',
            'fax': fax or '',
            'homepage': homepage or '',
            'processing_status': 'completed',
            'worker_id': worker_id
        })
        
        logger.info(f"✅ [{worker_id}] {institution_name} 처리 완료")
        return result
        
    except Exception as e:
        logger.error(f"❌ [{worker_id}] {institution_name} 처리 실패: {e}")
        result = institution_data.copy()
        result.update({
            'processing_status': 'failed',
            'error_message': str(e),
            'worker_id': worker_id
        })
        return result
        
    finally:
        if driver:
            try:
                driver.quit()
                logger.info(f"🧹 [{worker_id}] WebDriver 정리 완료")
            except Exception as e:
                logger.error(f"❌ [{worker_id}] WebDriver 정리 실패: {e}")

def search_google_for_phone(driver, institution_name: str, region: str, address: str) -> str:
    """구글 검색으로 전화번호 추출"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.keys import Keys
        from bs4 import BeautifulSoup
        import re
        import time
        import random
        
        # 검색 쿼리 생성
        search_query = f"{region} {institution_name} 전화번호"
        
        # 구글 검색
        driver.get('https://www.google.com')
        time.sleep(random.uniform(1.0, 2.0))
        
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, 'q'))
        )
        
        search_box.clear()
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.RETURN)
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'search'))
        )
        time.sleep(random.uniform(1.0, 2.0))
        
        # 전화번호 패턴 추출
        page_source = driver.page_source
        phone_patterns = [
            r'전화[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'tel[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'T[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'연락처[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
        ]
        
        for pattern in phone_patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            for match in matches:
                normalized = normalize_phone_number(match)
                if is_valid_phone_format(normalized):
                    return normalized
        
        return None
        
    except Exception as e:
        print(f"❌ 전화번호 검색 오류: {e}")
        return None

def search_google_for_fax(driver, institution_name: str, region: str, address: str) -> str:
    """구글 검색으로 팩스번호 추출"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.keys import Keys
        from bs4 import BeautifulSoup
        import re
        import time
        import random
        
        # 검색 쿼리 생성
        search_query = f"{region} {institution_name} 팩스번호"
        
        # 구글 검색
        driver.get('https://www.google.com')
        time.sleep(random.uniform(1.0, 2.0))
        
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, 'q'))
        )
        
        search_box.clear()
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.RETURN)
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'search'))
        )
        time.sleep(random.uniform(1.0, 2.0))
        
        # 팩스번호 패턴 추출
        page_source = driver.page_source
        fax_patterns = [
            r'팩스[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'F[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'전송[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*팩스',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*fax',
        ]
        
        for pattern in fax_patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            for match in matches:
                normalized = normalize_phone_number(match)
                if is_valid_phone_format(normalized):
                    return normalized
        
        return None
        
    except Exception as e:
        print(f"❌ 팩스번호 검색 오류: {e}")
        return None

def search_google_for_homepage(driver, institution_name: str, region: str, address: str) -> str:
    """구글 검색으로 홈페이지 추출"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.keys import Keys
        from bs4 import BeautifulSoup
        import re
        import time
        import random
        
        # 검색 쿼리 생성
        search_query = f"{region} {institution_name} 홈페이지"
        
        # 구글 검색
        driver.get('https://www.google.com')
        time.sleep(random.uniform(1.0, 2.0))
        
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, 'q'))
        )
        
        search_box.clear()
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.RETURN)
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'search'))
        )
        time.sleep(random.uniform(1.0, 2.0))
        
        # 링크 추출
        links = driver.find_elements(By.CSS_SELECTOR, 'a[href]')
        for link in links:
            href = link.get_attribute('href')
            if href and is_valid_homepage_url(href):
                return href
        
        return None
        
    except Exception as e:
        print(f"❌ 홈페이지 검색 오류: {e}")
        return None

def normalize_phone_number(phone: str) -> str:
    """전화번호 정규화"""
    import re
    numbers = re.findall(r'\d+', phone)
    if not numbers:
        return phone
    
    if len(numbers) >= 3:
        return f"{numbers[0]}-{numbers[1]}-{numbers[2]}"
    elif len(numbers) == 2:
        return f"{numbers[0]}-{numbers[1]}"
    else:
        return numbers[0]

def is_valid_phone_format(phone: str) -> bool:
    """전화번호 형식 유효성 검사"""
    import re
    try:
        if not phone:
            return False
        
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) < 8 or len(digits) > 11:
            return False
        
        valid_patterns = [
            r'^02\d{7,8}$',
            r'^0[3-6]\d{7,8}$',
            r'^070\d{7,8}$',
            r'^1[5-9]\d{6,7}$',
            r'^080\d{7,8}$',
        ]
        
        for pattern in valid_patterns:
            if re.match(pattern, digits):
                return True
        
        return False
        
    except Exception:
        return False

def is_valid_homepage_url(url: str) -> bool:
    """홈페이지 URL 유효성 검사"""
    import re
    try:
        if not url:
            return False
        
        # 기본 URL 패턴 검사
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, url):
            return False
        
        # 제외할 도메인
        exclude_domains = [
            'google.com', 'youtube.com', 'facebook.com', 'instagram.com',
            'naver.com', 'daum.net', 'tistory.com', 'blogger.com'
        ]
        
        for domain in exclude_domains:
            if domain in url:
                return False
        
        return True
        
    except Exception:
        return False

class CrawlingEngine:
    """병렬 크롤링 엔진 - ProcessPoolExecutor 기반"""
    
    def __init__(self, logger=None):
        """
        크롤링 엔진 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.system_analyzer = SystemAnalyzer(self.logger)
        self.current_workers = 0
        self.max_workers = self.system_analyzer.get_optimal_workers()
        self.executor = None
        
        # 크롤링 통계
        self.crawling_stats = {
            'total_institutions': 0,
            'processed_institutions': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'verified_contacts': 0,
            'start_time': None,
            'end_time': None
        }
        
        self.logger.info("🚀 크롤링 엔진 초기화 완료 (ProcessPoolExecutor 기반)")
        self.logger.info(f"⚙️  최적 워커 수: {self.max_workers}개")
    
    def initialize_workers(self, worker_count: int = None) -> bool:
        """
        워커 풀 초기화
        
        Args:
            worker_count: 워커 수 (기본값: None - 자동 설정)
            
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            if worker_count is None:
                worker_count = self.max_workers
            
            self.current_workers = min(worker_count, self.max_workers)
            
            # ProcessPoolExecutor 사용 (프로세스 분리)
            self.executor = ProcessPoolExecutor(max_workers=self.current_workers)
            
            self.logger.info(f"👥 워커 풀 초기화 완료: {self.current_workers}개 프로세스")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 워커 풀 초기화 실패: {e}")
            return False

    def process_institution_batch(self, institutions: List[Dict]) -> List[Dict]:
        """
        기관 배치 처리
        
        Args:
            institutions: 기관 정보 리스트
            
        Returns:
            List[Dict]: 처리 결과 리스트
        """
        try:
            self.logger.info(f"🔄 배치 처리 시작: {len(institutions)}개 기관")
            self.crawling_stats['total_institutions'] = len(institutions)
            self.crawling_stats['start_time'] = datetime.now()
            
            if not self.executor:
                if not self.initialize_workers():
                    return []
            
            # 시스템 모니터링 시작
            self.system_analyzer.start_monitoring()
            
            results = []
            futures = []
            
            # 작업 제출
            for i, institution in enumerate(institutions):
                future = self.executor.submit(process_institution_worker, institution, i)
                futures.append(future)
            
            # 결과 수집
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=300)  # 5분 타임아웃
                    if result:
                        results.append(result)
                        if result.get('processing_status') == 'completed':
                            self.crawling_stats['successful_extractions'] += 1
                        else:
                            self.crawling_stats['failed_extractions'] += 1
                    
                    self.crawling_stats['processed_institutions'] += 1
                    
                    # 진행률 로그
                    if self.crawling_stats['processed_institutions'] % 10 == 0:
                        self._log_progress()
                    
                except Exception as e:
                    self.logger.error(f"❌ 작업 처리 실패: {e}")
                    self.crawling_stats['failed_extractions'] += 1
                    continue
            
            self.crawling_stats['end_time'] = datetime.now()
            self.system_analyzer.stop_monitoring()
            
            self.logger.info(f"✅ 배치 처리 완료: {len(results)}개 결과")
            self._log_final_stats()
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 배치 처리 실패: {e}")
            self.system_analyzer.stop_monitoring()
            return []

    def process_region_data(self, region_data: pd.DataFrame, 
                           region_name: str) -> List[Dict]:
        """
        지역별 데이터 처리
        
        Args:
            region_data: 지역 데이터
            region_name: 지역명
            
        Returns:
            List[Dict]: 처리 결과
        """
        try:
            self.logger.info(f"🗺️  {region_name} 지역 데이터 처리 시작: {len(region_data)}개")
            
            # 데이터프레임을 딕셔너리 리스트로 변환
            institutions = region_data.to_dict('records')
            
            # 배치 처리
            results = self.process_institution_batch(institutions)
            
            self.logger.info(f"✅ {region_name} 지역 처리 완료: {len(results)}개 결과")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ {region_name} 지역 처리 실패: {e}")
            return []

    def process_chunked_data(self, data_chunks: List[pd.DataFrame], 
                            region_name: str) -> List[Dict]:
        """
        청크 단위 데이터 처리
        
        Args:
            data_chunks: 데이터 청크 리스트
            region_name: 지역명
            
        Returns:
            List[Dict]: 처리 결과
        """
        try:
            self.logger.info(f"📦 {region_name} 청크 처리 시작: {len(data_chunks)}개 청크")
            
            all_results = []
            for i, chunk in enumerate(data_chunks):
                self.logger.info(f"🔄 청크 {i+1}/{len(data_chunks)} 처리 중...")
                
                # 청크를 딕셔너리 리스트로 변환
                institutions = chunk.to_dict('records')
                
                # 배치 처리
                chunk_results = self.process_institution_batch(institutions)
                all_results.extend(chunk_results)
                
                # 청크 간 휴식
                if i < len(data_chunks) - 1:
                    rest_time = random.uniform(2.0, 5.0)
                    self.logger.info(f"⏳ 청크 간 휴식: {rest_time:.1f}초")
                    time.sleep(rest_time)
            
            self.logger.info(f"✅ {region_name} 청크 처리 완료: {len(all_results)}개 결과")
            return all_results
            
        except Exception as e:
            self.logger.error(f"❌ {region_name} 청크 처리 실패: {e}")
            return []

    def _log_progress(self):
        """진행률 로깅"""
        try:
            total = self.crawling_stats['total_institutions']
            processed = self.crawling_stats['processed_institutions']
            success = self.crawling_stats['successful_extractions']
            failed = self.crawling_stats['failed_extractions']
            
            if total > 0:
                progress = (processed / total) * 100
                success_rate = (success / processed) * 100 if processed > 0 else 0
                
                self.logger.info(f"📊 진행률: {processed}/{total} ({progress:.1f}%) | "
                               f"성공률: {success_rate:.1f}% | 실패: {failed}개")
            
        except Exception as e:
            self.logger.error(f"❌ 진행률 로깅 실패: {e}")

    def _log_final_stats(self):
        """최종 통계 로깅"""
        try:
            stats = self.crawling_stats
            
            if stats['start_time'] and stats['end_time']:
                duration = stats['end_time'] - stats['start_time']
                duration_str = str(duration).split('.')[0]  # 초 단위 제거
                
                self.logger.info("🎯 최종 크롤링 통계:")
                self.logger.info(f"  - 총 처리 시간: {duration_str}")
                self.logger.info(f"  - 전체 기관 수: {stats['total_institutions']}")
                self.logger.info(f"  - 처리 완료: {stats['processed_institutions']}")
                self.logger.info(f"  - 성공 추출: {stats['successful_extractions']}")
                self.logger.info(f"  - 실패 추출: {stats['failed_extractions']}")
                self.logger.info(f"  - 검증 완료: {stats['verified_contacts']}")
                
                if stats['processed_institutions'] > 0:
                    success_rate = (stats['successful_extractions'] / stats['processed_institutions']) * 100
                    self.logger.info(f"  - 성공률: {success_rate:.1f}%")
                    
                    avg_time = duration.total_seconds() / stats['processed_institutions']
                    self.logger.info(f"  - 평균 처리 시간: {avg_time:.1f}초/기관")
            
        except Exception as e:
            self.logger.error(f"❌ 최종 통계 로깅 실패: {e}")

    def save_results(self, results: List[Dict], filename: str = None) -> str:
        """
        결과 저장
        
        Args:
            results: 처리 결과 리스트
            filename: 저장할 파일명 (기본값: None - 자동 생성)
            
        Returns:
            str: 저장된 파일 경로
        """
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"crawling_results_{timestamp}.xlsx"
            
            # 결과를 DataFrame으로 변환
            df = pd.DataFrame(results)
            
            # Excel 파일로 저장 (encoding 파라미터 제거)
            df.to_excel(filename, index=False)
            
            self.logger.info(f"💾 결과 저장 완료: {filename}")
            self.logger.info(f"📊 저장된 데이터: {len(results)}개 기관")
            
            return filename
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            raise

    def get_crawling_stats(self) -> Dict:
        """크롤링 통계 반환"""
        return self.crawling_stats.copy()

    def reset_stats(self):
        """통계 초기화"""
        self.crawling_stats = {
            'total_institutions': 0,
            'processed_institutions': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'verified_contacts': 0,
            'start_time': None,
            'end_time': None
        }
        self.logger.info("📊 크롤링 통계 초기화 완료")

    def cleanup(self):
        """리소스 정리"""
        try:
            if self.executor:
                self.executor.shutdown(wait=True)
                self.executor = None
                self.logger.info("🧹 크롤링 엔진 정리 완료")
                
            self.system_analyzer.stop_monitoring()
            
        except Exception as e:
            self.logger.error(f"❌ 크롤링 엔진 정리 실패: {e}")

    def __del__(self):
        """소멸자"""
        self.cleanup() 