#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
병렬 처리 워커 관리 클래스
"""

import time
import random
import logging
import undetected_chromedriver as uc
from typing import Optional

class WorkerManager:
    """병렬 처리 워커 관리 클래스"""
    
    def __init__(self, logger=None):
        """
        워커 관리자 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # 기본 Chrome 옵션
        self.base_chrome_options = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-blink-features=AutomationControlled',
            '--disable-extensions',
            '--mute-audio',
            '--no-first-run',
            '--disable-infobars',
            '--disable-notifications',
            # 리소스 절약 옵션
            '--disable-images',
            '--disable-plugins',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--disable-ipc-flooding-protection',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-features=TranslateUI',
            '--disable-default-apps',
            '--disable-sync',
            # 메모리 최적화
            '--memory-pressure-off',
            '--aggressive-cache-discard',
            '--max-unused-resource-memory-usage-percentage=5'
        ]
        
        # User-Agent 목록
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
        ]
    
    def create_worker_driver(self, worker_id: int, window_size: str = "1366,768", memory_limit: int = 256) -> Optional[object]:
        """
        워커용 WebDriver 생성 (프로세스 분리 방식)
        
        Args:
            worker_id: 워커 ID
            window_size: 브라우저 윈도우 크기 (기본값: "1366,768")
            memory_limit: JS 힙 메모리 제한 MB (기본값: 256)
            
        Returns:
            WebDriver 인스턴스 또는 None
        """
        try:
            # 워커 간 시차 두기 (파일 접근 충돌 방지)
            startup_delay = random.uniform(1.0, 3.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            chrome_options = uc.ChromeOptions()
            
            # 기본 옵션 적용
            for option in self.base_chrome_options:
                chrome_options.add_argument(option)
            
            # 윈도우 크기 설정
            chrome_options.add_argument(f'--window-size={window_size}')
            
            # 메모리 제한 설정
            chrome_options.add_argument(f'--max_old_space_size={memory_limit}')
            
            # 안전한 포트 설정 (충돌 방지)
            debug_port = 9222 + (worker_id * 10)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # User-Agent 랜덤화
            chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
            
            # 드라이버 생성
            driver = uc.Chrome(options=chrome_options, version_main=None)
            
            # 타임아웃 설정
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(30)
            
            # 웹드라이버 감지 방지
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info(f"🔧 워커 {worker_id}: WebDriver 생성 완료 (포트: {debug_port})")
            
            return driver
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} WebDriver 생성 오류: {e}")
            return None
    
    def create_low_spec_driver(self, worker_id: int = 0) -> Optional[object]:
        """
        저사양 환경용 WebDriver 생성 (Intel i5-4210M 등)
        
        Args:
            worker_id: 워커 ID (기본값: 0)
            
        Returns:
            WebDriver 인스턴스 또는 None
        """
        try:
            # 저사양 환경용 시작 지연
            startup_delay = random.uniform(2.0, 4.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            chrome_options = uc.ChromeOptions()
            
            # 기본 옵션 적용
            for option in self.base_chrome_options:
                chrome_options.add_argument(option)
            
            # 저사양 환경 추가 최적화
            low_spec_options = [
                '--window-size=800,600',  # 더 작은 윈도우
                '--disable-javascript',   # JS 비활성화 (필요시)
                '--disable-application-cache',
                '--disk-cache-size=1',    # 디스크 캐시 최소화
                '--media-cache-size=1',   # 미디어 캐시 최소화
                '--max_old_space_size=128'  # 더 작은 메모리 할당
            ]
            
            for option in low_spec_options:
                chrome_options.add_argument(option)
            
            # 포트 설정
            debug_port = 9222 + (worker_id * 10)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # User-Agent 설정
            chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
            
            # 드라이버 생성
            driver = uc.Chrome(options=chrome_options)
            
            # 저사양 환경용 타임아웃 설정
            driver.implicitly_wait(8)
            driver.set_page_load_timeout(15)
            
            # 웹드라이버 감지 방지
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # 메모리 관리를 위한 초기 가비지 컬렉션
            import gc
            gc.collect()
            
            self.logger.info(f"🔧 저사양 워커 {worker_id}: WebDriver 생성 완료 (포트: {debug_port})")
            
            return driver
            
        except Exception as e:
            self.logger.error(f"❌ 저사양 워커 {worker_id} WebDriver 생성 오류: {e}")
            return None
    
    def create_high_performance_driver(self, worker_id: int) -> Optional[object]:
        """
        고성능 환경용 WebDriver 생성 (AMD Ryzen 5 3600 등)
        
        Args:
            worker_id: 워커 ID
            
        Returns:
            WebDriver 인스턴스 또는 None
        """
        try:
            # 고성능 환경용 짧은 시작 지연
            startup_delay = random.uniform(0.5, 1.5) * (worker_id + 1)
            time.sleep(startup_delay)
            
            chrome_options = uc.ChromeOptions()
            
            # 기본 옵션 적용
            for option in self.base_chrome_options:
                chrome_options.add_argument(option)
            
            # 고성능 환경 최적화
            high_perf_options = [
                '--window-size=1920,1080',  # 더 큰 윈도우
                '--max_old_space_size=512', # 더 많은 메모리 할당
                '--disk-cache-size=67108864',  # 64MB 캐시
                '--media-cache-size=67108864'
            ]
            
            for option in high_perf_options:
                chrome_options.add_argument(option)
            
            # 포트 설정
            debug_port = 9222 + (worker_id * 10)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # User-Agent 설정
            chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
            
            # 드라이버 생성
            driver = uc.Chrome(options=chrome_options, version_main=None)
            
            # 고성능 환경용 타임아웃 설정
            driver.implicitly_wait(5)
            driver.set_page_load_timeout(20)
            
            # 웹드라이버 감지 방지
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info(f"🔧 고성능 워커 {worker_id}: WebDriver 생성 완료 (포트: {debug_port})")
            
            return driver
            
        except Exception as e:
            self.logger.error(f"❌ 고성능 워커 {worker_id} WebDriver 생성 오류: {e}")
            return None
    
    def cleanup_driver(self, driver: object, worker_id: int = 0):
        """
        WebDriver 정리
        
        Args:
            driver: WebDriver 인스턴스
            worker_id: 워커 ID (기본값: 0)
        """
        try:
            if driver:
                # 브라우저 캐시 정리
                try:
                    driver.execute_script("window.localStorage.clear();")
                    driver.execute_script("window.sessionStorage.clear();")
                except:
                    pass
                
                # 드라이버 종료
                driver.quit()
                
                # 메모리 정리
                import gc
                gc.collect()
                
                self.logger.info(f"🧹 워커 {worker_id}: WebDriver 정리 완료")
                
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} WebDriver 정리 오류: {e}")


# 전역 함수들 (기존 코드와의 호환성을 위해)
def create_improved_worker_driver(worker_id: int):
    """
    개선된 워커용 WebDriver 생성 (호환성 함수)
    
    Args:
        worker_id: 워커 ID
        
    Returns:
        WebDriver 인스턴스 또는 None
    """
    manager = WorkerManager()
    return manager.create_worker_driver(worker_id)

def create_worker_driver(worker_id: int):
    """
    워커용 WebDriver 생성 (호환성 함수)
    
    Args:
        worker_id: 워커 ID
        
    Returns:
        WebDriver 인스턴스 또는 None
    """
    manager = WorkerManager()
    return manager.create_worker_driver(worker_id) 