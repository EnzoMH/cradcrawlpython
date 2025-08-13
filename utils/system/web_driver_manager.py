#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import logging
import random
import time
import tempfile
import socket
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.proxy import Proxy, ProxyType
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc

# 표준 ChromeDriver 관리 클래스 (undetected 문제 해결)
class StandardWebDriverManager:
    """표준 Selenium ChromeDriver 관리 클래스 - Proxy & User-Agent 로테이션"""
    
    def __init__(self, logger=None):
        """StandardWebDriverManager 초기화"""
        self.driver = None
        self.logger = logger or logging.getLogger(__name__)
        
        # 저사양 환경 설정
        self.request_delay_min = 2.0
        self.request_delay_max = 4.0
        
        # 포트 관리
        self.used_ports = set()
        self.base_port = 9222
        
        # 무료 프록시 리스트 (선택사항)
        self.free_proxies = [
            # 필요시 추가, 현재는 프록시 없이 User-Agent만 로테이션
        ]
        
        # User-Agent 풀
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
    
    def get_available_port(self, worker_id: int = 0) -> int:
        """사용 가능한 포트 번호 생성"""
        base_attempt = self.base_port + (worker_id * 10)
        
        for offset in range(50):
            port = base_attempt + offset
            
            if port in self.used_ports:
                continue
                
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    result = s.connect_ex(('localhost', port))
                    if result != 0:
                        self.used_ports.add(port)
                        return port
            except:
                continue
        
        fallback_port = self.base_port + worker_id + 1000
        self.used_ports.add(fallback_port)
        return fallback_port
    
    def create_standard_driver(self, worker_id: int = 0, port: int = None, timeout_minutes: int = 5) -> object:
        """표준 ChromeDriver 생성 (안정성 우선)"""
        try:
            # 워커 간 시차
            startup_delay = random.uniform(0.5, 2.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            # Chrome 옵션 설정
            chrome_options = Options()
            
            # 🔧 기본 안정성 옵션
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1366,768')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            
            # 🎭 User-Agent 로테이션
            user_agent = random.choice(self.user_agents)
            chrome_options.add_argument(f'--user-agent={user_agent}')
            self.logger.debug(f"🎭 워커 {worker_id} User-Agent: {user_agent[:50]}...")
            
            # 🔌 포트 설정
            if port:
                debug_port = port
            else:
                debug_port = self.get_available_port(worker_id)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # 📁 워커별 프로필 디렉토리
            profile_dir = tempfile.mkdtemp(prefix=f'chrome_std_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            
            # 🛡️ 매크로 감지 회피
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # 💾 메모리 최적화
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=256')
            chrome_options.add_argument('--disable-background-mode')
            
            # 🚀 ChromeDriver 서비스 설정 (자동 관리)
            service = Service(ChromeDriverManager().install())
            
            # 드라이버 생성
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # ⏱️ 타임아웃 설정 (user 요구사항 반영)
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            # 타임아웃 자동 삭제 설정
            self._setup_timeout_cleanup(timeout_minutes)
            
            # 🛡️ 웹드라이버 감지 방지 스크립트
            try:
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko']})")
            except Exception as e:
                self.logger.debug(f"⚠️ 웹드라이버 감지 방지 스크립트 실패: {e}")
            
            gc.collect()
            
            self.logger.info(f"✅ 표준 ChromeDriver 생성 완료 (워커 {worker_id}, 포트: {debug_port})")
            return self.driver
            
        except Exception as e:
            self.logger.error(f"❌ 표준 ChromeDriver 생성 실패 (워커 {worker_id}): {e}")
            
            # 포트 해제
            if port and port in self.used_ports:
                self.used_ports.remove(port)
            elif 'debug_port' in locals() and debug_port in self.used_ports:
                self.used_ports.remove(debug_port)
            
            return self._create_minimal_fallback_driver(worker_id)
    
    def _setup_timeout_cleanup(self, timeout_minutes: int):
        """타임아웃 후 자동 정리 설정"""
        import threading
        
        def timeout_cleanup():
            time.sleep(timeout_minutes * 60)  # 분을 초로 변환
            try:
                if self.driver:
                    self.logger.info(f"⏰ {timeout_minutes}분 타임아웃 - 드라이버 자동 정리")
                    self.cleanup()
            except Exception as e:
                self.logger.debug(f"타임아웃 정리 실패: {e}")
        
        cleanup_thread = threading.Thread(target=timeout_cleanup, daemon=True)
        cleanup_thread.start()
    
    def _create_minimal_fallback_driver(self, worker_id: int):
        """최소 기능 fallback 드라이버"""
        try:
            self.logger.warning(f"🔄 워커 {worker_id} minimal fallback 드라이버 생성")
            
            chrome_options = Options()
            
            # 최소 옵션만
            minimal_options = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--headless',  # headless로 안정성 확보
                '--window-size=800,600',
                '--disable-logging',
                '--log-level=3'
            ]
            
            for option in minimal_options:
                chrome_options.add_argument(option)
            
            # 기본 User-Agent
            chrome_options.add_argument(f'--user-agent={self.user_agents[0]}')
            
            # 프로필 디렉토리
            profile_dir = tempfile.mkdtemp(prefix=f'chrome_fallback_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            self.driver.implicitly_wait(15)
            self.driver.set_page_load_timeout(45)
            
            self.logger.info(f"✅ 워커 {worker_id} minimal fallback 드라이버 생성 완료")
            return self.driver
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} minimal fallback 드라이버 생성 실패: {e}")
            return None
    
    def cleanup(self):
        """드라이버 정리"""
        try:
            if self.driver:
                # 브라우저 캐시 정리
                try:
                    self.driver.execute_script("window.localStorage.clear();")
                    self.driver.execute_script("window.sessionStorage.clear();")
                except:
                    pass
                
                # 브라우저 종료
                self.driver.quit()
                self.driver = None
                
                # 메모리 정리
                gc.collect()
                
                self.logger.info("🧹 표준 WebDriver 정리 완료")
                
        except Exception as e:
            self.logger.error(f"❌ 표준 WebDriver 정리 실패: {e}")
    
    def get_driver(self):
        """드라이버 반환 (없으면 자동 생성)"""
        if not self.driver:
            self.driver = self.create_standard_driver(0)
        return self.driver

# 기존 WebDriver 관리 클래스 - 봇 우회 강화 (UndetectedChrome)
class WebDriverManager:
    """WebDriver 관리 클래스 - 봇 우회 강화"""
    
    def __init__(self, logger=None):
        """
        WebDriver 관리자 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.driver = None
        self.logger = logger or logging.getLogger(__name__)
        
        # i5-4210M 환경 설정
        self.request_delay_min = 2.0  # 최소 2초
        self.request_delay_max = 4.0  # 최대 4초
        
        # 봇 우회를 위한 포트 관리
        self.used_ports = set()
        self.base_port = 9222
        
    def get_available_port(self, worker_id: int = 0) -> int:
        """사용 가능한 포트 번호 생성"""
        import socket
        
        # 워커 ID 기반 기본 포트 계산
        base_attempt = self.base_port + (worker_id * 10)
        
        # 최대 50개 포트 시도
        for offset in range(50):
            port = base_attempt + offset
            
            # 이미 사용 중인 포트는 건너뛰기
            if port in self.used_ports:
                continue
                
            # 포트 사용 가능 여부 확인
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    result = s.connect_ex(('localhost', port))
                    if result != 0:  # 포트가 사용 중이 아님
                        self.used_ports.add(port)
                        return port
            except:
                continue
        
        # 기본 포트 반환
        fallback_port = self.base_port + worker_id + 1000
        self.used_ports.add(fallback_port)
        return fallback_port
    
    def create_bot_evasion_driver(self, worker_id: int = 0, port: int = None) -> object:
        """봇 우회를 위한 고급 드라이버 생성"""
        import random
        import time
        import os
        
        try:
            # 워커 간 시차 두기 (봇 감지 회피)
            startup_delay = random.uniform(0.5, 2.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            # undetected_chromedriver 캐시 정리 (Status code 3221225786 해결)
            self._cleanup_uc_cache(worker_id)
            
            chrome_options = uc.ChromeOptions()
            
            # 🛡️ 기본 봇 우회 옵션
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
            
            # 🚫 고급 봇 감지 회피 옵션
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-ipc-flooding-protection')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-features=TranslateUI')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--disable-sync')
            chrome_options.add_argument('--disable-plugins')
            
            # 🔧 Chrome 138 호환성 및 안정성
            chrome_options.add_argument('--no-crash-dialog')
            chrome_options.add_argument('--disable-crash-reporter')
            chrome_options.add_argument('--disable-hang-monitor')
            chrome_options.add_argument('--disable-prompt-on-repost')
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--disable-logging-redirect')
            chrome_options.add_argument('--log-level=3')
            
            # ⚡ 안정성 개선 옵션
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-background-networking')
            chrome_options.add_argument('--disable-component-update')
            chrome_options.add_argument('--disable-domain-reliability')
            chrome_options.add_argument('--disable-client-side-phishing-detection')
            
            # 💾 메모리 최적화
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=256')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
            chrome_options.add_argument('--disable-background-mode')
            
            # 🌐 포트 분배 (봇 우회 핵심)
            if port:
                debug_port = port
                self.logger.debug(f"🔌 워커 {worker_id}: 지정된 포트 {port} 사용")
            else:
                debug_port = self.get_available_port(worker_id)
                self.logger.debug(f"🔌 워커 {worker_id}: 자동 할당 포트 {debug_port} 사용")
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # 🎭 User-Agent 랜덤화 (봇 감지 회피)
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            ]
            chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # 🔐 추가 봇 우회 옵션 (Chrome 호환성 개선)
            # excludeSwitches는 Chrome 최신 버전에서 호환성 문제로 제거
            # chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            # chrome_options.add_experimental_option('useAutomationExtension', False)
            # detach 옵션 제거 (Chrome 호환성 문제)
            
            # 📁 프로필 디렉토리 분리 (워커별)
            import tempfile
            profile_dir = tempfile.mkdtemp(prefix=f'chrome_worker_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            
            # Chrome 138 호환성 - version_main=None 필수
            # driver_executable_path 명시적 지정 시도
            try:
                self.driver = uc.Chrome(
                    options=chrome_options, 
                    version_main=None,
                    driver_executable_path=None  # 자동 감지
                )
            except Exception as path_error:
                self.logger.warning(f"⚠️ 기본 경로로 드라이버 생성 실패, 재시도: {path_error}")
                # ChromeOptions 재생성 (재사용 방지)
                chrome_options = uc.ChromeOptions()
                
                # 기본 옵션만 다시 추가 (간소화)
                basic_options = [
                    '--no-sandbox',
                    '--disable-dev-shm-usage', 
                    '--disable-gpu',
                    '--window-size=1366,768',
                    '--disable-logging',
                    '--log-level=3'
                ]
                
                for option in basic_options:
                    chrome_options.add_argument(option)
                
                # 포트 재설정
                port = 9222 + worker_id + 5000
                chrome_options.add_argument(f'--remote-debugging-port={port}')
                
                # 재시도 with 새로운 설정
                time.sleep(random.uniform(1.0, 3.0))
                self.driver = uc.Chrome(options=chrome_options, version_main=None)
            
            # 타임아웃 설정
            self.driver.implicitly_wait(8)
            self.driver.set_page_load_timeout(15)
            
            # 🛡️ 웹드라이버 감지 방지 스크립트
            try:
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko']})")
            except Exception as e:
                self.logger.warning(f"⚠️ 웹드라이버 감지 방지 스크립트 실패: {e}")
            
            # 메모리 관리
            gc.collect()
            
            self.logger.info(f"🛡️ 봇 우회 드라이버 생성 완료 (워커 {worker_id}, 포트: {debug_port})")
            
            return self.driver
            
        except Exception as e:
            self.logger.error(f"❌ 봇 우회 드라이버 생성 실패 (워커 {worker_id}): {e}")
            
            # 포트 사용 실패시 해제
            try:
                if port and port in self.used_ports:
                    self.used_ports.remove(port)
                elif debug_port in self.used_ports:
                    self.used_ports.remove(debug_port)
            except:
                pass
            
            # 안전한 fallback 드라이버 생성 시도
            return self._create_fallback_driver(worker_id, port)
    
    def _cleanup_uc_cache(self, worker_id: int):
        """undetected_chromedriver 캐시 정리"""
        try:
            import shutil
            
            # undetected_chromedriver 캐시 디렉토리
            uc_cache_dirs = [
                os.path.expanduser("~/.undetected_chromedriver"),
                os.path.join(os.path.expanduser("~"), "AppData", "Roaming", "undetected_chromedriver"),
                os.path.join(os.path.expanduser("~"), "appdata", "roaming", "undetected_chromedriver")
            ]
            
            for cache_dir in uc_cache_dirs:
                if os.path.exists(cache_dir):
                    try:
                        # 워커별로 다른 시간에 정리 (충돌 방지)
                        if worker_id % 3 == 0:  # 3개 워커마다 1번씩만
                            self.logger.debug(f"🧹 워커 {worker_id}: UC 캐시 정리 - {cache_dir}")
                            
                            # 특정 파일들만 삭제 (전체 삭제는 위험)
                            for item in os.listdir(cache_dir):
                                item_path = os.path.join(cache_dir, item)
                                if item.endswith(('.exe', '.tmp', '.lock')):
                                    try:
                                        if os.path.isfile(item_path):
                                            os.remove(item_path)
                                    except:
                                        pass
                    except Exception as cleanup_error:
                        self.logger.debug(f"UC 캐시 정리 실패 (무시): {cleanup_error}")
        
        except Exception as e:
            self.logger.debug(f"UC 캐시 정리 과정 오류 (무시): {e}")
    
    def _create_fallback_driver(self, worker_id: int = 0, port: int = None):
        """안전한 fallback 드라이버 생성"""
        try:
            self.logger.warning(f"🔄 워커 {worker_id} fallback 드라이버 생성 시도")
            
            import time
            import random
            time.sleep(random.uniform(2.0, 4.0))
            
            # 환경별 다른 전략 시도
            strategies = [
                lambda wid: self._try_minimal_chrome(wid, port),
                lambda wid: self._try_headless_chrome(wid, port), 
                lambda wid: self._try_basic_chrome(wid, port)
            ]
            
            for strategy_idx, strategy in enumerate(strategies):
                try:
                    self.logger.info(f"🔧 워커 {worker_id} 전략 {strategy_idx + 1} 시도")
                    driver = strategy(worker_id)
                    if driver:
                        self.logger.info(f"✅ 워커 {worker_id} 전략 {strategy_idx + 1} 성공")
                        return driver
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 전략 {strategy_idx + 1} 실패: {e}")
                    continue
            
            self.logger.error(f"❌ 워커 {worker_id} 모든 fallback 전략 실패")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} fallback 드라이버 생성 실패: {e}")
            return None
    
    def _try_minimal_chrome(self, worker_id: int, assigned_port: int = None):
        """최소 옵션 Chrome 시도"""
        import tempfile
        
        chrome_options = uc.ChromeOptions()
        
        # 절대 최소 옵션
        minimal_options = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-logging',
            '--log-level=3',
            '--disable-extensions',
            '--disable-javascript'  # 속도 향상
        ]
        
        for option in minimal_options:
            chrome_options.add_argument(option)
        
        # 워커별 독립적인 프로필 디렉토리
        profile_dir = tempfile.mkdtemp(prefix=f'chrome_minimal_{worker_id}_')
        chrome_options.add_argument(f'--user-data-dir={profile_dir}')
        
        # 포트 설정 (지정된 포트 우선 사용)
        if assigned_port:
            port = assigned_port
        else:
            port = 9222 + worker_id + 15000
        chrome_options.add_argument(f'--remote-debugging-port={port}')
        
        # 실험적 옵션 제거로 안정성 향상
        driver = uc.Chrome(options=chrome_options, version_main=None)
        driver.implicitly_wait(15)
        driver.set_page_load_timeout(30)
        
        self.logger.info(f"✅ 워커 {worker_id}: minimal Chrome 드라이버 생성 완료 (포트: {port})")
        return driver
    
    def _try_headless_chrome(self, worker_id: int, assigned_port: int = None):
        """헤드리스 Chrome 시도"""
        import tempfile
        
        chrome_options = uc.ChromeOptions()
        
        # 헤드리스 모드로 더 안전하게
        headless_options = [
            '--headless',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1366,768',
            '--disable-logging',
            '--log-level=3',
            '--disable-javascript',  # 속도 향상
            '--disable-images'       # 리소스 절약
        ]
        
        for option in headless_options:
            chrome_options.add_argument(option)
        
        # 워커별 독립적인 프로필 디렉토리
        profile_dir = tempfile.mkdtemp(prefix=f'chrome_headless_{worker_id}_')
        chrome_options.add_argument(f'--user-data-dir={profile_dir}')
        
        # 포트 설정 (지정된 포트 우선 사용)
        if assigned_port:
            port = assigned_port
        else:
            port = 9222 + worker_id + 20000
        chrome_options.add_argument(f'--remote-debugging-port={port}')
        
        driver = uc.Chrome(options=chrome_options, version_main=None)
        driver.implicitly_wait(20)
        driver.set_page_load_timeout(40)
        
        self.logger.info(f"✅ 워커 {worker_id}: headless Chrome 드라이버 생성 완료 (포트: {port})")
        return driver
    
    def _try_basic_chrome(self, worker_id: int, assigned_port: int = None):
        """기본 Chrome 시도 (최후의 수단)"""
        import tempfile
        
        chrome_options = uc.ChromeOptions()
        
        # 기본 설정만
        basic_options = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--window-size=800,600',
            '--disable-logging',
            '--log-level=3'
        ]
        
        for option in basic_options:
            chrome_options.add_argument(option)
        
        # 워커별 독립적인 프로필 디렉토리
        profile_dir = tempfile.mkdtemp(prefix=f'chrome_basic_{worker_id}_')
        chrome_options.add_argument(f'--user-data-dir={profile_dir}')
        
        # 포트 설정 (지정된 포트 우선 사용)
        if assigned_port:
            port = assigned_port
        else:
            port = 9222 + worker_id + 25000
        chrome_options.add_argument(f'--remote-debugging-port={port}')
        
        # 실험적 옵션 없이
        driver = uc.Chrome(options=chrome_options, version_main=None)
        driver.implicitly_wait(30)
        driver.set_page_load_timeout(60)
        
        self.logger.info(f"✅ 워커 {worker_id}: basic Chrome 드라이버 생성 완료 (포트: {port})")
        return driver
    
    def recover_driver(self):
        """드라이버 복구 (봇 감지 시 재생성)"""
        try:
            if self.driver:
                try:
                    # 현재 드라이버 정리
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            
            # 잠시 대기 후 재생성
            import time
            import random
            time.sleep(random.uniform(2.0, 4.0))
            
            # 새로운 포트로 드라이버 재생성
            worker_id = random.randint(100, 999)  # 랜덤 워커 ID
            self.driver = self.create_bot_evasion_driver(worker_id)
            
            self.logger.info("🔄 드라이버 복구 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 드라이버 복구 실패: {e}")
    
    def initialize(self):
        """WebDriver 초기화 (봇 우회 모드 사용)"""
        self.driver = self.create_bot_evasion_driver(0)
        
    def cleanup(self):
        """WebDriver 정리"""
        try:
            if self.driver:
                # 브라우저 캐시 정리
                self.driver.execute_script("window.localStorage.clear();")
                self.driver.execute_script("window.sessionStorage.clear();")
                
                # 브라우저 종료
                self.driver.quit()
                self.driver = None
                
                # 메모리 정리
                gc.collect()
                
                # 시스템 캐시 정리 (Linux)
                if os.name == 'posix':
                    os.system('sync')
                    
                self.logger.info("🧹 WebDriver 정리 완료")
                
        except Exception as e:
            self.logger.error(f"❌ WebDriver 정리 실패: {e}")
    
    def get_driver(self):
        """드라이버 반환 (없으면 자동 생성)"""
        if not self.driver:
            self.initialize()
        return self.driver
    
    def check_driver_health(self):
        """WebDriver 상태 확인"""
        try:
            if not self.driver:
                return False
            
            # 간단한 JavaScript 실행으로 상태 확인
            self.driver.execute_script("return document.readyState;")
            return True
            
        except Exception as e:
            self.logger.warning(f"⚠️ WebDriver 상태 이상 감지: {e}")
            return False
    
    def safe_get(self, url, max_retries=3):
        """안전한 페이지 로드 (재시도 포함)"""
        for attempt in range(max_retries):
            try:
                # 드라이버 상태 확인
                if not self.check_driver_health():
                    self.logger.warning(f"🔄 시도 {attempt + 1}: WebDriver 복구 필요")
                    if not self.recover_driver():
                        continue
                
                # 페이지 로드
                self.driver.get(url)
                return True
                
            except Exception as e:
                self.logger.warning(f"⚠️ 시도 {attempt + 1} 실패: {e}")
                if attempt < max_retries - 1:
                    if not self.recover_driver():
                        continue
                else:
                    self.logger.error(f"❌ 모든 재시도 실패: {url}")
                    
        return False 