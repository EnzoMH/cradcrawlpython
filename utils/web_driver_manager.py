#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import logging
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class WebDriverManager:
    """WebDriver 관리 클래스"""
    
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
        
    def initialize(self):
        """WebDriver 초기화"""
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
            
            # 메모리 최적화
            chrome_options.add_argument('--disk-cache-size=32768')  # 32MB 캐시
            chrome_options.add_argument('--media-cache-size=32768')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--disable-application-cache')
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=256')  # JS 힙 크기
            
            self.driver = uc.Chrome(options=chrome_options)
            self.driver.implicitly_wait(5)  # 응답성 향상
            self.driver.set_page_load_timeout(10)  # 타임아웃 단축
            
            # 메모리 관리
            gc.collect()
            
            self.logger.info("🌐 WebDriver 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 초기화 실패: {e}")
            raise
            
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
        """현재 WebDriver 인스턴스 반환"""
        if not self.driver:
            self.initialize()
        return self.driver 