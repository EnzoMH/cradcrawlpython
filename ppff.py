#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
parallel_phone_fax_finder.py
병렬 처리 전화번호/팩스번호 기반 해당기관 검색 시스템 - 고급 봇 우회 버전
"""

import os
import sys
import logging
import pandas as pd
import time
import random
import re
import multiprocessing
import traceback
import socket
import tempfile
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import google.generativeai as genai
from dotenv import load_dotenv

# Utils 모듈 import
from utils.worker_manager import WorkerManager
from utils.system_monitor import SystemMonitor

class AdvancedPortManager:
    """고급 포트 관리 시스템 - 동적 로테이션"""
    
    def __init__(self):
        # 포트 범위를 더 넓게 설정 (봇 감지 회피)
        self.base_ports = [9222, 9333, 9444, 9555, 9666, 9777, 9888, 9999]
        self.used_ports = set()
        self.port_rotation_count = 0
        self.max_port_reuse = 3  # 포트 재사용 제한
        
    def get_rotated_port(self, worker_id: int) -> int:
        """워커별 동적 포트 할당"""
        # 포트 풀에서 순환 선택
        base_idx = (worker_id + self.port_rotation_count) % len(self.base_ports)
        base_port = self.base_ports[base_idx]
        
        # 최대 100개 포트 시도 (더 안전한 포트 확보)
        for offset in range(100):
            port = base_port + offset
            
            # 포트 사용 가능성 확인
            if self._is_port_available(port) and port not in self.used_ports:
                self.used_ports.add(port)
                return port
        
        # 모든 포트가 사용 중인 경우 강제 할당
        fallback_port = base_port + worker_id + 1000 + random.randint(0, 500)
        self.used_ports.add(fallback_port)
        return fallback_port
    
    def _is_port_available(self, port: int) -> bool:
        """포트 사용 가능 여부 확인"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex(('localhost', port))
                return result != 0  # 포트가 사용 중이 아님
        except:
            return False
    
    def release_port(self, port: int):
        """포트 해제"""
        self.used_ports.discard(port)
    
    def rotate_ports(self):
        """포트 로테이션 카운터 증가"""
        self.port_rotation_count += 1

class StealthWebDriverManager:
    """스텔스 WebDriver 관리 클래스 - 고급 봇 우회"""
    
    def __init__(self, logger=None):
        """스텔스 WebDriver 관리자 초기화"""
        self.logger = logger or logging.getLogger(__name__)
        self.port_manager = AdvancedPortManager()
        
        # 최신 User-Agent 풀 (2025년 7월 기준)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'
        ]
        
        # 화면 해상도 풀 (일반적인 해상도들)
        self.screen_sizes = [
            (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
            (1600, 900), (1280, 720), (1920, 1200), (2560, 1440)
        ]
    
    def create_stealth_driver(self, worker_id: int = 0) -> object:
        """스텔스 드라이버 생성 - HTTP 클라이언트 우선, 브라우저 백업"""
        try:
            # 워커별 시작 지연 (봇 감지 회피)
            startup_delay = random.uniform(1.0, 3.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            self.logger.info(f"🛡️ 워커 {worker_id}: HTTP 우선 클라이언트 생성 중...")
            
            # 🌍 1순위: HTTP 클라이언트 (브라우저 없이 동작, 가장 안정적)
            http_client = self._create_http_client(worker_id)
            if http_client:
                self.logger.info(f"✅ 워커 {worker_id}: HTTP 클라이언트 생성 성공")
                return http_client
            
            # 🚗 2순위: Chrome 안정화 (최소 옵션으로 안정성 확보)
            chrome_driver = self._create_chrome_stable_driver(worker_id)
            if chrome_driver:
                self.logger.info(f"✅ 워커 {worker_id}: Chrome 안정화 드라이버 생성 성공")
                return chrome_driver
            
            # 🌐 3순위: Edge (Windows 기본 브라우저)
            edge_driver = self._create_edge_driver(worker_id)
            if edge_driver:
                self.logger.info(f"✅ 워커 {worker_id}: Edge 드라이버 생성 성공")
                return edge_driver
            
            # 🦊 4순위: Firefox (최후 백업)
            firefox_driver = self._create_firefox_driver(worker_id)
            if firefox_driver:
                self.logger.info(f"✅ 워커 {worker_id}: Firefox 드라이버 생성 성공")
                return firefox_driver
            
            self.logger.error(f"❌ 워커 {worker_id}: 모든 드라이버 및 클라이언트 생성 실패")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 드라이버 생성 오류 - {e}")
            return None
    
    def _create_firefox_driver(self, worker_id: int) -> object:
        """Firefox 드라이버 생성 (가장 안정적) - 수정된 버전"""
        try:
            from selenium import webdriver
            from selenium.webdriver.firefox.options import Options as FirefoxOptions
            from selenium.webdriver.firefox.service import Service as FirefoxService
            
            self.logger.info(f"🦊 워커 {worker_id}: Firefox 드라이버 생성 시도")
            
            # Firefox 옵션 설정
            firefox_options = FirefoxOptions()
            
            # 🛡️ Firefox 전용 기본 옵션 (Chrome 옵션 제거)
            # firefox_options.add_argument('--headless')  # 필요시 활성화
            
            # 🎭 핑거프린트 무작위화 (Firefox preferences 사용)
            firefox_options.set_preference("general.useragent.override", random.choice(self.user_agents))
            firefox_options.set_preference("dom.webdriver.enabled", False)
            firefox_options.set_preference("useAutomationExtension", False)
            
            # 🔕 알림 및 팝업 비활성화
            firefox_options.set_preference("dom.push.enabled", False)
            firefox_options.set_preference("dom.webnotifications.enabled", False)
            firefox_options.set_preference("dom.popup_maximum", 0)
            
            # 💾 메모리 및 캐시 최적화
            firefox_options.set_preference("browser.cache.disk.enable", False)
            firefox_options.set_preference("browser.cache.memory.enable", False)
            firefox_options.set_preference("browser.cache.offline.enable", False)
            firefox_options.set_preference("network.http.use-cache", False)
            
            # 🌏 한국 언어 설정
            firefox_options.set_preference("intl.accept_languages", "ko-KR,ko,en-US,en")
            firefox_options.set_preference("browser.startup.homepage", "about:blank")
            
            # 🔒 보안 및 개인정보 설정
            firefox_options.set_preference("privacy.trackingprotection.enabled", False)
            firefox_options.set_preference("geo.enabled", False)
            firefox_options.set_preference("media.navigator.enabled", False)
            
            # 📏 화면 크기 설정 (Firefox는 창 크기를 직접 설정)
            firefox_options.set_preference("browser.startup.windowwidth", 1366)
            firefox_options.set_preference("browser.startup.windowheight", 768)
            
            # Firefox 드라이버 생성 (프로필 디렉토리 없이)
            driver = webdriver.Firefox(options=firefox_options)
            
            # 창 크기 직접 설정 (더 안전한 방법)
            try:
                driver.set_window_size(1366, 768)
            except Exception as resize_error:
                self.logger.warning(f"⚠️ 창 크기 설정 실패: {resize_error}")
            
            # 타임아웃 설정
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(30)
            
            # 스텔스 JavaScript 적용
            self._apply_firefox_stealth(driver)
            
            self.logger.info(f"✅ 워커 {worker_id}: Firefox 드라이버 생성 성공")
            return driver
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: Firefox 드라이버 생성 실패 - {e}")
            return None
    
    def _create_edge_driver(self, worker_id: int) -> object:
        """Edge 드라이버 생성 (Windows 최적화)"""
        try:
            from selenium import webdriver
            from selenium.webdriver.edge.options import Options as EdgeOptions
            
            self.logger.info(f"🌐 워커 {worker_id}: Edge 드라이버 생성 시도")
            
            # Edge 옵션 설정
            edge_options = EdgeOptions()
            
            # 🛡️ 기본 스텔스 옵션
            edge_options.add_argument('--no-sandbox')
            edge_options.add_argument('--disable-dev-shm-usage')
            edge_options.add_argument('--disable-gpu')
            edge_options.add_argument('--window-size=1366,768')
            edge_options.add_argument('--disable-blink-features=AutomationControlled')
            edge_options.add_argument('--disable-extensions')
            edge_options.add_argument('--mute-audio')
            edge_options.add_argument('--no-first-run')
            edge_options.add_argument('--disable-infobars')
            edge_options.add_argument('--disable-notifications')
            
            # 🎭 핑거프린트 무작위화
            edge_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
            
            # 📁 워커별 독립 프로필
            profile_dir = tempfile.mkdtemp(prefix=f'edge_worker_{worker_id}_')
            edge_options.add_argument(f'--user-data-dir={profile_dir}')
            
            # Edge 드라이버 생성
            driver = webdriver.Edge(options=edge_options)
            
            # 타임아웃 설정
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(30)
            
            # 스텔스 JavaScript 적용
            self._apply_post_creation_stealth(driver, worker_id)
            
            return driver
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: Edge 드라이버 생성 실패 - {e}")
            return None
    
    def _create_chrome_stable_driver(self, worker_id: int) -> object:
        """Chrome 안정화 드라이버 생성 (초안전 모드)"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options as ChromeOptions
            from selenium.webdriver.chrome.service import Service as ChromeService
            
            self.logger.info(f"🚗 워커 {worker_id}: Chrome 초안전 모드 드라이버 생성 시도")
            
            # Chrome 서비스 설정
            chrome_service = None
            try:
                # ChromeDriver 경로 확인 및 서비스 생성 (여러 경로 시도)
                possible_paths = [
                    os.path.join("chromedriver-win64", "chromedriver.exe"),  # 새로운 폴더 구조
                    os.path.join("chromedriver", "chromedriver.exe"),        # 기존 폴더 구조
                    "chromedriver.exe"  # 현재 디렉토리
                ]
                
                chromedriver_path = None
                for path in possible_paths:
                    if os.path.exists(path):
                        chromedriver_path = path
                        break
                
                if chromedriver_path:
                    chrome_service = ChromeService(chromedriver_path)
                    self.logger.info(f"📁 ChromeDriver 경로 사용: {chromedriver_path}")
                else:
                    self.logger.info("🔍 시스템 PATH에서 ChromeDriver 자동 탐지")
            except Exception as service_error:
                self.logger.warning(f"⚠️ ChromeService 설정 실패, 기본값 사용: {service_error}")
            
            # 🛡️ 초안전 Chrome 옵션 (최소한만 사용)
            chrome_options = ChromeOptions()
            
            # 절대 필수 옵션만 (검증된 것만)
            essential_options = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1366,768',
                '--disable-logging',
                '--log-level=3',
                '--silent',
                '--disable-extensions',
                '--no-first-run'
            ]
            
            for option in essential_options:
                try:
                    chrome_options.add_argument(option)
                except Exception as opt_error:
                    self.logger.warning(f"⚠️ 옵션 설정 실패: {option} - {opt_error}")
            
            # User-Agent 설정 (가장 일반적인 것 사용)
            try:
                basic_ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                chrome_options.add_argument(f'--user-agent={basic_ua}')
            except Exception as ua_error:
                self.logger.warning(f"⚠️ User-Agent 설정 실패: {ua_error}")
            
            # 프로필 디렉토리 (간단하게)
            try:
                profile_dir = tempfile.mkdtemp(prefix=f'chrome_safe_{worker_id}_')
                chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            except Exception as profile_error:
                self.logger.warning(f"⚠️ 프로필 디렉토리 설정 실패: {profile_error}")
            
            # Chrome 드라이버 생성 (서비스 사용/미사용 모두 시도)
            driver = None
            try:
                if chrome_service:
                    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
                else:
                    driver = webdriver.Chrome(options=chrome_options)
            except Exception as creation_error:
                self.logger.warning(f"⚠️ 첫 번째 Chrome 생성 시도 실패: {creation_error}")
                
                # 최후 시도: 옵션 없이
                try:
                    minimal_options = ChromeOptions()
                    minimal_options.add_argument('--no-sandbox')
                    minimal_options.add_argument('--disable-dev-shm-usage')
                    driver = webdriver.Chrome(options=minimal_options)
                except Exception as minimal_error:
                    self.logger.warning(f"⚠️ 최소 옵션 Chrome 생성도 실패: {minimal_error}")
                    return None
            
            if driver:
                # 타임아웃 설정
                try:
                    driver.implicitly_wait(10)
                    driver.set_page_load_timeout(30)
                except Exception as timeout_error:
                    self.logger.warning(f"⚠️ 타임아웃 설정 실패: {timeout_error}")
                
                self.logger.info(f"✅ 워커 {worker_id}: Chrome 초안전 모드 성공")
                return driver
            
            return None
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: Chrome 초안전 모드 생성 실패 - {e}")
            return None
    
    def _apply_post_creation_stealth(self, driver, worker_id: int):
        """드라이버 생성 후 추가 스텔스 설정 적용"""
        try:
            # 2025년 최신 봇 우회: 페이지 로드 전 CDP 명령어들
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": random.choice(self.user_agents),
                "acceptLanguage": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "platform": "Win32"
            })
            
            # Viewport 설정 (더 자연스러운 크기)
            selected_size = random.choice(self.screen_sizes)
            driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride', {
                'width': selected_size[0],
                'height': selected_size[1],
                "deviceScaleFactor": 1,
                'mobile': False
            })
            
            # 타임존 설정 (한국 표준시)
            driver.execute_cdp_cmd('Emulation.setTimezoneOverride', {
                'timezoneId': 'Asia/Seoul'
            })
            
            self.logger.info(f"🛡️ 워커 {worker_id}: 추가 스텔스 설정 완료")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: 추가 스텔스 설정 실패 - {e}")
    
    def _apply_advanced_javascript_stealth(self, driver):
        """2025년 고급 JavaScript 스텔스 적용"""
        try:
            # 강화된 JavaScript 스텔스 코드
            stealth_script = """
            // 2025년 최신 봇 감지 우회
            
            // 1. WebDriver 관련 속성들 완전 제거
            delete navigator.__proto__.webdriver;
            delete navigator.webdriver;
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
                configurable: true
            });
            
            // 2. Chrome 객체 자연스럽게 설정
            if (!window.chrome) {
                window.chrome = {};
            }
            window.chrome.runtime = {
                onConnect: undefined,
                onMessage: undefined,
                sendMessage: () => {},
                connect: () => {}
            };
            window.chrome.loadTimes = function() {
                return {
                    commitLoadTime: Math.random() * 1000 + 1000,
                    connectionInfo: 'http/1.1',
                    finishDocumentLoadTime: Math.random() * 1000 + 2000,
                    finishLoadTime: Math.random() * 1000 + 2500,
                    firstPaintAfterLoadTime: 0,
                    firstPaintTime: Math.random() * 1000 + 1500,
                    navigationType: 'Other',
                    npnNegotiatedProtocol: 'unknown',
                    requestTime: Math.random() * 1000 + 500,
                    startLoadTime: Math.random() * 1000 + 800,
                    wasAlternateProtocolAvailable: false,
                    wasFetchedViaSpdy: false,
                    wasNpnNegotiated: false
                };
            };
            
            // 3. 플러그인 시뮬레이션 (더 현실적)
            Object.defineProperty(navigator, 'plugins', {
                get: () => [{
                    0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: null},
                    description: "Portable Document Format",
                    filename: "internal-pdf-viewer",
                    length: 1,
                    name: "Chrome PDF Plugin"
                }, {
                    0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: null},
                    description: "Portable Document Format", 
                    filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                    length: 1,
                    name: "Chrome PDF Viewer"
                }]
            });
            
            // 4. 언어 설정 (한국어 우선)
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ko-KR', 'ko', 'en-US', 'en'],
                configurable: true
            });
            Object.defineProperty(navigator, 'language', {
                get: () => 'ko-KR',
                configurable: true
            });
            
            // 5. Permission API 우회 (2025년 강화)
            const originalPermissions = navigator.permissions;
            navigator.permissions = {
                query: function(parameters) {
                    if (parameters.name === 'notifications') {
                        return Promise.resolve({state: 'default'});
                    }
                    if (parameters.name === 'geolocation') {
                        return Promise.resolve({state: 'prompt'});
                    }
                    return originalPermissions ? originalPermissions.query(parameters) : Promise.resolve({state: 'granted'});
                }
            };
            
            // 6. MediaDevices 우회 (카메라/마이크 감지 방지)
            if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {
                const original = navigator.mediaDevices.enumerateDevices;
                navigator.mediaDevices.enumerateDevices = function() {
                    return original.apply(this, arguments).then(devices => {
                        return devices.map(device => {
                            if (device.kind === 'videoinput') {
                                return {...device, label: 'camera'};
                            }
                            if (device.kind === 'audioinput') {
                                return {...device, label: 'microphone'};  
                            }
                            return device;
                        });
                    });
                };
            }
            
            // 7. WebGL Fingerprint 변조
            const getParameter = WebGLRenderingContext.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) {
                    return 'Intel Inc.';
                }
                if (parameter === 37446) {
                    return 'Intel(R) Iris(R) Plus Graphics 640';
                }
                return getParameter(parameter);
            };
            
            // 8. Canvas Fingerprint 방지
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(...args) {
                const context = this.getContext('2d');
                if (context) {
                    const imageData = context.getImageData(0, 0, this.width, this.height);
                    for (let i = 0; i < imageData.data.length; i += 4) {
                        imageData.data[i] += Math.floor(Math.random() * 3) - 1;
                        imageData.data[i + 1] += Math.floor(Math.random() * 3) - 1; 
                        imageData.data[i + 2] += Math.floor(Math.random() * 3) - 1;
                    }
                    context.putImageData(imageData, 0, 0);
                }
                return originalToDataURL.apply(this, args);
            };
            
            // 9. Automation 관련 속성 제거
            Object.defineProperty(window, 'navigator', {
                value: new Proxy(navigator, {
                    has: (target, key) => (key === 'webdriver') ? false : key in target,
                    get: (target, key) => (key === 'webdriver') ? undefined : target[key]
                })
            });
            
            // 10. CDP Runtime 숨기기 (2025년 새로운 감지 방법 차단)
            delete window.chrome.runtime.sendMessage;
            delete window.chrome.runtime.connect;
            
            console.log('🛡️ 2025년 고급 스텔스 모드 활성화 완료');
            """
            
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": stealth_script
            })
            
        except Exception as e:
            self.logger.warning(f"⚠️ JavaScript 스텔스 적용 실패: {e}")
    
    def cleanup_driver(self, driver, worker_id: int):
        """드라이버 정리"""
        try:
            if driver:
                # 포트 해제
                try:
                    port_info = driver.service.port if hasattr(driver, 'service') else None
                    if port_info:
                        self.port_manager.release_port(port_info)
                except:
                    pass
                
                driver.quit()
                self.logger.info(f"🧹 워커 {worker_id}: 드라이버 정리 완료")
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 드라이버 정리 실패 - {e}")

    def _apply_firefox_stealth(self, driver):
        """Firefox 전용 스텔스 적용"""
        try:
            # Firefox 전용 스텔스 JavaScript
            firefox_stealth_script = """
            // Firefox 전용 봇 감지 우회
            
            // 1. WebDriver 속성 제거
            delete navigator.__proto__.webdriver;
            delete navigator.webdriver;
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
                configurable: true
            });
            
            // 2. Firefox 특화 설정
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ko-KR', 'ko', 'en-US', 'en'],
                configurable: true
            });
            
            // 3. Platform 정보 설정
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32',
                configurable: true
            });
            
            console.log('🦊 Firefox 스텔스 모드 활성화 완료');
            """
            
            driver.execute_script(firefox_stealth_script)
            self.logger.info("🛡️ Firefox 스텔스 설정 완료")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Firefox 스텔스 설정 실패: {e}")

    def _create_http_client(self, worker_id: int) -> object:
        """HTTP 클라이언트 생성 (브라우저 없이 동작)"""
        try:
            import requests
            from types import SimpleNamespace
            
            self.logger.info(f"🌍 워커 {worker_id}: HTTP 클라이언트 생성 시도")
            
            # requests 세션 생성
            session = requests.Session()
            
            # 헤더 설정
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            # 타임아웃 설정
            session.timeout = 30
            
            # Selenium 인터페이스 모방을 위한 래퍼 클래스 생성
            class HTTPDriverWrapper:
                def __init__(self, session, logger):
                    self.session = session
                    self.logger = logger
                    self.current_url = ""
                    self.page_source = ""
                
                def get(self, url):
                    """페이지 가져오기"""
                    try:
                        response = self.session.get(url)
                        response.raise_for_status()
                        self.current_url = url
                        self.page_source = response.text
                        self.logger.info(f"🌍 HTTP 요청 성공: {url}")
                        return True
                    except Exception as e:
                        self.logger.warning(f"⚠️ HTTP 요청 실패: {url} - {e}")
                        return False
                
                def quit(self):
                    """세션 종료"""
                    try:
                        self.session.close()
                    except:
                        pass
                
                def find_element(self, by, value):
                    """요소 찾기 (HTTP에서는 제한적)"""
                    # HTTP 모드에서는 BeautifulSoup으로 파싱
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(self.page_source, 'html.parser')
                    return soup
                
                def execute_script(self, script):
                    """JavaScript 실행 (HTTP에서는 무시)"""
                    pass
                
                def set_window_size(self, width, height):
                    """창 크기 설정 (HTTP에서는 무시)"""
                    pass
                
                def implicitly_wait(self, timeout):
                    """암시적 대기 (HTTP에서는 무시)"""
                    pass
                
                def set_page_load_timeout(self, timeout):
                    """페이지 로드 타임아웃 (HTTP에서는 무시)"""
                    pass
            
            wrapper = HTTPDriverWrapper(session, self.logger)
            self.logger.info(f"✅ 워커 {worker_id}: HTTP 클라이언트 래퍼 생성 성공")
            return wrapper
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: HTTP 클라이언트 생성 실패 - {e}")
            return None

# 로깅 설정하는 함수
def setup_logger(name="ParallelPhoneFaxFinder"):
    """로깅 시스템 설정하는 메소드"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'parallel_phone_fax_finder_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(name)

# ================================
# 백업된 기존 워커 함수 (2025-01-18 백업)
# 메소드 로직 50% 이상 변경으로 백업 정책 적용
# ================================
"""
def process_batch_worker_original_backup(batch_data: List[Dict], worker_id: int, api_key: str = None) -> List[Dict]:
    # 배치 데이터 처리하는 워커 함수 - 스텔스 모드 (백업된 원본)
    # Args:
    #     batch_data: 처리할 데이터 배치
    #     worker_id: 워커 ID  
    #     api_key: Gemini API 키 (선택사항)
    # Returns:
    #     List[Dict]: 처리된 결과 리스트
    # (기존 구현은 원본 데이터 컬럼 정보가 손실되는 문제가 있어 새로운 버전으로 대체)
"""

def process_batch_worker(batch_data: List[Dict], worker_id: int, api_key: str = None) -> List[Dict]:
    """
    배치 데이터 처리하는 워커 함수 - 행 추적 및 원본 데이터 보존 버전
    
    Args:
        batch_data: 처리할 데이터 배치 (행 ID 포함)
        worker_id: 워커 ID
        api_key: Gemini API 키 (선택사항)
        
    Returns:
        List[Dict]: 처리된 결과 리스트 (원본 데이터 + 검색 결과)
    """
    try:
        logger = setup_logger(f"stealth_worker_{worker_id}")
        logger.info(f"🛡️ 스텔스 워커 {worker_id} 시작: {len(batch_data)}개 데이터 처리")
        
        # StealthWebDriverManager를 사용한 스텔스 드라이버 생성
        stealth_manager = StealthWebDriverManager(logger)
        driver = stealth_manager.create_stealth_driver(worker_id)
        
        if not driver:
            logger.error(f"❌ 스텔스 워커 {worker_id}: 드라이버 생성 실패")
            return []
        
        # AI 모델 초기화 (사용 가능한 경우)
        ai_model = None
        if api_key:
            try:
                genai.configure(api_key=api_key)
                ai_model = genai.GenerativeModel('gemini-2.0-flash-lite-001')
                logger.info(f"🤖 워커 {worker_id}: AI 모델 초기화 성공")
            except Exception as e:
                logger.warning(f"⚠️ 워커 {worker_id}: AI 모델 초기화 실패 - {e}")
        
        # 🎯 고급 검색 패턴 정의 (다양한 검색 전략)
        search_patterns = {
            'phone': [
                '"{phone_number}"',
                '{phone_number} 전화번호',
                '{phone_number} 연락처',
                '{phone_number} 기관',
                '전화 {phone_number}',
                '{phone_number} 대표번호',
                '{phone_number} 문의처',
                '{phone_number} 사무실',
                '연락처 {phone_number}',
                '{phone_number} 공식',
                '{phone_number} site:kr',
                '{phone_number} 관공서',
                '{phone_number} 센터'
            ],
            'fax': [
                '"{fax_number}"',
                '{fax_number} 팩스',
                '{fax_number} 팩스번호',
                '{fax_number} 기관',
                '팩스 {fax_number}',
                '{fax_number} FAX',
                '{fax_number} 전송',
                '{fax_number} 사무실',
                'FAX {fax_number}',
                '{fax_number} site:kr',
                '{fax_number} 관공서',
                '{fax_number} 센터',
                '{fax_number} 공식'
            ]
        }
        
        # 🏢 확장된 기관명 추출 패턴 (더 정확한 기관 인식)
        institution_patterns = [
            # 행정기관
            r'([가-힣]+(?:동|구|시|군|읍|면)\s*(?:주민센터|행정복지센터|사무소|동사무소))',
            r'([가-힣]+(?:구청|시청|군청|도청|청사))',
            r'([가-힣]+(?:구|시|군|도)\s*(?:청|청사))',
            
            # 교육기관
            r'([가-힣]+(?:대학교|대학|학교|초등학교|중학교|고등학교|유치원))',
            r'([가-힣]+(?:교육청|교육지원청|교육지원센터))',
            
            # 의료기관
            r'([가-힣]+(?:병원|의료원|보건소|의원|클리닉|한의원))',
            r'([가-힣]+(?:보건|의료)\s*(?:센터|소))',
            
            # 복지/문화시설
            r'([가-힣]+(?:복지관|문화센터|도서관|체육관|체육센터|수영장))',
            r'([가-힣]+(?:복지|문화|체육|여성|청소년)\s*(?:센터|관))',
            
            # 공공기관
            r'([가-힣]+(?:협회|단체|재단|법인|조합|공사|공단|공기업))',
            r'([가-힣]+(?:관리사무소|관리소|관리공단))',
            
            # 일반 패턴 (더 유연한 매칭)
            r'([가-힣\s]{2,25}(?:주민센터|행정복지센터|사무소|청|병원|학교|센터|관|소))',
            r'([가-힣\s]{3,20}(?:대학교|대학|공사|공단|재단|법인))',
            
            # 특수 기관
            r'([가-힣]+(?:경찰서|소방서|우체국|세무서|법원|검찰청))',
            r'([가-힣]+(?:상공회의소|상공회|농협|수협|신협))'
        ]
        
        results = []
        
        for idx, row_data in enumerate(batch_data):
            try:
                # 🎯 행 추적 정보 추출
                row_id = row_data.get('고유_행ID', f'UNKNOWN_{idx}')
                original_row_num = row_data.get('원본_행번호', idx)
                
                phone_number = row_data.get('전화번호', '')
                fax_number = row_data.get('팩스번호', '')
                
                # 정규화
                normalized_phone = normalize_phone_number(phone_number) if phone_number and phone_number != 'nan' else ''
                normalized_fax = normalize_phone_number(fax_number) if fax_number and fax_number != 'nan' else ''
                
                logger.info(f"📞 워커 {worker_id} 처리 중 ({idx+1}/{len(batch_data)}) [행ID:{row_id}]: 전화({normalized_phone}), 팩스({normalized_fax})")
                
                # 전화번호 기관 검색
                phone_institution = ''
                if normalized_phone:
                    phone_institution = search_multiple_engines_for_institution(
                        driver, normalized_phone, 'phone', search_patterns, 
                        institution_patterns, ai_model, logger
                    )
                
                # 팩스번호 기관 검색
                fax_institution = ''
                if normalized_fax:
                    fax_institution = search_multiple_engines_for_institution(
                        driver, normalized_fax, 'fax', search_patterns, 
                        institution_patterns, ai_model, logger
                    )
                
                # 🔄 결과 저장 - 원본 데이터 전체 보존 + 검색 결과 추가
                result = row_data.copy()  # 원본 데이터 전체 복사
                
                # 검색 결과 컬럼 추가/업데이트
                result.update({
                    '전화번호_정규화': normalized_phone,
                    '팩스번호_정규화': normalized_fax,
                    '전화번호_검색기관': phone_institution if phone_institution else '미발견',
                    '팩스번호_검색기관': fax_institution if fax_institution else '미발견',
                    '처리워커': f"워커_{worker_id}",
                    '처리시간': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    '검색상태': '완료'
                })
                
                results.append(result)
                
                # 🛡️ 스텔스 요청 지연 (인간 행동 패턴 시뮬레이션)
                stealth_delay = random.uniform(4, 7)  # 더 긴 지연으로 봇 감지 회피
                time.sleep(stealth_delay)
                
            except Exception as e:
                logger.error(f"❌ 워커 {worker_id} 행 처리 실패 {idx}: {e}")
                continue
        
        # 정리 - 스텔스 매니저 사용
        stealth_manager.cleanup_driver(driver, worker_id)
        
        logger.info(f"✅ 스텔스 워커 {worker_id} 완료: {len(results)}개 결과")
        return results
        
    except Exception as e:
        logger.error(f"❌ 스텔스 워커 {worker_id} 전체 실패: {e}")
        traceback.print_exc()
        return []

def normalize_phone_number(phone_number: str) -> str:
    """전화번호 정규화하는 메소드"""
    if pd.isna(phone_number) or phone_number == '':
        return ''
    
    # 숫자만 추출
    numbers = re.findall(r'\d+', str(phone_number))
    if len(numbers) >= 3:
        return f"{numbers[0]}-{numbers[1]}-{numbers[2]}"
    elif len(numbers) == 2:
        return f"{numbers[0]}-{numbers[1]}"
    else:
        return str(phone_number)

def search_multiple_engines_for_institution(driver, number: str, number_type: str, search_patterns: Dict, 
                                          institution_patterns: List, ai_model, logger) -> Optional[str]:
    """다중 검색 엔진으로 전화번호/팩스번호 기관 검색 - Google, Naver, Daum"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.keys import Keys
        
        # 🌏 검색 엔진 목록 (한국 특화 우선)
        search_engines = [
            {
                'name': 'Naver',
                'url': 'https://search.naver.com/search.naver',
                'search_box_selector': 'input#query',
                'search_box_name': 'query',
                'results_selector': '.lst_total',
                'delay': (2.0, 3.5)
            },
            {
                'name': 'Daum',
                'url': 'https://search.daum.net/search',
                'search_box_selector': 'input#q',
                'search_box_name': 'q',
                'results_selector': '.inner_search',
                'delay': (1.5, 3.0)
            },
            {
                'name': 'Google',
                'url': 'https://www.google.com',
                'search_box_selector': 'input[name="q"]',
                'search_box_name': 'q',
                'results_selector': '#search',
                'delay': (2.5, 4.0)
            }
        ]
        
        patterns = search_patterns.get(number_type, [])
        
        # 각 검색 엔진별로 시도
        for engine in search_engines:
            logger.info(f"🔍 {engine['name']} 검색 시작: {number} ({number_type})")
            
            try:
                # 🎯 한국 검색 엔진에 특화된 검색어 사용
                if engine['name'] in ['Naver', 'Daum']:
                    korean_patterns = [
                        f'"{number}" 전화번호',
                        f'"{number}" 기관',
                        f'"{number}" 연락처',
                        f'{number} 어디',
                        f'{number} 어느곳',
                        f'{number} 기관명'
                    ]
                    search_patterns_list = korean_patterns if number_type == 'phone' else [p.replace('전화번호', '팩스번호') for p in korean_patterns]
                else:
                    search_patterns_list = patterns[:3]  # Google은 기존 패턴 사용
                
                # 여러 검색 패턴 시도
                for pattern in search_patterns_list:
                    if number_type == 'phone':
                        search_query = pattern.format(phone_number=number) if '{phone_number}' in pattern else pattern
                    else:  # fax
                        search_query = pattern.format(fax_number=number) if '{fax_number}' in pattern else pattern
                    
                    logger.info(f"🔎 {engine['name']} 패턴 검색: {search_query}")
                    
                    # 인간형 검색 실행
                    result = _perform_human_like_search(driver, engine, search_query, institution_patterns, logger)
                    
                    if result:
                        logger.info(f"✅ {engine['name']}에서 기관명 발견: {result}")
                        return result
                    
                    # 패턴 간 지연
                    pattern_delay = random.uniform(2.0, 4.0)
                    time.sleep(pattern_delay)
                
            except Exception as engine_error:
                logger.warning(f"⚠️ {engine['name']} 검색 실패: {engine_error}")
                continue
            
            # 엔진 간 지연 (봇 감지 회피)
            engine_delay = random.uniform(3.0, 6.0)
            time.sleep(engine_delay)
        
        # AI 모델 최종 시도 (모든 검색 엔진 실패시)
        if ai_model:
            logger.info("🤖 AI 모델 최종 시도")
            return _ai_fallback_search(number, number_type, ai_model, logger)
        
        return None
        
    except Exception as e:
        logger.error(f"❌ 다중 검색 엔진 검색 실패: {number} ({number_type}) - {e}")
        return None

def _perform_human_like_search(driver, engine_config: Dict, search_query: str, 
                              institution_patterns: List, logger) -> Optional[str]:
    """인간형 검색 수행 (다중 엔진 지원 + HTTP 클라이언트 호환)"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.keys import Keys
        from bs4 import BeautifulSoup
        import urllib.parse
        
        # 🔍 HTTP 클라이언트 감지 (hasattr로 확인)
        is_http_client = hasattr(driver, 'session') and hasattr(driver, 'page_source')
        
        if is_http_client:
            logger.info(f"🌍 HTTP 클라이언트로 {engine_config['name']} 검색 수행")
            return _perform_http_search(driver, engine_config, search_query, institution_patterns, logger)
        
        # 🌐 일반 브라우저 검색 (기존 방식)
        if engine_config['name'] == 'Naver':
            driver.get('https://www.naver.com')
        elif engine_config['name'] == 'Daum':  
            driver.get('https://www.daum.net')
        else:  # Google
            driver.get('https://www.google.com')
        
        # 페이지 로드 대기
        time.sleep(random.uniform(1.5, 3.0))
        
        # 💭 인간처럼 생각하는 시간
        thinking_delay = random.uniform(0.8, 2.0)
        time.sleep(thinking_delay)
        
        # 🔍 검색창 찾기 (엔진별 선택자 사용)
        try:
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, engine_config['search_box_selector']))
            )
        except:
            # 대체 방법: name 속성 사용
            search_box = driver.find_element(By.NAME, engine_config['search_box_name'])
        
        # 🧹 검색창 비우기
        search_box.clear()
        time.sleep(random.uniform(0.3, 0.7))
        
        # ⌨️ 인간처럼 한 글자씩 타이핑
        for char in search_query:
            search_box.send_keys(char)
            typing_delay = random.uniform(0.05, 0.15)
            time.sleep(typing_delay)
        
        # 💭 타이핑 완료 후 검토 시간
        review_delay = random.uniform(0.5, 1.2)
        time.sleep(review_delay)
        
        # 🔍 검색 실행
        search_box.send_keys(Keys.RETURN)
        
        # 🔄 결과 페이지 대기
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, engine_config['results_selector']))
        )
        
        # 🎯 결과 확인 시간
        result_delay = random.uniform(*engine_config['delay'])
        time.sleep(result_delay)
        
        # 📜 가끔 스크롤 (인간 행동 시뮬레이션)
        if random.choice([True, False]):
            scroll_amount = random.randint(200, 600)
            driver.execute_script(f"window.scrollTo(0, {scroll_amount});")
            time.sleep(random.uniform(1.0, 2.0))
        
        # 페이지 분석
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # 🤖 봇 감지 확인
        page_text = soup.get_text().lower()
        if any(keyword in page_text for keyword in ['captcha', 'unusual traffic', 'bot', '비정상적인 요청', '자동화']):
            logger.warning(f"🤖 {engine_config['name']}에서 봇 감지 가능성")
            time.sleep(random.uniform(10.0, 20.0))
            return None
        
        # 기관명 추출
        return extract_institution_from_page(soup, search_query, institution_patterns, None, logger)
        
    except Exception as e:
        logger.warning(f"⚠️ {engine_config['name']} 인간형 검색 실패: {search_query} - {e}")
        return None

def _perform_http_search(http_client, engine_config: Dict, search_query: str, 
                        institution_patterns: List, logger) -> Optional[str]:
    """HTTP 클라이언트 전용 검색"""
    try:
        import urllib.parse
        from bs4 import BeautifulSoup
        
        # 🌏 검색 엔진별 URL 구성
        encoded_query = urllib.parse.quote(search_query)
        
        if engine_config['name'] == 'Naver':
            search_url = f"https://search.naver.com/search.naver?query={encoded_query}"
        elif engine_config['name'] == 'Daum':
            search_url = f"https://search.daum.net/search?q={encoded_query}"
        else:  # Google
            search_url = f"https://www.google.com/search?q={encoded_query}"
        
        logger.info(f"🌍 HTTP 요청: {search_url}")
        
        # 🔍 HTTP 요청 실행
        success = http_client.get(search_url)
        if not success:
            logger.warning(f"⚠️ HTTP 요청 실패: {search_url}")
            return None
        
        # 🎯 인간 행동 시뮬레이션 지연
        human_delay = random.uniform(*engine_config['delay'])
        time.sleep(human_delay)
        
        # 📄 응답 분석
        soup = BeautifulSoup(http_client.page_source, 'html.parser')
        
        # 🤖 봇 감지 확인
        page_text = soup.get_text().lower()
        if any(keyword in page_text for keyword in ['captcha', 'unusual traffic', 'bot', '비정상적인 요청', '자동화']):
            logger.warning(f"🤖 HTTP {engine_config['name']}에서 봇 감지 가능성")
            return None
        
        # 기관명 추출
        return extract_institution_from_page(soup, search_query, institution_patterns, None, logger)
        
    except Exception as e:
        logger.warning(f"⚠️ HTTP {engine_config['name']} 검색 실패: {search_query} - {e}")
        return None

def _ai_fallback_search(number: str, number_type: str, ai_model, logger) -> Optional[str]:
    """AI 모델 기반 최종 검색"""
    try:
        prompt = f"""
한국의 {number_type}번호 '{number}'와 관련된 기관명을 추론해주세요.

다음과 같은 패턴을 고려해주세요:
- 지역번호 기반 추론 (예: 02는 서울, 031은 경기 등)
- 일반적인 기관 전화번호 패턴
- 공공기관, 의료기관, 교육기관, 복지시설 등

기관명만 간단히 답변해주세요. 확실하지 않으면 '미확인'이라고 답변해주세요.
"""
        
        response = ai_model.generate_content(prompt)
        result = response.text.strip()
        
        if result and result != '미확인' and len(result) > 2:
            logger.info(f"🤖 AI 추론 결과: {result}")
            return result
        
        return None
        
    except Exception as e:
        logger.error(f"❌ AI 최종 검색 실패: {e}")
        return None

def extract_institution_from_page(soup: BeautifulSoup, number: str, institution_patterns: List, 
                                 ai_model, logger) -> Optional[str]:
    """검색 결과 페이지에서 기관명 추출하는 메소드"""
    try:
        # 페이지 텍스트 가져오기
        page_text = soup.get_text()
        
        # 정규식 패턴으로 기관명 찾기
        for pattern in institution_patterns:
            matches = re.findall(pattern, page_text)
            if matches:
                # 가장 적절한 매치 선택
                for match in matches:
                    if is_valid_institution_name(match):
                        return match.strip()
        
        # AI 모델 사용 (사용 가능한 경우)
        if ai_model:
            return extract_with_ai(page_text, number, ai_model, logger)
        
        return None
        
    except Exception as e:
        logger.error(f"❌ 기관명 추출 실패: {e}")
        return None

def is_valid_institution_name(name: str) -> bool:
    """유효한 기관명인지 확인하는 메소드 - 확장 버전"""
    if not name or len(name) < 2:
        return False
    
    # 🏛️ 확장된 유효한 기관명 키워드
    valid_keywords = [
        # 행정기관
        '주민센터', '행정복지센터', '사무소', '동사무소', '청', '구청', '시청', '군청', '도청', '청사',
        
        # 교육기관  
        '학교', '초등학교', '중학교', '고등학교', '대학', '대학교', '유치원', '교육청', '교육지원청', '교육지원센터',
        
        # 의료기관
        '병원', '의료원', '보건소', '의원', '클리닉', '한의원', '보건센터', '의료센터',
        
        # 복지/문화시설
        '센터', '복지관', '도서관', '체육관', '체육센터', '수영장', '문화센터', '여성센터', '청소년센터',
        
        # 공공기관
        '협회', '단체', '재단', '법인', '조합', '공사', '공단', '공기업', '관리사무소', '관리소', '관리공단',
        
        # 특수기관
        '경찰서', '소방서', '우체국', '세무서', '법원', '검찰청', '상공회의소', '상공회', '농협', '수협', '신협'
    ]
    
    # ❌ 제외할 키워드 (잘못된 인식 방지)
    invalid_keywords = [
        '번호', '전화', '팩스', 'fax', '연락처', '문의', '검색', '결과', '사이트', 'site',
        '홈페이지', 'www', 'http', 'com', 'co.kr', '광고', '상품', '서비스'
    ]
    
    # 제외 키워드 확인
    name_lower = name.lower()
    if any(invalid in name_lower for invalid in invalid_keywords):
        return False
    
    return any(keyword in name for keyword in valid_keywords)

def extract_with_ai(page_text: str, number: str, ai_model, logger) -> Optional[str]:
    """AI 모델로 기관명 추출하는 메소드"""
    try:
        # 텍스트 길이 제한 (토큰 제한 고려)
        limited_text = page_text[:3000]
        
        prompt = f"""
다음 텍스트에서 번호 '{number}'와 관련된 기관명을 찾아주세요.
기관명은 주민센터, 사무소, 구청, 시청, 병원, 학교, 센터 등이 포함된 공공기관이나 단체명입니다.

텍스트:
{limited_text}

기관명만 정확히 추출해서 답변해주세요. 없으면 '없음'이라고 답변해주세요.
"""
        
        response = ai_model.generate_content(prompt)
        result = response.text.strip()
        
        if result and result != '없음' and is_valid_institution_name(result):
            return result
        
        return None
        
    except Exception as e:
        logger.error(f"❌ AI 추출 실패: {e}")
        return None

class ParallelPhoneFaxFinder:
    """병렬 처리 전화번호/팩스번호 기관 찾기 클래스"""
    
    def __init__(self):
        """병렬 전화번호/팩스번호 기관 찾기 시스템 초기화하는 메소드"""
        self.logger = setup_logger()
        self.system_monitor = SystemMonitor(self.logger)
        
        # 환경 변수 로드
        load_dotenv()
        
        # 병렬 처리 설정 (AMD Ryzen 5 5500U 환경 최적화)
        self.max_workers = 10  # 6코어 12스레드 활용
        self.batch_size = 350   # 워커당 처리할 데이터 수
        
        # 통계
        self.total_processed = 0
        self.phone_success_count = 0
        self.fax_success_count = 0
        
        self.logger.info("🛡️ 스텔스 병렬 전화번호/팩스번호 기관 찾기 시스템 초기화 완료")
        self.logger.info(f"🚀 AMD Ryzen 5 5500U 최적화: {self.max_workers}개 워커, 배치 크기: {self.batch_size}")
        self.logger.info("🔥 고급 봇 감지 우회 기능 활성화")
    
    def load_excel_data(self, file_path: str) -> pd.DataFrame:
        """엑셀 데이터 로드하는 메소드"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
            
            # 엑셀 파일 로드
            df = pd.read_excel(file_path)
            
            self.logger.info(f"📊 데이터 로드 완료: {len(df)}행")
            self.logger.info(f"📋 컬럼: {list(df.columns)}")
            
            # 전화번호나 팩스번호가 있는 행만 필터링
            phone_column = '전화번호'
            fax_column = '팩스번호'
            
            # 두 컬럼 중 하나라도 값이 있는 행 선택
            condition = (
                (df[phone_column].notna() & (df[phone_column] != '')) |
                (df[fax_column].notna() & (df[fax_column] != ''))
            )
            
            df_filtered = df[condition]
            
            phone_count = df_filtered[df_filtered[phone_column].notna() & (df_filtered[phone_column] != '')].shape[0]
            fax_count = df_filtered[df_filtered[fax_column].notna() & (df_filtered[fax_column] != '')].shape[0]
            
            self.logger.info(f"📞 전화번호가 있는 행: {phone_count}개")
            self.logger.info(f"📠 팩스번호가 있는 행: {fax_count}개")
            self.logger.info(f"🎯 처리 대상: {len(df_filtered)}행")
            
            return df_filtered
                
        except Exception as e:
            self.logger.error(f"❌ 엑셀 데이터 로드 실패: {e}")
            return pd.DataFrame()
    
    def split_data_into_batches(self, df: pd.DataFrame) -> List[List[Dict]]:
        """데이터를 배치로 분할하는 메소드 - 행 추적 시스템 포함"""
        try:
            # 🎯 행 추적을 위해 인덱스 리셋 및 고유 ID 추가
            df_with_index = df.reset_index(drop=True)
            df_with_index['원본_행번호'] = df_with_index.index
            df_with_index['고유_행ID'] = df_with_index['원본_행번호'].apply(lambda x: f"ROW_{x:06d}")
            
            # DataFrame을 딕셔너리 리스트로 변환 (행 정보 포함)
            data_list = df_with_index.to_dict('records')
            
            self.logger.info(f"📋 행 추적 시스템 적용: {len(data_list)}개 행에 고유 ID 부여")
            
            # 배치로 분할
            batches = []
            for i in range(0, len(data_list), self.batch_size):
                batch = data_list[i:i + self.batch_size]
                batches.append(batch)
            
            self.logger.info(f"📦 데이터 분할 완료: {len(batches)}개 배치")
            for i, batch in enumerate(batches):
                batch_row_ids = [row['고유_행ID'] for row in batch[:3]]  # 처음 3개만 표시
                if len(batch) > 3:
                    batch_row_ids.append(f"... 외 {len(batch)-3}개")
                self.logger.info(f"   배치 {i+1}: {len(batch)}개 데이터 [{', '.join(batch_row_ids)}]")
            
            return batches
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 분할 실패: {e}")
            return []
    
    def process_parallel(self, df: pd.DataFrame) -> List[Dict]:
        """병렬 처리 실행하는 메소드"""
        try:
            self.logger.info("🚀 병렬 처리 시작!")
            
            # 시스템 모니터링 시작
            self.system_monitor.start_monitoring()
            
            # 데이터 배치 분할
            batches = self.split_data_into_batches(df)
            
            if not batches:
                return []
            
            # API 키 가져오기
            api_key = os.getenv('GEMINI_API_KEY')
            
            all_results = []
            
            # ProcessPoolExecutor를 사용한 병렬 처리
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # 각 배치를 워커에 할당
                future_to_worker = {}
                for worker_id, batch in enumerate(batches[:self.max_workers]):
                    future = executor.submit(process_batch_worker, batch, worker_id, api_key)
                    future_to_worker[future] = worker_id
                
                # 남은 배치들 처리
                remaining_batches = batches[self.max_workers:]
                next_worker_id = self.max_workers
                
                # 완료된 작업 처리
                for future in as_completed(future_to_worker):
                    worker_id = future_to_worker[future]
                    
                    try:
                        result = future.result()
                        all_results.extend(result)
                        
                        self.logger.info(f"✅ 워커 {worker_id} 완료: {len(result)}개 결과")
                        
                        # 남은 배치가 있으면 새로운 작업 시작
                        if remaining_batches:
                            next_batch = remaining_batches.pop(0)
                            new_future = executor.submit(process_batch_worker, next_batch, next_worker_id, api_key)
                            future_to_worker[new_future] = next_worker_id
                            next_worker_id += 1
                        
                    except Exception as e:
                        self.logger.error(f"❌ 워커 {worker_id} 오류: {e}")
                        continue
            
            # 시스템 모니터링 중지
            self.system_monitor.stop_monitoring()
            
            self.logger.info(f"🎉 병렬 처리 완료: {len(all_results)}개 총 결과")
            
            return all_results
            
        except Exception as e:
            self.logger.error(f"❌ 병렬 처리 실패: {e}")
            return []
    
    def save_results_to_desktop(self, results: List[Dict]) -> str:
        """결과를 데스크톱에 저장하는 메소드 - 원본 데이터 + 검색 결과 통합 버전"""
        try:
            # rawdatafile 폴더에 저장 (기존 데이터와 함께 관리)
            save_directory = "rawdatafile"
            if not os.path.exists(save_directory):
                os.makedirs(save_directory)
            
            # 파일명 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"통합_전화팩스기관검색결과_{timestamp}.xlsx"
            filepath = os.path.join(save_directory, filename)
            
            # DataFrame 생성
            df_results = pd.DataFrame(results)
            
            # 🎯 컬럼 순서 정리 (가독성 향상)
            if not df_results.empty:
                # 중요 컬럼들을 앞으로 배치
                priority_columns = [
                    '고유_행ID', '원본_행번호', '기관명', '주소', 
                    '전화번호', '전화번호_정규화', '전화번호_검색기관',
                    '팩스번호', '팩스번호_정규화', '팩스번호_검색기관',
                    '처리워커', '처리시간', '검색상태'
                ]
                
                # 존재하는 컬럼만 선택
                existing_priority = [col for col in priority_columns if col in df_results.columns]
                remaining_columns = [col for col in df_results.columns if col not in existing_priority]
                
                # 컬럼 순서 재정렬
                df_results = df_results[existing_priority + remaining_columns]
            
            # 🔄 다중 시트로 저장 (결과 + 통계)
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # 메인 결과 저장
                df_results.to_excel(writer, index=False, sheet_name='통합검색결과')
                
                # 📊 통계 시트 생성
                self._create_statistics_sheet(writer, df_results)
            
            self.logger.info(f"💾 결과 저장 완료: {filepath}")
            
            # 🎯 개선된 통계 정보 (새로운 컬럼 구조에 맞춤)
            total_processed = len(results)
            
            # 전화번호 검색 성공률
            phone_successful = len([r for r in results if r.get('전화번호_검색기관', '미발견') != '미발견'])
            phone_total = len([r for r in results if r.get('전화번호_정규화', '')])
            
            # 팩스번호 검색 성공률  
            fax_successful = len([r for r in results if r.get('팩스번호_검색기관', '미발견') != '미발견'])
            fax_total = len([r for r in results if r.get('팩스번호_정규화', '')])
            
            phone_rate = (phone_successful / phone_total) * 100 if phone_total > 0 else 0
            fax_rate = (fax_successful / fax_total) * 100 if fax_total > 0 else 0
            
            self.logger.info(f"📊 최종 처리 통계:")
            self.logger.info(f"   - 총 처리: {total_processed}개 행")
            self.logger.info(f"   - 전화번호 대상: {phone_total}개, 성공: {phone_successful}개 ({phone_rate:.1f}%)")
            self.logger.info(f"   - 팩스번호 대상: {fax_total}개, 성공: {fax_successful}개 ({fax_rate:.1f}%)")
            self.logger.info(f"   - 전체 성공률: {((phone_successful + fax_successful) / (phone_total + fax_total) * 100):.1f}%" if (phone_total + fax_total) > 0 else "   - 전체 성공률: 0.0%")
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            return ""
    
    def _create_statistics_sheet(self, writer, df_results: pd.DataFrame):
        """통계 시트 생성하는 보조 메소드"""
        try:
            # 📊 통계 데이터 준비
            stats_data = []
            
            total_rows = len(df_results)
            phone_total = len(df_results[df_results['전화번호_정규화'].notna() & (df_results['전화번호_정규화'] != '')])
            fax_total = len(df_results[df_results['팩스번호_정규화'].notna() & (df_results['팩스번호_정규화'] != '')])
            
            phone_success = len(df_results[df_results['전화번호_검색기관'] != '미발견'])
            fax_success = len(df_results[df_results['팩스번호_검색기관'] != '미발견'])
            
            stats_data.extend([
                ['구분', '총 개수', '성공 개수', '성공률(%)'],
                ['전체 행', total_rows, phone_success + fax_success, f"{((phone_success + fax_success) / (phone_total + fax_total) * 100):.1f}" if (phone_total + fax_total) > 0 else "0.0"],
                ['전화번호', phone_total, phone_success, f"{(phone_success / phone_total * 100):.1f}" if phone_total > 0 else "0.0"],
                ['팩스번호', fax_total, fax_success, f"{(fax_success / fax_total * 100):.1f}" if fax_total > 0 else "0.0"],
                ['', '', '', ''],
                ['처리 정보', '', '', ''],
                ['처리 시작 시간', df_results['처리시간'].min() if '처리시간' in df_results.columns else 'N/A', '', ''],
                ['처리 완료 시간', df_results['처리시간'].max() if '처리시간' in df_results.columns else 'N/A', '', ''],
                ['사용된 워커 수', len(df_results['처리워커'].unique()) if '처리워커' in df_results.columns else 'N/A', '', '']
            ])
            
            # 통계 DataFrame 생성 및 저장
            df_stats = pd.DataFrame(stats_data)
            df_stats.to_excel(writer, index=False, header=False, sheet_name='처리통계')
            
        except Exception as e:
            self.logger.warning(f"⚠️ 통계 시트 생성 실패: {e}")
    
    def run(self, excel_path: str) -> bool:
        """전체 병렬 프로세스 실행하는 메소드"""
        try:
            self.logger.info("🚀 병렬 전화번호/팩스번호 기관 찾기 시작!")
            
            # 1. 데이터 로드
            df = self.load_excel_data(excel_path)
            if df.empty:
                return False
            
            # 2. 병렬 처리 실행
            results = self.process_parallel(df)
            
            if not results:
                self.logger.error("❌ 처리된 결과가 없습니다")
                return False
            
            # 3. 결과 저장
            output_path = self.save_results_to_desktop(results)
            
            if output_path:
                self.logger.info(f"✅ 완료! 결과 파일: {output_path}")
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 실행 중 오류: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        # 파일 경로
        excel_path = os.path.join("rawdatafile", "failed_data_250715.xlsx")
        
        # 병렬 전화번호/팩스번호 기관 찾기 실행
        finder = ParallelPhoneFaxFinder()
        success = finder.run(excel_path)
        
        if success:
            print("🎉 병렬 전화번호/팩스번호 기관명 검색이 완료되었습니다!")
        else:
            print("❌ 처리 중 오류가 발생했습니다.")
            
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")

if __name__ == "__main__":
    # Windows의 multiprocessing 이슈 해결
    multiprocessing.freeze_support()
    main() 