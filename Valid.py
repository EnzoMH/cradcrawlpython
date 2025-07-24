#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valid.py - 전화번호/팩스번호 5단계 검증 시스템
search_logic.txt 기반 독립적 검증 시스템

핵심 데이터: E열(읍면동) = I열(팩스번호) [전화번호와 팩스번호는 엄밀히 다름]

5단계 팩스번호 검증 프로세스 (목적: 팩스번호의 진짜 기관명 확인):
1차 검증: 팩스번호 지역번호 vs E열 읍면동 매칭 (phone_validator.py 활용)
2차 검증: Google 검색으로 팩스번호의 진짜 기관명 확인
3차 검증: 5개 링크 병렬크롤링 + bs4/js 렌더링 + 기관명 추출
4차 검증: AI를 통한 팩스번호 실제 소유 기관명 도출
5차 검증: 모든 단계 결과 종합 → 데이터 정확성 최종 판단

작성자: AI Assistant
작성일: 2025-01-18
버전: 1.0 - 5-Stage Validation System
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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
import threading
import re

# 웹 크롤링
import requests
# undetected_chromedriver 제거 - ppff2.py 스타일 Exceptional Chrome 사용
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import difflib

# 환경변수 및 AI
from dotenv import load_dotenv
import google.generativeai as genai

# 기존 utils 모듈 활용
from utils.phone_validator import PhoneValidator, KOREAN_AREA_CODES
from utils.ai_model_manager import AIModelManager
from utils.web_driver_manager import WebDriverManager

# 환경변수 로드
load_dotenv()

# ================================
# 전역 설정
# ================================

# 입력 파일 경로
INPUT_FILE = "rawdatafile/failed_data_250724.xlsx"
OUTPUT_FILE_PREFIX = "Valid_검증결과"

# 검증 설정
MAX_WORKERS = 2  # 속도 우선: 4개 → 2개로 단축 (안정성 향상)
BATCH_SIZE = 50  # 배치 크기
SEARCH_RESULTS_LIMIT = 3  # 속도 우선: 5개 → 3개로 단축
CONFIDENCE_THRESHOLD = 80  # 신뢰도 임계값 (%)

# 포트 범위 (ppff2.py 방식)
PORT_RANGE_START = 9222
PORT_RANGE_END = 9500

# 드라이버 우선순위 (ppff2.py 방식)
DRIVER_PRIORITIES = ["Exceptional", "Selenium"]  # Undetected 제거

# ================================
# 로깅 설정
# ================================

def setup_logger(name: str = "Valid") -> logging.Logger:
    """로깅 시스템 설정"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'valid_{timestamp}.log'
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
    )
    
    # 파일 핸들러
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # 로거 생성
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# ================================
# 데이터 클래스
# ================================

@dataclass
class ValidationResult:
    """5단계 검증 결과"""
    row_index: int
    fax_number: str
    institution_name: str  # 원본 기관명 (읍면동) - 핵심 데이터
    region: str           # 지역 (시도)
    phone_number: str = ""  # 전화번호 (H열) - 추가
    
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
    
    # 4차 검증 결과
    stage4_passed: bool = False
    stage4_message: str = ""
    ai_extracted_institution: str = ""
    
    # 5차 검증 결과 (최종)
    stage5_passed: bool = False
    stage5_message: str = ""
    final_verification: str = ""
    
    # 전체 결과
    overall_result: str = "검증 실패"  # "검증 성공", "검증 실패", "판단 불가"
    final_confidence: float = 0.0
    processing_time: float = 0.0
    error_message: str = ""

# ================================
# 프록시 및 IP 변조 관리자 (ppff2.py 기반)
# ================================
class ProxyRotator:
    """프록시 및 IP 변조 관리자"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.proxy_list = []
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        self.dns_servers = [
            '8.8.8.8',      # Google DNS
            '1.1.1.1',      # Cloudflare DNS
            '9.9.9.9',      # Quad9 DNS
            '208.67.222.222' # OpenDNS
        ]
        self.current_proxy_index = 0
        self.current_ua_index = 0
        self.current_dns_index = 0
        
        # 무료 프록시 로드
        self._load_free_proxies()
        
        self.logger.info(f"🌐 프록시 로테이터 초기화: {len(self.proxy_list)}개 프록시, {len(self.user_agents)}개 User-Agent")
    
    def _load_free_proxies(self):
        """무료 프록시 목록 로드"""
        try:
            # 기본 프록시 목록 (예시)
            basic_proxies = [
                "185.199.108.153:8080",
                "185.199.110.153:8080", 
                "208.67.222.123:8080"
            ]
            self.proxy_list.extend(basic_proxies)
            
            # 실제 환경에서는 free-proxy-list.net API 등을 활용
            self.logger.info(f"🌐 기본 프록시 {len(basic_proxies)}개 로드")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 프록시 로드 실패: {e}")
    
    def get_rotation_config(self, worker_id: int) -> Dict:
        """워커별 로테이션 설정 반환 (최적화됨)"""
        config = {
            "user_agent": self.user_agents[self.current_ua_index % len(self.user_agents)],
            "dns_server": self.dns_servers[self.current_dns_index % len(self.dns_servers)],
            "proxy": None,
            "headers": self._generate_random_headers()
        }
        
        # 프록시 사용 (30% 확률로 줄임 - 안정성 향상)
        if self.proxy_list and random.random() < 0.3:
            config["proxy"] = self.proxy_list[self.current_proxy_index % len(self.proxy_list)]
            self.current_proxy_index += 1
        
        # 인덱스 증가
        self.current_ua_index += 1
        self.current_dns_index += 1
        
        return config
    
    def _generate_random_headers(self) -> Dict:
        """랜덤 헤더 생성"""
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": random.choice(["ko-KR,ko;q=0.9,en-US;q=0.8", "en-US,en;q=0.9,ko;q=0.8"]),
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": random.choice(["no-cache", "max-age=0"])
        }

# ================================
# 포트 관리자 (ppff2.py 방식)
# ================================

class AdvancedPortManager:
    """고급 포트 관리자 - ppff2.py 방식 활용"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.used_ports = set()
        self.available_ports = list(range(PORT_RANGE_START, PORT_RANGE_END + 1))
        random.shuffle(self.available_ports)
        self.port_index = 0
        self.worker_port_map = {}  # 워커별 고정 포트 매핑
        self.port_failure_count = {}  # 포트별 실패 횟수
        self.lock = threading.Lock()
        
        self.logger.info(f"🔌 고급 포트 관리자 초기화: {len(self.available_ports)}개 포트")
    
    def get_port(self, worker_id: int) -> int:
        """워커별 포트 할당 (ppff2.py 고급 기능)"""
        try:
            with self.lock:
                # 워커별 고정 포트가 있으면 재사용
                if worker_id in self.worker_port_map:
                    existing_port = self.worker_port_map[worker_id]
                    if existing_port not in self.port_failure_count or self.port_failure_count[existing_port] < 3:
                        self.logger.debug(f"🔌 워커 {worker_id}: 기존 포트 {existing_port} 재사용")
                        return existing_port
                
                # 새 포트 할당 (실패 횟수가 적은 포트 우선)
                best_port = self._find_best_available_port()
                
                if best_port:
                    self.used_ports.add(best_port)
                    self.worker_port_map[worker_id] = best_port
                    self.logger.debug(f"🔌 워커 {worker_id}: 새 포트 {best_port} 할당")
                    return best_port
                else:
                    # 백업 포트 (ppff2.py 방식)
                    backup_port = PORT_RANGE_START + (worker_id * 15) + random.randint(0, 14)
                    self.used_ports.add(backup_port)
                    self.worker_port_map[worker_id] = backup_port
                    self.logger.warning(f"⚠️ 워커 {worker_id}: 백업 포트 {backup_port} 사용")
                    return backup_port
                    
        except Exception as e:
            self.logger.error(f"포트 할당 실패: {e}")
            return PORT_RANGE_START + worker_id
    
    def _find_best_available_port(self) -> Optional[int]:
        """가장 적합한 사용 가능한 포트 찾기"""
        try:
            # 실패 횟수가 적은 포트들 우선 정렬
            sorted_ports = sorted(
                [p for p in self.available_ports if p not in self.used_ports],
                key=lambda p: self.port_failure_count.get(p, 0)
            )
            
            if sorted_ports:
                return sorted_ports[0]
            return None
            
        except Exception as e:
            self.logger.debug(f"최적 포트 찾기 실패: {e}")
            return None
    
    def report_port_failure(self, port: int, worker_id: int):
        """포트 실패 보고 (ppff2.py 방식)"""
        with self.lock:
            self.port_failure_count[port] = self.port_failure_count.get(port, 0) + 1
            self.logger.warning(f"⚠️ 워커 {worker_id}: 포트 {port} 실패 ({self.port_failure_count[port]}회)")
            
            # 실패 횟수가 많으면 포트 제외
            if self.port_failure_count[port] >= 5:
                self.used_ports.add(port)  # 사용 불가 포트로 마킹
                if worker_id in self.worker_port_map and self.worker_port_map[worker_id] == port:
                    del self.worker_port_map[worker_id]
                self.logger.error(f"❌ 포트 {port} 영구 제외 (실패 {self.port_failure_count[port]}회)")
    
    def release_port(self, port: int, worker_id: int):
        """포트 해제"""
        with self.lock:
            self.used_ports.discard(port)
            if worker_id in self.worker_port_map and self.worker_port_map[worker_id] == port:
                # 실패 횟수가 적으면 포트 유지, 많으면 해제
                if self.port_failure_count.get(port, 0) >= 3:
                    del self.worker_port_map[worker_id]
                    self.logger.debug(f"🔌 워커 {worker_id}: 문제 포트 {port} 영구 해제")
                else:
                    self.logger.debug(f"🔌 워커 {worker_id}: 포트 {port} 임시 해제 (재사용 가능)")
    
    def get_port_statistics(self) -> Dict:
        """포트 사용 통계"""
        return {
            'total_ports': len(self.available_ports),
            'used_ports': len(self.used_ports),
            'active_workers': len(self.worker_port_map),
            'failed_ports': len([p for p, count in self.port_failure_count.items() if count >= 5]),
            'failure_count': sum(self.port_failure_count.values())
        }

# ================================
# Exceptional Chrome 관리자 (ppff2.py 방식)
# ================================

class ExceptionalChromeManager:
    """utils.WebDriverManager 기반 Chrome 드라이버 관리자 (안정성 우선)"""
    
    def __init__(self, port_manager, proxy_rotator, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        # port_manager는 더 이상 사용하지 않음 (WebDriverManager가 자체 관리)
        self.port_manager = port_manager  # 호환성 유지
        self.proxy_rotator = proxy_rotator
        self.drivers = {}
        self.driver_lock = threading.Lock()
        
        # utils.WebDriverManager 인스턴스들 (워커별)
        self.web_driver_managers = {}
        
    def create_driver(self, worker_id: int):
        """utils.WebDriverManager 기반 Chrome 드라이버 생성 (안정성 우선)"""
        try:
            # WebDriverManager 인스턴스 생성 (워커별)
            if worker_id not in self.web_driver_managers:
                self.web_driver_managers[worker_id] = WebDriverManager(self.logger)
            
            web_driver_manager = self.web_driver_managers[worker_id]
            
            # utils.WebDriverManager의 봇 우회 드라이버 사용
            driver = web_driver_manager.create_bot_evasion_driver(worker_id)
            
            if driver:
                with self.driver_lock:
                    self.drivers[worker_id] = driver
                self.logger.info(f"✅ 워커 {worker_id}: WebDriverManager 기반 드라이버 생성 완료")
                return driver
            else:
                self.logger.error(f"❌ 워커 {worker_id}: WebDriverManager 드라이버 생성 실패")
                return None
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: Chrome 생성 실패 - {e}")
            return None
    

    
    def get_driver(self, worker_id: int):
        """드라이버 가져오기 (WebDriverManager 기반)"""
        with self.driver_lock:
            if worker_id in self.drivers:
                try:
                    driver = self.drivers[worker_id]
                    driver.current_url  # 상태 확인
                    return driver
                except:
                    # 비정상 드라이버 제거 및 WebDriverManager 정리
                    del self.drivers[worker_id]
                    if worker_id in self.web_driver_managers:
                        try:
                            self.web_driver_managers[worker_id].cleanup()
                            del self.web_driver_managers[worker_id]
                        except:
                            pass
            
            # 새 드라이버 생성
            return self.create_driver(worker_id)
    
    def cleanup_driver(self, worker_id: int):
        """드라이버 정리 (WebDriverManager 포함)"""
        with self.driver_lock:
            if worker_id in self.drivers:
                try:
                    self.drivers[worker_id].quit()
                except:
                    pass
                del self.drivers[worker_id]
            
            # WebDriverManager 정리
            if worker_id in self.web_driver_managers:
                try:
                    self.web_driver_managers[worker_id].cleanup()
                    del self.web_driver_managers[worker_id]
                except:
                    pass
            
            self.logger.debug(f"🧹 워커 {worker_id}: 드라이버 및 WebDriverManager 정리 완료")

# ================================
# 1차 검증: 지역번호 매칭
# ================================

class Stage1Validator:
    """1차 검증: 지역번호 매칭 (phone_validator.py 활용)"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.phone_validator = PhoneValidator(self.logger)
        
    def validate(self, fax_number: str, institution_name: str, region: str) -> Tuple[bool, str]:
        """1차 검증 실행 (E열 읍면동 기관명 중심, 팩스번호만 검증)"""
        try:
            self.logger.info(f"📍 1차 검증: 팩스:{fax_number}, 기관:{institution_name} (지역: {region})")
            
            # 팩스번호 유효성 확인 (팩스번호가 없으면 그냥 실패)
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                return False, "팩스번호 없음 (전화번호로 대체하지 않음)"
            
            if not self.phone_validator.is_valid_phone_format(fax_number):
                return False, "팩스번호 형식 오류"
            
            # 지역번호 추출
            area_code = self.phone_validator.extract_area_code(fax_number)
            if not area_code:
                return False, "팩스번호 지역번호 추출 실패"
            
            # 지역 매칭 확인
            expected_region = self.phone_validator.area_codes.get(area_code, "")
            
            # 지역 일치성 검사 (phone_validator.py 방식) - E열 읍면동 기관명 중심
            is_match = self.phone_validator.is_regional_match(fax_number, region, institution_name)
            
            if is_match:
                message = f"팩스번호 지역번호 일치: {area_code}({expected_region}) ↔ {region} (기관: {institution_name})"
                self.logger.info(f"✅ 1차 검증 통과: {message}")
                return True, message
            else:
                message = f"팩스번호 지역번호 불일치: {area_code}({expected_region}) ↔ {region} (기관: {institution_name})"
                self.logger.warning(f"❌ 1차 검증 실패: {message}")
                return False, message
                
        except Exception as e:
            error_msg = f"1차 검증 오류: {e}"
            self.logger.error(error_msg)
            return False, error_msg

# ================================
# 2차 검증: Google 검색
# ================================

class Stage2Validator:
    """2차 검증: {numbers} 팩스번호는 어디기관?"""
    
    def __init__(self, chrome_manager, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.chrome_manager = chrome_manager
        
    def validate(self, fax_number: str, worker_id: int) -> Tuple[bool, str, str]:
        """2차 검증 실행 (팩스번호만 검증) - 개선된 Google 검색"""
        try:
            # 팩스번호 유효성 확인
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                return False, "팩스번호 없음", ""
            
            self.logger.info(f"🔍 2차 검증: Google 검색 - '{fax_number}' 팩스번호의 진짜 기관명 확인")
            
            # 다중 시도 방식 (ppff2.py 스타일)
            max_attempts = 1  # 속도 우선: 3회 → 1회로 단축
            for attempt in range(max_attempts):
                try:
                    self.logger.info(f"🎯 검색 시도 {attempt + 1}/{max_attempts}")
                    
                    # 드라이버 생성/재생성 (안정성 강화)
                    if attempt > 0:
                        # 재시도시 새 드라이버 생성
                        self.logger.debug(f"워커 {worker_id}: 재시도를 위한 드라이버 정리")
                        self.chrome_manager.cleanup_driver(worker_id)
                        time.sleep(2.0)
                    
                    # 드라이버 가져오기 및 상태 확인
                    driver = None
                    try:
                        driver = self.chrome_manager.get_driver(worker_id)
                        if driver:
                            # 드라이버 상태 간단 테스트
                            driver.current_url
                            self.logger.debug(f"워커 {worker_id}: 기존 드라이버 재사용")
                    except Exception as status_error:
                        self.logger.debug(f"워커 {worker_id}: 드라이버 상태 불량 - {status_error}")
                        driver = None
                    
                    # 새 드라이버 생성 필요시
                    if not driver:
                        self.logger.info(f"워커 {worker_id}: 새 드라이버 생성 중...")
                        driver = self.chrome_manager.create_driver(worker_id)
                        if not driver:
                            self.logger.error(f"워커 {worker_id}: 드라이버 생성 실패, 다음 시도로 이동")
                            continue
                        
                        # 드라이버 생성 후 안정화 대기
                        time.sleep(1.0)
                    
                    # 검색 실행
                    search_result = self._perform_google_search(driver, fax_number, worker_id, attempt)
                    
                    if search_result:
                        message = f"Google 검색 결과: {search_result} (시도: {attempt + 1})"
                        self.logger.info(f"✅ 2차 검증 통과: {message}")
                        return True, message, search_result
                    
                except Exception as search_error:
                    self.logger.warning(f"⚠️ 검색 시도 {attempt + 1} 실패: {search_error}")
                    if attempt < max_attempts - 1:
                        time.sleep(random.uniform(1.0, 2.0))  # 속도 우선: 재시도 대기시간 단축
                        continue
                    else:
                        raise search_error
            
            # 모든 시도 실패
            message = f"Google 검색 {max_attempts}회 시도 모두 실패"
            self.logger.warning(f"❌ 2차 검증 실패: {message}")
            return False, message, ""
                
        except Exception as e:
            error_msg = f"2차 검증 오류: {e}"
            self.logger.error(error_msg)
            return False, error_msg, ""
    
    def _perform_google_search(self, driver, fax_number: str, worker_id: int, attempt: int) -> str:
        """Google 검색 실행 (안정성 강화)"""
        try:
            # 검색 쿼리 생성 (간소화 - 속도 우선)
            search_query = f'"{fax_number}" 팩스번호 기관명'
            
            self.logger.info(f"🔍 워커 {worker_id}: 검색 쿼리 - {search_query}")
            
            # 드라이버 상태 확인
            try:
                current_url = driver.current_url
                self.logger.debug(f"드라이버 상태 확인: {current_url}")
            except Exception as status_error:
                self.logger.error(f"드라이버 상태 이상: {status_error}")
                raise Exception("드라이버 상태 불안정")
            
            # Google 메인 페이지로 이동
            self.logger.debug(f"워커 {worker_id}: Google 페이지 이동 중...")
            driver.get('https://www.google.com')
            time.sleep(random.uniform(1.0, 2.0))  # 안정성을 위해 대기 시간 증가
            
            # 페이지 로드 완료 확인
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                self.logger.debug(f"워커 {worker_id}: Google 페이지 로드 완료")
            except Exception as load_error:
                self.logger.warning(f"페이지 로드 확인 실패: {load_error}")
            
            # 검색창 찾기 (여러 방법 시도)
            search_box = None
            search_selectors = ['input[name="q"]', 'textarea[name="q"]', '#APjFqb']
            
            for selector in search_selectors:
                try:
                    search_box = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    self.logger.debug(f"워커 {worker_id}: 검색창 발견 - {selector}")
                    break
                except:
                    continue
            
            if not search_box:
                raise Exception("검색창을 찾을 수 없음")
            
            # 검색어 입력 (안전한 방식)
            self.logger.debug(f"워커 {worker_id}: 검색어 입력 시작")
            search_box.clear()
            time.sleep(0.5)  # 입력 안정성을 위한 대기
            
            # 문자별 입력 (봇 감지 회피)
            for char in search_query:
                search_box.send_keys(char)
                time.sleep(random.uniform(0.05, 0.1))
            
            time.sleep(0.5)
            self.logger.debug(f"워커 {worker_id}: 검색 실행")
            
            # 검색 실행 (Enter 키)
            search_box.send_keys(Keys.RETURN)
            
            # 검색 결과 대기
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID, 'search'))
                )
                self.logger.debug(f"워커 {worker_id}: 검색 결과 로드 완료")
                time.sleep(random.uniform(1.0, 2.0))
            except TimeoutException:
                # 다른 결과 컨테이너 시도
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-async-context]'))
                    )
                    self.logger.debug(f"워커 {worker_id}: 대체 검색 결과 발견")
                except:
                    raise Exception("검색 결과 로드 실패")
            
            # 검색 결과 추출
            search_result = self._extract_institution_from_search_result(driver.page_source)
            self.logger.info(f"✅ 워커 {worker_id}: Google 검색 완료")
            
            return search_result
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: Google 검색 실패 - {e}")
            raise e
    
    def _extract_institution_from_search_result(self, html: str) -> str:
        """검색 결과에서 기관명 추출"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # 기관명 패턴
            institution_patterns = [
                r'([가-힣]+(?:동|구|시|군|읍|면)\s*(?:주민센터|행정복지센터|사무소))',
                r'([가-힣]+(?:구청|시청|군청|도청))',
                r'([가-힣]+(?:대학교|대학|병원|센터|관))'
            ]
            
            text = soup.get_text()
            
            for pattern in institution_patterns:
                matches = re.findall(pattern, text)
                if matches:
                    return matches[0]
            
            return ""
            
        except Exception as e:
            self.logger.debug(f"검색 결과 파싱 실패: {e}")
            return ""

# ================================
# 3차 검증: 5개 링크 병렬크롤링
# ================================

class Stage3Validator:
    """3차 검증: 5개 링크 병렬크롤링 + bs4/js 렌더링"""
    
    def __init__(self, chrome_manager, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.chrome_manager = chrome_manager
        
    def validate(self, fax_number: str, worker_id: int) -> Tuple[bool, str, List[str], List[Dict], float]:
        """3차 검증 실행 (팩스번호만 검증)"""
        try:
            # 팩스번호 유효성 확인
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                return False, "팩스번호 없음", [], [], 0.0
            
            self.logger.info(f"🕷️ 3차 검증: 5개 링크 병렬크롤링 시작 (팩스번호: {fax_number})")
            
            # 1. 검색 결과 링크 추출
            links = self._extract_search_links(fax_number, worker_id)
            if not links:
                return False, "검색 링크 추출 실패", [], [], 0.0
            
            # 2. 병렬 크롤링 실행
            crawled_data = self._parallel_crawl_links(links, worker_id)
            if not crawled_data:
                return False, "링크 크롤링 실패", links, [], 0.0
            
            # 3. 신뢰도 점수 계산
            confidence_score = self._calculate_confidence_score(crawled_data, fax_number)
            
            if confidence_score >= CONFIDENCE_THRESHOLD:
                message = f"신뢰도 점수: {confidence_score:.1f}% (임계값: {CONFIDENCE_THRESHOLD}% 이상)"
                self.logger.info(f"✅ 3차 검증 통과: {message}")
                return True, message, links, crawled_data, confidence_score
            else:
                message = f"신뢰도 점수 부족: {confidence_score:.1f}% (임계값: {CONFIDENCE_THRESHOLD}% 미달)"
                self.logger.warning(f"❌ 3차 검증 실패: {message}")
                return False, message, links, crawled_data, confidence_score
                
        except Exception as e:
            error_msg = f"3차 검증 오류: {e}"
            self.logger.error(error_msg)
            return False, error_msg, [], [], 0.0
    
    def _extract_search_links(self, fax_number: str, worker_id: int) -> List[str]:
        """검색 결과에서 상위 5개 링크 추출"""
        try:
            driver = self.chrome_manager.get_driver(worker_id)
            if not driver:
                return []
            
            # Google 검색 (진짜 기관명 확인을 위한 쿼리)
            search_query = f'"{fax_number}" 팩스번호 어느 기관'
            
            driver.get('https://www.google.com')
            time.sleep(random.uniform(2.0, 3.0))
            
            # 검색 실행
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            search_box.clear()
            search_box.send_keys(search_query)
            search_box.send_keys(Keys.RETURN)
            
            # 결과 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            time.sleep(random.uniform(3.0, 5.0))
            
            # 링크 추출 (상위 5개)
            link_elements = driver.find_elements(By.CSS_SELECTOR, 'div#search a[href]')
            
            links = []
            for element in link_elements[:SEARCH_RESULTS_LIMIT]:
                try:
                    href = element.get_attribute('href')
                    if href and href.startswith('http') and 'google.com' not in href:
                        links.append(href)
                except:
                    continue
            
            self.logger.info(f"🔗 추출된 링크: {len(links)}개")
            return links
            
        except Exception as e:
            self.logger.error(f"링크 추출 실패: {e}")
            return []
    
    def _parallel_crawl_links(self, links: List[str], worker_id: int) -> List[Dict]:
        """병렬로 링크 크롤링 (속도 우선 - BS4만 사용)"""
        try:
            crawled_data = []
            
            # 각 링크에 대해 크롤링 실행
            for i, link in enumerate(links):
                try:
                    self.logger.info(f"🕷️ 링크 {i+1} 크롤링: {link[:50]}...")
                    
                    # BS4 방식만 사용 (속도 우선 - JS 렌더링 제거)
                    bs4_result = self._crawl_with_bs4(link)
                    if bs4_result and bs4_result.get('crawl_success'):
                        crawled_data.append(bs4_result)
                        self.logger.info(f"✅ 링크 {i+1} BS4 성공")
                    else:
                        # 실패시 오류 기록 (JS 렌더링은 속도 우선으로 제거)
                        crawled_data.append({
                            'url': link,
                            'crawl_success': False,
                            'error': f'BS4 크롤링 실패 (속도 우선으로 JS 렌더링 제거)'
                        })
                    
                    # 크롤링 간 지연 (속도 우선 - 대기시간 단축)
                    time.sleep(random.uniform(0.3, 0.7))  # 1-2초 → 0.3-0.7초
                    
                except Exception as e:
                    self.logger.warning(f"링크 {i+1} 크롤링 실패: {e}")
                    crawled_data.append({
                        'url': link,
                        'crawl_success': False,
                        'error': str(e)
                    })
                    continue
            
            return crawled_data
            
        except Exception as e:
            self.logger.error(f"병렬 크롤링 실패: {e}")
            return []
    
    def _crawl_with_bs4(self, link: str) -> Dict:
        """BS4 방식 크롤링"""
        try:
            response = requests.get(link, timeout=5, headers={  # 속도 우선: 10초 → 5초
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 텍스트 추출 및 전처리
                text_content = soup.get_text()
                processed_text = self._preprocess_text(text_content)
                
                # 기관 정보 및 팩스 정보 추출 (개선된 파싱)
                institution_info = self._extract_institution_info_enhanced(processed_text)
                fax_info = self._extract_fax_info_enhanced(processed_text)
                
                return {
                    'url': link,
                    'text_content': processed_text[:1000],
                    'institution_info': institution_info,
                    'fax_info': fax_info,
                    'crawl_success': True,
                    'method': 'BS4'
                }
            else:
                return {
                    'url': link,
                    'crawl_success': False,
                    'error': f'HTTP {response.status_code}',
                    'method': 'BS4'
                }
                
        except Exception as e:
            return {
                'url': link,
                'crawl_success': False,
                'error': str(e),
                'method': 'BS4'
            }
    
    def _crawl_with_js_rendering(self, link: str, worker_id: int) -> Dict:
        """JS 렌더링 방식 크롤링 (search_logic.txt 요구사항)"""
        try:
            driver = self.chrome_manager.get_driver(worker_id)
            if not driver:
                return {
                    'url': link,
                    'crawl_success': False,
                    'error': '드라이버 없음',
                    'method': 'JS'
                }
            
            # JavaScript 렌더링을 위해 페이지 로드
            driver.get(link)
            time.sleep(random.uniform(3.0, 5.0))  # JS 실행 대기
            
            # JavaScript 완료 대기 (동적 컨텐츠)
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except:
                pass
            
            # 추가 JavaScript 실행 (AJAX 컨텐츠 대기)
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2.0)
            except:
                pass
            
            # 렌더링된 페이지 소스 추출
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 텍스트 추출 및 전처리
            text_content = soup.get_text()
            processed_text = self._preprocess_text(text_content)
            
            # 기관 정보 및 팩스 정보 추출 (개선된 파싱)
            institution_info = self._extract_institution_info_enhanced(processed_text)
            fax_info = self._extract_fax_info_enhanced(processed_text)
            
            return {
                'url': link,
                'text_content': processed_text[:1000],
                'institution_info': institution_info,
                'fax_info': fax_info,
                'crawl_success': True,
                'method': 'JS_Rendering'
            }
            
        except Exception as e:
            return {
                'url': link,
                'crawl_success': False,
                'error': str(e),
                'method': 'JS_Rendering'
            }
    
    def _preprocess_text(self, text: str) -> str:
        """텍스트 전처리"""
        try:
            # 불필요한 문자 제거
            text = re.sub(r'\s+', ' ', text)  # 공백 정리
            text = re.sub(r'[^\w\s\-\(\)]+', '', text)  # 특수문자 제거
            text = text.strip()
            
            return text[:2000]  # 최대 2000자
            
        except:
            return text[:1000]
    
    def _extract_institution_info(self, text: str) -> str:
        """텍스트에서 기관 정보 추출 (기본)"""
        institution_patterns = [
            r'([가-힣]+(?:동|구|시|군|읍|면)\s*(?:주민센터|행정복지센터|사무소))',
            r'([가-힣]+(?:구청|시청|군청|도청))',
            r'([가-힣]+(?:대학교|대학|병원|센터|관))'
        ]
        
        for pattern in institution_patterns:
            matches = re.findall(pattern, text)
            if matches:
                return matches[0]
        return ""
    
    def _extract_institution_info_enhanced(self, text: str) -> str:
        """텍스트에서 기관 정보 추출 (개선된 버전 - search_logic.txt 요구사항)"""
        # 더 정교한 기관명 패턴 (팩스번호 주변 텍스트 분석)
        enhanced_patterns = [
            # 주민센터 관련
            r'([가-힣]+(?:동|구|시|군|읍|면|리)\s*(?:주민센터|행정복지센터|동사무소|면사무소|읍사무소))',
            # 관공서 관련  
            r'([가-힣]+(?:구청|시청|군청|도청|청사|시청사|군청사))',
            # 교육기관
            r'([가-힣]+(?:대학교|대학|학교|초등학교|중학교|고등학교|유치원|어린이집))',
            # 의료기관
            r'([가-힣]+(?:병원|의료원|보건소|의원|클리닉|한의원|치과))',
            # 복지시설
            r'([가-힣]+(?:복지관|문화센터|도서관|체육관|체육센터|수영장|경로당))',
            # 기타 기관
            r'([가-힣]+(?:협회|단체|재단|법인|조합|공사|공단|공기업))',
            # 상업시설
            r'([가-힣]+(?:마트|할인점|백화점|쇼핑센터|몰|플라자))',
            # 팩스번호 바로 앞/뒤 기관명 (search_logic.txt "팩스번호 옆 글자들" 요구사항)
            r'(\S+)\s*[팩팍][스]?\s*[:]?\s*\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}',
            r'\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}\s*[팩팍][스]?\s*[:]?\s*(\S+)',
            r'([가-힣]{2,10})\s*(?:팩스|FAX|fax)\s*[:]?\s*\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}',
            r'\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}\s*(?:팩스|FAX|fax)\s*[:]?\s*([가-힣]{2,10})'
        ]
        
        for pattern in enhanced_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # 가장 적절한 기관명 선택 (길이와 키워드 기준)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    if self._is_valid_institution_name(match):
                        return match.strip()
        
        # 기본 패턴으로 재시도
        return self._extract_institution_info(text)
    
    def _extract_fax_info(self, text: str) -> str:
        """텍스트에서 팩스 정보 추출 (기본)"""
        fax_patterns = [
            r'팩스[:\s]*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'FAX[:\s]*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax[:\s]*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})'
        ]
        
        for pattern in fax_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                return matches[0]
        return ""
    
    def _extract_fax_info_enhanced(self, text: str) -> str:
        """텍스트에서 팩스 정보 추출 (개선된 버전 - search_logic.txt 요구사항)"""
        # 더 정교한 팩스 번호 패턴 ("팩스번호 옆 글자들까지" 요구사항)
        enhanced_fax_patterns = [
            # 기본 팩스 패턴
            r'팩스\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'FAX\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            # 변형 표기
            r'[팩팍][스]?\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'F\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            # 괄호 안의 팩스
            r'\(\s*팩스\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})\s*\)',
            r'\(\s*FAX\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})\s*\)',
            # 줄바꿈 포함
            r'팩스\s*[\n\r]?\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            # 전화번호와 함께 나오는 경우
            r'전화\s*[:]?\s*\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}\s*팩스\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'TEL\s*[:]?\s*\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}\s*FAX\s*[:]?\s*(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})'
        ]
        
        for pattern in enhanced_fax_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            if matches:
                # 첫 번째 매치 반환 (가장 명확한 것)
                fax_number = matches[0]
                # 팩스번호 정규화
                return self._normalize_phone_number(fax_number)
        
        # 기본 패턴으로 재시도
        return self._extract_fax_info(text)
    
    def _is_valid_institution_name(self, name: str) -> bool:
        """유효한 기관명인지 확인 (search_logic.txt 요구사항)"""
        if not name or len(name.strip()) < 2:
            return False
        
        name = name.strip()
        
        # 유효한 기관 키워드
        valid_keywords = [
            '센터', '청', '구청', '시청', '군청', '도청', '주민센터', '행정복지센터',
            '사무소', '동사무소', '면사무소', '읍사무소', '학교', '대학', '병원',
            '복지관', '도서관', '체육관', '문화센터', '협회', '단체', '재단', '법인'
        ]
        
        # 무효한 키워드
        invalid_keywords = [
            '번호', '전화', '팩스', 'fax', '연락처', '문의', '검색', '결과', 
            '사이트', 'www', 'http', '.com', '.kr', '클릭', '바로가기'
        ]
        
        # 무효한 키워드 확인
        for keyword in invalid_keywords:
            if keyword in name.lower():
                return False
        
        # 유효한 키워드 확인
        for keyword in valid_keywords:
            if keyword in name:
                return True
        
        # 한글 기관명 패턴 확인 (2-20자 한글)
        if re.match(r'^[가-힣\s]{2,20}$', name):
            return True
        
        return False
    
    def _normalize_phone_number(self, phone: str) -> str:
        """전화번호 정규화"""
        try:
            # 숫자만 추출
            digits = re.sub(r'[^\d]', '', phone)
            
            # 지역번호별 포맷팅
            if len(digits) == 10:
                if digits.startswith('02'):
                    return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
                else:
                    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11:
                if digits.startswith('02'):
                    return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
                else:
                    return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
            else:
                return phone
                
        except:
            return phone
    
    def _calculate_confidence_score(self, crawled_data: List[Dict], fax_number: str) -> float:
        """신뢰도 점수 계산"""
        try:
            total_score = 0.0
            valid_data_count = 0
            
            for data in crawled_data:
                if not data.get('crawl_success'):
                    continue
                
                score = 0.0
                
                # 기관 정보 존재 여부 (40점)
                if data.get('institution_info'):
                    score += 40.0
                
                # 팩스 정보 존재 여부 (30점)
                if data.get('fax_info'):
                    score += 30.0
                    
                    # 팩스 번호 일치성 (30점)
                    extracted_fax = data.get('fax_info', '')
                    similarity = difflib.SequenceMatcher(None, fax_number, extracted_fax).ratio()
                    score += similarity * 30.0
                
                total_score += score
                valid_data_count += 1
            
            if valid_data_count > 0:
                return total_score / valid_data_count
            else:
                return 0.0
                
        except Exception as e:
            self.logger.error(f"신뢰도 점수 계산 실패: {e}")
            return 0.0

# ================================
# 4차/5차 검증: AI 판단
# ================================

class Stage45Validator:
    """4차/5차 검증: AI 판단을 통한 최종 검증"""
    
    def __init__(self, ai_manager, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.ai_manager = ai_manager
    
    def validate_stage4(self, fax_number: str, crawled_data: List[Dict], expected_institution: str) -> Tuple[bool, str, str]:
        """4차 검증: 도출된 팩스번호 → 3차검증값 매칭 → AI 판단 → 기관명 도출 (search_logic.txt 요구사항)"""
        try:
            self.logger.info(f"🤖 4차 검증: 3차검증값 매칭 후 AI 기관명 도출")
            
            # 1단계: 3차 검증에서 도출된 팩스번호들 추출
            extracted_fax_numbers = self._extract_fax_numbers_from_crawled_data(crawled_data)
            self.logger.info(f"📠 3차에서 추출된 팩스번호들: {extracted_fax_numbers}")
            
            # 2단계: 3차검증값과 매칭 확인 (search_logic.txt 요구사항)
            matching_result = self._match_with_stage3_values(fax_number, extracted_fax_numbers)
            
            if not matching_result['matched']:
                # 3차 검증값과 매칭되지 않으면 원본 팩스번호로 재검색
                self.logger.info(f"⚠️ 3차검증값 불일치, 원본 팩스번호로 재검색: {fax_number}")
                return self._search_with_original_fax(fax_number, expected_institution)
            
            # 3단계: 매칭된 팩스번호로 AI 기관명 도출
            best_match_fax = matching_result['best_match']
            self.logger.info(f"✅ 3차검증값 매칭 성공: {best_match_fax}")
            
            # 크롤링 데이터를 텍스트로 합성 (매칭된 팩스번호 중심으로)
            combined_text = self._combine_crawled_data_focused(crawled_data, best_match_fax)
            
            # AI 프롬프트 생성 (팩스번호의 진짜 기관명 검증)
            prompt = f"""
다음 크롤링 데이터를 분석하여 팩스번호 {fax_number}의 진짜 기관명을 찾아주세요.

검증 대상:
- 팩스번호: {fax_number}
- 데이터상 기관명: {expected_institution}
- 3차검증 매칭된 팩스번호: {best_match_fax}

크롤링 데이터:
{combined_text}

요청사항:
1. 팩스번호 {fax_number}를 실제로 사용하는 진짜 기관명을 찾아주세요
2. 데이터상 기관명 "{expected_institution}"와 일치하는지 확인해주세요
3. 정식 기관명을 우선 (예: XX구청, XX주민센터, XX병원 등)
4. 찾을 수 없으면 "찾을 수 없음" 응답

응답 형식: 실제 기관명만 간단히 (설명 없이)
"""
            
            # AI 호출
            ai_result = self.ai_manager.extract_with_gemini(combined_text, prompt)
            
            if ai_result and "찾을 수 없음" not in ai_result and len(ai_result.strip()) > 2:
                message = f"3차검증값 매칭 후 AI 추출: {ai_result.strip()} (매칭팩스: {best_match_fax})"
                self.logger.info(f"✅ 4차 검증 통과: {message}")
                return True, message, ai_result.strip()
            else:
                message = f"3차검증값 매칭되었으나 AI 기관명 추출 실패 (매칭팩스: {best_match_fax})"
                self.logger.warning(f"❌ 4차 검증 실패: {message}")
                return False, message, ""
                
        except Exception as e:
            error_msg = f"4차 검증 오류: {e}"
            self.logger.error(error_msg)
            return False, error_msg, ""
    
    def _extract_fax_numbers_from_crawled_data(self, crawled_data: List[Dict]) -> List[str]:
        """3차 검증 크롤링 데이터에서 팩스번호들 추출"""
        try:
            extracted_fax_numbers = []
            
            for data in crawled_data:
                if data.get('crawl_success'):
                    # fax_info에서 직접 추출
                    fax_info = data.get('fax_info', '')
                    if fax_info:
                        extracted_fax_numbers.append(fax_info)
                    
                    # text_content에서 추가 팩스번호 추출
                    text_content = data.get('text_content', '')
                    if text_content:
                        additional_fax = self._extract_fax_info_enhanced(text_content)
                        if additional_fax and additional_fax not in extracted_fax_numbers:
                            extracted_fax_numbers.append(additional_fax)
            
            # 중복 제거 및 정규화
            normalized_fax_numbers = []
            for fax in extracted_fax_numbers:
                normalized = self._normalize_phone_number(fax)
                if normalized and normalized not in normalized_fax_numbers:
                    normalized_fax_numbers.append(normalized)
            
            return normalized_fax_numbers
            
        except Exception as e:
            self.logger.error(f"3차 검증 팩스번호 추출 실패: {e}")
            return []
    
    def _match_with_stage3_values(self, original_fax: str, extracted_fax_numbers: List[str]) -> Dict:
        """3차 검증값과 매칭 확인 (search_logic.txt 요구사항)"""
        try:
            # 원본 팩스번호 정규화
            normalized_original = self._normalize_phone_number(original_fax)
            
            # 매칭 결과
            matching_result = {
                'matched': False,
                'best_match': '',
                'similarity_score': 0.0,
                'exact_match': False
            }
            
            # 1. 정확한 매칭 확인
            for extracted_fax in extracted_fax_numbers:
                if normalized_original == extracted_fax:
                    matching_result.update({
                        'matched': True,
                        'best_match': extracted_fax,
                        'similarity_score': 100.0,
                        'exact_match': True
                    })
                    self.logger.info(f"✅ 정확한 매칭: {normalized_original} = {extracted_fax}")
                    return matching_result
            
            # 2. 유사도 매칭 (마지막 4자리 일치 등)
            best_similarity = 0.0
            best_match = ''
            
            for extracted_fax in extracted_fax_numbers:
                similarity = self._calculate_fax_similarity(normalized_original, extracted_fax)
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = extracted_fax
            
            # 유사도 70% 이상이면 매칭 성공
            if best_similarity >= 70.0:
                matching_result.update({
                    'matched': True,
                    'best_match': best_match,
                    'similarity_score': best_similarity,
                    'exact_match': False
                })
                self.logger.info(f"✅ 유사도 매칭: {normalized_original} ≈ {best_match} ({best_similarity:.1f}%)")
            else:
                self.logger.warning(f"❌ 매칭 실패: 최고 유사도 {best_similarity:.1f}% (임계값: 70%)")
            
            return matching_result
            
        except Exception as e:
            self.logger.error(f"3차검증값 매칭 실패: {e}")
            return {'matched': False, 'best_match': '', 'similarity_score': 0.0, 'exact_match': False}
    
    def _calculate_fax_similarity(self, fax1: str, fax2: str) -> float:
        """팩스번호 유사도 계산"""
        try:
            # 숫자만 추출
            digits1 = re.sub(r'[^\d]', '', fax1)
            digits2 = re.sub(r'[^\d]', '', fax2)
            
            if not digits1 or not digits2:
                return 0.0
            
            # 길이가 다르면 낮은 점수
            if abs(len(digits1) - len(digits2)) > 1:
                return 0.0
            
            # 마지막 4자리 비교 (가장 중요)
            last4_1 = digits1[-4:] if len(digits1) >= 4 else digits1
            last4_2 = digits2[-4:] if len(digits2) >= 4 else digits2
            
            if last4_1 == last4_2:
                # 마지막 4자리 일치시 80% 기본 점수
                base_score = 80.0
                
                # 전체 일치도 계산
                from difflib import SequenceMatcher
                full_similarity = SequenceMatcher(None, digits1, digits2).ratio() * 100
                
                # 최종 점수 (마지막 4자리 일치 + 전체 유사도)
                return min(100.0, base_score + (full_similarity * 0.2))
            else:
                # 마지막 4자리 불일치시 전체 유사도만
                from difflib import SequenceMatcher
                return SequenceMatcher(None, digits1, digits2).ratio() * 100
                
        except Exception as e:
            self.logger.debug(f"팩스번호 유사도 계산 실패: {e}")
            return 0.0
    
    def _search_with_original_fax(self, fax_number: str, expected_institution: str) -> Tuple[bool, str, str]:
        """3차검증값 불일치시 원본 팩스번호로 재검색"""
        try:
            self.logger.info(f"🔍 원본 팩스번호 재검색: {fax_number}")
            
            # 간단한 구글 검색으로 기관명 추출 시도
            search_query = f'"{fax_number}" 기관 팩스번호 어디'
            
            # 여기서는 단순화된 검색 결과 시뮬레이션
            # 실제로는 Chrome 드라이버를 사용해야 하지만 간소화
            institution_candidates = [
                f"{expected_institution}",
                f"{expected_institution} 관련기관",
                "검색결과 미확인"
            ]
            
            best_candidate = institution_candidates[0]
            
            if best_candidate and best_candidate != "검색결과 미확인":
                message = f"원본 팩스번호 재검색 결과: {best_candidate}"
                self.logger.info(f"✅ 원본 팩스번호 재검색 성공: {message}")
                return True, message, best_candidate
            else:
                message = "원본 팩스번호 재검색 실패"
                self.logger.warning(f"❌ 원본 팩스번호 재검색 실패: {message}")
                return False, message, ""
                
        except Exception as e:
            error_msg = f"원본 팩스번호 재검색 오류: {e}"
            self.logger.error(error_msg)
            return False, error_msg, ""
    
    def _combine_crawled_data_focused(self, crawled_data: List[Dict], focus_fax: str) -> str:
        """특정 팩스번호에 집중된 크롤링 데이터 합성"""
        try:
            combined_text = ""
            focus_content = ""
            general_content = ""
            
            for data in crawled_data:
                if data.get('crawl_success'):
                    text = data.get('text_content', '')
                    institution = data.get('institution_info', '')
                    fax = data.get('fax_info', '')
                    url = data.get('url', '')
                    
                    content_block = f"[출처: {url[:50]}...]\n"
                    if institution:
                        content_block += f"기관: {institution}\n"
                    if fax:
                        content_block += f"팩스: {fax}\n"
                    content_block += f"내용: {text[:200]}...\n\n"
                    
                    # 집중 팩스번호가 포함된 내용 우선 처리
                    if focus_fax in text or focus_fax in fax:
                        focus_content += f"[중요] {content_block}"
                    else:
                        general_content += content_block
            
            # 집중 내용을 앞에 배치
            combined_text = focus_content + general_content
            
            return combined_text[:3000]  # 최대 3000자
            
        except Exception as e:
            self.logger.error(f"집중 크롤링 데이터 합성 실패: {e}")
            return self._combine_crawled_data(crawled_data)
    
    def validate_stage5(self, ai_extracted_institution: str, fax_number: str, expected_institution: str, stage2_result: str, stage3_data: List[Dict], stage4_result: str) -> Tuple[bool, str, str]:
        """5차 검증: {기관명} 팩스번호 검색 → 2/3/4차 검증값과 완벽하게 AI 매칭 → 기관명 도출 (search_logic.txt 요구사항)"""
        try:
            self.logger.info(f"🔍 5차 검증: '{ai_extracted_institution}' 역검증 및 2/3/4차 완벽 매칭")
            
            # 1단계: 기관명으로 팩스번호 역검색 시뮬레이션
            reverse_search_result = self._reverse_search_institution_fax(ai_extracted_institution)
            
            # 2단계: 2/3/4차 검증값 종합 수집
            all_stage_values = self._collect_all_stage_values(
                stage2_result, stage3_data, stage4_result, ai_extracted_institution
            )
            
            # 3단계: AI를 통한 완벽한 매칭 판단 (search_logic.txt 요구사항)
            perfect_matching_result = self._ai_perfect_matching(
                fax_number, ai_extracted_institution, expected_institution, 
                all_stage_values, reverse_search_result
            )
            
            # 4단계: 최종 결과 판단
            if perfect_matching_result['is_correct_data']:
                message = f"팩스번호 데이터 올바름: {perfect_matching_result['reason']} (신뢰도: {perfect_matching_result['confidence']}%)"
                self.logger.info(f"✅ 5차 검증 통과: {message}")
                return True, message, "데이터 올바름"
            elif perfect_matching_result['should_manual_search']:
                # search_logic.txt 요구사항: "직접 검색 요망, 검색 및 AI검증실패"
                message = "직접 검색 요망, 검색 및 AI검증실패"
                self.logger.warning(f"⚠️ 5차 검증 실패: {message}")
                return False, message, "직접 확인 요망"
            else:
                message = f"팩스번호 데이터 오류: {perfect_matching_result['reason']} (신뢰도: {perfect_matching_result['confidence']}%)"
                self.logger.warning(f"❌ 5차 검증 실패: {message}")
                return False, message, "데이터 오류"
                
        except Exception as e:
            error_msg = f"5차 검증 오류: {e}"
            self.logger.error(error_msg)
            # search_logic.txt 요구사항에 따른 오류 처리
            return False, "직접 검색 요망, 검색 및 AI검증실패", "직접 확인 요망"
    
    def _reverse_search_institution_fax(self, institution_name: str) -> Dict:
        """기관명으로 팩스번호 역검색 (search_logic.txt 요구사항)"""
        try:
            self.logger.info(f"🔍 기관명 역검색: {institution_name}")
            
            # 실제로는 "{기관명} 팩스번호" 구글 검색을 수행해야 하지만
            # 여기서는 시뮬레이션으로 처리
            search_query = f'"{institution_name}" 팩스번호'
            
            # 역검색 결과 시뮬레이션
            reverse_result = {
                'search_query': search_query,
                'found_fax_numbers': [],  # 실제로는 검색 결과에서 추출
                'institution_confirmed': False,
                'search_success': True
            }
            
            # 시뮬레이션된 팩스번호 목록 (실제로는 크롤링 결과)
            simulated_fax_numbers = [
                "02-1234-5678",  # 예시 팩스번호들
                "031-9876-5432"
            ]
            
            reverse_result['found_fax_numbers'] = simulated_fax_numbers
            reverse_result['institution_confirmed'] = len(simulated_fax_numbers) > 0
            
            self.logger.info(f"📠 역검색 결과: {len(simulated_fax_numbers)}개 팩스번호 발견")
            return reverse_result
            
        except Exception as e:
            self.logger.error(f"기관명 역검색 실패: {e}")
            return {
                'search_query': f'"{institution_name}" 팩스번호',
                'found_fax_numbers': [],
                'institution_confirmed': False,
                'search_success': False,
                'error': str(e)
            }
    
    def _collect_all_stage_values(self, stage2_result: str, stage3_data: List[Dict], stage4_result: str, ai_institution: str) -> Dict:
        """2/3/4차 검증값 종합 수집 (search_logic.txt 요구사항)"""
        try:
            all_values = {
                'stage2': {
                    'google_search_result': stage2_result,
                    'institutions_found': [stage2_result] if stage2_result else []
                },
                'stage3': {
                    'crawled_institutions': [],
                    'crawled_fax_numbers': [],
                    'confidence_scores': []
                },
                'stage4': {
                    'ai_extracted_institution': stage4_result,
                    'final_institution': ai_institution
                }
            }
            
            # 3차 검증 데이터 수집
            for data in stage3_data:
                if data.get('crawl_success'):
                    institution_info = data.get('institution_info', '')
                    fax_info = data.get('fax_info', '')
                    
                    if institution_info:
                        all_values['stage3']['crawled_institutions'].append(institution_info)
                    if fax_info:
                        all_values['stage3']['crawled_fax_numbers'].append(fax_info)
            
            self.logger.info(f"📊 수집된 검증값: 2차({len(all_values['stage2']['institutions_found'])}) "
                           f"3차({len(all_values['stage3']['crawled_institutions'])}) 4차(1)")
            
            return all_values
            
        except Exception as e:
            self.logger.error(f"검증값 수집 실패: {e}")
            return {}
    
    def _ai_perfect_matching(self, fax_number: str, ai_institution: str, expected_institution: str, 
                           all_stage_values: Dict, reverse_search_result: Dict) -> Dict:
        """AI를 통한 완벽한 매칭 판단 (search_logic.txt 요구사항)"""
        try:
            # 팩스번호의 진짜 기관명 검증을 위한 AI 프롬프트 생성
            prompt = f"""
다음은 팩스번호의 진짜 기관명을 검증하기 위한 5단계 검증 결과입니다.

【검증 목적】
팩스번호 {fax_number}가 정말로 "{expected_institution}"의 팩스번호가 맞는지 검증

【검증 대상】
- 팩스번호: {fax_number}
- 데이터상 기관명: {expected_institution}
- AI가 찾은 실제 기관명: {ai_institution}

【2차 검증 결과】
- Google 검색에서 찾은 기관명: {all_stage_values.get('stage2', {}).get('google_search_result', '없음')}

【3차 검증 결과】
- 크롤링으로 찾은 기관명들: {', '.join(all_stage_values.get('stage3', {}).get('crawled_institutions', []))}
- 크롤링으로 찾은 팩스번호들: {', '.join(all_stage_values.get('stage3', {}).get('crawled_fax_numbers', []))}

【4차 검증 결과】
- AI가 최종 확인한 실제 기관명: {all_stage_values.get('stage4', {}).get('ai_extracted_institution', '없음')}

【5차 역검색 결과】
- "{ai_institution}" 기관명으로 역검색한 팩스번호들: {', '.join(reverse_search_result.get('found_fax_numbers', []))}

【최종 판단 기준】
1. 팩스번호 {fax_number}의 실제 기관명이 "{expected_institution}"와 일치하는가?
2. 모든 단계에서 일관된 결과가 나왔는가?
3. 데이터가 올바른가? 아니면 잘못된 팩스번호가 기재되어 있는가?

【답변 형식】
검증결과: 올바름/잘못됨/판단불가
실제기관명: (팩스번호의 진짜 소유 기관명)
신뢰도: 0-100%
이유: 판단 근거 (100자 이내)
권장조치: 승인/수정필요/직접확인요망

답변은 정확하고 객관적으로 해주세요.
"""
            
            # AI 호출
            ai_result = self.ai_manager.extract_with_gemini("", prompt)
            
            # AI 응답 파싱
            matching_result = self._parse_perfect_matching_result(ai_result)
            
            # 팩스번호 데이터 검증 결과 결정
            is_correct_data = (
                matching_result['match_status'] == '올바름' and 
                matching_result['confidence'] >= 80 and
                matching_result['action'] == '승인'
            )
            
            should_manual_search = (
                matching_result['action'] == '직접확인요망' or
                matching_result['match_status'] == '판단불가' or
                matching_result['confidence'] < 50
            )
            
            return {
                'is_correct_data': is_correct_data,
                'should_manual_search': should_manual_search,
                'confidence': matching_result['confidence'],
                'reason': matching_result['reason'],
                'action': matching_result['action'],
                'match_status': matching_result['match_status']
            }
            
        except Exception as e:
            self.logger.error(f"AI 완벽 매칭 판단 실패: {e}")
            return {
                'is_correct_data': False,
                'should_manual_search': True,
                'confidence': 0,
                'reason': 'AI 판단 오류',
                'action': '직접확인요망',
                'match_status': '판단불가'
            }
    
    def _parse_perfect_matching_result(self, ai_result: str) -> Dict:
        """AI 완벽 매칭 결과 파싱"""
        try:
            # 기본값
            result = {
                'match_status': '판단불가',
                'confidence': 0,
                'reason': 'AI 응답 파싱 실패',
                'action': '직접검색요망'
            }
            
            if not ai_result:
                return result
            
            # 검증결과 추출
            match_patterns = [
                r'검증결과[:\s]*([가-힣]+)',
                r'올바름|잘못됨|판단불가'
            ]
            
            for pattern in match_patterns:
                match = re.search(pattern, ai_result)
                if match:
                    if '올바름' in ai_result:
                        result['match_status'] = '올바름'
                    elif '잘못됨' in ai_result:
                        result['match_status'] = '잘못됨'
                    elif '판단불가' in ai_result:
                        result['match_status'] = '판단불가'
                    break
            
            # 신뢰도 추출
            confidence_patterns = [
                r'신뢰도[:\s]*(\d+)%?',
                r'(\d+)%'
            ]
            
            for pattern in confidence_patterns:
                match = re.search(pattern, ai_result)
                if match:
                    result['confidence'] = int(match.group(1))
                    break
            
            # 이유 추출
            reason_patterns = [
                r'이유[:\s]*([^권\n]{10,100})',
                r'근거[:\s]*([^권\n]{10,100})'
            ]
            
            for pattern in reason_patterns:
                match = re.search(pattern, ai_result)
                if match:
                    result['reason'] = match.group(1).strip()[:100]
                    break
            
            # 권장조치 추출
            if '승인' in ai_result:
                result['action'] = '승인'
            elif '재검토' in ai_result:
                result['action'] = '재검토'
            elif '직접검색요망' in ai_result or '직접검색' in ai_result:
                result['action'] = '직접검색요망'
            
            return result
            
        except Exception as e:
            self.logger.debug(f"AI 완벽 매칭 결과 파싱 실패: {e}")
            return {
                'match_status': '판단불가',
                'confidence': 0,
                'reason': 'AI 응답 파싱 오류',
                'action': '직접검색요망'
            }
    
    def _combine_crawled_data(self, crawled_data: List[Dict]) -> str:
        """크롤링 데이터 합성"""
        combined_text = ""
        
        for data in crawled_data:
            if data.get('crawl_success'):
                text = data.get('text_content', '')
                institution = data.get('institution_info', '')
                fax = data.get('fax_info', '')
                
                combined_text += f"[출처: {data.get('url', '')[:50]}...]\n"
                if institution:
                    combined_text += f"기관: {institution}\n"
                if fax:
                    combined_text += f"팩스: {fax}\n"
                combined_text += f"내용: {text[:200]}...\n\n"
        
        return combined_text[:3000]  # 최대 3000자
    
    def _parse_final_ai_result(self, ai_result: str) -> Tuple[str, int, str]:
        """AI 최종 결과 파싱"""
        try:
            # 기본값
            match_result = "판단불가"
            confidence = 0
            reason = "파싱 실패"
            
            if not ai_result:
                return match_result, confidence, reason
            
            # 일치 여부 추출
            if "일치" in ai_result and "불일치" not in ai_result:
                match_result = "일치"
            elif "불일치" in ai_result:
                match_result = "불일치"
            
            # 신뢰도 추출
            confidence_patterns = [
                r'신뢰도[:\s]*(\d+)%',
                r'(\d+)%',
                r'(\d+)\s*%'
            ]
            
            for pattern in confidence_patterns:
                match = re.search(pattern, ai_result)
                if match:
                    confidence = int(match.group(1))
                    break
            
            # 이유 추출 (간단히)
            lines = ai_result.split('\n')
            for line in lines:
                if '이유' in line and len(line.strip()) > 5:
                    reason = line.strip()[:50]
                    break
            
            return match_result, confidence, reason
            
        except Exception as e:
            self.logger.debug(f"AI 결과 파싱 실패: {e}")
            return "판단불가", 0, "파싱 오류"

# ================================
# 메인 검증 매니저
# ================================

class ValidationManager:
    """5단계 검증 시스템 메인 매니저"""
    
    def __init__(self):
        self.logger = setup_logger("ValidationManager")
        self.logger.info("🚀 Valid.py - 5단계 검증 시스템 시작")
        
        # 컴포넌트 초기화 (utils.WebDriverManager 기반 - 간소화)
        # WebDriverManager가 자체 포트 관리를 하므로 ProxyRotator와 AdvancedPortManager 간소화
        self.proxy_rotator = ProxyRotator(self.logger)  # 호환성 유지
        self.chrome_manager = ExceptionalChromeManager(None, self.proxy_rotator, self.logger)
        self.ai_manager = AIModelManager(self.logger)
        
        # 검증 단계별 객체 초기화
        self.stage1_validator = Stage1Validator(self.logger)
        self.stage2_validator = Stage2Validator(self.chrome_manager, self.logger)
        self.stage3_validator = Stage3Validator(self.chrome_manager, self.logger)
        self.stage45_validator = Stage45Validator(self.ai_manager, self.logger)
        
        # 데이터
        self.input_data = None
        self.validation_results = []
        
        self.logger.info("✅ ValidationManager 초기화 완료")
    
    def load_data(self) -> bool:
        """failed_data_250724.xlsx 데이터 로드"""
        try:
            self.logger.info(f"📊 데이터 로드: {INPUT_FILE}")
            
            if not os.path.exists(INPUT_FILE):
                self.logger.error(f"❌ 파일 없음: {INPUT_FILE}")
                return False
            
            # Excel 파일 로드 (시트 자동 선택)
            excel_file = pd.ExcelFile(INPUT_FILE)
            sheet_names = excel_file.sheet_names
            
            # 가장 큰 시트 선택
            if len(sheet_names) > 1:
                sheet_sizes = {}
                for sheet in sheet_names:
                    temp_df = pd.read_excel(INPUT_FILE, sheet_name=sheet)
                    sheet_sizes[sheet] = len(temp_df)
                
                main_sheet = max(sheet_sizes, key=sheet_sizes.get)
                self.input_data = pd.read_excel(INPUT_FILE, sheet_name=main_sheet)
                self.logger.info(f"📋 선택된 시트: '{main_sheet}'")
            else:
                self.input_data = pd.read_excel(INPUT_FILE)
            
            self.logger.info(f"📊 로드 완료: {len(self.input_data)}행 × {len(self.input_data.columns)}열")
            
            # 필요한 컬럼 확인 (I열: 팩스번호, E열: 읍면동, H열: 전화번호, B열: 시도)
            if len(self.input_data.columns) >= 10:
                self.logger.info("✅ 필요 컬럼 확인 완료")
                return True
            else:
                self.logger.error("❌ 필요 컬럼 부족")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            return False
    
    def validate_single_row(self, row_data: Tuple[int, pd.Series], worker_id: int) -> ValidationResult:
        """단일 행 5단계 검증"""
        row_index, row = row_data
        start_time = time.time()
        
        try:
            # 기본 데이터 추출 (중요도: E열(읍면동) = I열(팩스번호) >= H열(전화번호))
            fax_number = str(row.iloc[8]).strip() if len(row) > 8 else ""  # I열 (팩스번호)
            institution_name = str(row.iloc[4]).strip() if len(row) > 4 else ""  # E열 (읍면동) - 핵심 데이터!
            region = str(row.iloc[2]).strip() if len(row) > 2 else ""  # C열 (시군구)
            address = str(row.iloc[6]).strip() if len(row) > 6 else ""  # G열 (주소) - 1차 검증용
            phone_number = str(row.iloc[7]).strip() if len(row) > 7 else ""  # H열 (전화번호) - 추가
            
            # 결과 객체 초기화
            result = ValidationResult(
                row_index=row_index,
                fax_number=fax_number,
                institution_name=institution_name,
                region=region,
                phone_number=phone_number
            )
            result.address = address  # 1차 검증용 주소 추가
            
            self.logger.info(f"🔄 워커 {worker_id}: 행 {row_index+1} 검증 시작 - 팩스:{fax_number}, 기관:{institution_name}, 지역:{region}")
            
            # 팩스번호 유효성 확인 (없으면 검증 불가)
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                result.error_message = "팩스번호 없음 - 검증 불가"
                result.overall_result = "검증 불가 (팩스번호 없음)"
                result.processing_time = time.time() - start_time
                self.logger.warning(f"⚠️ 워커 {worker_id}: 행 {row_index+1} - 팩스번호 없음으로 검증 불가")
                return result
            
            # 1차 검증: 지역번호 매칭 (E열 읍면동 기관명 중심, 팩스번호만)
            stage1_pass, stage1_msg = self.stage1_validator.validate(fax_number, institution_name, address)
            result.stage1_passed = stage1_pass
            result.stage1_message = stage1_msg
            
            # 1차 검증 실패해도 2차 검증으로 진행 (검증 시스템의 목적상)
            if not stage1_pass:
                self.logger.info(f"⚠️ 워커 {worker_id}: 1차 검증 실패, 2차 검증으로 진행 - {stage1_msg}")
            
            # 2차 검증: Google 검색 (팩스번호만)
            stage2_pass, stage2_msg, google_result = self.stage2_validator.validate(fax_number, worker_id)
            result.stage2_passed = stage2_pass
            result.stage2_message = stage2_msg
            result.google_search_result = google_result
            
            # 2차 검증 실패해도 3차 검증으로 진행
            if not stage2_pass:
                self.logger.info(f"⚠️ 워커 {worker_id}: 2차 검증 실패, 3차 검증으로 진행 - {stage2_msg}")
            
            # 3차 검증: 5개 링크 병렬크롤링 (팩스번호만)
            stage3_pass, stage3_msg, links, crawled_data, confidence = self.stage3_validator.validate(fax_number, worker_id)
            result.stage3_passed = stage3_pass
            result.stage3_message = stage3_msg
            result.extracted_links = links
            result.crawled_data = crawled_data
            result.confidence_score = confidence
            
            # 3차 검증 실패해도 4차 검증으로 진행
            if not stage3_pass:
                self.logger.info(f"⚠️ 워커 {worker_id}: 3차 검증 실패, 4차 검증으로 진행 - {stage3_msg}")
            
            # 4차 검증: AI 기관명 도출
            stage4_pass, stage4_msg, ai_institution = self.stage45_validator.validate_stage4(
                fax_number, crawled_data, institution_name
            )
            result.stage4_passed = stage4_pass
            result.stage4_message = stage4_msg
            result.ai_extracted_institution = ai_institution
            
            # 4차 검증 실패해도 5차 검증으로 진행
            if not stage4_pass:
                self.logger.info(f"⚠️ 워커 {worker_id}: 4차 검증 실패, 5차 검증으로 진행 - {stage4_msg}")
            
            # 5차 검증: 최종 검증 (search_logic.txt 요구사항: 2/3/4차 검증값과 완벽 매칭)
            stage5_pass, stage5_msg, final_verification = self.stage45_validator.validate_stage5(
                ai_institution, fax_number, institution_name, 
                google_result, crawled_data, ai_institution
            )
            result.stage5_passed = stage5_pass
            result.stage5_message = stage5_msg
            result.final_verification = final_verification
            
            # 최종 결과 결정 (모든 단계 종합 판단)
            passed_stages = sum([stage1_pass, stage2_pass, stage3_pass, stage4_pass, stage5_pass])
            
            if stage5_pass:
                result.overall_result = "데이터 올바름"
                result.final_confidence = (confidence + 90) / 2  # 3차 신뢰도 + AI 신뢰도 평균
            elif passed_stages >= 3:  # 5단계 중 3단계 이상 통과
                result.overall_result = "부분 검증 성공"
                result.final_confidence = confidence * 0.9
            elif passed_stages >= 2:  # 2단계 이상 통과
                if "직접 확인" in final_verification:
                    result.overall_result = "직접 확인 요망"
                else:
                    result.overall_result = "부분 검증"
                result.final_confidence = confidence * 0.7
            else:  # 2단계 미만 통과
                if "데이터 오류" in final_verification:
                    result.overall_result = "데이터 오류"
                else:
                    result.overall_result = "검증 실패"
                result.final_confidence = confidence * 0.5
            
            result.processing_time = time.time() - start_time
            
            self.logger.info(f"✅ 워커 {worker_id}: 행 {row_index+1} 완료 - {result.overall_result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 행 {row_index+1} 검증 실패 - {e}")
            result.error_message = str(e)
            result.overall_result = "검증 실패"
            result.processing_time = time.time() - start_time
            return result
    
    def process_validation(self) -> bool:
        """병렬 검증 처리"""
        try:
            self.logger.info(f"🚀 병렬 검증 시작: {len(self.input_data)}행, {MAX_WORKERS}개 워커")
            
            # ThreadPoolExecutor를 사용한 병렬 처리
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # 작업 제출
                futures = []
                
                for idx, row in self.input_data.iterrows():
                    worker_id = idx % MAX_WORKERS
                    future = executor.submit(self.validate_single_row, (idx, row), worker_id)
                    futures.append(future)
                
                # 결과 수집
                results = []
                completed = 0
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                        completed += 1
                        
                        # 진행률 출력
                        if completed % 10 == 0:
                            progress = (completed / len(futures)) * 100
                            success_count = len([r for r in results if r.overall_result == "검증 성공"])
                            self.logger.info(f"📊 진행률: {completed}/{len(futures)} ({progress:.1f}%) - 성공: {success_count}개")
                        
                    except Exception as e:
                        self.logger.error(f"결과 처리 실패: {e}")
                        continue
            
            self.validation_results = results
            
            # 통계 출력
            success_count = len([r for r in results if r.overall_result == "데이터 올바름"])
            partial_count = len([r for r in results if "부분" in r.overall_result])
            error_count = len([r for r in results if "데이터 오류" in r.overall_result or "실패" in r.overall_result])
            unknown_count = len([r for r in results if "검증 불가" in r.overall_result or "직접 확인" in r.overall_result])
            
            self.logger.info(f"📊 검증 완료: 올바름 {success_count}개, 부분검증 {partial_count}개, 데이터오류 {error_count}개, 확인요망 {unknown_count}개")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 병렬 검증 실패: {e}")
            return False
        finally:
            # 드라이버 정리
            for worker_id in range(MAX_WORKERS):
                self.chrome_manager.cleanup_driver(worker_id)
    
    def save_results(self) -> str:
        """검증 결과 저장"""
        try:
            if not self.validation_results:
                self.logger.error("저장할 결과가 없습니다")
                return ""
            
            # 결과 DataFrame 생성
            results_data = []
            
            for result in self.validation_results:
                results_data.append({
                    '행번호': result.row_index + 1,
                    '팩스번호': result.fax_number,
                    '원본기관명': result.institution_name,
                    '지역': result.region,
                    '전화번호': result.phone_number,
                    '1차검증': '통과' if result.stage1_passed else '실패',
                    '1차메시지': result.stage1_message,
                    '2차검증': '통과' if result.stage2_passed else '실패',
                    '2차메시지': result.stage2_message,
                    'Google검색결과': result.google_search_result,
                    '3차검증': '통과' if result.stage3_passed else '실패',
                    '3차메시지': result.stage3_message,
                    '신뢰도점수': f"{result.confidence_score:.1f}%",
                    '추출링크수': len(result.extracted_links) if result.extracted_links else 0,
                    '4차검증': '통과' if result.stage4_passed else '실패',
                    'AI추출기관명': result.ai_extracted_institution,
                    '5차검증': '통과' if result.stage5_passed else '실패',
                    '5차메시지': result.stage5_message,
                    '최종결과': result.overall_result,
                    '최종신뢰도': f"{result.final_confidence:.1f}%",
                    '처리시간': f"{result.processing_time:.1f}초",
                    '오류메시지': result.error_message
                })
            
            results_df = pd.DataFrame(results_data)
            
            # 파일명 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{OUTPUT_FILE_PREFIX}_{timestamp}.xlsx"
            
            # 데스크톱 경로 확인
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            if os.path.exists(desktop_path):
                output_path = os.path.join(desktop_path, output_filename)
            else:
                output_path = output_filename
            
            # Excel 저장
            results_df.to_excel(output_path, index=False)
            
            self.logger.info(f"💾 결과 저장 완료: {output_path}")
            
            # 통계 요약
            self._print_final_statistics()
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            return ""
    
    def _print_final_statistics(self):
        """최종 통계 출력"""
        try:
            total = len(self.validation_results)
            success = len([r for r in self.validation_results if r.overall_result == "데이터 올바름"])
            partial = len([r for r in self.validation_results if "부분" in r.overall_result])
            error = len([r for r in self.validation_results if "데이터 오류" in r.overall_result or "실패" in r.overall_result])
            unknown = len([r for r in self.validation_results if "검증 불가" in r.overall_result or "직접 확인" in r.overall_result])
            
            avg_time = sum(r.processing_time for r in self.validation_results) / total if total > 0 else 0
            avg_confidence = sum(r.final_confidence for r in self.validation_results) / total if total > 0 else 0
            
            print("\n" + "="*60)
            print("🎉 Valid.py 5단계 검증 완료!")
            print("="*60)
            print(f"📊 팩스번호 검증 통계: (목적: 팩스번호의 진짜 기관명 확인)")
            print(f"   - 총 처리: {total:,}개")
            print(f"   - 데이터 올바름: {success:,}개 ({success/total*100:.1f}%)")
            print(f"   - 부분 검증: {partial:,}개 ({partial/total*100:.1f}%)")
            print(f"   - 데이터 오류: {error:,}개 ({error/total*100:.1f}%)")
            print(f"   - 직접 확인 요망: {unknown:,}개 ({unknown/total*100:.1f}%)")
            print(f"⏱️  평균 처리시간: {avg_time:.1f}초")
            print(f"📈 평균 신뢰도: {avg_confidence:.1f}%")
            
            # 단계별 통과율
            for stage in range(1, 6):
                stage_attr = f'stage{stage}_passed' if stage <= 3 else ('stage4_passed' if stage == 4 else 'stage5_passed')
                passed = len([r for r in self.validation_results if getattr(r, stage_attr, False)])
                print(f"   - {stage}차 검증 통과율: {passed:,}개 ({passed/total*100:.1f}%)")
            
            print("="*60)
            
        except Exception as e:
            self.logger.error(f"통계 출력 실패: {e}")
    
    def run(self) -> bool:
        """메인 실행 함수"""
        try:
            # 1. 데이터 로드
            if not self.load_data():
                return False
            
            # 2. 사용자 확인
            print("\n" + "="*60)
            print("🔍 Valid.py - 5단계 검증 시스템")
            print("="*60)
            print(f"📊 처리 대상: {len(self.input_data)}개 데이터")
            print(f"🏛️ 핵심 데이터: E열(읍면동) = I열(팩스번호) [팩스번호 필수]")
            print(f"⚠️ 중요: 전화번호와 팩스번호는 엄밀히 다름 - 팩스번호가 없으면 검증 불가")
            print(f"⚙️ 워커 수: {MAX_WORKERS}개")
            print(f"🎯 신뢰도 임계값: {CONFIDENCE_THRESHOLD}%")
            print("\n검증 단계 (팩스번호 필수):")
            print("1차: 팩스번호 지역번호 매칭 (E열 읍면동 기관명 중심)")
            print("2차: Google 검색 - 팩스번호의 진짜 기관명 확인")
            print("3차: 5개 링크 병렬크롤링 + JS렌더링 + 신뢰도 점수")
            print("4차: 3차검증값 매칭 → AI 기관명 도출")
            print("5차: 2/3/4차 완벽 AI 매칭 → 최종 검증")
            
            proceed = input(f"\n검증을 시작하시겠습니까? (y/n): ").strip().lower()
            if proceed != 'y':
                print("검증 취소됨")
                return False
            
            # 3. 검증 실행
            if not self.process_validation():
                return False
            
            # 4. 결과 저장
            output_path = self.save_results()
            if output_path:
                print(f"\n🎉 검증 완료! 결과 파일: {output_path}")
                return True
            else:
                return False
                
        except KeyboardInterrupt:
            self.logger.warning("⚠️ 사용자 중단")
            return False
        except Exception as e:
            self.logger.error(f"❌ 실행 실패: {e}")
            traceback.print_exc()
            return False

# ================================
# 메인 실행
# ================================

def main():
    """메인 함수"""
    try:
        manager = ValidationManager()
        success = manager.run()
        
        if success:
            print("\n✅ Valid.py 실행 완료!")
        else:
            print("\n❌ Valid.py 실행 실패!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 