#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
진보된 데이터 검증 및 크롤링 시스템 (comp.py)
SystemAnalyzer 기반 동적 워커 관리, UA 로테이션, 실시간 헤드리스 토글 지원

처리 대상: rawdatafile/failed_data_250809.csv (3,557행)
출력: G/H/J/K 컬럼 + N열부터 검증값/링크/AI응답
"""

import os, sys, json, time, logging, traceback, threading
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import socket
# import keyboard  # 실시간 헤드리스 토글용 (선택적)

from dotenv import load_dotenv

# 기존 유틸리티 import
from utils.valid.phone_validator import PhoneValidator
from utils.system.web_driver_manager import WebDriverManager
from utils.system.system_analyzer import SystemAnalyzer
from utils.valid.verification_engine import VerificationEngine
from utils.crawler.google_search_engine import GoogleSearchEngine
from utils.crawler.homepage_crawler import HomepageCrawler
from utils.crawler.prt.user_agent_rotator import UserAgentRotator
from utils.ai_model_manager import AIModelManager
from utils.data.data_processor import DataProcessor
from utils.data.excel_processor import ExcelProcessor

# 설정 import
from config.performance_profiles import PerformanceManager
from config.crawling_settings import CrawlingSettings
from config.settings import get_optimal_config, CRAWLING_PARAMS


class AdvancedPortManager:
    """Valid3.py 방식: 고급 포트 관리 시스템 (4개 포트만 엄격 관리)"""
    
    def __init__(self, logger):
        """AdvancedPortManager 초기화 (4개 포트 전용)"""
        self.logger = logger
        
        # 포트 범위 설정 (4개만 엄격히 관리)
        self.port_range_start = 9222
        self.port_range_end = 9226  # 9222-9225 (4개만)
        self.available_ports = set(range(self.port_range_start, self.port_range_end))
        self.used_ports = set()
        self.blacklisted_ports = set()  # 차단된 포트들
        self.port_assignments = {}  # 워커별 포트 할당 기록
        
        # 포트 사용 통계
        self.allocation_count = 0
        self.release_count = 0
        
        self.logger.info(f"🔌 AdvancedPortManager 초기화: {len(self.available_ports)}개 포트 관리 ({self.port_range_start}-{self.port_range_end-1})")
    
    def allocate_port(self, worker_id: int) -> int:
        """워커에게 포트 할당 (4개 제한)"""
        try:
            # 워커 ID를 0~3으로 제한
            limited_worker_id = worker_id % 4
            
            # 이미 할당된 포트가 있으면 재사용
            if limited_worker_id in self.port_assignments:
                existing_port = self.port_assignments[limited_worker_id]
                if existing_port not in self.blacklisted_ports and self._is_port_available(existing_port):
                    self.logger.debug(f"🔌 워커 {worker_id} (제한:{limited_worker_id}): 기존 포트 {existing_port} 재사용")
                    return existing_port
                else:
                    # 블랙리스트에 있거나 사용 불가하면 해제하고 새로 할당
                    self.logger.warning(f"⚠️ 워커 {limited_worker_id}: 기존 포트 {existing_port} 문제됨, 새 포트 할당")
                    self.release_port(existing_port, limited_worker_id)
            
            # 사용 가능한 포트 찾기
            available_ports = self.available_ports - self.used_ports - self.blacklisted_ports
            
            if not available_ports:
                # 사용 가능한 포트가 없으면 강제로 오래된 포트 해제
                if self.used_ports:
                    oldest_port = min(self.used_ports)
                    self.logger.warning(f"⚠️ 사용 가능한 포트 없음, 강제 해제: {oldest_port}")
                    self.release_port(oldest_port)
                    available_ports = self.available_ports - self.used_ports - self.blacklisted_ports
                
                if not available_ports:
                    # 그래도 없으면 블랙리스트 일부 해제
                    if self.blacklisted_ports:
                        released_port = self.blacklisted_ports.pop()
                        self.logger.warning(f"⚠️ 블랙리스트 포트 해제: {released_port}")
                        available_ports = {released_port}
                    else:
                        # 최후의 수단: 기본 포트 사용
                        emergency_port = self.port_range_start + limited_worker_id
                        self.logger.error(f"🚨 긴급 포트 할당: {emergency_port}")
                        return emergency_port
            
            # 포트 할당
            allocated_port = min(available_ports)  # 가장 작은 번호부터 사용
            self.used_ports.add(allocated_port)
            self.port_assignments[limited_worker_id] = allocated_port
            self.allocation_count += 1
            
            self.logger.debug(f"🔌 워커 {worker_id} (제한:{limited_worker_id}): 포트 {allocated_port} 새로 할당 (총 사용중: {len(self.used_ports)}/4)")
            return allocated_port
            
        except Exception as e:
            self.logger.error(f"❌ 포트 할당 실패 (워커 {worker_id}): {e}")
            # 긴급 포트 반환
            emergency_port = self.port_range_start + (worker_id % 4)
            self.logger.warning(f"🚨 긴급 포트 할당: {emergency_port}")
            return emergency_port
    
    def release_port(self, port: int, worker_id: int = None):
        """포트 즉시 해제"""
        try:
            if port in self.used_ports:
                self.used_ports.remove(port)
                self.release_count += 1
                
                # 워커 할당 기록에서 제거
                if worker_id is not None and worker_id in self.port_assignments:
                    if self.port_assignments[worker_id] == port:
                        del self.port_assignments[worker_id]
                else:
                    # worker_id가 없으면 전체 할당 기록에서 찾아서 제거
                    for wid, assigned_port in list(self.port_assignments.items()):
                        if assigned_port == port:
                            del self.port_assignments[wid]
                            break
                
                self.logger.debug(f"🔓 포트 {port} 즉시 해제 완료 (남은 사용중: {len(self.used_ports)}/4)")
            else:
                self.logger.debug(f"⚠️ 포트 {port} 이미 해제됨")
                
        except Exception as e:
            self.logger.error(f"❌ 포트 해제 실패 ({port}): {e}")
    
    def blacklist_port(self, port: int, reason: str = "차단됨"):
        """포트를 블랙리스트에 추가"""
        try:
            self.blacklisted_ports.add(port)
            if port in self.used_ports:
                self.used_ports.remove(port)
            
            self.logger.warning(f"🚫 포트 {port} 블랙리스트 추가: {reason}")
            
        except Exception as e:
            self.logger.error(f"❌ 포트 블랙리스트 실패 ({port}): {e}")
    
    def _is_port_available(self, port: int) -> bool:
        """포트 사용 가능 여부 확인"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex(('localhost', port))
                return result != 0  # 포트가 사용 중이 아님
        except:
            return False
    
    def release_all_ports(self):
        """모든 포트 해제"""
        try:
            released_count = len(self.used_ports)
            self.used_ports.clear()
            self.port_assignments.clear()
            self.logger.info(f"🔓 모든 포트 해제 완료: {released_count}개")
        except Exception as e:
            self.logger.error(f"❌ 모든 포트 해제 실패: {e}")
    
    def get_port_status(self) -> Dict:
        """포트 상태 정보 반환"""
        return {
            'total_ports': len(self.available_ports),
            'used_ports': len(self.used_ports),
            'available_ports': len(self.available_ports) - len(self.used_ports) - len(self.blacklisted_ports),
            'blacklisted_ports': len(self.blacklisted_ports),
            'port_assignments': dict(self.port_assignments),
            'allocation_count': self.allocation_count,
            'release_count': self.release_count
        }


@dataclass
class CompValidationResult:
    """comp.py용 검증 결과 구조체"""
    row_index: int
    
    # 원본 데이터
    region: str  # 시도
    district: str  # 시군구  
    institution_name: str  # 읍면동 (기관명)
    address: str  # 주소
    phone: str  # 전화번호
    fax: str  # 팩스번호
    
    # G/H/J/K 컬럼 결과
    phone_real_institution: str = ""  # G: 전화번호의 실제기관
    phone_verified: str = ""  # H: 올바른 전화번호
    fax_real_institution: str = ""  # J: 팩스번호의 실제기관  
    fax_verified: str = ""  # K: 올바른 팩스번호
    
    # N~S 컬럼: 1~6차 검증값
    validation_1st: str = ""  # N: 1차검증값 (지역번호 매칭)
    validation_2nd: str = ""  # O: 2차검증값 (구글 검색)
    validation_3rd: str = ""  # P: 3차검증값 (링크 수집)
    validation_4th: str = ""  # Q: 4차검증값 (병렬 크롤링)
    validation_5th: str = ""  # R: 5차검증값 (AI 기관명 도출)
    validation_6th: str = ""  # S: 6차검증값 (종합 매칭)
    
    # T~X 컬럼: 추출 링크들
    extracted_links: List[str] = field(default_factory=list)
    
    # Y~AC 컬럼: 링크별 AI 응답
    ai_responses: List[str] = field(default_factory=list)
    
    # 처리 메타데이터
    processing_time: float = 0.0
    error_message: str = ""
    success: bool = False


class CompCrawlingSystem:
    """SystemAnalyzer 기반 진보된 크롤링 시스템"""
    
    def __init__(self):
        """시스템 초기화"""
        self.setup_logging()
        self.load_environment()
        self.initialize_components()
        self.setup_headless_toggle()
        
        self.logger.info("🚀 CompCrawlingSystem 초기화 완료")
    
    def setup_logging(self):
        """로깅 설정"""
        # logs 디렉토리 생성
        logs_dir = "logs"
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{logs_dir}/comp_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("CompCrawling")
    
    def load_environment(self):
        """환경변수 로드"""
        load_dotenv()
        
        # Gemini API 키 로드 (4개)
        self.gemini_keys = []
        for i in range(1, 5):
            key = os.getenv(f'GEMINI_API_KEY_{i}')
            if key:
                self.gemini_keys.append(key)
        
        if not self.gemini_keys:
            raise ValueError("❌ GEMINI_API_KEY를 찾을 수 없습니다.")
        
        self.logger.info(f"🔑 Gemini API 키 {len(self.gemini_keys)}개 로드 완료")
    
    def initialize_components(self):
        """핵심 컴포넌트 초기화"""
        # SystemAnalyzer로 동적 워커 관리 (단일 초기화)
        self.system_analyzer = SystemAnalyzer(self.logger)
        self.system_analyzer.start_monitoring()
        # 엄격한 제한: 최대 4개 워커로 고정
        self.max_workers = 4
        
        # 성능 관리자
        self.performance_manager = PerformanceManager()
        self.crawling_settings = CrawlingSettings()
        
        # UA 로테이터
        self.ua_rotator = UserAgentRotator(self.logger)
        
        # 검증 엔진들
        self.phone_validator = PhoneValidator()
        self.verification_engine = VerificationEngine()
        self.google_search_engine = GoogleSearchEngine()
        self.homepage_crawler = HomepageCrawler()
        
        # Valid3.py 방식의 드라이버 관리 (워커별 WebDriverManager + 포트 관리)
        self.web_driver_managers = {}  # 워커별 WebDriverManager 딕셔너리
        self.driver_lock = threading.Lock()  # 드라이버 생성/해제 시 동기화
        self.max_pool_size = 4  # 엄격히 4개로 고정
        
        # 포트 관리자 추가
        self.port_manager = AdvancedPortManager(self.logger)
        
        # AI 모델 관리자 (키 로테이션)
        self.ai_manager = AIModelManager()
        self.current_key_index = 0
        
        # 데이터 처리
        self.data_processor = DataProcessor()
        self.excel_processor = ExcelProcessor()
        
        # 상태 관리
        self.is_headless = False  # 기본값: 헤드리스 OFF
        self.processed_count = 0
        self.batch_size = 20  # 배치 크기도 줄여서 안정성 확보
        
        self.logger.info("⚙️ 모든 컴포넌트 초기화 완료")
        self.logger.info(f"🔧 엄격한 제한: 최대 워커 {self.max_workers}개, 드라이버 풀 {self.max_pool_size}개 고정")
        self.logger.info(f"📦 배치 크기: {self.batch_size}개 (안정성 우선)")
        self.logger.info(f"🛡️ 드라이버 무한증식 방지: 생성 완료 후 재사용 모드")
    
    def setup_headless_toggle(self):
        """실시간 헤드리스 토글 설정"""
        self.headless_lock = threading.Lock()
        
        # 헤드리스 토글 함수 (수동 호출용)
        def toggle_headless():
            with self.headless_lock:
                self.is_headless = not self.is_headless
                mode = "ON" if self.is_headless else "OFF"
                self.logger.info(f"🖥️ 헤드리스 모드 {mode} 전환")
                
                # 기존 드라이버들 재시작 (새 헤드리스 설정 적용)
                self._restart_all_drivers()
        
        # 토글 함수를 인스턴스 변수로 저장 (나중에 수동 호출 가능)
        self.toggle_headless = toggle_headless
        
        # 키보드 라이브러리가 없어도 동작하도록 수정
        try:
            import keyboard
            
            # 백그라운드에서 키 입력 감지
            def key_listener():
                try:
                    while True:
                        if keyboard.is_pressed('h'):
                            toggle_headless()
                            time.sleep(1)  # 연속 입력 방지
                        time.sleep(0.1)
                except:
                    pass  # 키보드 후킹 실패시 무시
            
            threading.Thread(target=key_listener, daemon=True).start()
            self.logger.info("⌨️ 헤드리스 토글 리스너 활성화 ('h' 키로 전환)")
        except ImportError:
            self.logger.info("⌨️ keyboard 라이브러리 없음 - 수동 토글만 지원 (system.toggle_headless() 호출)")
    
    def _restart_all_drivers(self):
        """Valid3.py 방식: 모든 워커 드라이버 재시작"""
        with self.driver_lock:
            old_managers_count = len(self.web_driver_managers)
            
            # 모든 기존 WebDriverManager 정리
            self.cleanup_all_drivers()
            
            self.logger.info(f"🔄 {old_managers_count}개 WebDriverManager 재시작 (헤드리스 모드 적용)")
    

    
    def get_driver_for_worker(self, worker_id: int):
        """Valid3.py 방식: 워커별 WebDriverManager 인스턴스 획득 (엄격히 4개 제한 + 드라이버 재사용)"""
        with self.driver_lock:
            # 워커 ID를 0~3으로 제한 (4개로 엄격히 고정)
            limited_worker_id = worker_id % 4
            
            # 기존 WebDriverManager 확인 및 재사용
            if limited_worker_id in self.web_driver_managers:
                web_manager = self.web_driver_managers[limited_worker_id]
                
                # 기존 드라이버가 살아있는지 확인
                if hasattr(web_manager, 'driver') and web_manager.driver:
                    try:
                        # 드라이버 상태 체크 (간단한 명령 실행)
                        web_manager.driver.execute_script("return document.readyState;")
                        self.logger.debug(f"🔄 워커 {worker_id} (제한:{limited_worker_id}): 기존 드라이버 재사용")
                        return web_manager
                    except:
                        # 드라이버가 죽었으면 새로 생성할 예정
                        self.logger.debug(f"⚠️ 워커 {limited_worker_id}: 기존 드라이버 비정상, 재생성 필요")
                        pass
                
                # WebDriverManager는 있지만 드라이버가 없으면 재사용
                self.logger.debug(f"🔄 워커 {worker_id} (제한:{limited_worker_id}): 기존 WebDriverManager 재사용")
                return web_manager
            else:
                # 새로운 WebDriverManager 생성 (최대 4개까지만)
                if len(self.web_driver_managers) >= 4:
                    # 이미 4개가 있으면 기존 것 재사용 (Round-robin)
                    existing_worker_id = list(self.web_driver_managers.keys())[limited_worker_id % len(self.web_driver_managers)]
                    self.logger.debug(f"🔄 워커 {worker_id} -> 기존 워커 {existing_worker_id} 재사용 (4개 제한)")
                    return self.web_driver_managers[existing_worker_id]
                
                self.logger.debug(f"🔧 워커 {limited_worker_id} WebDriverManager 새로 생성 중... ({len(self.web_driver_managers)+1}/4)")
                new_manager = WebDriverManager(logger=self.logger)
                self.web_driver_managers[limited_worker_id] = new_manager
                self.logger.debug(f"✅ 워커 {limited_worker_id} WebDriverManager 생성 완료")
                
                return new_manager
    
    def get_worker_driver(self, worker_id: int):
        """워커별 실제 드라이버 인스턴스 반환 (드라이버 재사용 + 포트 관리)"""
        try:
            # WebDriverManager 획득
            web_manager = self.get_driver_for_worker(worker_id)
            limited_worker_id = worker_id % 4  # 0~3으로 제한
            
            # 기존 드라이버가 살아있으면 바로 반환 (무한증식 방지!)
            if hasattr(web_manager, 'driver') and web_manager.driver:
                try:
                    # 드라이버 상태 체크
                    web_manager.driver.execute_script("return document.readyState;")
                    self.logger.debug(f"♻️ 워커 {worker_id} (제한:{limited_worker_id}): 기존 드라이버 재사용 (무한증식 방지)")
                    return web_manager.driver
                except:
                    # 드라이버가 죽었으면 새로 생성
                    self.logger.debug(f"💀 워커 {limited_worker_id}: 기존 드라이버 비정상, 새로 생성")
                    web_manager.driver = None
            
            # 새 드라이버 생성이 필요한 경우에만 포트 할당
            assigned_port = self.port_manager.allocate_port(limited_worker_id)
            
            # 헤드리스 설정에 따라 드라이버 생성
            with self.headless_lock:
                headless = self.is_headless
            
            try:
                if headless:
                    # 헤드리스 드라이버 (포트 지정)
                    driver = web_manager._try_headless_chrome(limited_worker_id, assigned_port)
                else:
                    # 일반 드라이버 (봇 감지 우회, 포트 지정)
                    driver = web_manager.create_bot_evasion_driver(limited_worker_id, assigned_port)
                
                # WebDriverManager에 드라이버 저장 (재사용을 위해)
                web_manager.driver = driver
                
                # User-Agent 설정
                try:
                    user_agent = self.ua_rotator.get_random_user_agent()
                    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                        "userAgent": user_agent
                    })
                except Exception as ua_error:
                    self.logger.warning(f"UA 설정 실패 (무시): {ua_error}")
                
                self.logger.info(f"🆕 워커 {worker_id} (제한:{limited_worker_id}) 새 드라이버 생성 ({'헤드리스' if headless else '일반'}, 포트:{assigned_port})")
                return driver
                
            except Exception as driver_error:
                # 드라이버 생성 실패시 포트 해제
                self.port_manager.release_port(assigned_port, limited_worker_id)
                self.logger.error(f"❌ 워커 {worker_id} 드라이버 생성 실패, 포트 {assigned_port} 해제: {driver_error}")
                
                # 포트를 블랙리스트에 추가하고 재시도
                self.port_manager.blacklist_port(assigned_port, f"드라이버 생성 실패: {driver_error}")
                
                # 다른 포트로 재시도 (1회만)
                retry_port = self.port_manager.allocate_port(limited_worker_id)
                if retry_port != assigned_port:
                    self.logger.warning(f"🔄 워커 {worker_id} 다른 포트로 재시도: {retry_port}")
                    try:
                        if headless:
                            driver = web_manager._try_headless_chrome(limited_worker_id, retry_port)
                        else:
                            driver = web_manager.create_bot_evasion_driver(limited_worker_id, retry_port)
                        
                        # 재시도 성공시 WebDriverManager에 저장
                        web_manager.driver = driver
                        self.logger.info(f"✅ 워커 {worker_id} 재시도 성공 (포트:{retry_port})")
                        return driver
                    except Exception as retry_error:
                        self.port_manager.release_port(retry_port, limited_worker_id)
                        self.port_manager.blacklist_port(retry_port, f"재시도 실패: {retry_error}")
                        raise
                else:
                    raise
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} 드라이버 획득 실패: {e}")
            raise
    
    def load_csv_data(self, file_path: str) -> pd.DataFrame:
        """CSV 데이터 로드 및 전처리"""
        try:
            # CSV 로드
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            self.logger.info(f"📊 CSV 로드 완료: {len(df)}행")
            
            # 컬럼명 정리 (공백 제거)
            df.columns = [col.strip() for col in df.columns]
            
            # 컬럼 매핑
            column_mapping = {
                '시도': 'region',
                '시군구': 'district', 
                '읍면동': 'institution_name',
                '주    소': 'address',
                '전화번호': 'phone',
                '팩스번호': 'fax'
            }
            
            # 존재하는 컬럼만 매핑
            actual_mapping = {}
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    actual_mapping[old_name] = new_name
            
            df = df.rename(columns=actual_mapping)
            
            # 데이터 정리
            for col in ['region', 'district', 'institution_name', 'address']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].replace('nan', '').replace('#N/A', '')
            
            # 전화/팩스 정규화
            for col in ['phone', 'fax']:
                if col in df.columns:
                    df[col] = df[col].astype(str).apply(self._normalize_phone_number)
            
            # 빈 행 제거
            df = df.dropna(subset=['institution_name', 'fax'], how='all')
            
            self.logger.info(f"✅ 데이터 전처리 완료: {len(df)}행")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ CSV 로드 실패: {e}")
            raise
    
    def _normalize_phone_number(self, phone: str) -> str:
        """전화번호 정규화"""
        if not phone or phone in ['nan', '#N/A', '']:
            return ""
        
        # 숫자만 추출
        import re
        digits = re.sub(r'[^\d]', '', str(phone))
        
        if len(digits) < 8:
            return ""
        
        return digits
    
    def process_validation_pipeline(self, row_data: pd.Series, row_index: int, worker_id: int) -> CompValidationResult:
        """단일 행 검증 파이프라인 실행"""
        start_time = time.time()
        
        result = CompValidationResult(
            row_index=row_index,
            region=row_data.get('region', ''),
            district=row_data.get('district', ''), 
            institution_name=row_data.get('institution_name', ''),
            address=row_data.get('address', ''),
            phone=row_data.get('phone', ''),
            fax=row_data.get('fax', '')
        )
        
        try:
            # 1차 검증: 지역번호 매칭
            self._validate_stage1(result, worker_id)
            
            # 2차 검증: 구글 검색
            self._validate_stage2(result, worker_id)
            
            # 3차 검증: 링크 수집  
            self._validate_stage3(result, worker_id)
            
            # 4차 검증: 병렬 크롤링
            self._validate_stage4(result, worker_id)
            
            # 5차 검증: AI 기관명 도출
            self._validate_stage5(result, worker_id)
            
            # 6차 검증: 종합 매칭
            self._validate_stage6(result, worker_id)
            
            result.success = True
            
        except Exception as e:
            self.logger.error(f"❌ 행 {row_index} 검증 실패: {e}")
            result.error_message = str(e)
            result.success = False
        
        result.processing_time = time.time() - start_time
        return result
    
    def _validate_stage1(self, result: CompValidationResult, worker_id: int):
        """1차 검증: 지역번호 매칭"""
        try:
            if not result.fax or not result.address:
                result.validation_1st = "데이터 부족"
                return
            
            # 정규화된 팩스번호로 지역 일치성 검사
            is_match = self.phone_validator.is_regional_match(result.fax, result.address)
            
            if is_match:
                result.validation_1st = "지역 일치"
            else:
                result.validation_1st = "지역 불일치"
                
        except Exception as e:
            result.validation_1st = f"검증 오류: {e}"
    
    def _validate_stage2(self, result: CompValidationResult, worker_id: int):
        """2차 검증: 구글 검색 (UA 회피) - search_logic.txt 4-8행 구현"""
        try:
            driver = self.get_worker_driver(worker_id)
            
            # 팩스번호로 구글 검색 ("{ numbers } 팩스번호는 어디기관?")
            search_query = f"fax {result.fax}"
            
            # Google 검색 시도
            search_results = self.google_search_engine.search(
                query=search_query,
                driver=driver,
                max_results=5
            )
            
            # 검색 결과 없으면 HTTP 요청으로 Naver, Daum 추가 검색
            if not search_results:
                search_results = self._fallback_search_engines(search_query, result.fax)
            
            if search_results:
                result.validation_2nd = f"검색 성공: {len(search_results)}개 결과"
                
                # 스니펫에서 관련 링크들 추출하여 저장 (최대 5개)
                extracted_links = self._extract_links_from_search_results(search_results)
                
                # 첫 번째 결과에서 기관명 추출 시도
                first_result = search_results[0]
                institution = self._extract_institution_from_snippet(
                    first_result.get('snippet', ''), 
                    result.fax
                )
                
                if institution:
                    result.fax_real_institution = institution
                    result.fax_verified = result.fax
                    
                # 전화번호도 동일하게 검색하여 G/H 컬럼 채우기
                if result.phone:
                    phone_institution, phone_verified = self._search_phone_number(result.phone, worker_id)
                    if phone_institution:
                        result.phone_real_institution = phone_institution
                        result.phone_verified = phone_verified
            else:
                result.validation_2nd = "모든 검색엔진에서 결과 없음"
                
        except Exception as e:
            result.validation_2nd = f"검색 오류: {e}"
    
    def _validate_stage3(self, result: CompValidationResult, worker_id: int):
        """3차 검증: 링크 수집 (상위 5개) - search_logic.txt 10-13행 구현"""
        try:
            # 2차 검증에서 이미 링크가 수집되었으면 재사용
            if hasattr(result, '_stage2_links') and result._stage2_links:
                result.extracted_links = result._stage2_links[:5]
                result.validation_3rd = f"2차에서 수집된 링크 {len(result.extracted_links)}개 활용"
                return
            
            driver = self.get_worker_driver(worker_id)
            
            # 더 구체적인 검색으로 링크 수집
            search_queries = [
                f"fax {result.fax}",
                f"{result.fax} {result.institution_name}",
                f"{result.institution_name} {result.district} 팩스번호"
            ]
            
            all_links = []
            
            for query in search_queries:
                if len(all_links) >= 5:
                    break
                    
                search_results = self.google_search_engine.search(
                    query=query,
                    driver=driver,
                    max_results=3
                )
                
                for res in search_results:
                    url = res.get('url', '')
                    if url and url.startswith('http') and url not in all_links:
                        all_links.append(url)
                        if len(all_links) >= 5:
                            break
            
            result.extracted_links = all_links[:5]
            
            if result.extracted_links:
                result.validation_3rd = f"링크 {len(result.extracted_links)}개 수집 완료"
            else:
                result.validation_3rd = "유효한 링크 수집 실패"
                
        except Exception as e:
            result.validation_3rd = f"링크 수집 오류: {e}"
    
    def _validate_stage4(self, result: CompValidationResult, worker_id: int):
        """4차 검증: 병렬 크롤링 - search_logic.txt 10-14행 구현"""
        try:
            if not result.extracted_links:
                result.validation_4th = "크롤링할 링크 없음"
                return
            
            # 엄격히 4개로 제한된 워커 사용
            max_workers = min(len(result.extracted_links), 4)
            
            # 병렬 크롤러 활성화: 각 워커가 링크로 들어감
            crawl_results = []
            confidence_scores = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_link = {
                    executor.submit(self._advanced_crawl_single_link, link, result.fax, result.institution_name, worker_id + i): link
                    for i, link in enumerate(result.extracted_links)
                }
                
                for future in as_completed(future_to_link):
                    link = future_to_link[future]
                    try:
                        crawl_data = future.result(timeout=30)  # 30초 타임아웃
                        crawl_results.append(crawl_data)
                        confidence_scores.append(crawl_data.get('confidence_score', 0.0))
                    except Exception as e:
                        self.logger.warning(f"⚠️ 링크 크롤링 실패 {link}: {e}")
                        crawl_results.append({
                            'url': link, 
                            'error': str(e),
                            'confidence_score': 0.0
                        })
                        confidence_scores.append(0.0)
            
            # 크롤링 결과 분석 및 신뢰도 점수 계산
            success_count = sum(1 for r in crawl_results if 'error' not in r)
            avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
            
            result.validation_4th = f"크롤링: {success_count}/{len(result.extracted_links)} 성공 (신뢰도: {avg_confidence:.1f}%)"
            
            # 크롤링된 데이터를 result에 저장 (5차 검증용)
            result._crawled_data = crawl_results
            result._avg_confidence = avg_confidence
            
            # AI 응답을 위한 데이터 준비
            self._prepare_ai_responses(result, crawl_results)
            
        except Exception as e:
            result.validation_4th = f"크롤링 오류: {e}"
    
    def _crawl_single_link(self, url: str, fax: str, institution: str) -> Dict:
        """단일 링크 크롤링"""
        try:
            # VerificationEngine으로 고급 파싱
            parsed_data = self.verification_engine.parse_homepage(
                url=url,
                target_phone=fax,
                institution_name=institution
            )
            
            return {
                'url': url,
                'title': parsed_data.get('title', ''),
                'content': parsed_data.get('content', ''),
                'phone_numbers': parsed_data.get('phone_numbers', []),
                'confidence': parsed_data.get('confidence_score', 0.0)
            }
            
        except Exception as e:
            return {'url': url, 'error': str(e)}
    
    def _prepare_ai_responses(self, result: CompValidationResult, crawl_results: List[Dict]):
        """AI 응답 준비 (각 링크별)"""
        ai_responses = []
        
        for i, crawl_data in enumerate(crawl_results):
            try:
                if 'error' in crawl_data:
                    ai_responses.append(f"크롤링 실패: {crawl_data['error']}")
                    continue
                
                # AI에게 분석 요청
                context = f"""
                URL: {crawl_data['url']}
                제목: {crawl_data.get('title', '')}
                내용: {crawl_data.get('content', '')[:1000]}...
                
                찾는 팩스번호: {result.fax}
                예상 기관: {result.institution_name}
                """
                
                ai_response = self._get_ai_analysis(context, result.fax, result.institution_name)
                ai_responses.append(ai_response)
                
            except Exception as e:
                ai_responses.append(f"AI 분석 실패: {e}")
        
        # 5개까지 패딩
        while len(ai_responses) < 5:
            ai_responses.append("")
        
        result.ai_responses = ai_responses[:5]
    
    def _get_ai_analysis(self, context: str, fax: str, institution: str) -> str:
        """AI 분석 요청 (키 로테이션)"""
        try:
            # API 키 로테이션
            api_key = self.gemini_keys[self.current_key_index % len(self.gemini_keys)]
            self.current_key_index += 1
            
            prompt = f"""
            다음 웹페이지 정보를 분석하여 팩스번호 {fax}가 {institution}과 관련이 있는지 판단해주세요:
            
            {context}
            
            다음 형식으로 답변:
            1. 관련성: (높음/보통/낮음/없음)
            2. 발견된 기관명: 
            3. 발견된 팩스번호:
            4. 신뢰도: (0-100%)
            """
            
            # AIModelManager 사용
            response = self.ai_manager.get_gemini_response(prompt, api_key=api_key)
            return response[:200]  # 응답 길이 제한
            
        except Exception as e:
            return f"AI 분석 오류: {e}"
    
    def _validate_stage5(self, result: CompValidationResult, worker_id: int):
        """5차 검증: AI 기관명 도출 - search_logic.txt 16-17행 구현"""
        try:
            # 도출된 팩스번호 -> { numbers } 팩스번호 어디기관? -> 3차 검증값 매칭 -> AI 판단 -> 기관명 도출
            
            # 4차에서 추출된 팩스번호들 수집
            discovered_fax_numbers = self._extract_fax_numbers_from_crawled_data(result)
            
            if discovered_fax_numbers:
                # 각 발견된 팩스번호로 역검색
                reverse_search_results = []
                for fax_num in discovered_fax_numbers:
                    reverse_result = self._reverse_search_fax_to_institution(fax_num, worker_id)
                    reverse_search_results.append(reverse_result)
                
                # AI로 3차 검증값과 매칭 분석
                best_institution = self._ai_analyze_institution_matching(
                    result.fax, 
                    result.institution_name,
                    result.extracted_links,
                    reverse_search_results,
                    getattr(result, '_crawled_data', [])
                )
                
                if best_institution:
                    # 검증된 실제 기관명 업데이트
                    result.fax_real_institution = best_institution
                    result.validation_5th = f"AI 기관명 도출: {best_institution}"
                else:
                    result.validation_5th = "AI 기관명 도출 실패"
            else:
                result.validation_5th = "4차에서 팩스번호 추출 없음"
                
        except Exception as e:
            result.validation_5th = f"AI 도출 오류: {e}"
    
    def _validate_stage6(self, result: CompValidationResult, worker_id: int):
        """6차 검증: 종합 매칭 - search_logic.txt 19-21행 구현"""
        try:
            # { 기관명 } 팩스번호 검색 -> 2/3/4/5 차 검증값과 완벽하게 AI와 매칭시 기관명 도출
            
            if not result.fax_real_institution:
                result.validation_6th = "직접 검색 요망, 검색 및 AI검증실패"
                return
            
            # 도출된 기관명으로 역검색하여 팩스번호 확인
            final_institution = result.fax_real_institution
            reverse_fax_search = self._search_institution_fax(final_institution, result.fax, worker_id)
            
            # 모든 검증 단계 데이터 수집
            all_validation_data = {
                'stage1': result.validation_1st,
                'stage2': result.validation_2nd,
                'stage3': result.validation_3rd,
                'stage4': result.validation_4th,
                'stage5': result.validation_5th,
                'extracted_links': result.extracted_links,
                'ai_responses': result.ai_responses,
                'crawled_data': getattr(result, '_crawled_data', []),
                'confidence': getattr(result, '_avg_confidence', 0.0)
            }
            
            # AI로 완벽 매칭 분석
            perfect_match_result = self._ai_perfect_matching_analysis(
                result.fax,
                result.institution_name,
                final_institution,
                all_validation_data,
                reverse_fax_search
            )
            
            # 최종 판정
            if perfect_match_result.get('is_perfect_match', False):
                confidence = perfect_match_result.get('confidence', 0)
                result.validation_6th = f"완벽 매칭 성공 (신뢰도: {confidence}%)"
                
                # 최종 검증된 데이터 업데이트
                result.fax_verified = perfect_match_result.get('verified_fax', result.fax)
                result.fax_real_institution = perfect_match_result.get('verified_institution', final_institution)
            else:
                failure_reason = perfect_match_result.get('reason', '알 수 없는 오류')
                result.validation_6th = f"직접 검색 요망: {failure_reason}"
                
        except Exception as e:
            result.validation_6th = f"종합 매칭 오류: {e}"
    
    def _extract_institution_from_snippet(self, snippet: str, fax: str) -> str:
        """스니펫에서 기관명 추출"""
        # 간단한 기관명 추출 로직
        keywords = ['센터', '주민센터', '구청', '시청', '동', '면', '읍']
        
        for keyword in keywords:
            if keyword in snippet:
                # 키워드 주변 텍스트에서 기관명 추출
                import re
                pattern = rf'[가-힣\s]*{keyword}[가-힣\s]*'
                match = re.search(pattern, snippet)
                if match:
                    return match.group().strip()
        
        return ""
    
    def _fallback_search_engines(self, query: str, fax: str) -> List[Dict]:
        """HTTP 요청으로 Naver, Daum 추가 검색"""
        try:
            import requests
            from bs4 import BeautifulSoup
            
            results = []
            
            # UA 로테이션 적용
            headers = {'User-Agent': self.ua_rotator.get_random_user_agent()}
            
            # Naver 검색
            try:
                naver_url = f"https://search.naver.com/search.naver?query={query}"
                response = requests.get(naver_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # 네이버 검색 결과 파싱 (간단한 예시)
                    for item in soup.select('.total_tit')[:3]:
                        title = item.get_text(strip=True)
                        url = item.get('href', '')
                        if title and url:
                            results.append({'title': title, 'url': url, 'snippet': title})
            except:
                pass
            
            # Daum 검색
            try:
                daum_url = f"https://search.daum.net/search?q={query}"
                response = requests.get(daum_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # 다음 검색 결과 파싱 (간단한 예시)
                    for item in soup.select('.tit_main')[:3]:
                        title = item.get_text(strip=True)
                        url = item.get('href', '')
                        if title and url:
                            results.append({'title': title, 'url': url, 'snippet': title})
            except:
                pass
            
            return results
            
        except Exception as e:
            self.logger.warning(f"⚠️ 폴백 검색 엔진 실패: {e}")
            return []
    
    def _extract_links_from_search_results(self, search_results: List[Dict]) -> List[str]:
        """검색 결과에서 링크 추출"""
        links = []
        for result in search_results:
            url = result.get('url', '')
            if url and url.startswith('http') and url not in links:
                links.append(url)
                if len(links) >= 5:
                    break
        return links
    
    def _search_phone_number(self, phone: str, worker_id: int) -> Tuple[str, str]:
        """전화번호로 기관 검색 (G/H 컬럼용)"""
        try:
            driver = self.get_worker_driver(worker_id)
            # 큰따옴표 없이 검색쿼리 생성
            search_query = f"{phone} 어디 전화번호"
            
            search_results = self.google_search_engine.search(
                query=search_query,
                driver=driver,
                max_results=3
            )
            
            if search_results:
                first_result = search_results[0]
                institution = self._extract_institution_from_snippet(
                    first_result.get('snippet', ''), 
                    phone
                )
                return institution, phone if institution else ""
            
            return "", ""
            
        except Exception as e:
            self.logger.warning(f"⚠️ 전화번호 검색 실패: {e}")
            return "", ""
    
    def _advanced_crawl_single_link(self, url: str, fax: str, institution: str, worker_id: int) -> Dict:
        """고급 단일 링크 크롤링 (BS4 + JS 렌더링 폴백)"""
        try:
            # VerificationEngine으로 고급 파싱 (BS4 → Selenium 폴백)
            parsed_data = self.verification_engine.parse_homepage(
                url=url,
                target_phone=fax,
                institution_name=institution
            )
            
            # 신뢰도 점수 계산
            confidence = self._calculate_link_confidence(parsed_data, fax, institution)
            
            return {
                'url': url,
                'title': parsed_data.get('title', ''),
                'content': parsed_data.get('content', ''),
                'phone_numbers': parsed_data.get('phone_numbers', []),
                'fax_numbers': parsed_data.get('fax_numbers', []),
                'institution_names': parsed_data.get('institution_names', []),
                'confidence_score': confidence,
                'parsing_method': parsed_data.get('method', 'bs4')
            }
            
        except Exception as e:
            return {'url': url, 'error': str(e), 'confidence_score': 0.0}
    
    def _calculate_link_confidence(self, parsed_data: Dict, target_fax: str, target_institution: str) -> float:
        """링크 크롤링 결과의 신뢰도 점수 계산"""
        confidence = 0.0
        
        # 팩스번호 일치 확인
        fax_numbers = parsed_data.get('fax_numbers', [])
        if target_fax in fax_numbers:
            confidence += 50.0
        
        # 기관명 유사도 확인
        institution_names = parsed_data.get('institution_names', [])
        for inst_name in institution_names:
            similarity = self._calculate_institution_similarity(target_institution, inst_name)
            confidence += similarity * 0.3
        
        # 제목에서 관련성 확인
        title = parsed_data.get('title', '')
        if target_institution in title:
            confidence += 20.0
        
        return min(confidence, 100.0)
    
    def _calculate_institution_similarity(self, original: str, extracted: str) -> float:
        """기관명 유사도 계산"""
        if not original or not extracted:
            return 0.0
        
        # 간단한 문자열 유사도 (Jaccard similarity)
        set1 = set(original)
        set2 = set(extracted)
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return (intersection / union * 100) if union > 0 else 0.0
    
    def _extract_fax_numbers_from_crawled_data(self, result: CompValidationResult) -> List[str]:
        """크롤링된 데이터에서 팩스번호 추출"""
        fax_numbers = []
        crawled_data = getattr(result, '_crawled_data', [])
        
        for data in crawled_data:
            if 'error' not in data:
                # 크롤링된 팩스번호들 수집
                fax_nums = data.get('fax_numbers', [])
                for fax in fax_nums:
                    normalized = self._normalize_phone_number(fax)
                    if normalized and normalized not in fax_numbers:
                        fax_numbers.append(normalized)
        
        return fax_numbers
    
    def _reverse_search_fax_to_institution(self, fax_number: str, worker_id: int) -> Dict:
        """팩스번호로 기관 역검색"""
        try:
            driver = self.get_worker_driver(worker_id)
            search_query = f"fax {fax_number}"
            
            search_results = self.google_search_engine.search(
                query=search_query,
                driver=driver,
                max_results=3
            )
            
            institutions = []
            for result in search_results:
                snippet = result.get('snippet', '')
                institution = self._extract_institution_from_snippet(snippet, fax_number)
                if institution:
                    institutions.append(institution)
            
            return {
                'fax_number': fax_number,
                'found_institutions': institutions,
                'search_count': len(search_results)
            }
            
        except Exception as e:
            return {
                'fax_number': fax_number,
                'error': str(e),
                'found_institutions': []
            }
    
    def _ai_analyze_institution_matching(self, original_fax: str, original_institution: str, 
                                       extracted_links: List[str], reverse_results: List[Dict], 
                                       crawled_data: List[Dict]) -> str:
        """AI로 기관명 매칭 분석"""
        try:
            # 컨텍스트 구성
            context = f"""
            원본 정보:
            - 팩스번호: {original_fax}
            - 예상 기관: {original_institution}
            
            크롤링 결과:
            - 추출된 링크 수: {len(extracted_links)}
            - 크롤링 성공: {sum(1 for d in crawled_data if 'error' not in d)}개
            
            역검색 결과:
            """
            
            for reverse_result in reverse_results:
                institutions = reverse_result.get('found_institutions', [])
                context += f"- {reverse_result['fax_number']}: {', '.join(institutions)}\n"
            
            prompt = f"""
            다음 정보를 바탕으로 가장 적절한 기관명을 선택해주세요:
            
            {context}
            
            답변 형식:
            기관명: [선택된 기관명]
            신뢰도: [0-100]
            사유: [선택 이유]
            """
            
            # API 키 로테이션
            api_key = self.gemini_keys[self.current_key_index % len(self.gemini_keys)]
            self.current_key_index += 1
            
            response = self.ai_manager.get_gemini_response(prompt, api_key=api_key)
            
            # AI 응답에서 기관명 추출
            lines = response.split('\n')
            for line in lines:
                if line.startswith('기관명:'):
                    return line.replace('기관명:', '').strip()
            
            return ""
            
        except Exception as e:
            self.logger.warning(f"⚠️ AI 기관명 분석 실패: {e}")
            return ""
    
    def _search_institution_fax(self, institution_name: str, target_fax: str, worker_id: int) -> Dict:
        """기관명으로 팩스번호 검색"""
        try:
            driver = self.get_worker_driver(worker_id)
            search_query = f"{institution_name} 팩스번호"
            
            search_results = self.google_search_engine.search(
                query=search_query,
                driver=driver,
                max_results=5
            )
            
            found_fax_numbers = []
            for result in search_results:
                snippet = result.get('snippet', '')
                # 스니펫에서 팩스번호 추출
                import re
                fax_pattern = r'[\d\-\(\)\s]{8,}'
                matches = re.findall(fax_pattern, snippet)
                for match in matches:
                    normalized = self._normalize_phone_number(match)
                    if normalized and len(normalized) >= 8:
                        found_fax_numbers.append(normalized)
            
            # 타겟 팩스번호와 일치하는지 확인
            is_match = target_fax in found_fax_numbers
            
            return {
                'institution': institution_name,
                'found_fax_numbers': found_fax_numbers,
                'target_fax': target_fax,
                'is_match': is_match,
                'confidence': 100 if is_match else 0
            }
            
        except Exception as e:
            return {
                'institution': institution_name,
                'error': str(e),
                'is_match': False
            }
    
    def _ai_perfect_matching_analysis(self, original_fax: str, original_institution: str, 
                                    ai_institution: str, all_validation_data: Dict, 
                                    reverse_fax_search: Dict) -> Dict:
        """AI 완벽 매칭 분석"""
        try:
            context = f"""
            완벽 매칭 분석 요청:
            
            원본 데이터:
            - 팩스번호: {original_fax}
            - 기관명: {original_institution}
            
            AI 도출 기관명: {ai_institution}
            
            검증 단계별 결과:
            - 1차 (지역매칭): {all_validation_data['stage1']}
            - 2차 (구글검색): {all_validation_data['stage2']}
            - 3차 (링크수집): {all_validation_data['stage3']}
            - 4차 (병렬크롤링): {all_validation_data['stage4']}
            - 5차 (AI분석): {all_validation_data['stage5']}
            
            역검색 결과: {reverse_fax_search.get('is_match', False)}
            신뢰도: {all_validation_data.get('confidence', 0)}%
            """
            
            prompt = f"""
            다음 정보를 종합하여 완벽한 매칭인지 판단해주세요:
            
            {context}
            
            답변 형식:
            완벽매칭: [예/아니오]
            신뢰도: [0-100]
            검증된팩스: [최종 팩스번호]
            검증된기관: [최종 기관명]
            사유: [판단 근거]
            """
            
            # API 키 로테이션
            api_key = self.gemini_keys[self.current_key_index % len(self.gemini_keys)]
            self.current_key_index += 1
            
            response = self.ai_manager.get_gemini_response(prompt, api_key=api_key)
            
            # 응답 파싱
            result = {
                'is_perfect_match': False,
                'confidence': 0,
                'verified_fax': original_fax,
                'verified_institution': ai_institution,
                'reason': '분석 실패'
            }
            
            lines = response.split('\n')
            for line in lines:
                if line.startswith('완벽매칭:'):
                    result['is_perfect_match'] = '예' in line
                elif line.startswith('신뢰도:'):
                    try:
                        confidence = int(''.join(filter(str.isdigit, line)))
                        result['confidence'] = confidence
                    except:
                        pass
                elif line.startswith('검증된팩스:'):
                    result['verified_fax'] = line.replace('검증된팩스:', '').strip()
                elif line.startswith('검증된기관:'):
                    result['verified_institution'] = line.replace('검증된기관:', '').strip()
                elif line.startswith('사유:'):
                    result['reason'] = line.replace('사유:', '').strip()
            
            return result
            
        except Exception as e:
            self.logger.warning(f"⚠️ AI 완벽 매칭 분석 실패: {e}")
            return {
                'is_perfect_match': False,
                'confidence': 0,
                'verified_fax': original_fax,
                'verified_institution': ai_institution,
                'reason': f'분석 오류: {e}'
            }
    
    def _optimize_batch_processing(self):
        """SystemAnalyzer 기반 배치 처리 최적화"""
        try:
            # 현재 시스템 리소스 상태 확인
            resources = self.system_analyzer.get_current_resources()
            
            if resources:
                memory_usage = resources.get('memory_percent', 0)
                cpu_usage = resources.get('cpu_percent', 0)
                
                # 메모리 사용량 기반 배치 크기 조정
                recommended_batch = self.system_analyzer.get_recommended_batch_size()
                if recommended_batch != self.batch_size:
                    old_size = self.batch_size
                    self.batch_size = recommended_batch
                    self.logger.info(f"📦 배치 크기 조정: {old_size} → {self.batch_size}")
                
                # 리소스 상태 로그
                self.logger.debug(f"📊 시스템 상태: CPU {cpu_usage:.1f}%, 메모리 {memory_usage:.1f}%")
                
        except Exception as e:
            self.logger.warning(f"⚠️ 배치 최적화 실패: {e}")
    
    def _handle_system_overload(self):
        """시스템 과부하 처리"""
        try:
            # 현재 워커 수 감소
            current_workers = self.system_analyzer.current_workers
            if current_workers > 1:
                self.system_analyzer.adjust_workers('decrease')
                self.logger.info(f"🔧 시스템 과부하로 워커 감소: {current_workers} → {self.system_analyzer.current_workers}")
            
            # 불필요한 드라이버 정리
            self._cleanup_idle_drivers()
            
            # 메모리 정리
            self._cleanup_memory()
            
        except Exception as e:
            self.logger.error(f"❌ 과부하 처리 실패: {e}")
    
    def _cleanup_idle_drivers(self):
        """Valid3.py 방식: 유휴 드라이버 정리 (엄격히 4개 제한)"""
        try:
            # 4개 초과 시에만 정리
            current_manager_count = len(self.web_driver_managers)
            
            if current_manager_count > 4:
                # 4개를 초과하는 매니저들 정리 (높은 번호부터)
                workers_to_remove = sorted(self.web_driver_managers.keys(), reverse=True)[:current_manager_count - 4]
                
                for worker_id in workers_to_remove:
                    self.cleanup_worker_driver(worker_id)
                    
            self.logger.info(f"🧹 드라이버 정리 완료: {current_manager_count} -> {len(self.web_driver_managers)} (최대 4개 유지)")
                            
        except Exception as e:
            self.logger.warning(f"⚠️ 유휴 드라이버 정리 실패: {e}")
    
    def cleanup_worker_driver(self, worker_id: int):
        """Valid3.py 방식: 워커별 드라이버 완전 정리 (포트 해제 포함)"""
        try:
            with self.driver_lock:
                limited_worker_id = worker_id % 4  # 0~3으로 제한
                
                if limited_worker_id in self.web_driver_managers:
                    web_manager = self.web_driver_managers[limited_worker_id]
                    
                    # 워커가 사용중인 포트 해제
                    if limited_worker_id in self.port_manager.port_assignments:
                        assigned_port = self.port_manager.port_assignments[limited_worker_id]
                        self.port_manager.release_port(assigned_port, limited_worker_id)
                        self.logger.debug(f"🔓 워커 {worker_id} (제한:{limited_worker_id}) 포트 {assigned_port} 해제")
                    
                    # 드라이버 참조 제거 (무한증식 방지)
                    if hasattr(web_manager, 'driver'):
                        web_manager.driver = None
                    
                    # WebDriverManager의 정리 메서드 호출
                    if hasattr(web_manager, 'cleanup_all_drivers'):
                        web_manager.cleanup_all_drivers()
                    elif hasattr(web_manager, 'cleanup'):
                        web_manager.cleanup()
                    
                    # 딕셔너리에서 제거
                    del self.web_driver_managers[limited_worker_id]
                    self.logger.debug(f"🧹 워커 {worker_id} (제한:{limited_worker_id}) WebDriverManager 완전 정리 (포트 해제 포함)")
        except Exception as e:
            self.logger.debug(f"⚠️ 워커 {worker_id} 정리 중 오류 (무시): {e}")
    
    def force_kill_all_chrome_processes(self):
        """Valid3.py 방식: 크롬 프로세스 강제 종료 (비상용)"""
        try:
            import subprocess
            import platform
            
            if platform.system() == "Windows":
                # Windows에서 Chrome 프로세스 강제 종료
                try:
                    subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'], 
                                 capture_output=True, text=True, timeout=10)
                    subprocess.run(['taskkill', '/f', '/im', 'chromedriver.exe'], 
                                 capture_output=True, text=True, timeout=10)
                    self.logger.info("🔨 Windows Chrome 프로세스 강제 종료 완료")
                except Exception as e:
                    self.logger.warning(f"⚠️ Windows Chrome 강제 종료 실패: {e}")
            else:
                # Linux/Mac에서 Chrome 프로세스 강제 종료
                try:
                    subprocess.run(['pkill', '-f', 'chrome'], 
                                 capture_output=True, text=True, timeout=10)
                    subprocess.run(['pkill', '-f', 'chromedriver'], 
                                 capture_output=True, text=True, timeout=10)
                    self.logger.info("🔨 Unix Chrome 프로세스 강제 종료 완료")
                except Exception as e:
                    self.logger.warning(f"⚠️ Unix Chrome 강제 종료 실패: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ Chrome 프로세스 강제 종료 실패: {e}")
    
    def cleanup_all_drivers(self):
        """Valid3.py 방식: 모든 드라이버 완전 정리 (포트 관리 포함)"""
        try:
            self.logger.info(f"🧹 전체 드라이버 정리 시작: {len(self.web_driver_managers)}개")
            
            # 모든 워커의 WebDriverManager 정리
            worker_ids = list(self.web_driver_managers.keys())
            for worker_id in worker_ids:
                self.cleanup_worker_driver(worker_id)
            
            # 드라이버 딕셔너리 초기화
            self.web_driver_managers.clear()
            
            # 모든 포트 해제
            self.port_manager.release_all_ports()
            
            # 강제 Chrome 프로세스 종료 (비상용)
            self.force_kill_all_chrome_processes()
            
            # 메모리 정리
            import gc
            gc.collect()
            
            self.logger.info("🧹 전체 드라이버 정리 완료 (포트 해제 포함)")
            
        except Exception as e:
            self.logger.error(f"❌ 전체 드라이버 정리 실패: {e}")
    
    def cleanup(self):
        """시스템 전체 정리"""
        try:
            self.logger.info("🧹 시스템 정리 시작")
            
            # 모든 드라이버 정리
            self.cleanup_all_drivers()
            
            # SystemAnalyzer 정리
            if hasattr(self, 'system_analyzer'):
                self.system_analyzer.cleanup()
            
            # 체크포인트 파일 최종 정리 (선택적)
            try:
                self._cleanup_old_checkpoint_files("rawdatafile")
                self.logger.info("🗑️ 체크포인트 파일 최종 정리 완료")
            except:
                pass  # 정리 실패해도 프로그램 종료에는 영향 없음
            
            # 메모리 정리
            self._cleanup_memory()
            
            self.logger.info("🧹 시스템 정리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 시스템 정리 실패: {e}")
    
    def _cleanup_memory(self):
        """메모리 정리"""
        try:
            import gc
            gc.collect()
            
            # 현재 메모리 사용량 로그
            memory_mb = self.system_analyzer.get_memory_usage_mb()
            self.logger.info(f"🧹 메모리 정리 완료 (현재: {memory_mb}MB)")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 메모리 정리 실패: {e}")
    
    def process_batch_data(self, df: pd.DataFrame) -> List[CompValidationResult]:
        """배치 데이터 처리 (100행씩)"""
        all_results = []
        total_rows = len(df)
        
        for batch_start in range(0, total_rows, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total_rows)
            batch_df = df.iloc[batch_start:batch_end]
            
            self.logger.info(f"📦 배치 처리: {batch_start+1}~{batch_end}/{total_rows}")
            
            # 엄격히 4개로 제한된 워커 사용
            optimal_workers = 4
            
            # 병렬 처리
            batch_results = []
            with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
                future_to_row = {
                    executor.submit(
                        self.process_validation_pipeline, 
                        row, 
                        batch_start + idx, 
                        idx % optimal_workers
                    ): (batch_start + idx, row)
                    for idx, (_, row) in enumerate(batch_df.iterrows())
                }
                
                for future in as_completed(future_to_row):
                    row_index, row_data = future_to_row[future]
                    try:
                        result = future.result()
                        batch_results.append(result)
                    except Exception as e:
                        self.logger.error(f"❌ 행 {row_index} 처리 실패: {e}")
                        # 실패한 행에 대한 기본 결과 생성
                        failed_result = CompValidationResult(
                            row_index=row_index,
                            region=row_data.get('region', ''),
                            district=row_data.get('district', ''),
                            institution_name=row_data.get('institution_name', ''),
                            address=row_data.get('address', ''),
                            phone=row_data.get('phone', ''),
                            fax=row_data.get('fax', ''),
                            error_message=str(e)
                        )
                        batch_results.append(failed_result)
            
            all_results.extend(batch_results)
            self.processed_count += len(batch_results)
            
            # 중간 결과 저장 (JSON 체크포인트)
            self._save_checkpoint(batch_results, batch_start)
            
            # SystemAnalyzer 기반 동적 최적화
            self._optimize_batch_processing()
            
            # 배치 완료 로그
            success_count = sum(1 for r in batch_results if r.success)
            self.logger.info(f"✅ 배치 완료: {success_count}/{len(batch_results)} 성공")
            
            # 시스템 상태 체크 및 조정
            if not self.system_analyzer.is_system_healthy():
                self.logger.warning("⚠️ 시스템 과부하 감지, 워커 조정 및 대기...")
                self._handle_system_overload()
                time.sleep(10)
            else:
                # 건강한 상태면 잠시 쉬고 다음 배치
                time.sleep(2)
        
        return all_results
    
    def _save_checkpoint(self, batch_results: List[CompValidationResult], batch_start: int):
        """중간 결과 JSON 저장 (최신 1개만 유지, 이전 파일 자동 삭제)"""
        try:
            # rawdatafile 디렉토리 확인
            checkpoint_dir = "rawdatafile"
            if not os.path.exists(checkpoint_dir):
                os.makedirs(checkpoint_dir)
            
            # 이전 체크포인트 파일들 삭제
            self._cleanup_old_checkpoint_files(checkpoint_dir)
            
            # 체크포인트 데이터 구성
            checkpoint_data = {
                'batch_info': {
                    'batch_start': batch_start,
                    'batch_size': len(batch_results),
                    'timestamp': datetime.now().isoformat(),
                    'total_processed': self.processed_count
                },
                'system_status': {
                    'current_workers': 4,  # 고정값
                    'memory_usage': self.system_analyzer.get_memory_usage_mb(),
                    'is_healthy': self.system_analyzer.is_system_healthy(),
                    'active_drivers': len(self.web_driver_managers),
                    'port_status': self.port_manager.get_port_status()
                },
                'results': []
            }
            
            # 개별 결과 데이터
            for result in batch_results:
                result_data = {
                    'row_index': result.row_index,
                    'region': result.region,
                    'district': result.district,
                    'institution_name': result.institution_name,
                    'address': result.address,
                    'phone': result.phone,
                    'fax': result.fax,
                    'phone_real_institution': result.phone_real_institution,
                    'phone_verified': result.phone_verified,
                    'fax_real_institution': result.fax_real_institution,
                    'fax_verified': result.fax_verified,
                    'validation_1st': result.validation_1st,
                    'validation_2nd': result.validation_2nd,
                    'validation_3rd': result.validation_3rd,
                    'validation_4th': result.validation_4th,
                    'validation_5th': result.validation_5th,
                    'validation_6th': result.validation_6th,
                    'extracted_links': result.extracted_links,
                    'ai_responses': result.ai_responses,
                    'processing_time': result.processing_time,
                    'error_message': result.error_message,
                    'success': result.success
                }
                checkpoint_data['results'].append(result_data)
            
            # 새로운 체크포인트 파일 저장 (최신 1개만)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            checkpoint_path = f"{checkpoint_dir}/checkpoint_latest_{timestamp}.json"
            
            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
            
            # 배치 성공률 로그
            success_count = sum(1 for r in batch_results if r.success)
            success_rate = (success_count / len(batch_results) * 100) if batch_results else 0
            
            self.logger.info(f"💾 체크포인트 저장 (최신 1개만 유지): {os.path.basename(checkpoint_path)}")
            self.logger.info(f"📊 배치 성공률: {success_rate:.1f}% ({success_count}/{len(batch_results)})")
            
        except Exception as e:
            self.logger.error(f"❌ 체크포인트 저장 실패: {e}")
    
    def _cleanup_old_checkpoint_files(self, checkpoint_dir: str):
        """이전 체크포인트 파일들 삭제"""
        try:
            import glob
            
            # checkpoint_*.json 패턴의 모든 파일 찾기
            old_checkpoint_files = glob.glob(f"{checkpoint_dir}/checkpoint_*.json")
            
            # 이전 파일들 삭제
            deleted_count = 0
            for file_path in old_checkpoint_files:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                    self.logger.debug(f"🗑️ 이전 체크포인트 삭제: {os.path.basename(file_path)}")
                except Exception as e:
                    self.logger.warning(f"⚠️ 파일 삭제 실패 {file_path}: {e}")
            
            if deleted_count > 0:
                self.logger.info(f"🗑️ 이전 체크포인트 {deleted_count}개 파일 정리 완료")
                
        except Exception as e:
            self.logger.warning(f"⚠️ 이전 체크포인트 정리 실패: {e}")
    
    def save_final_csv(self, results: List[CompValidationResult], original_df: pd.DataFrame) -> str:
        """최종 CSV 저장"""
        try:
            # rawdatafile 디렉토리 확인 및 생성
            output_dir = "rawdatafile"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                self.logger.info(f"📁 출력 디렉토리 생성: {output_dir}")
            
            # 원본 데이터프레임 복사
            output_df = original_df.copy()
            
            # G/H/J/K 컬럼 추가 (전화/팩스 실제기관, 올바른 번호)
            output_df['G_전화실제기관'] = ""
            output_df['H_올바른전화번호'] = ""
            output_df['J_팩스실제기관'] = ""
            output_df['K_올바른팩스번호'] = ""
            
            # N~S 컬럼: 1~6차 검증값들
            validation_columns = [
                'N_1차검증값_지역매칭',
                'O_2차검증값_구글검색', 
                'P_3차검증값_링크수집',
                'Q_4차검증값_병렬크롤링',
                'R_5차검증값_AI분석',
                'S_6차검증값_종합매칭'
            ]
            
            for col in validation_columns:
                output_df[col] = ""
            
            # T~X 컬럼: 추출링크 1~5
            link_columns = []
            for i in range(5):
                col_name = f'{chr(84+i)}_추출링크{i+1}'
                link_columns.append(col_name)
                output_df[col_name] = ""
            
            # Y~AC 컬럼: 링크별 AI응답 1~5
            ai_response_columns = []
            for i in range(5):
                col_name = f'{chr(89+i)}_링크{i+1}_AI응답'
                ai_response_columns.append(col_name)
                output_df[col_name] = ""
            
            # 결과 데이터 채우기
            results_dict = {r.row_index: r for r in results}
            
            for idx in range(len(output_df)):
                if idx in results_dict:
                    result = results_dict[idx]
                    
                    # G/H/J/K 컬럼 채우기
                    output_df.at[idx, 'G_전화실제기관'] = result.phone_real_institution
                    output_df.at[idx, 'H_올바른전화번호'] = result.phone_verified
                    output_df.at[idx, 'J_팩스실제기관'] = result.fax_real_institution
                    output_df.at[idx, 'K_올바른팩스번호'] = result.fax_verified
                    
                    # N~S 컬럼 채우기 (검증값들)
                    validation_values = [
                        result.validation_1st,
                        result.validation_2nd,
                        result.validation_3rd,
                        result.validation_4th,
                        result.validation_5th,
                        result.validation_6th
                    ]
                    
                    for i, (col, value) in enumerate(zip(validation_columns, validation_values)):
                        output_df.at[idx, col] = value
                    
                    # T~X 컬럼 채우기 (링크들)
                    for i, link in enumerate(result.extracted_links[:5]):
                        if i < len(link_columns):
                            output_df.at[idx, link_columns[i]] = link
                    
                    # Y~AC 컬럼 채우기 (AI 응답들)
                    for i, response in enumerate(result.ai_responses[:5]):
                        if i < len(ai_response_columns):
                            output_df.at[idx, ai_response_columns[i]] = response
            
            # 최종 CSV 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"{output_dir}/comp_result_{timestamp}.csv"
            
            # CSV 저장 (UTF-8 BOM으로 한글 호환성 확보)
            output_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            # 통계 정보 추가
            total_rows = len(output_df)
            processed_rows = len(results)
            success_rows = sum(1 for r in results if r.success)
            
            self.logger.info(f"💾 최종 결과 저장 완료")
            self.logger.info(f"   📁 파일: {output_path}")
            self.logger.info(f"   📊 통계: 전체 {total_rows}행, 처리 {processed_rows}행, 성공 {success_rows}행")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"❌ CSV 저장 실패: {e}")
            raise
    

    
    def run(self, csv_file_path: str):
        """메인 실행 함수"""
        start_time = time.time()
        
        try:
            self.logger.info("🚀 CompCrawlingSystem 실행 시작")
            self.logger.info(f"📁 입력 파일: {csv_file_path}")
            
            # 0. 시작 시 이전 체크포인트 파일들 정리
            self._cleanup_old_checkpoint_files("rawdatafile")
            
            # 1. CSV 로드 및 검증
            if not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {csv_file_path}")
            
            df = self.load_csv_data(csv_file_path)
            self.logger.info(f"📊 로드된 데이터: {len(df)}행")
            
            # 2. 시스템 상태 확인
            if not self.system_analyzer.is_system_healthy():
                self.logger.warning("⚠️ 시스템 상태가 불안정합니다. 계속 진행하시겠습니까?")
                # 여기서 사용자 입력을 받거나 자동으로 최적화할 수 있음
            
            # 3. 배치 처리 (진행률 표시)
            self.logger.info(f"🔄 배치 처리 시작 (배치 크기: {self.batch_size})")
            results = self.process_batch_data(df)
            
            # 4. 결과 저장
            output_path = self.save_final_csv(results, df)
            
            # 5. 최종 통계 출력
            end_time = time.time()
            total_time = end_time - start_time
            
            success_count = sum(1 for r in results if r.success)
            success_rate = (success_count / len(results) * 100) if results else 0
            
            self.logger.info("=" * 60)
            self.logger.info("🎉 CompCrawlingSystem 실행 완료")
            self.logger.info(f"📊 처리 통계:")
            self.logger.info(f"   - 전체 행수: {len(df):,}행")
            self.logger.info(f"   - 처리 완료: {len(results):,}행")
            self.logger.info(f"   - 성공률: {success_rate:.1f}% ({success_count:,}/{len(results):,})")
            self.logger.info(f"   - 총 소요시간: {total_time:.1f}초 ({total_time/60:.1f}분)")
            self.logger.info(f"   - 평균 처리속도: {len(results)/total_time:.1f}행/초")
            self.logger.info(f"📁 결과 파일: {output_path}")
            self.logger.info("=" * 60)
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"❌ 실행 실패: {e}")
            self.logger.error(f"📋 오류 상세: {traceback.format_exc()}")
            raise
        finally:
            self.cleanup()


def main():
    """메인 함수"""
    # CSV 파일 경로 (절대경로와 상대경로 모두 지원)
    csv_file_path = r"rawdatafile\failed_data_250809.csv"
    
    # 절대경로 대안 (파일이 없으면 시도)
    if not os.path.exists(csv_file_path):
        alternative_path = r"C:\Users\MyoengHo Shin\pjt\info_crawl\rawdatafile\failed_data_250809.csv"
        if os.path.exists(alternative_path):
            csv_file_path = alternative_path
        else:
            print(f"❌ 파일을 찾을 수 없습니다:")
            print(f"   1. {os.path.abspath('rawdatafile/failed_data_250809.csv')}")
            print(f"   2. {alternative_path}")
            return
    
    print("🚀 CompCrawlingSystem 시작")
    print(f"📁 입력 파일: {csv_file_path}")
    print("🔧 헤드리스 모드 토글: 'h' 키 (keyboard 라이브러리 설치 시)")
    print("⏸️ 중단: Ctrl+C")
    print("=" * 60)
    
    system = None
    
    try:
        system = CompCrawlingSystem()
        output_path = system.run(csv_file_path)
        print(f"\n🎉 모든 작업 완료!")
        print(f"📁 결과 파일: {output_path}")
        
    except KeyboardInterrupt:
        print("\n⏸️ 사용자에 의해 중단됨")
        if system:
            print("🧹 정리 작업 중...")
            
    except FileNotFoundError as e:
        print(f"\n❌ 파일 오류: {e}")
        
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류 발생: {e}")
        print("📋 자세한 오류 내용은 logs/ 폴더의 로그 파일을 확인하세요.")
        
    finally:
        if system:
            try:
                system.cleanup()
            except:
                pass
        print("\n👋 프로그램 종료")


if __name__ == "__main__":
    main()
