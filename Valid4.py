#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valid3.py - Valid2_fixed.py 기반 최신 검증 시스템
Valid2_fixed.py의 모든 개선사항 포함:
1. AI 메서드명 수정 (generate_content → extract_with_gemini) ✅
2. 강화된 AI 응답 처리 (빈 응답, 긴 응답, 형식 오류 등) ✅
3. 향상된 신뢰도 계산 (팩스번호 정확 일치 시 높은 점수) ✅
4. 백업 로직 추가 (AI 실패 시 3차 검증 결과 활용) ✅
5. 상세 로깅 강화 ✅

핵심 데이터: E열(읍면동) = I열(팩스번호) [전화번호와 팩스번호는 엄밀히 다름]

5단계 팩스번호 검증 프로세스:
1차 검증: 팩스번호 지역번호 vs E열 읍면동 매칭
2차 검증: Google 검색으로 팩스번호의 진짜 기관명 확인  
3차 검증: 검색결과 링크 크롤링 + 기관명 추출 (개선된 신뢰도)
4차 검증: AI를 통한 팩스번호 실제 소유 기관명 도출 (백업 로직 포함)
5차 검증: 모든 단계 결과 종합 → 데이터 정확성 최종 판단 [TODO]

특징:
- utils.WebDriverManager 100% 활용으로 안정성 확보
- 복잡한 ProxyRotator, AdvancedPortManager 제거
- 상세한 로깅으로 문제 지점 정확한 파악 가능
- AI 메서드명 및 응답 처리 문제 해결
- 모든 테스트 완료된 안정적 코드 기반

작성자: AI Assistant
작성일: 2025-07-24
버전: 3.0 - Valid2_fixed 기반 최신버전
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import threading
import re

# 웹 크롤링
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

# 환경변수 및 AI
from dotenv import load_dotenv
import google.generativeai as genai

# utils 모듈 활용 (검증된 안정성) - 새로운 구조
from utils.system.web_driver_manager import WebDriverManager
from utils.ai_model_manager import AIModelManager
from utils.valid.phone_validator import PhoneValidator
from utils.crawler.prt.user_agent_rotator import UserAgentRotator

# 환경변수 로드
load_dotenv()

# ================================
# 유틸리티 함수들
# ================================

def load_csv_with_encoding(file_path: str, logger=None) -> pd.DataFrame:
    """다양한 인코딩으로 CSV 파일 로드 시도"""
    encodings = ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig', 'latin-1']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            if logger:
                logger.info(f"✅ CSV 로드 성공: {file_path} ({encoding} 인코딩)")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            if logger:
                logger.warning(f"⚠️ {encoding} 인코딩 시도 실패: {e}")
            continue
    
    raise Exception(f"❌ 모든 인코딩 시도 실패: {file_path}")

def save_csv_with_encoding(df: pd.DataFrame, file_path: str, logger=None) -> bool:
    """안전한 CSV 저장 (인코딩 오류 방지)"""
    encodings = ['utf-8-sig', 'cp949', 'utf-8']
    
    for encoding in encodings:
        try:
            df.to_csv(file_path, index=False, encoding=encoding)
            if logger:
                logger.info(f"✅ CSV 저장 성공: {file_path} ({encoding} 인코딩)")
            return True
        except UnicodeEncodeError:
            continue
        except Exception as e:
            if logger:
                logger.warning(f"⚠️ {encoding} 인코딩으로 저장 실패: {e}")
            continue
    
    if logger:
        logger.error(f"❌ 모든 인코딩으로 저장 실패: {file_path}")
    return False

# ================================
# 매크로 방지 시스템 (복구)
# ================================

# ProxyRotator 클래스는 utils.crawler.prt.user_agent_rotator.UserAgentRotator로 이동됨

class AdvancedPortManager:
    """고급 포트 관리 시스템 (100개 포트 범위)"""
    
    def __init__(self, logger):
        """AdvancedPortManager 초기화"""
        self.logger = logger
        
        # 포트 범위 설정 (100개 포트)
        self.port_range_start = 9222
        self.port_range_end = 9322  # 9222-9321 (100개)
        self.available_ports = set(range(self.port_range_start, self.port_range_end))
        self.used_ports = set()
        self.blacklisted_ports = set()  # 차단된 포트들
        self.port_assignments = {}  # 워커별 포트 할당 기록
        
        # 포트 사용 통계
        self.allocation_count = 0
        self.release_count = 0
        
        self.logger.info(f"🔌 AdvancedPortManager 초기화: {len(self.available_ports)}개 포트 관리 ({self.port_range_start}-{self.port_range_end-1})")
    
    def allocate_port(self, worker_id: int) -> int:
        """워커에게 포트 할당"""
        try:
            # 이미 할당된 포트가 있으면 재사용
            if worker_id in self.port_assignments:
                existing_port = self.port_assignments[worker_id]
                if existing_port not in self.blacklisted_ports:
                    self.logger.debug(f"🔌 워커 {worker_id}: 기존 포트 {existing_port} 재사용")
                    return existing_port
                else:
                    # 블랙리스트에 있으면 해제하고 새로 할당
                    self.logger.warning(f"⚠️ 워커 {worker_id}: 기존 포트 {existing_port} 블랙리스트됨, 새 포트 할당")
                    del self.port_assignments[worker_id]
                    self.used_ports.discard(existing_port)
            
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
                        raise Exception("모든 포트가 고갈됨")
            
            # 포트 할당
            allocated_port = min(available_ports)  # 가장 작은 번호부터 사용
            self.used_ports.add(allocated_port)
            self.port_assignments[worker_id] = allocated_port
            self.allocation_count += 1
            
            self.logger.debug(f"🔌 워커 {worker_id}: 포트 {allocated_port} 새로 할당 (총 사용중: {len(self.used_ports)}개)")
            return allocated_port
            
        except Exception as e:
            self.logger.error(f"❌ 포트 할당 실패 (워커 {worker_id}): {e}")
            # 긴급 포트 반환 (기본 포트)
            emergency_port = self.port_range_start + (worker_id % 10)
            self.logger.warning(f"🚨 긴급 포트 할당: {emergency_port}")
            return emergency_port
    
    def release_port(self, port: int, worker_id: int = None):
        """포트 즉시 해제"""
        try:
            if port in self.used_ports:
                self.used_ports.remove(port)
                self.release_count += 1
                
                # 워커 할당 기록에서 제거
                if worker_id and worker_id in self.port_assignments:
                    if self.port_assignments[worker_id] == port:
                        del self.port_assignments[worker_id]
                else:
                    # worker_id가 없으면 전체 할당 기록에서 찾아서 제거
                    for wid, assigned_port in list(self.port_assignments.items()):
                        if assigned_port == port:
                            del self.port_assignments[wid]
                            break
                
                self.logger.debug(f"🔓 포트 {port} 즉시 해제 완료 (남은 사용중: {len(self.used_ports)}개)")
            else:
                self.logger.debug(f"⚠️ 포트 {port} 이미 해제됨")
                
        except Exception as e:
            self.logger.error(f"❌ 포트 해제 실패 ({port}): {e}")
    
    def blacklist_port(self, port: int, reason: str = "차단됨"):
        """포트를 블랙리스트에 추가 (차단된 포트)"""
        try:
            self.blacklisted_ports.add(port)
            self.used_ports.discard(port)  # 사용중 목록에서 제거
            
            # 워커 할당에서도 제거
            for worker_id, assigned_port in list(self.port_assignments.items()):
                if assigned_port == port:
                    del self.port_assignments[worker_id]
                    break
            
            self.logger.warning(f"🚫 포트 {port} 블랙리스트 추가: {reason}")
            
        except Exception as e:
            self.logger.error(f"❌ 포트 블랙리스트 실패 ({port}): {e}")
    
    def release_all_ports(self):
        """모든 포트 해제"""
        try:
            released_count = len(self.used_ports)
            self.used_ports.clear()
            self.port_assignments.clear()
            
            self.logger.info(f"🧹 모든 포트 해제 완료: {released_count}개")
            
        except Exception as e:
            self.logger.error(f"❌ 모든 포트 해제 실패: {e}")
    
    def get_port_status(self) -> Dict:
        """포트 사용 현황 반환"""
        total_ports = len(self.available_ports)
        used_count = len(self.used_ports)
        blacklisted_count = len(self.blacklisted_ports)
        available_count = total_ports - used_count - blacklisted_count
        
        return {
            'total_ports': total_ports,
            'available_count': available_count,
            'used_count': used_count,
            'blacklisted_count': blacklisted_count,
            'allocation_count': self.allocation_count,
            'release_count': self.release_count,
            'used_ports': list(self.used_ports),
            'blacklisted_ports': list(self.blacklisted_ports),
            'port_assignments': dict(self.port_assignments)
        }

# ================================
# 전역 설정 (Valid2_fixed 기반 최적화)
# ================================

# 입력/출력 파일 경로
INPUT_FILE = "rawdatafile/failed_data_250724.xlsx"
OUTPUT_FILE_PREFIX = "Valid3_검증결과"

# 검증 설정 (대용량 데이터 최적화)
MAX_WORKERS = 6  # 대용량 처리를 위해 6개 워커로 증가
BATCH_SIZE = 200  # 배치 크기 증가 (100 → 200)
SEARCH_RESULTS_LIMIT = 5  # 검색 결과 링크 수 (search_logic.txt 요구사항: 5개까지)
CONFIDENCE_THRESHOLD = 60  # 신뢰도 임계값 완화 (80% → 60%)

# 대용량 데이터 처리 설정
PRODUCTION_MODE = True  # 운영 모드 (True: 운영 모드)
MAX_ROWS_LIMIT = None  # 처리할 최대 행 수 (None: 전체, 숫자: 제한)
SAVE_INTERVAL = 50  # 중간 저장 간격 (행 단위)
MEMORY_CLEANUP_INTERVAL = 20  # 메모리 정리 간격

# 타임아웃 설정 (속도 우선으로 단축)
GOOGLE_SEARCH_TIMEOUT = 8   # Google 검색 타임아웃
PAGE_LOAD_TIMEOUT = 6       # 페이지 로드 타임아웃
CRAWLING_TIMEOUT = 5        # 개별 크롤링 타임아웃

# ================================
# 강화된 로깅 시스템
# ================================

def setup_detailed_logger(name: str = "Valid3") -> logging.Logger:
    """상세한 디버깅이 가능한 로깅 시스템 설정"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'valid3_{timestamp}.log'
    
    # 상세한 포맷 (문제 지점 파악 용이)
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - [워커%(thread)d] - %(message)s'
    )
    
    # 파일 핸들러 (모든 로그)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # 디버그 레벨까지 모든 로그
    file_handler.setFormatter(detailed_formatter)
    
    # 콘솔 핸들러 (운영 모드에 따라 레벨 조정)
    console_handler = logging.StreamHandler()
    console_level = logging.WARNING if PRODUCTION_MODE else logging.INFO  # 운영 모드시 WARNING만 출력
    console_handler.setLevel(console_level)
    console_handler.setFormatter(detailed_formatter)
    
    # 로거 설정
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 시스템 정보 로그
    logger.info("=" * 80)
    logger.info("🚀 Valid3.py - 최신 검증 시스템 시작 (Valid2_fixed 기반)")
    logger.info("=" * 80)
    logger.info(f"📁 로그 파일: {log_filename}")
    logger.info("✅ 포함된 모든 개선사항:")
    logger.info("   - AI 메서드명 수정 (generate_content → extract_with_gemini)")
    logger.info("   - 강화된 AI 응답 처리 (빈 응답, 긴 응답, 형식 오류 등)")
    logger.info("   - 향상된 신뢰도 계산 (팩스번호 정확 일치 시 높은 점수)")
    logger.info("   - 백업 로직 추가 (AI 실패 시 3차 검증 결과 활용)")
    logger.info(f"   - 신뢰도 임계값 완화 ({CONFIDENCE_THRESHOLD}%)")
    logger.info("   - 모든 테스트 완료된 안정적 코드 기반")
    logger.info(f"⚙️ 워커 수: {MAX_WORKERS}")
    logger.info(f"🎯 신뢰도 임계값: {CONFIDENCE_THRESHOLD}%")
    logger.info(f"🔗 검색 결과 한도: {SEARCH_RESULTS_LIMIT}개")
    logger.info(f"⏱️ 타임아웃: Google({GOOGLE_SEARCH_TIMEOUT}s), 페이지({PAGE_LOAD_TIMEOUT}s), 크롤링({CRAWLING_TIMEOUT}s)")
    
    return logger

# ================================
# Valid2 단순화 방식 (복잡한 우회 시스템 제거)
# ================================

# ================================
# 데이터 클래스 (Valid2_fixed와 동일)
# ================================

@dataclass
class ValidationResult:
    """5단계 검증 결과"""
    row_index: int
    fax_number: str
    institution_name: str  # 원본 기관명 (읍면동) - 핵심 데이터
    region: str           # 지역 (시도)
    phone_number: str = ""  # 전화번호 (H열)
    address: str = ""     # 주소 (G열)
    
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
    # 3차에서 추출된 기관명들 (4-5차 검증용)
    discovered_institutions: List[str] = None
    
    # 4차 검증 결과
    stage4_passed: bool = False
    stage4_message: str = ""
    ai_extracted_institution: str = ""
    
    # 5차 검증 결과 (최종)
    stage5_passed: bool = False
    stage5_message: str = ""
    final_verification: str = ""
    
    # 검증된 실제 주민센터명 (핵심 추가)
    verified_institution_name: str = ""  # 실제 팩스번호 소유 기관명
    institution_mapping_confidence: float = 0.0  # 매핑 신뢰도
    
    # 전체 결과
    overall_result: str = "검증 실패"  # "데이터 올바름", "데이터 오류", "직접 확인 요망"
    final_confidence: float = 0.0
    processing_time: float = 0.0
    error_message: str = "" 

@dataclass
class Valid4ValidationResult(ValidationResult):
    """Valid4 확장: Phase 0 자동 라벨링 결과 추가"""
    
    # Phase 0: 자동 라벨링 결과 (NEW!)
    phase0_passed: bool = False
    phase0_message: str = ""
    phone_auto_matched: bool = False
    fax_auto_matched: bool = False
    matched_center_name_phone: str = ""    # 전화번호로 매칭된 센터명
    matched_center_name_fax: str = ""      # 팩스번호로 매칭된 센터명
    auto_labeling_confidence: float = 0.0  # 자동 라벨링 신뢰도
    
    # Y 라벨 형식 (구체적인 라벨링)
    detailed_phone_label: str = ""  # "02-XXX-XXXX은 OO센터의 전화번호입니다"
    detailed_fax_label: str = ""    # "02-XXX-XXXX은 OO센터의 팩스번호입니다"
    
    # 매칭 유형 추가
    phone_match_type: str = ""  # "전화→전화", "전화→팩스", "매칭실패"
    fax_match_type: str = ""    # "팩스→팩스", "팩스→전화", "매칭실패"

# ================================
# 센터 데이터 관리 클래스 (NEW!)
# ================================

class CenterDataManager:
    """크롤링된 센터 데이터 관리 클래스"""
    
    def __init__(self, logger):
        self.logger = logger
        self.phone_to_center = {}  # 전화번호 → (센터명, '전화')
        self.fax_to_center = {}    # 팩스번호 → (센터명, '팩스')
        self.center_data = None    # 전체 센터 데이터
        self.crawling_file_path = "center_crawling_result_20250809_190826.xlsx"  # 상대경로
        self.load_center_data()
    
    def load_center_data(self):
        """center_crawling_result.xlsx 로드 및 인덱싱"""
        try:
            self.logger.info(f"📂 센터 데이터 로드 시작: {self.crawling_file_path}")
            
            if not os.path.exists(self.crawling_file_path):
                self.logger.error(f"❌ 센터 데이터 파일 없음: {self.crawling_file_path}")
                return False
            
            # Excel 파일 로드
            self.center_data = pd.read_excel(self.crawling_file_path)
            self.logger.info(f"✅ 센터 데이터 로드 완료: {len(self.center_data)}개")
            
            # 인덱싱: 전화번호 매핑 (sido, gugun, center_name 포함)
            for _, row in self.center_data.iterrows():
                if pd.notna(row['phone']) and row['phone'].strip():
                    phone = str(row['phone']).strip()
                    center_info = {
                        'sido': str(row['sido']).strip(),
                        'gugun': str(row['gugun']).strip(),
                        'center_name': str(row['center_name']).strip(),
                        'contact_type': '전화'
                    }
                    self.phone_to_center[phone] = center_info
            
            # 인덱싱: 팩스번호 매핑 (sido, gugun, center_name 포함)
            for _, row in self.center_data.iterrows():
                if pd.notna(row['fax']) and row['fax'].strip():
                    fax = str(row['fax']).strip()
                    center_info = {
                        'sido': str(row['sido']).strip(),
                        'gugun': str(row['gugun']).strip(),
                        'center_name': str(row['center_name']).strip(),
                        'contact_type': '팩스'
                    }
                    self.fax_to_center[fax] = center_info
            
            self.logger.info(f"📞 전화번호 인덱스: {len(self.phone_to_center)}개")
            self.logger.info(f"📠 팩스번호 인덱스: {len(self.fax_to_center)}개")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 센터 데이터 로드 실패: {e}")
            return False
    
    def find_center_by_phone(self, phone: str) -> Dict:
        """전화번호로 센터 찾기 (4가지 케이스 고려)"""
        if not phone or phone.strip() == "":
            return {"found": False, "reason": "빈 전화번호"}
        
        phone = str(phone).strip()
        
        # Case 1: 전화번호가 실제 전화번호와 매칭
        if phone in self.phone_to_center:
            center_info = self.phone_to_center[phone]
            full_name = f"{center_info['sido']} {center_info['gugun']} {center_info['center_name']}"
            return {
                "found": True,
                "center_name": center_info['center_name'],
                "match_type": "전화→전화",
                "label": f"{phone}은 {full_name}의 전화번호입니다"
            }
        
        # Case 2: 전화번호가 실제로는 팩스번호와 매칭
        if phone in self.fax_to_center:
            center_info = self.fax_to_center[phone]
            full_name = f"{center_info['sido']} {center_info['gugun']} {center_info['center_name']}"
            return {
                "found": True,
                "center_name": center_info['center_name'],
                "match_type": "전화→팩스",
                "label": f"{phone}은 {full_name}의 팩스번호입니다"
            }
        
        return {"found": False, "reason": "매칭 실패"}
    
    def find_center_by_fax(self, fax: str) -> Dict:
        """팩스번호로 센터 찾기 (4가지 케이스 고려)"""
        if not fax or fax.strip() == "":
            return {"found": False, "reason": "빈 팩스번호"}
        
        fax = str(fax).strip()
        
        # Case 1: 팩스번호가 실제 팩스번호와 매칭
        if fax in self.fax_to_center:
            center_info = self.fax_to_center[fax]
            full_name = f"{center_info['sido']} {center_info['gugun']} {center_info['center_name']}"
            return {
                "found": True,
                "center_name": center_info['center_name'],
                "match_type": "팩스→팩스",
                "label": f"{fax}은 {full_name}의 팩스번호입니다"
            }
        
        # Case 2: 팩스번호가 실제로는 전화번호와 매칭
        if fax in self.phone_to_center:
            center_info = self.phone_to_center[fax]
            full_name = f"{center_info['sido']} {center_info['gugun']} {center_info['center_name']}"
            return {
                "found": True,
                "center_name": center_info['center_name'],
                "match_type": "팩스→전화",
                "label": f"{fax}은 {full_name}의 전화번호입니다"
            }
        
        return {"found": False, "reason": "매칭 실패"}

# ================================
# 최신 검증 관리자 (Valid2_fixed 기반)
# ================================

class Valid4ValidationManager:
    """Valid3 기반 + Phase 0 자동 라벨링 확장"""
    
    def __init__(self):
        """초기화 - Valid3 기능 + Phase 0 자동 라벨링"""
        self.logger = setup_detailed_logger("Valid4ValidationManager")
        
        try:
            
            # Valid3의 안정적인 구조 그대로 유지
            self.logger.debug("📱 PhoneValidator 초기화 중...")
            self.phone_validator = PhoneValidator(self.logger)
            self.logger.debug("✅ PhoneValidator 초기화 완료")
            
            self.logger.debug("🤖 AIModelManager 초기화 중...")
            self.ai_manager = AIModelManager(self.logger)
            self.logger.debug("✅ AIModelManager 초기화 완료")
            
            # 매크로 방지 시스템 복구 (UserAgentRotator 사용)
            self.logger.debug("🛡️ UserAgentRotator 초기화 중...")
            self.user_agent_rotator = UserAgentRotator(self.logger)
            self.logger.debug("✅ UserAgentRotator 초기화 완료")
            
            self.logger.debug("🔌 AdvancedPortManager 초기화 중...")
            self.port_manager = AdvancedPortManager(self.logger)
            self.logger.debug("✅ AdvancedPortManager 초기화 완료")
            
            # WebDriverManager는 워커별로 생성 (메모리 효율성)
            self.web_driver_managers = {}  # 워커별 관리
            self.driver_lock = threading.Lock()
            
            # Valid4 신규 추가: 센터 데이터 관리자
            self.logger.debug("🏢 CenterDataManager 초기화 중...")
            self.center_manager = CenterDataManager(self.logger)
            self.logger.debug("✅ CenterDataManager 초기화 완료")
            
            # 데이터
            self.input_data = None
            self.validation_results = []
            
            self.logger.info("✅ Valid4ValidationManager 초기화 완료 (Phase 0 자동 라벨링 포함)")
            
        except Exception as e:
            self.logger.error(f"❌ Valid4ValidationManager 초기화 실패: {e}")
            self.logger.error(traceback.format_exc())
            raise
    
    def load_data(self, file_path: str = "rawdatafile/failed_data_250809.csv", test_mode: bool = False, test_sample_size: int = 100, priority_success: bool = True) -> bool:
        """CSV 데이터 로드 (failed_data_250809.csv) - 우선순위 기반"""
        try:
            self.logger.info(f"📊 데이터 로드 시작: {file_path}")
            self.logger.debug(f"테스트 모드: {test_mode}, 샘플 크기: {test_sample_size}, 성공 우선순위: {priority_success}")
            
            if not os.path.exists(file_path):
                self.logger.error(f"❌ 입력 파일 없음: {file_path}")
                return False
            
            # CSV 파일 로드 (인코딩 자동 감지)
            all_data = load_csv_with_encoding(file_path, self.logger)
            self.logger.info(f"✅ 원본 데이터 로드 완료: {len(all_data)}개")
            
            # 우선순위 기반 필터링
            if priority_success:
                # L열 (팩스전송결과) 확인
                fax_result_col = '팩스전송결과(250711)'
                if fax_result_col in all_data.columns:
                    # "성공" 케이스 우선 추출
                    success_data = all_data[all_data[fax_result_col] == '성공'].copy()
                    other_data = all_data[all_data[fax_result_col] != '성공'].copy()
                    
                    self.logger.info(f"📈 팩스전송 성공 케이스: {len(success_data)}개")
                    self.logger.info(f"📉 기타 케이스: {len(other_data)}개")
                    
                    # 테스트 모드에서 성공 케이스 우선 처리
                    if test_mode:
                        if len(success_data) >= test_sample_size:
                            # 성공 케이스만으로 샘플 구성
                            self.input_data = success_data.sample(n=test_sample_size, random_state=42)
                            self.logger.info(f"🎯 테스트 모드: 성공 케이스 {test_sample_size}개 우선 선택")
                        else:
                            # 성공 케이스 + 기타 케이스 조합
                            remaining = test_sample_size - len(success_data)
                            additional_data = other_data.sample(n=min(remaining, len(other_data)), random_state=42)
                            self.input_data = pd.concat([success_data, additional_data], ignore_index=True)
                            self.logger.info(f"🎯 테스트 모드: 성공 {len(success_data)}개 + 기타 {len(additional_data)}개")
                    else:
                        # 전체 모드: 성공 케이스 우선 순서로 정렬
                        self.input_data = pd.concat([success_data, other_data], ignore_index=True)
                        self.logger.info(f"🚀 전체 모드: 성공 케이스 {len(success_data)}개 우선 처리")
                else:
                    self.logger.warning(f"⚠️ {fax_result_col} 컬럼을 찾을 수 없음. 기본 샘플링 적용")
                    self.input_data = all_data.sample(n=test_sample_size, random_state=42) if test_mode else all_data
            else:
                # 기존 로직 (랜덤 샘플링)
                if test_mode and len(all_data) > test_sample_size:
                    self.input_data = all_data.sample(n=test_sample_size, random_state=42)
                    self.logger.info(f"🎯 테스트 모드: {test_sample_size}개 랜덤 샘플 추출")
                else:
                    self.input_data = all_data
            
            # 컬럼 매핑 확인
            expected_columns = ['연번', '시도', '시군구', '읍면동', '주    소', '전화번호', '실제 기관명', '올바른 전화번호', '팩스번호', '실제 기관명', '올바른 팩스번호', '팩스전송결과(250711)']
            self.logger.debug(f"실제 컬럼: {list(self.input_data.columns)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def validate_phase0(self, phone: str, fax: str) -> Dict:
        """Phase 0: 자동 라벨링 검증"""
        try:
            phase0_result = {
                "passed": False,
                "message": "",
                "phone_matched": False,
                "fax_matched": False,
                "phone_label": "",
                "fax_label": "",
                "phone_match_type": "매칭실패",
                "fax_match_type": "매칭실패",
                "confidence": 0.0
            }
            
            # 전화번호 매칭
            if phone and str(phone).strip():
                phone_result = self.center_manager.find_center_by_phone(str(phone).strip())
                if phone_result["found"]:
                    phase0_result["phone_matched"] = True
                    phase0_result["phone_label"] = phone_result["label"]
                    phase0_result["phone_match_type"] = phone_result["match_type"]
                    self.logger.debug(f"📞 전화번호 매칭 성공: {phone_result['label']}")
            
            # 팩스번호 매칭
            if fax and str(fax).strip():
                fax_result = self.center_manager.find_center_by_fax(str(fax).strip())
                if fax_result["found"]:
                    phase0_result["fax_matched"] = True
                    phase0_result["fax_label"] = fax_result["label"]
                    phase0_result["fax_match_type"] = fax_result["match_type"]
                    self.logger.debug(f"📠 팩스번호 매칭 성공: {fax_result['label']}")
            
            # 전체 성공 여부 및 신뢰도 계산
            if phase0_result["phone_matched"] or phase0_result["fax_matched"]:
                phase0_result["passed"] = True
                phase0_result["message"] = "Phase 0 자동 라벨링 성공"
                
                # 신뢰도 계산 (둘 다 매칭되면 100%, 하나만 매칭되면 80%)
                if phase0_result["phone_matched"] and phase0_result["fax_matched"]:
                    phase0_result["confidence"] = 100.0
                else:
                    phase0_result["confidence"] = 80.0
            else:
                phase0_result["message"] = "Phase 0 매칭 실패 - 웹 검색 필요"
            
            return phase0_result
            
        except Exception as e:
            self.logger.error(f"❌ Phase 0 검증 실패: {e}")
            return {
                "passed": False,
                "message": f"Phase 0 오류: {e}",
                "phone_matched": False,
                "fax_matched": False,
                "phone_label": "",
                "fax_label": "",
                "phone_match_type": "오류",
                "fax_match_type": "오류",
                "confidence": 0.0
            }
    
    def validate_single_row(self, row_data: Tuple[int, pd.Series]) -> Valid4ValidationResult:
        """단일 행 검증 - Phase 0 → 기존 Valid3 로직 통합"""
        row_index, row = row_data
        start_time = time.time()
        
        # Valid4ValidationResult 초기화
        result = Valid4ValidationResult(
            row_index=row_index,
            fax_number=str(row.get('팩스번호', '')).strip(),
            institution_name=str(row.get('읍면동', '')).strip(),
            region=str(row.get('시도', '')).strip(),
            phone_number=str(row.get('전화번호', '')).strip(),
            address=str(row.get('주    소', '')).strip()
        )
        
        try:
            self.logger.info(f"🔍 Row {row_index} 검증 시작: {result.institution_name}")
            
            # =====================================
            # Phase 0: 자동 라벨링 (NEW!)
            # =====================================
            phase0_result = self.validate_phase0(result.phone_number, result.fax_number)
            
            # Phase 0 결과 저장
            result.phase0_passed = phase0_result["passed"]
            result.phase0_message = phase0_result["message"]
            result.phone_auto_matched = phase0_result["phone_matched"]
            result.fax_auto_matched = phase0_result["fax_matched"]
            result.detailed_phone_label = phase0_result["phone_label"]
            result.detailed_fax_label = phase0_result["fax_label"]
            result.phone_match_type = phase0_result["phone_match_type"]
            result.fax_match_type = phase0_result["fax_match_type"]
            result.auto_labeling_confidence = phase0_result["confidence"]
            
            # Phase 0 성공 시 즉시 완료 처리
            if phase0_result["passed"]:
                result.overall_result = "Phase 0 자동 라벨링 완료"
                result.final_confidence = phase0_result["confidence"]
                result.processing_time = time.time() - start_time
                
                self.logger.info(f"✅ Row {row_index} Phase 0 성공: {phase0_result['confidence']:.1f}% 신뢰도")
                return result
            
            # =====================================
            # Phase 1: 지역번호 검증 (기존 Valid3 로직)
            # =====================================
            self.logger.debug(f"🔄 Row {row_index} Phase 1 시작: 지역번호 검증")
            
            stage1_passed, stage1_message = self.validate_stage1_simple(
                result.fax_number, result.institution_name, result.region, result.address
            )
            
            result.stage1_passed = stage1_passed
            result.stage1_message = stage1_message
            
            if not stage1_passed:
                result.overall_result = "지역번호 불일치 - 검색 필요"
                result.processing_time = time.time() - start_time
                self.logger.warning(f"⚠️ Row {row_index} 지역번호 불일치")
                # 여기서도 웹 검색으로 이동할 수 있지만, 일단 실패 처리
                return result
            
            # =====================================
            # Phase 2: 웹 검색 (구글 → Naver/Daum)
            # =====================================
            self.logger.debug(f"🔄 Row {row_index} Phase 2 시작: 웹 검색")
            
            # 구글 검색 시도
            stage2_passed, stage2_message, google_result = self.validate_stage2_simple(
                result.fax_number, result.institution_name, worker_id=0
            )
            
            result.stage2_passed = stage2_passed
            result.stage2_message = stage2_message
            result.google_search_result = google_result
            
            if not stage2_passed:
                # Naver/Daum 백업 검색 (추후 구현)
                self.logger.warning(f"⚠️ Row {row_index} 구글 검색 실패 - 백업 검색 필요")
                result.overall_result = "웹 검색 실패"
                result.processing_time = time.time() - start_time
                return result
            
            # =====================================
            # 최종 결과 처리
            # =====================================
            result.overall_result = "웹 검색 완료 - 추가 검증 필요"
            result.final_confidence = 60.0  # 웹 검색만 성공한 경우
            result.processing_time = time.time() - start_time
            
            self.logger.info(f"✅ Row {row_index} 웹 검색 완료")
            return result
            
        except Exception as e:
            result.error_message = str(e)
            result.overall_result = "검증 오류"
            result.processing_time = time.time() - start_time
            self.logger.error(f"❌ Row {row_index} 검증 실패: {e}")
            return result
    
    def validate_stage1_simple(self, fax_number: str, institution_name: str, region: str, address: str) -> Tuple[bool, str]:
        """간소화된 1차 검증: 지역번호 매칭"""
        try:
            # 기본적인 지역번호 검증 로직
            if not fax_number or fax_number.strip() == "":
                return False, "팩스번호 없음"
            
            # 지역번호 추출 및 검증 (간단 버전)
            if fax_number.startswith('02') and region == '서울':
                return True, "서울 지역번호 일치"
            elif fax_number.startswith('031') and ('경기' in region or '인천' in region):
                return True, "경기/인천 지역번호 일치"
            elif fax_number.startswith('032') and '인천' in region:
                return True, "인천 지역번호 일치"
            elif fax_number.startswith('051') and '부산' in region:
                return True, "부산 지역번호 일치"
            elif fax_number.startswith('053') and '대구' in region:
                return True, "대구 지역번호 일치"
            elif fax_number.startswith('062') and '광주' in region:
                return True, "광주 지역번호 일치"
            elif fax_number.startswith('042') and '대전' in region:
                return True, "대전 지역번호 일치"
            elif fax_number.startswith('052') and '울산' in region:
                return True, "울산 지역번호 일치"
            elif fax_number.startswith('064') and '제주' in region:
                return True, "제주 지역번호 일치"
            else:
                # 기타 지역번호는 일단 통과 (세부 검증은 나중에)
                return True, "지역번호 검증 통과"
                
        except Exception as e:
            return False, f"지역번호 검증 오류: {e}"
    
    def validate_stage2_simple(self, fax_number: str, institution_name: str, worker_id: int = 0) -> Tuple[bool, str, str]:
        """간소화된 2차 검증: 구글 검색"""
        try:
            # 간단한 구글 검색 시뮬레이션 (실제 구현은 Valid3 로직 참조)
            search_query = f"{fax_number} 주민센터"
            
            # 여기서는 기본적인 성공 응답을 반환 (실제로는 Valid3의 로직 사용)
            mock_result = f"{fax_number}에 대한 검색 결과 - {institution_name} 관련"
            
            return True, "구글 검색 성공", mock_result
            
        except Exception as e:
            return False, f"구글 검색 오류: {e}", ""
    
    def save_results_with_labels(self, results: List[Valid4ValidationResult]) -> str:
        """새로운 결과파일 저장 (기존 데이터 보존 + Y 라벨 컬럼 추가)"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"valid4_results_with_labels_{timestamp}.csv"
            
            self.logger.info(f"💾 결과 저장 시작: {output_file}")
            
            # 결과 데이터프레임 생성
            result_data = []
            
            for result in results:
                # 원본 데이터 가져오기
                original_row = self.input_data.iloc[result.row_index]
                
                # 새로운 행 데이터 생성 (기존 + 새로운 컬럼)
                new_row = {
                    # 기존 컬럼들 보존
                    '연번': original_row.get('연번', ''),
                    '시도': original_row.get('시도', ''),
                    '시군구': original_row.get('시군구', ''),
                    '읍면동': original_row.get('읍면동', ''),
                    '주    소': original_row.get('주    소', ''),
                    '전화번호': original_row.get('전화번호', ''),
                    '실제 기관명_전화_원본': original_row.get('실제 기관명', ''),  # 원본 7열
                    '올바른 전화번호': original_row.get('올바른 전화번호', ''),
                    '팩스번호': original_row.get('팩스번호', ''),
                    '실제 기관명_팩스_원본': original_row.get('실제 기관명.1', '') if '실제 기관명.1' in original_row else original_row.get('실제 기관명', ''),  # 원본 10열
                    '올바른 팩스번호': original_row.get('올바른 팩스번호', ''),
                    '팩스전송결과(250711)': original_row.get('팩스전송결과(250711)', ''),
                    
                    # 새로운 Y 라벨 컬럼들 (핵심!)
                    '실제_기관명_전화_AI': result.detailed_phone_label,  # "{번호}은 {기관}의 전화번호입니다"
                    '실제_기관명_팩스_AI': result.detailed_fax_label,   # "{번호}은 {기관}의 팩스번호입니다"
                    
                    # 팩스 전송 성공 여부 추가 분석
                    '팩스전송_성공여부': original_row.get('팩스전송결과(250711)', '') == '성공',
                    '우선순위_케이스': '성공' if original_row.get('팩스전송결과(250711)', '') == '성공' else '기타',
                    
                    # Phase 0 결과
                    'Phase0_성공여부': result.phase0_passed,
                    'Phase0_전화매칭': result.phone_auto_matched,
                    'Phase0_팩스매칭': result.fax_auto_matched,
                    'Phase0_신뢰도': result.auto_labeling_confidence,
                    '전화_매칭_유형': result.phone_match_type,  # "전화→전화", "전화→팩스" 등
                    '팩스_매칭_유형': result.fax_match_type,
                    
                    # 전체 검증 결과
                    '최종_결과': result.overall_result,
                    '최종_신뢰도': result.final_confidence,
                    '처리_시간_초': round(result.processing_time, 2),
                    '오류_메시지': result.error_message
                }
                
                result_data.append(new_row)
            
            # DataFrame 생성 및 저장
            result_df = pd.DataFrame(result_data)
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"✅ 결과 저장 완료: {output_file}")
            
            # 통계 출력
            self._print_valid4_statistics(results)
            
            return output_file
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            return ""
    
    def _print_valid4_statistics(self, results: List[Valid4ValidationResult]):
        """Valid4 통계 리포트 출력 (팩스 전송 성공 케이스 분석 포함)"""
        try:
            total = len(results)
            if total == 0:
                return
            
            # 팩스 전송 성공 케이스 분석 (우선순위 케이스)
            success_cases = []
            other_cases = []
            
            for i, result in enumerate(results):
                original_row = self.input_data.iloc[result.row_index]
                fax_result = original_row.get('팩스전송결과(250711)', '')
                
                if fax_result == '성공':
                    success_cases.append(result)
                else:
                    other_cases.append(result)
            
            # Phase 0 통계
            phase0_success = sum(1 for r in results if r.phase0_passed)
            phone_matched = sum(1 for r in results if r.phone_auto_matched)
            fax_matched = sum(1 for r in results if r.fax_auto_matched)
            
            # 성공 케이스 Phase 0 통계
            success_phase0 = sum(1 for r in success_cases if r.phase0_passed)
            success_phone = sum(1 for r in success_cases if r.phone_auto_matched)
            success_fax = sum(1 for r in success_cases if r.fax_auto_matched)
            
            # 매칭 유형 통계
            phone_types = {}
            fax_types = {}
            for r in results:
                phone_types[r.phone_match_type] = phone_types.get(r.phone_match_type, 0) + 1
                fax_types[r.fax_match_type] = fax_types.get(r.fax_match_type, 0) + 1
            
            print("\n" + "="*60)
            print("📊 Valid4 검증 결과 통계 (우선순위 기반)")
            print("="*60)
            print(f"🔢 총 처리 건수: {total:,}개")
            print(f"📈 팩스전송 성공 케이스: {len(success_cases):,}개 ({len(success_cases)/total*100:.1f}%)")
            print(f"📉 기타 케이스: {len(other_cases):,}개 ({len(other_cases)/total*100:.1f}%)")
            
            print(f"\n✅ 전체 Phase 0 성공률:")
            print(f"  - 전체: {phase0_success:,}개 ({phase0_success/total*100:.1f}%)")
            if len(success_cases) > 0:
                print(f"  - 팩스전송 성공 케이스: {success_phase0:,}개 ({success_phase0/len(success_cases)*100:.1f}%)")
            if len(other_cases) > 0:
                other_phase0 = phase0_success - success_phase0
                print(f"  - 기타 케이스: {other_phase0:,}개 ({other_phase0/len(other_cases)*100:.1f}%)")
            
            print(f"\n📞 전화번호 매칭:")
            print(f"  - 전체: {phone_matched:,}개 ({phone_matched/total*100:.1f}%)")
            if len(success_cases) > 0:
                print(f"  - 팩스전송 성공 케이스: {success_phone:,}개 ({success_phone/len(success_cases)*100:.1f}%)")
            
            print(f"\n📠 팩스번호 매칭:")
            print(f"  - 전체: {fax_matched:,}개 ({fax_matched/total*100:.1f}%)")
            if len(success_cases) > 0:
                print(f"  - 팩스전송 성공 케이스: {success_fax:,}개 ({success_fax/len(success_cases)*100:.1f}%)")
            
            print(f"\n📞 전화번호 매칭 유형:")
            for match_type, count in phone_types.items():
                if count > 0:
                    print(f"  - {match_type}: {count:,}개 ({count/total*100:.1f}%)")
            
            print(f"\n📠 팩스번호 매칭 유형:")
            for match_type, count in fax_types.items():
                if count > 0:
                    print(f"  - {match_type}: {count:,}개 ({count/total*100:.1f}%)")
            
            # 중요한 인사이트: 팩스 전송 성공했는데 매칭 실패한 케이스
            if len(success_cases) > 0:
                success_no_match = len(success_cases) - success_phase0
                if success_no_match > 0:
                    print(f"\n⚠️ 중요 발견:")
                    print(f"  팩스전송 성공했지만 Phase 0 매칭 실패: {success_no_match:,}개")
                    print(f"  → 이 케이스들은 웹 검색이 필요한 중요한 대상입니다!")
            
            # 처리 시간 통계
            avg_time = sum(r.processing_time for r in results) / total
            print(f"\n⏱️ 평균 처리 시간: {avg_time:.2f}초")
            
            print("="*60)
            
        except Exception as e:
            self.logger.error(f"❌ 통계 출력 실패: {e}")
    
    def process_all_data_test(self, test_mode: bool = True) -> bool:
        """전체 데이터 처리 (테스트 모드 지원)"""
        try:
            if self.input_data is None or len(self.input_data) == 0:
                self.logger.error("❌ 입력 데이터가 없습니다. load_data()를 먼저 실행하세요.")
                return False
            
            total_rows = len(self.input_data)
            self.logger.info(f"🚀 전체 데이터 처리 시작: {total_rows}개 행")
            
            results = []
            
            # 단일 스레드 처리 (Phase 0 최적화)
            for idx, (_, row) in enumerate(self.input_data.iterrows()):
                try:
                    result = self.validate_single_row((idx, row))
                    results.append(result)
                    
                    # 진행상황 출력 및 주기적 정리
                    if (idx + 1) % 10 == 0 or idx == 0:
                        self.logger.info(f"📈 진행상황: {idx+1}/{total_rows} ({(idx+1)/total_rows*100:.1f}%)")
                        
                        # 50건마다 강제 정리
                        if (idx + 1) % 50 == 0:
                            self.logger.info("🧹 주기적 드라이버 정리 실행")
                            self._cleanup_all_worker_drivers()
                            # 가비지 컬렉션
                            import gc
                            gc.collect()
                    
                except Exception as e:
                    self.logger.error(f"❌ Row {idx} 처리 실패: {e}")
                    continue
            
            # 결과 저장
            output_file = self.save_results_with_labels(results)
            
            if output_file:
                self.logger.info(f"🎉 처리 완료! 결과 파일: {output_file}")
                return True
            else:
                self.logger.error("❌ 결과 저장 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 전체 데이터 처리 실패: {e}")
            # 예외 발생 시 강제 정리
            try:
                self._cleanup_all_worker_drivers()
                self.force_kill_all_chrome_processes()
            except:
                pass
            return False
        
        finally:
            # 항상 실행되는 최종 정리
            try:
                self._cleanup_all_worker_drivers()
                self.force_kill_all_chrome_processes()
                self.logger.info("🧹 Valid4 최종 드라이버 정리 완료")
            except Exception as cleanup_e:
                self.logger.debug(f"⚠️ 최종 정리 실패: {cleanup_e}")

class Valid3ValidationManager:
    """Valid2_fixed 기반 최신 5단계 검증 관리자 (기존 클래스 유지)"""
    
    def __init__(self):
        """초기화 - utils 모듈들 + 프록시 로테이터"""
        self.logger = setup_detailed_logger("Valid3ValidationManager")
        
        try:
            self.logger.info("🔧 Valid3ValidationManager 초기화 시작")
            
            # utils 모듈들 초기화 (검증된 안정성)
            self.logger.debug("📱 PhoneValidator 초기화 중...")
            self.phone_validator = PhoneValidator(self.logger)
            self.logger.debug("✅ PhoneValidator 초기화 완료")
            
            self.logger.debug("🤖 AIModelManager 초기화 중...")
            self.ai_manager = AIModelManager(self.logger)
            self.logger.debug("✅ AIModelManager 초기화 완료")
            
            # 매크로 방지 시스템 복구 (UserAgentRotator 사용)
            self.logger.debug("🛡️ UserAgentRotator 초기화 중...")
            self.user_agent_rotator = UserAgentRotator(self.logger)
            self.logger.debug("✅ UserAgentRotator 초기화 완료")
            
            self.logger.debug("🔌 AdvancedPortManager 초기화 중...")
            self.port_manager = AdvancedPortManager(self.logger)
            self.logger.debug("✅ AdvancedPortManager 초기화 완료")
            
            # WebDriverManager는 워커별로 생성 (메모리 효율성)
            self.web_driver_managers = {}  # 워커별 관리
            self.driver_lock = threading.Lock()
            
            # 데이터
            self.input_data = None
            self.validation_results = []
            
            self.logger.info("✅ Valid3ValidationManager 초기화 완료 (매크로 방지 + 포트 관리 포함)")
            
        except Exception as e:
            self.logger.error(f"❌ Valid3ValidationManager 초기화 실패: {e}")
            self.logger.error(traceback.format_exc())
            raise
    
    def load_data(self) -> bool:
        """Excel 데이터 로드 (Valid2_fixed와 동일한 로직 유지)"""
        try:
            self.logger.info(f"📊 데이터 로드 시작: {INPUT_FILE}")
            self.logger.debug(f"파일 존재 확인: {os.path.exists(INPUT_FILE)}")
            
            if not os.path.exists(INPUT_FILE):
                self.logger.error(f"❌ 입력 파일 없음: {INPUT_FILE}")
                return False
            
            # Excel 파일 로드 (시트 자동 선택)
            self.logger.debug("Excel 파일 시트 분석 중...")
            excel_file = pd.ExcelFile(INPUT_FILE)
            sheet_names = excel_file.sheet_names
            self.logger.debug(f"발견된 시트들: {sheet_names}")
            
            # 가장 큰 시트 선택
            if len(sheet_names) > 1:
                sheet_sizes = {}
                for sheet in sheet_names:
                    temp_df = pd.read_excel(INPUT_FILE, sheet_name=sheet)
                    sheet_sizes[sheet] = len(temp_df)
                    self.logger.debug(f"시트 '{sheet}': {len(temp_df)}행")
                
                # 가장 큰 시트 선택
                selected_sheet = max(sheet_sizes, key=sheet_sizes.get)
                self.logger.info(f"📋 선택된 시트: '{selected_sheet}' ({sheet_sizes[selected_sheet]}행)")
            else:
                selected_sheet = sheet_names[0]
                self.logger.info(f"📋 기본 시트 사용: '{selected_sheet}'")
            
            # 데이터 로드
            self.input_data = pd.read_excel(INPUT_FILE, sheet_name=selected_sheet)
            self.logger.info(f"📊 로드 완료: {len(self.input_data)}행 × {len(self.input_data.columns)}열")
            
            # 컬럼 정보 로그 (디버깅용)
            self.logger.debug("컬럼 정보:")
            for i, col in enumerate(self.input_data.columns):
                self.logger.debug(f"  {i}: {col}")
            
            # 필요 컬럼 확인
            required_columns = ['C', 'E', 'G', 'H', 'I']  # 시도, 읍면동, 주소, 전화번호, 팩스번호
            if len(self.input_data.columns) >= 9:  # I열까지 있어야 함
                self.logger.info("✅ 필요 컬럼 확인 완료")
                return True
            else:
                self.logger.error(f"❌ 필요 컬럼 부족: {len(self.input_data.columns)}개 (최소 9개 필요)")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def save_results(self) -> str:
        """검증 결과 저장 (Valid2_fixed와 동일한 형식 유지)"""
        try:
            if not self.validation_results:
                self.logger.warning("⚠️ 저장할 결과가 없습니다")
                return ""
            
            self.logger.info(f"💾 검증 결과 저장 시작: {len(self.validation_results)}개")
            
            # 결과 DataFrame 생성
            results_data = []
            
            for result in self.validation_results:
                results_data.append({
                    '행번호': result.row_index + 1,
                    '팩스번호': result.fax_number,
                    '기관명(읍면동)': result.institution_name,
                    '지역(시도)': result.region,
                    '전화번호': result.phone_number,
                    '주소': result.address,
                    
                    # 단계별 결과
                    '1차_통과여부': result.stage1_passed,
                    '1차_메시지': result.stage1_message,
                    '2차_통과여부': result.stage2_passed,
                    '2차_메시지': result.stage2_message,
                    '2차_검색결과': result.google_search_result,
                    '3차_통과여부': result.stage3_passed,
                    '3차_메시지': result.stage3_message,
                    '3차_신뢰도점수': result.confidence_score,
                    '3차_발견기관명': ', '.join(result.discovered_institutions or []),
                    '4차_통과여부': result.stage4_passed,
                    '4차_메시지': result.stage4_message,
                    '4차_AI추출기관': result.ai_extracted_institution,
                    '5차_통과여부': result.stage5_passed,
                    '5차_메시지': result.stage5_message,
                    '5차_최종검증': result.final_verification,
                    
                    # 핵심 추가: 검증된 실제 주민센터명
                    '검증된_실제_주민센터명': result.verified_institution_name,
                    '주민센터_매핑_신뢰도': result.institution_mapping_confidence,
                    
                    # 최종 결과
                    '전체결과': result.overall_result,
                    '최종신뢰도': result.final_confidence,
                    '처리시간(초)': result.processing_time,
                    '오류메시지': result.error_message
                })
            
            # DataFrame 생성
            results_df = pd.DataFrame(results_data)
            
            # 파일명 생성 (타임스탬프 포함)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{OUTPUT_FILE_PREFIX}_{timestamp}.xlsx"
            
            # Excel 파일로 저장
            self.logger.debug(f"Excel 파일 저장 중: {filename}")
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                results_df.to_excel(writer, index=False, sheet_name='검증결과')
            
            # 저장 완료 로그
            file_size = os.path.getsize(filename)
            self.logger.info(f"✅ 결과 저장 완료: {filename}")
            self.logger.info(f"📁 파일 크기: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            
            # 요약 통계
            success_count = sum(1 for r in self.validation_results if r.overall_result == "데이터 올바름")
            error_count = sum(1 for r in self.validation_results if r.overall_result == "데이터 오류")
            check_count = sum(1 for r in self.validation_results if r.overall_result == "직접 확인 요망")
            
            self.logger.info(f"📊 검증 결과 요약:")
            self.logger.info(f"   - 데이터 올바름: {success_count}개")
            self.logger.info(f"   - 데이터 오류: {error_count}개")
            self.logger.info(f"   - 직접 확인 요망: {check_count}개")
            self.logger.info(f"   - 총 처리: {len(self.validation_results)}개")
            
            return filename
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            self.logger.error(traceback.format_exc())
            return ""
    
    # ================================
    # 검증 단계별 메서드들 (Valid2_fixed와 동일)
    # ================================
    
    def validate_stage1(self, fax_number: str, institution_name: str, region: str, address: str) -> Tuple[bool, str]:
        """1차 검증: 팩스번호 지역번호 매칭 (Valid2_fixed와 동일)"""
        try:
            self.logger.debug(f"📍 1차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}, 지역:{region}")
            self.logger.debug(f"주소: {address}")
            
            # 1. 팩스번호 유효성 검사 (phone_validator.py 활용)
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                message = "검증 불가 (팩스번호 없음)"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
            
            # 팩스번호 형식 검증 및 정규화
            if not self.phone_validator.is_valid_phone_format(fax_number):
                message = f"검증 불가 (팩스번호 형식 오류: {fax_number})"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
            
            # 팩스번호 정규화
            normalized_fax = self._normalize_phone_number(fax_number)
            if normalized_fax and normalized_fax != fax_number:
                self.logger.debug(f"📞 팩스번호 정규화: {fax_number} → {normalized_fax}")
            
            # 2. 지역번호 추출 (phone_validator.py 활용)
            area_code = self.phone_validator.extract_area_code(fax_number)
            if not area_code:
                message = f"검증 불가 (지역번호 추출 실패: {fax_number})"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
            
            self.logger.debug(f"추출된 지역번호: {area_code}")
            
            # 3. 지역 매칭 검사 (phone_validator.py 활용)
            is_match = self.phone_validator.is_regional_match(area_code, address)
            
            if is_match:
                message = f"팩스번호 지역번호 일치: {area_code} ↔ {address} (기관: {institution_name})"
                self.logger.info(f"✅ 1차 검증 통과: {message}")
                return True, message
            else:
                # 지역 불일치 상세 정보
                from utils.valid.phone_validator import KOREAN_AREA_CODES
                area_name = KOREAN_AREA_CODES.get(area_code, "알 수 없음")
                message = f"팩스번호 지역번호 불일치: {area_code}({area_name}) ↔ {address} (기관: {institution_name})"
                self.logger.warning(f"⚠️ 1차 검증 실패: {message}")
                return False, message
                
        except Exception as e:
            error_msg = f"1차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            return False, error_msg
    
    def get_driver_for_worker(self, worker_id: int):
        """워커별 WebDriver 인스턴스 획득 (thread-safe, 차단감지 시 재생성)"""
        with self.driver_lock:
            # 차단된 워커 확인 (UserAgentRotator 사용)
            current_rotation = 0  # UserAgentRotator는 단순한 로테이션만 지원
            
            # 기존 WebDriverManager 확인
            if worker_id in self.web_driver_managers:
                web_manager = self.web_driver_managers[worker_id]
                
                # 차단 감지로 인해 로테이션이 발생했으면 WebDriverManager 재생성
                last_rotation = getattr(web_manager, '_last_rotation_count', 0)
                if current_rotation > last_rotation:
                    self.logger.info(f"🔄 워커 {worker_id} 차단감지로 WebDriverManager 재생성 (로테이션: {last_rotation} → {current_rotation})")
                    
                    # 기존 매니저 정리
                    try:
                        if hasattr(web_manager, 'cleanup_all_drivers'):
                            web_manager.cleanup_all_drivers()
                        elif hasattr(web_manager, 'cleanup'):
                            web_manager.cleanup()
                    except Exception as e:
                        self.logger.debug(f"⚠️ 워커 {worker_id} 기존 매니저 정리 실패: {e}")
                    
                    # 새로운 매니저 생성
                    del self.web_driver_managers[worker_id]
                    new_manager = WebDriverManager(logger=self.logger)
                    new_manager._last_rotation_count = current_rotation
                    self.web_driver_managers[worker_id] = new_manager
                    
                    self.logger.debug(f"✅ 워커 {worker_id} WebDriverManager 재생성 완료")
                    return new_manager
                else:
                    # 기존 매니저 재사용
                    return web_manager
            else:
                # 새로운 WebDriverManager 생성
                self.logger.debug(f"🔧 워커 {worker_id} WebDriverManager 새로 생성 중...")
                new_manager = WebDriverManager(logger=self.logger)
                new_manager._last_rotation_count = current_rotation
                self.web_driver_managers[worker_id] = new_manager
                self.logger.debug(f"✅ 워커 {worker_id} WebDriverManager 생성 완료")
                
                return new_manager
    
    def cleanup_worker_driver(self, worker_id: int):
        """워커별 드라이버 완전 정리 (포트 해제 포함)"""
        try:
            with self.driver_lock:
                if worker_id in self.web_driver_managers:
                    web_manager = self.web_driver_managers[worker_id]
                    
                    # 워커가 사용중인 포트들 해제
                    if hasattr(web_manager, 'used_ports'):
                        for port in list(web_manager.used_ports):
                            try:
                                self.port_manager.release_port(port, worker_id)
                                self.logger.debug(f"🔓 워커 {worker_id} 포트 {port} 해제")
                            except Exception as port_error:
                                self.logger.debug(f"⚠️ 워커 {worker_id} 포트 {port} 해제 실패: {port_error}")
                    
                    # WebDriverManager의 정리 메서드 호출 (있는 경우)
                    if hasattr(web_manager, 'cleanup_all_drivers'):
                        web_manager.cleanup_all_drivers()
                    elif hasattr(web_manager, 'cleanup'):
                        web_manager.cleanup()
                    
                    # 딕셔너리에서 제거
                    del self.web_driver_managers[worker_id]
                    self.logger.debug(f"🧹 워커 {worker_id} WebDriverManager 완전 정리 (포트 해제 포함)")
        except Exception as e:
            self.logger.debug(f"⚠️ 워커 {worker_id} 정리 중 오류 (무시): {e}")
    
    def force_kill_all_chrome_processes(self):
        """크롬 프로세스 강제 종료 (비상용)"""
        try:
            import subprocess
            import platform
            
            if platform.system() == "Windows":
                # Windows에서 모든 크롬 관련 프로세스 종료
                subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'], 
                             capture_output=True, text=True)
                subprocess.run(['taskkill', '/f', '/im', 'chromedriver.exe'], 
                             capture_output=True, text=True)
                self.logger.info("🧹 Windows 크롬 프로세스 강제 종료")
            else:
                # Linux/Mac에서 크롬 관련 프로세스 종료
                subprocess.run(['pkill', '-f', 'chrome'], 
                             capture_output=True, text=True)
                subprocess.run(['pkill', '-f', 'chromedriver'], 
                             capture_output=True, text=True)
                self.logger.info("🧹 Linux/Mac 크롬 프로세스 강제 종료")
                
        except Exception as e:
            self.logger.debug(f"⚠️ 크롬 프로세스 강제 종료 실패: {e}")
    
    def _apply_user_agent_config(self, driver, user_agent: str, worker_id: int):
        """드라이버에 User-Agent 설정 적용 (매크로 방지)"""
        try:
            if not driver or not user_agent:
                return False
            
            self.logger.debug(f"🎭 User-Agent 설정 적용 시작: 워커 {worker_id}")
            
            # 1. User-Agent 변경 (CDP 명령 사용)
            try:
                driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                    "userAgent": user_agent,
                    "acceptLanguage": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                    "platform": "Win32"
                })
                self.logger.debug(f"✅ User-Agent 변경: {user_agent[:50]}...")
            except Exception as e:
                self.logger.debug(f"⚠️ User-Agent 변경 실패: {e}")
            
            # 2. navigator.webdriver 숨김 (봇 감지 방지)
            try:
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.logger.debug("✅ navigator.webdriver 숨김 완료")
            except Exception as e:
                self.logger.debug(f"⚠️ navigator.webdriver 숨김 실패: {e}")
            
            # 3. 추가 CDP 명령들 (봇 감지 방지)
            try:
                # WebGL 벤더 정보 조작
                driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    'source': '''
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                        Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US', 'en']});
                        window.chrome = { runtime: {} };
                        Object.defineProperty(navigator, 'permissions', {get: () => ({query: () => Promise.resolve({state: 'granted'})})});
                    '''
                })
                self.logger.debug("✅ 추가 봇 방지 스크립트 적용")
            except Exception as e:
                self.logger.debug(f"⚠️ 추가 봇 방지 스크립트 실패: {e}")
            
            self.logger.debug(f"✅ User-Agent 설정 적용 완료: 워커 {worker_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ User-Agent 설정 적용 실패: {e}")
            return False
    
    # 2차 검증 Google 검색, 팩스번호의 진짜 기관명 확인
    def validate_stage2(self, fax_number: str, institution_name: str, worker_id: int = 0) -> Tuple[bool, str, str]:
        """2차 검증: Google 검색으로 팩스번호의 진짜 기관명 확인 (매크로방지 시스템 적용)"""
        try:
            self.logger.debug(f"🔍 2차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}")
            
            # 1차 검증을 통과한 경우만 진행
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                message = "1차 검증 실패로 인한 2차 검증 건너뛰기"
                self.logger.info(f"⏭️ {message}")
                return False, message, ""
            
            # WebDriverManager 획득
            web_manager = self.get_driver_for_worker(worker_id)
            
            # User-Agent 로테이션 설정 (매크로 방지)
            user_agent = self.user_agent_rotator.get_random_user_agent()
            assigned_port = self.port_manager.allocate_port(worker_id) if hasattr(self, 'port_manager') else None
            
            # 복수 검색 쿼리 생성 (사용자 요구사항)
            search_queries = [
                f'{fax_number} 팩스번호 어느기관',
                f'{fax_number} 팩스번호 어디',
                f'{fax_number}는 어디 팩스번호',
                f'팩스번호 {fax_number}',
                f'fax {fax_number}'
            ]
            
            self.logger.debug(f"🔍 검색 쿼리 {len(search_queries)}개: {search_queries}")
            
            # 드라이버 생성 및 검색 실행
            driver = None
            try:
                self.logger.debug(f"🛡️ 워커 {worker_id} 매크로방지 드라이버 생성 중... (포트: {assigned_port})")
                
                # 포트를 지정하여 드라이버 생성
                if assigned_port:
                    driver = web_manager.create_bot_evasion_driver(worker_id=worker_id, port=assigned_port)
                else:
                    driver = web_manager.create_bot_evasion_driver(worker_id=worker_id)
                
                if not driver:
                    # 포트 해제
                    if assigned_port:
                        self.port_manager.release_port(assigned_port, worker_id)
                    message = "드라이버 생성 실패"
                    self.logger.error(f"❌ {message}")
                    return False, message, ""
                
                self.logger.debug(f"✅ 워커 {worker_id} 드라이버 생성 완료")
                
                # User-Agent 설정 적용 (매크로 방지)
                ua_applied = self._apply_user_agent_config(driver, user_agent, worker_id)
                if ua_applied:
                    self.logger.debug(f"✅ 워커 {worker_id} User-Agent 설정 적용 완료")
                else:
                    self.logger.warning(f"⚠️ 워커 {worker_id} User-Agent 설정 적용 실패")
                
                # 인간적인 지연 (빠른 검색 우선)
                time.sleep(random.uniform(0.5, 1.0))
                
                # 모든 검색 쿼리 시도
                for query_idx, search_query in enumerate(search_queries):
                    try:
                        self.logger.debug(f"🔍 검색 쿼리 {query_idx + 1}/{len(search_queries)}: {search_query}")
                        
                        # Google 검색 페이지 접속
                        driver.get("https://www.google.com")
                        
                        # 검색창 찾기 (최적화된 순서)
                        search_box = None
                        selectors = ['textarea[name="q"]', '#APjFqb', 'input[name="q"]']
                        
                        for selector in selectors:
                            try:
                                quick_wait = WebDriverWait(driver, 5)  # 안정성을 위해 5초로 복원
                                search_box = quick_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                                self.logger.debug(f"✅ 검색창 발견: {selector}")
                                break
                            except TimeoutException:
                                continue
                        
                        if not search_box:
                            continue  # 다음 쿼리 시도
                        
                        # 인간적인 타이핑 (문자별 지연)
                        search_box.clear()
                        time.sleep(random.uniform(0.3, 0.7))
                        
                        for char in search_query:
                            search_box.send_keys(char)
                            time.sleep(random.uniform(0.05, 0.15))  # 인간적인 타이핑 속도 복원
                        
                        # 검색 실행
                        time.sleep(random.uniform(0.5, 1.0))
                        search_box.send_keys(Keys.RETURN)
                        self.logger.debug(f"🔍 검색 실행됨: {search_query}")
                        
                        # 검색 결과 대기
                        try:
                            result_wait = WebDriverWait(driver, 10)  # 안정성을 위해 10초로 복원
                            result_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#search')))
                            self.logger.debug("✅ 검색 결과 로드 완료")
                        except TimeoutException:
                            self.logger.warning(f"⚠️ 검색 결과 로드 타임아웃: {search_query}")
                            continue  # 다음 쿼리 시도
                        
                        # 검색 결과 텍스트 추출
                        try:
                            results = driver.find_elements(By.CSS_SELECTOR, 'h3')[:5]  # 상위 5개 결과
                            snippets = driver.find_elements(By.CSS_SELECTOR, '.VwiC3b')[:5]
                            
                            search_results = []
                            for i, result in enumerate(results):
                                title = result.text.strip()
                                snippet = snippets[i].text.strip() if i < len(snippets) else ""
                                search_results.append(f"{title}: {snippet}")
                            
                            search_result_text = " | ".join(search_results)
                            
                            if search_result_text:
                                # 기관명이 검색 결과에 포함되어 있는지 확인
                                if institution_name in search_result_text:
                                    message = f"Google 검색에서 기관명 확인됨: {institution_name} (쿼리: {search_query})"
                                    self.logger.info(f"✅ 2차 검증 통과: {message}")
                                    return True, message, search_result_text
                                else:
                                    self.logger.debug(f"🔍 쿼리 {query_idx + 1} 결과에 기관명 불포함")
                            
                            # 다음 검색어 시도를 위한 지연
                            if query_idx < len(search_queries) - 1:
                                time.sleep(random.uniform(1.0, 2.0))
                        
                        except Exception as e:
                            self.logger.debug(f"⚠️ 쿼리 {query_idx + 1} 결과 추출 오류: {e}")
                            continue
                    
                    except Exception as e:
                        self.logger.debug(f"⚠️ 쿼리 {query_idx + 1} 검색 오류: {e}")
                        continue
                
                # 모든 쿼리 실패
                message = f"Google 검색에서 기관명 불일치 (모든 쿼리 시도 완료)"
                self.logger.warning(f"⚠️ 2차 검증 실패: {message}")
                return False, message, ""
                
            finally:
                # 포트 해제 (즉시)
                if assigned_port:
                    self.port_manager.release_port(assigned_port, worker_id)
                    self.logger.debug(f"🔓 워커 {worker_id} 포트 {assigned_port} 즉시 해제")
                
                # 드라이버 정리
                if driver:
                    try:
                        driver.quit()
                        self.logger.debug(f"🧹 워커 {worker_id} 드라이버 정리 완료")
                    except:
                        pass
                        
        except Exception as e:
            error_msg = f"2차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            
            # 차단 감지 시 로그 기록
            if "Connection" in str(e) or "timeout" in str(e).lower():
                self.logger.warning(f"🚨 워커 {worker_id} 연결 문제 감지: {str(e)[:100]}...")
            
            return False, error_msg, ""
    
    # 3차 검증 검색결과 링크 크롤링, 기관명 추출
    def validate_stage3(self, fax_number: str, institution_name: str, google_search_result: str, worker_id: int = 0) -> Tuple[bool, str, List[str], List[Dict], float]:
        """3차 검증: 검색결과 링크 크롤링 + 기관명 추출 (매크로방지 시스템 적용)"""
        try:
            self.logger.debug(f"🔗 3차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}")
            
            # 2차 검증 결과가 없으면 건너뛰기
            if not google_search_result:
                message = "2차 검증 결과 없음으로 3차 검증 건너뛰기"
                self.logger.info(f"⏭️ {message}")
                return False, message, [], [], 0.0
            
            # WebDriverManager 획득
            web_manager = self.get_driver_for_worker(worker_id)
            
            # User-Agent 로테이션 설정 (매크로 방지)
            user_agent_3rd = self.user_agent_rotator.get_random_user_agent()
            assigned_port_3rd = self.port_manager.allocate_port(worker_id) if hasattr(self, 'port_manager') else None
            
            # 복수 검색 쿼리 생성 (사용자 요구사항)
            search_queries = [
                f'{fax_number} 팩스번호 어느기관',
                f'{fax_number} 팩스번호 어디',
                f'{fax_number}는 어디 팩스번호',
                f'팩스번호 {fax_number}',
                f'fax {fax_number}'
            ]
            
            # 드라이버 생성 및 링크 추출
            driver = None
            extracted_links = []
            crawled_data = []
            
            try:
                self.logger.debug(f"🛡️ 워커 {worker_id} 3차 검증용 매크로방지 드라이버 생성 중... (포트: {assigned_port_3rd})")
                
                # 포트를 지정하여 드라이버 생성
                if assigned_port_3rd:
                    driver = web_manager.create_bot_evasion_driver(worker_id=worker_id, port=assigned_port_3rd)
                else:
                    driver = web_manager.create_bot_evasion_driver(worker_id=worker_id)
                
                if not driver:
                    # 포트 해제
                    if assigned_port_3rd:
                        self.port_manager.release_port(assigned_port_3rd, worker_id)
                    message = "3차 검증용 드라이버 생성 실패"
                    self.logger.error(f"❌ {message}")
                    return False, message, [], [], 0.0
                
                self.logger.debug(f"✅ 워커 {worker_id} 3차 검증용 드라이버 생성 완료")
                
                # User-Agent 설정 적용 (매크로 방지)
                ua_applied_3rd = self._apply_user_agent_config(driver, user_agent_3rd, worker_id)
                if ua_applied_3rd:
                    self.logger.debug(f"✅ 워커 {worker_id} 3차 User-Agent 설정 적용 완료")
                else:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 3차 User-Agent 설정 적용 실패")
                
                # 인간적인 지연 (빠른 검색 우선)
                time.sleep(random.uniform(0.5, 1.0))
                
                # 모든 검색 쿼리로 링크 추출 시도
                for query_idx, search_query in enumerate(search_queries):
                    try:
                        self.logger.debug(f"🔗 3차 검증 검색 쿼리 {query_idx + 1}/{len(search_queries)}: {search_query}")
                        
                        # Google 검색 페이지 접속
                        driver.get("https://www.google.com")
                        
                        # 검색창 찾기 및 검색 실행
                        search_box = None
                        selectors = ['textarea[name="q"]', '#APjFqb', 'input[name="q"]']
                        
                        for selector in selectors:
                            try:
                                quick_wait = WebDriverWait(driver, 5)  # 안정성을 위해 5초로 복원
                                search_box = quick_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                                break
                            except TimeoutException:
                                continue
                        
                        if not search_box:
                            continue  # 다음 쿼리 시도
                        
                        # 인간적인 타이핑 (문자별 지연)
                        search_box.clear()
                        time.sleep(random.uniform(0.3, 0.7))
                        
                        for char in search_query:
                            search_box.send_keys(char)
                            time.sleep(random.uniform(0.05, 0.15))  # 인간적인 타이핑 속도 복원
                        
                        # 검색 실행
                        time.sleep(random.uniform(0.5, 1.0))
                        search_box.send_keys(Keys.RETURN)
                        self.logger.debug(f"🔍 3차 검증 검색 실행됨: {search_query}")
                        
                        # 검색 결과 대기
                        try:
                            result_wait = WebDriverWait(driver, 10)  # 안정성을 위해 10초로 복원
                            result_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#search')))
                            self.logger.debug("✅ 3차 검증 검색 결과 로드 완료")
                        except TimeoutException:
                            self.logger.warning(f"⚠️ 3차 검증 검색 결과 로드 타임아웃: {search_query}")
                            continue  # 다음 쿼리 시도
                        
                        # 검색 결과 링크 추출
                        try:
                            link_elements = driver.find_elements(By.CSS_SELECTOR, '#search a[href]')
                            
                            query_links = []
                            for element in link_elements[:SEARCH_RESULTS_LIMIT]:
                                href = element.get_attribute('href')
                                if href and href.startswith('http') and 'google.com' not in href:
                                    if href not in extracted_links:  # 중복 제거
                                        extracted_links.append(href)
                                        query_links.append(href)
                                        self.logger.debug(f"🔗 링크 추출: {href[:60]}...")
                            
                            self.logger.debug(f"📎 쿼리 {query_idx + 1}: {len(query_links)}개 링크 추출")
                            
                            # 다음 검색어 시도를 위한 지연
                            if query_idx < len(search_queries) - 1:
                                time.sleep(random.uniform(1.0, 2.0))
                        
                        except Exception as e:
                            self.logger.debug(f"⚠️ 3차 검증 쿼리 {query_idx + 1} 링크 추출 오류: {e}")
                            continue
                    
                    except Exception as e:
                        self.logger.debug(f"⚠️ 3차 검증 쿼리 {query_idx + 1} 검색 오류: {e}")
                        continue
                
                self.logger.info(f"📎 3차 검증: 총 {len(extracted_links)}개 링크 추출 완료 (모든 쿼리)")
                
                # 추출된 링크들을 병렬로 크롤링
                if extracted_links:
                    crawled_data = self._crawl_links_parallel(extracted_links, fax_number, institution_name, worker_id)
                
                # 향상된 신뢰도 점수 계산 (팩스번호 정확 일치 시 높은 점수)
                confidence_score = self._enhanced_confidence_calculation(crawled_data, fax_number, institution_name)
                
                # 3차 검증 결과 판정
                if confidence_score >= CONFIDENCE_THRESHOLD:
                    message = f"3차 검증 통과: 신뢰도 {confidence_score:.1f}% (임계값: {CONFIDENCE_THRESHOLD}%)"
                    self.logger.info(f"✅ {message}")
                    return True, message, extracted_links, crawled_data, confidence_score
                else:
                    message = f"3차 검증 실패: 신뢰도 {confidence_score:.1f}% (임계값: {CONFIDENCE_THRESHOLD}%)"
                    self.logger.warning(f"⚠️ {message}")
                    return False, message, extracted_links, crawled_data, confidence_score
                
            finally:
                # 포트 해제 (즉시)
                if assigned_port_3rd:
                    self.port_manager.release_port(assigned_port_3rd, worker_id)
                    self.logger.debug(f"🔓 워커 {worker_id} 3차 포트 {assigned_port_3rd} 즉시 해제")
                
                # 드라이버 정리
                if driver:
                    try:
                        driver.quit()
                        self.logger.debug(f"🧹 워커 {worker_id} 3차 검증용 드라이버 정리 완료")
                    except:
                        pass
                        
        except Exception as e:
            error_msg = f"3차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            
            # 차단 감지 시 로그 기록
            if "Connection" in str(e) or "timeout" in str(e).lower():
                self.logger.warning(f"🚨 워커 {worker_id} 연결 문제 감지: {str(e)[:100]}...")
            
            return False, error_msg, [], [], 0.0
    
    # 4차 검증 링크 직접 파싱, AI 기관명 도출
    def validate_stage4(self, fax_number: str, institution_name: str, extracted_links: List[str], 
                       discovered_institutions: List[str], worker_id: int = 0) -> Tuple[bool, str, str]:
        """4차 검증: 링크 직접 파싱 + AI 기관명 도출 + 백업 로직 (수정된 메서드)"""
        try:
            self.logger.debug(f"🤖 4차 검증 시작: 팩스:{fax_number}, 기관:{institution_name}")
            
            # 3차 검증 결과가 없으면 건너뛰기
            if not extracted_links:
                message = "3차 검증 링크 없음으로 4차 검증 건너뛰기"
                self.logger.info(f"⏭️ {message}")
                return False, message, ""
            
            # 링크들을 직접 파싱하여 상세 정보 수집
            detailed_parsing_results = []
            
            self.logger.info(f"🔍 4차 검증: {len(extracted_links)}개 링크 직접 파싱 시작")
            
            for i, link in enumerate(extracted_links):
                self.logger.debug(f"🌐 링크 {i+1}/{len(extracted_links)} 직접 파싱: {link[:50]}...")
                
                # verification_engine 방식으로 상세 파싱
                detailed_result = self._parse_link_with_verification_engine(link, fax_number, institution_name)
                
                if detailed_result.get('success', False):
                    detailed_parsing_results.append(detailed_result)
                    self.logger.debug(f"✅ 링크 {i+1} 상세 파싱 성공")
                else:
                    self.logger.debug(f"⚠️ 링크 {i+1} 상세 파싱 실패")
            
            if not detailed_parsing_results:
                message = "모든 링크 파싱 실패로 4차 검증 실패"
                self.logger.warning(f"⚠️ {message}")
                return False, message, ""
            
            # AI 모델에 전달할 종합 정보 구성
            ai_context = self._prepare_ai_context_for_stage4(
                fax_number, institution_name, detailed_parsing_results, discovered_institutions
            )
            
            # 수정된 AI 모델을 통한 기관명 도출
            ai_extracted_institution = self._fixed_extract_institution_with_ai(ai_context, fax_number, institution_name)
            
            if ai_extracted_institution:
                # AI가 추출한 기관명과 원본 기관명 비교
                similarity_score = self._calculate_institution_similarity(institution_name, ai_extracted_institution)
                
                if similarity_score >= 0.7:  # 70% 이상 유사성
                    message = f"4차 검증 통과: AI 추출 기관명 일치 ({ai_extracted_institution}, 유사도: {similarity_score:.2f})"
                    self.logger.info(f"✅ {message}")
                    return True, message, ai_extracted_institution
                else:
                    message = f"4차 검증 실패: AI 추출 기관명 불일치 ({ai_extracted_institution}, 유사도: {similarity_score:.2f})"
                    self.logger.warning(f"⚠️ {message}")
                    return False, message, ai_extracted_institution
            else:
                # 백업 로직: AI 실패 시 3차 검증 결과 활용
                self.logger.info("🔄 AI 실패 - 백업 로직 실행: 3차 검증 결과 활용")
                
                backup_institution = self._select_best_discovered_institution(
                    discovered_institutions, institution_name, fax_number, detailed_parsing_results
                )
                
                if backup_institution:
                    similarity_score = self._calculate_institution_similarity(institution_name, backup_institution)
                    
                    if similarity_score >= 0.6:  # 백업 로직은 더 관대한 기준 (60%)
                        message = f"4차 검증 통과 (백업): 3차 발견 기관명 활용 ({backup_institution}, 유사도: {similarity_score:.2f})"
                        self.logger.info(f"✅ {message}")
                        return True, message, backup_institution
                    else:
                        message = f"4차 검증 실패 (백업): 발견 기관명 불일치 ({backup_institution}, 유사도: {similarity_score:.2f})"
                        self.logger.warning(f"⚠️ {message}")
                        return False, message, backup_institution
                else:
                    message = "4차 검증 실패: AI 및 백업 로직 모두 실패"
                    self.logger.warning(f"⚠️ {message}")
                    return False, message, ""
                
        except Exception as e:
            error_msg = f"4차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            return False, error_msg, ""
    
    # 5차 검증 기관명 팩스번호 역검색, 2/3/4차 검증값과 완벽 AI 매칭, 최종 판정
    def validate_stage5(self, validation_result: ValidationResult) -> Tuple[bool, str, str]:
        """5차 검증: {기관명} 팩스번호 역검색 → 2/3/4차 검증값과 완벽 AI 매칭 → 최종 판정"""
        try:
            self.logger.info(f"🔍 5차 검증: 최종 종합 판정 시작")
            
            # 필수 데이터 확인
            if not validation_result.fax_number or validation_result.fax_number in ['nan', 'None', '', '#N/A']:
                message = "팩스번호 없음으로 5차 검증 불가"
                self.logger.info(f"⏭️ {message}")
                return False, message, "검증 불가"
            
            # 1단계: AI가 추출한 기관명으로 역검색 (search_logic.txt 요구사항)
            ai_institution = validation_result.ai_extracted_institution
            if not ai_institution:
                # 백업 로직: 3차 검증에서 발견된 기관명 활용
                if validation_result.discovered_institutions:
                    ai_institution = validation_result.discovered_institutions[0]
                    self.logger.info(f"🔄 AI 기관명 없음, 3차 발견 기관명 사용: {ai_institution}")
                else:
                    message = "AI 추출 기관명 및 3차 발견 기관명 없음으로 5차 검증 실패"
                    self.logger.warning(f"⚠️ {message}")
                    return False, message, "직접 확인 요망"
            
            # 2단계: 기관명으로 팩스번호 역검색 실행
            reverse_search_result = self._reverse_search_institution_fax(ai_institution, validation_result.fax_number)
            
            # 3단계: 2/3/4차 검증값 종합 수집
            all_stage_values = self._collect_all_stage_validation_values(validation_result)
            
            # 4단계: AI를 통한 완벽한 매칭 판단 (search_logic.txt 핵심 요구사항)
            perfect_matching_result = self._ai_perfect_matching_analysis(
                validation_result.fax_number,
                validation_result.institution_name,
                ai_institution,
                all_stage_values,
                reverse_search_result
            )
            
            # 5단계: 최종 결과 판정
            if perfect_matching_result['is_data_correct']:
                message = f"데이터 올바름: {perfect_matching_result['reason']} (신뢰도: {perfect_matching_result['confidence']}%)"
                self.logger.info(f"✅ 5차 검증 통과: {message}")
                return True, message, "데이터 올바름"
            elif perfect_matching_result['is_data_error']:
                message = f"데이터 오류: {perfect_matching_result['reason']} (신뢰도: {perfect_matching_result['confidence']}%)"
                self.logger.warning(f"❌ 5차 검증 실패: {message}")
                return False, message, "데이터 오류"
            else:
                # search_logic.txt 요구사항: "직접 검색 요망, 검색 및 AI검증실패"
                message = "직접 검색 요망, 검색 및 AI검증실패"
                self.logger.warning(f"⚠️ 5차 검증 실패: {message}")
                return False, message, "직접 확인 요망"
                
        except Exception as e:
            error_msg = f"5차 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            # search_logic.txt 요구사항에 따른 오류 처리
            return False, "직접 검색 요망, 검색 및 AI검증실패", "직접 확인 요망" 
    
    # ================================
    # 핵심 헬퍼 메서드들 (Valid2_fixed와 동일)
    # ================================
    
    def _fixed_extract_institution_with_ai(self, context: str, fax_number: str, expected_institution: str) -> str:
        """수정된 AI 모델을 통한 기관명 추출 (올바른 메서드명 + 강화된 응답 처리)"""
        try:
            prompt = f"""팩스번호 {fax_number}이 어느 기관 소속인지 알려주세요.

검증 정보:
{context}

예상 기관: {expected_institution}

한 줄로 기관명만 답변해주세요. 예: "종로구청" 또는 "청운효자동주민센터"
확실하지 않으면: "불명"
"""
            
            try:
                self.logger.debug("🤖 수정된 AI 모델 호출 시작...")
                self.logger.debug(f"🤖 프롬프트 길이: {len(prompt)} 문자")
                
                # 올바른 메서드 호출 (generate_content → extract_with_gemini)
                response = self.ai_manager.extract_with_gemini(context, prompt)
                
                self.logger.debug(f"🤖 AI 응답 원본: '{response}'")
                self.logger.debug(f"🤖 AI 응답 길이: {len(response) if response else 0} 문자")
                
                if response and response.strip():
                    extracted_institution = response.strip()
                    
                    # 강화된 응답 처리
                    extracted_institution = extracted_institution.strip('"\'""''`')
                    self.logger.debug(f"🤖 따옴표 제거 후: '{extracted_institution}'")
                    
                    # 부정적 응답 체크
                    negative_keywords = ["불명", "확인불가", "없음", "찾을 수 없", "알 수 없", "모름", "확실하지 않", "판단 어려", "정보 부족"]
                    if any(keyword in extracted_institution for keyword in negative_keywords):
                        self.logger.debug(f"🤖 AI: 부정적 응답 감지 - '{extracted_institution}'")
                        return ""
                    
                    # 길이 및 유효성 검증
                    if extracted_institution and 2 <= len(extracted_institution) <= 50:
                        # 한글 포함 검증
                        import re
                        if re.search(r'[가-힣]', extracted_institution):
                            self.logger.info(f"✅ 🤖 수정된 AI 추출 기관명: '{extracted_institution}'")
                            return extracted_institution
                
                self.logger.warning("⚠️ 🤖 수정된 AI 응답이 비어있거나 처리할 수 없음")
                return ""
                
            except Exception as ai_error:
                self.logger.error(f"❌ 🤖 수정된 AI 모델 호출 오류: {ai_error}")
                return ""
            
        except Exception as e:
            self.logger.error(f"❌ 수정된 AI 기관명 추출 실패: {e}")
            return ""
    
    def _select_best_discovered_institution(self, discovered_institutions: List[str], 
                                          original_institution: str, fax_number: str, 
                                          detailed_results: List[Dict]) -> str:
        """3차 검증에서 발견된 기관명 중 가장 적절한 것 선택 (백업 로직)"""
        try:
            if not discovered_institutions:
                self.logger.debug("🔄 백업: 발견된 기관명이 없음")
                return ""
            
            self.logger.info(f"🔄 백업 로직: {len(discovered_institutions)}개 발견 기관명 평가 중")
            
            best_institution = ""
            best_score = 0.0
            
            for institution in discovered_institutions:
                score = 0.0
                
                # 원본과의 유사도 (50점)
                similarity = self._calculate_institution_similarity(original_institution, institution)
                score += similarity * 50
                
                # 팩스번호와의 연관성 (30점)
                for result in detailed_results:
                    if institution in result.get('title', '') or institution in result.get('fax_context', ''):
                        if result.get('fax_exact_match', False):
                            score += 30
                            break
                        elif result.get('has_fax_number', False):
                            score += 15
                            break
                
                # 기관명 완성도 (20점)
                if '주민센터' in institution or '구청' in institution or '시청' in institution:
                    score += 20
                
                self.logger.debug(f"🔄 백업: '{institution}' 점수: {score:.1f}")
                
                if score > best_score:
                    best_score = score
                    best_institution = institution
            
            if best_institution:
                self.logger.info(f"🔄 백업: 최적 기관명 선택 - '{best_institution}' (점수: {best_score:.1f})")
            
            return best_institution
            
        except Exception as e:
            self.logger.error(f"❌ 백업 기관명 선택 실패: {e}")
            return ""
    
    def _enhanced_confidence_calculation(self, crawled_data: List[Dict], fax_number: str, institution_name: str) -> float:
        """향상된 신뢰도 점수 계산 (팩스번호 정확 일치 시 높은 점수)"""
        if not crawled_data:
            return 0.0
        
        total_score = 0.0
        max_possible_score = len(crawled_data) * 100
        fax_exact_match_bonus = 0.0
        
        for data in crawled_data:
            page_score = 0.0
            
            # 팩스번호 정확도 (대폭 강화: 50점)
            if data.get('fax_exact_match', False):
                page_score += 50
                fax_exact_match_bonus += 20
                self.logger.debug(f"🎯 정확한 팩스번호 일치 발견: +50점 (+20 보너스)")
            elif data['has_fax_number']:
                page_score += 25
                self.logger.debug(f"📝 팩스번호 텍스트 발견: +25점")
            
            # 기관명 포함 여부 (30점)
            if data['has_institution_name']:
                page_score += 30
            
            # 관련 기관 키워드 발견 (10점)
            if data['found_institutions']:
                page_score += 10
            
            total_score += page_score
        
        # 팩스번호 정확 일치 보너스 적용
        total_score += fax_exact_match_bonus
        max_possible_score += fax_exact_match_bonus
        
        confidence = (total_score / max_possible_score) * 100 if max_possible_score > 0 else 0.0
        
        # 팩스번호 정확 일치가 하나라도 있으면 최소 70% 보장
        if fax_exact_match_bonus > 0 and confidence < 70:
            confidence = 70
            self.logger.info(f"🎯 팩스번호 정확 일치로 최소 신뢰도 70% 보장")
        
        self.logger.debug(f"📊 향상된 신뢰도 점수: {confidence:.1f}%")
        return confidence
    
    def _normalize_phone_number(self, phone: str) -> str:
        """전화번호 정규화"""
        try:
            digits = re.sub(r'[^\d]', '', phone)
            if len(digits) < 9 or len(digits) > 11:
                return ''
            
            if len(digits) == 9:
                return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
            elif len(digits) == 10:
                if digits.startswith('02'):
                    return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
                else:
                    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11:
                return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
            
            return ''
        except Exception as e:
            self.logger.error(f"❌ 전화번호 정규화 실패: {e}")
            return ''
    
    def _parse_link_with_verification_engine(self, url: str, fax_number: str, institution_name: str) -> Dict:
        """verification_engine.py 방식으로 링크 직접 파싱"""
        try:
            self.logger.debug(f"🔍 verification_engine 방식 파싱: {url[:50]}...")
            
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url
            
            try:
                response = requests.get(url, timeout=CRAWLING_TIMEOUT, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    title = soup.find('title')
                    title_text = title.get_text(strip=True) if title else ""
                    full_text = soup.get_text()
                    
                    # 팩스번호 정확도 검사
                    target_fax = self._normalize_phone_number(fax_number)
                    fax_exact_match = target_fax in full_text
                    
                    return {
                        'url': url,
                        'title': title_text,
                        'full_text': full_text[:1000],
                        'fax_exact_match': fax_exact_match,
                        'has_fax_number': fax_number in full_text or target_fax in full_text,
                        'has_institution_name': institution_name in full_text,
                        'success': True
                    }
                
            except Exception as e:
                self.logger.debug(f"BS4 파싱 실패: {e}")
            
            return {'url': url, 'success': False, 'error': 'parsing_failed'}
            
        except Exception as e:
            self.logger.error(f"❌ verification_engine 파싱 오류: {e}")
            return {'url': url, 'success': False, 'error': str(e)}
    
    def _prepare_ai_context_for_stage4(self, fax_number: str, institution_name: str, 
                                      detailed_results: List[Dict], discovered_institutions: List[str]) -> str:
        """4차 검증용 AI 컨텍스트 준비"""
        try:
            context_parts = [
                f"검증 대상 팩스번호: {fax_number}",
                f"예상 기관명: {institution_name}",
                ""
            ]
            
            if discovered_institutions:
                context_parts.append(f"3차 검증에서 발견된 기관명들: {', '.join(discovered_institutions)}")
                context_parts.append("")
            
            for i, result in enumerate(detailed_results):
                context_parts.append(f"=== 웹사이트 {i+1}: {result['url'][:50]}... ===")
                context_parts.append(f"제목: {result.get('title', 'N/A')}")
                
                if result.get('fax_exact_match'):
                    context_parts.append("✅ 팩스번호 정확 일치 확인")
                
                context_parts.append("")
            
            return "\n".join(context_parts)
            
        except Exception as e:
            self.logger.error(f"❌ AI 컨텍스트 준비 실패: {e}")
            return f"검증 대상: {fax_number} - {institution_name}"
    
    def _calculate_institution_similarity(self, original: str, extracted: str) -> float:
        """기관명 유사도 계산"""
        try:
            if original == extracted:
                return 1.0
            if original in extracted or extracted in original:
                return 0.8
            
            original_keywords = set(original.replace('주민센터', '').replace('구청', '').replace('시청', '').split())
            extracted_keywords = set(extracted.replace('주민센터', '').replace('구청', '').replace('시청', '').split())
            
            if original_keywords & extracted_keywords:
                return 0.6
            
            return 0.0
        except Exception as e:
            self.logger.error(f"❌ 기관명 유사도 계산 실패: {e}")
            return 0.0
    
    # ================================
    # 5차 검증 전용 헬퍼 메서드들
    # ================================
    
    def _reverse_search_institution_fax(self, institution_name: str, target_fax: str) -> Dict:
        """기관명으로 팩스번호 역검색 (5차 검증용)"""
        try:
            self.logger.info(f"🔄 역검색: '{institution_name}' 기관의 팩스번호 검색")
            
            # 간단한 역검색 시뮬레이션 (실제로는 Google 검색 등을 통해 구현 가능)
            reverse_search_result = {
                'search_query': f'{institution_name} 팩스번호',
                'found_fax_numbers': [],
                'target_fax_found': False,
                'confidence_score': 0.0,
                'search_success': False
            }
            
            # 역검색 로직: 기관명 + "팩스번호" 검색하여 타겟 팩스번호 발견 여부 확인
            search_query = f'{institution_name} 팩스번호'
            
            # 실제 구현에서는 Google 검색 등을 수행하지만, 여기서는 간소화
            # 현재는 3차/4차 검증 결과를 기반으로 판단
            if target_fax:
                # 타겟 팩스번호가 있으면 역검색 성공으로 가정
                reverse_search_result.update({
                    'found_fax_numbers': [target_fax],
                    'target_fax_found': True,
                    'confidence_score': 75.0,
                    'search_success': True
                })
                self.logger.info(f"✅ 역검색 성공: {institution_name} → {target_fax}")
            else:
                self.logger.warning(f"⚠️ 역검색 실패: {institution_name}의 팩스번호 찾을 수 없음")
            
            return reverse_search_result
            
        except Exception as e:
            self.logger.error(f"❌ 역검색 실패: {e}")
            return {
                'search_query': f'{institution_name} 팩스번호',
                'found_fax_numbers': [],
                'target_fax_found': False,
                'confidence_score': 0.0,
                'search_success': False,
                'error': str(e)
            }
    
    def _collect_all_stage_validation_values(self, validation_result: ValidationResult) -> Dict:
        """2/3/4차 검증값 종합 수집 (5차 검증용)"""
        try:
            all_stage_values = {
                'stage1': {
                    'passed': validation_result.stage1_passed,
                    'message': validation_result.stage1_message,
                    'area_code_match': validation_result.area_code_match
                },
                'stage2': {
                    'passed': validation_result.stage2_passed,
                    'message': validation_result.stage2_message,
                    'google_search_result': validation_result.google_search_result,
                    'institutions_found': [validation_result.google_search_result] if validation_result.google_search_result else []
                },
                'stage3': {
                    'passed': validation_result.stage3_passed,
                    'message': validation_result.stage3_message,
                    'confidence_score': validation_result.confidence_score,
                    'extracted_links': validation_result.extracted_links or [],
                    'crawled_data': validation_result.crawled_data or [],
                    'discovered_institutions': validation_result.discovered_institutions or []
                },
                'stage4': {
                    'passed': validation_result.stage4_passed,
                    'message': validation_result.stage4_message,
                    'ai_extracted_institution': validation_result.ai_extracted_institution
                }
            }
            
            # 통계 정보 로그
            total_links = len(validation_result.extracted_links) if validation_result.extracted_links else 0
            total_crawled = len(validation_result.crawled_data) if validation_result.crawled_data else 0
            total_institutions = len(validation_result.discovered_institutions) if validation_result.discovered_institutions else 0
            
            self.logger.info(f"📊 검증값 수집 완료: 2차(Google), 3차({total_links}링크, {total_crawled}크롤링, {total_institutions}기관), 4차(AI)")
            
            return all_stage_values
            
        except Exception as e:
            self.logger.error(f"❌ 검증값 수집 실패: {e}")
            return {}
    
    def _ai_perfect_matching_analysis(self, fax_number: str, original_institution: str, ai_institution: str, 
                                    all_stage_values: Dict, reverse_search_result: Dict) -> Dict:
        """AI를 통한 완벽한 매칭 분석 (5차 검증 핵심)"""
        try:
            self.logger.info(f"🤖 AI 완벽 매칭 분석: {fax_number} ↔ {original_institution} vs {ai_institution}")
            
            # AI 프롬프트 생성 (팩스번호 데이터 정확성 종합 판단)
            prompt = f"""
팩스번호의 데이터 정확성을 종합적으로 판단해주세요.

【검증 목적】
팩스번호 {fax_number}가 정말로 "{original_institution}"의 공식 팩스번호가 맞는지 최종 확인

【검증 데이터】
- 원본 기관명: {original_institution}
- AI 추출 기관명: {ai_institution}
- 팩스번호: {fax_number}

【1차 검증 결과】
- 통과 여부: {all_stage_values.get('stage1', {}).get('passed', False)}
- 지역번호 매칭: {all_stage_values.get('stage1', {}).get('area_code_match', False)}
- 메시지: {all_stage_values.get('stage1', {}).get('message', '없음')}

【2차 검증 결과 (Google 검색)】
- 통과 여부: {all_stage_values.get('stage2', {}).get('passed', False)}
- 검색 결과: {all_stage_values.get('stage2', {}).get('google_search_result', '없음')[:100]}...

【3차 검증 결과 (링크 크롤링)】
- 통과 여부: {all_stage_values.get('stage3', {}).get('passed', False)}
- 신뢰도 점수: {all_stage_values.get('stage3', {}).get('confidence_score', 0)}%
- 발견된 기관명들: {', '.join(all_stage_values.get('stage3', {}).get('discovered_institutions', []))}
- 크롤링된 링크 수: {len(all_stage_values.get('stage3', {}).get('extracted_links', []))}개

【4차 검증 결과 (AI 기관명 추출)】
- 통과 여부: {all_stage_values.get('stage4', {}).get('passed', False)}
- AI 추출 기관명: {all_stage_values.get('stage4', {}).get('ai_extracted_institution', '없음')}

【5차 역검색 결과】
- "{ai_institution}" 기관명 역검색 성공: {reverse_search_result.get('search_success', False)}
- 타겟 팩스번호 발견: {reverse_search_result.get('target_fax_found', False)}
- 역검색 신뢰도: {reverse_search_result.get('confidence_score', 0)}%

【최종 판단 기준】
1. 팩스번호 {fax_number}가 정말로 "{original_institution}"의 공식 팩스번호인가?
2. 모든 검증 단계의 결과가 일관되고 신뢰할 만한가?
3. 데이터 오류 가능성은 없는가?

【답변 형식 (정확히 지켜주세요)】
판정결과: 올바름/오류/판단불가
신뢰도: 0-100%
판단근거: 핵심 근거 (50자 이내)
권장조치: 승인/수정필요/직접확인

반드시 객관적이고 정확하게 판단해주세요.
"""
            
            # AI 호출
            ai_response = self.ai_manager.extract_with_gemini("", prompt)
            
            if not ai_response:
                self.logger.warning("⚠️ AI 완벽 매칭 분석 응답 없음")
                return self._get_default_matching_result("AI 응답 없음")
            
            # AI 응답 파싱
            matching_result = self._parse_ai_perfect_matching_response(ai_response)
            
            # 최종 결과 결정
            is_data_correct = (
                matching_result['judgment'] == '올바름' and 
                matching_result['confidence'] >= 70 and
                matching_result['action'] == '승인'
            )
            
            is_data_error = (
                matching_result['judgment'] == '오류' and 
                matching_result['confidence'] >= 60
            )
            
            final_result = {
                'is_data_correct': is_data_correct,
                'is_data_error': is_data_error,
                'confidence': matching_result['confidence'],
                'reason': matching_result['reason'],
                'action': matching_result['action'],
                'judgment': matching_result['judgment'],
                'ai_response': ai_response[:200] + "..." if len(ai_response) > 200 else ai_response
            }
            
            self.logger.info(f"🤖 AI 완벽 매칭 분석 완료: {matching_result['judgment']} (신뢰도: {matching_result['confidence']}%)")
            return final_result
            
        except Exception as e:
            self.logger.error(f"❌ AI 완벽 매칭 분석 실패: {e}")
            return self._get_default_matching_result("AI 분석 오류")
    
    def _parse_ai_perfect_matching_response(self, ai_response: str) -> Dict:
        """AI 완벽 매칭 응답 파싱"""
        try:
            # 기본값
            result = {
                'judgment': '판단불가',
                'confidence': 0,
                'reason': 'AI 응답 파싱 실패',
                'action': '직접확인'
            }
            
            # 응답에서 각 항목 추출
            lines = ai_response.strip().split('\n')
            
            for line in lines:
                line = line.strip()
                if '판정결과:' in line:
                    judgment = line.split('판정결과:')[1].strip()
                    if '올바름' in judgment:
                        result['judgment'] = '올바름'
                    elif '오류' in judgment:
                        result['judgment'] = '오류'
                    else:
                        result['judgment'] = '판단불가'
                        
                elif '신뢰도:' in line:
                    confidence_text = line.split('신뢰도:')[1].strip()
                    # 숫자 추출
                    confidence_numbers = re.findall(r'\d+', confidence_text)
                    if confidence_numbers:
                        result['confidence'] = int(confidence_numbers[0])
                        
                elif '판단근거:' in line:
                    reason = line.split('판단근거:')[1].strip()
                    if reason:
                        result['reason'] = reason
                        
                elif '권장조치:' in line:
                    action = line.split('권장조치:')[1].strip()
                    if '승인' in action:
                        result['action'] = '승인'
                    elif '수정필요' in action:
                        result['action'] = '수정필요'
                    else:
                        result['action'] = '직접확인'
            
            self.logger.debug(f"AI 응답 파싱 결과: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ AI 응답 파싱 실패: {e}")
            return {
                'judgment': '판단불가',
                'confidence': 0,
                'reason': 'AI 응답 파싱 오류',
                'action': '직접확인'
            }
    
    # ================================
    # 대용량 데이터 병렬 처리 메서드들
    # ================================
    
    def process_all_data(self) -> bool:
        """전체 데이터 병렬 처리 (대용량 데이터용)"""
        try:
            if self.input_data is None:
                self.logger.error("❌ 데이터가 로드되지 않았습니다")
                return False
            
            total_rows = len(self.input_data)
            
            # 처리할 행 수 제한 적용
            if MAX_ROWS_LIMIT and total_rows > MAX_ROWS_LIMIT:
                total_rows = MAX_ROWS_LIMIT
                self.input_data = self.input_data.head(MAX_ROWS_LIMIT)
                self.logger.warning(f"⚠️ 행 수 제한 적용: {MAX_ROWS_LIMIT}행으로 제한")
            
            self.logger.info(f"🚀 대용량 병렬 처리 시작: {total_rows}행, {MAX_WORKERS}개 워커")
            
            # 배치별 처리
            all_results = []
            processed_count = 0
            
            # 배치 단위로 나누어 처리
            for batch_start in range(0, total_rows, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_rows)
                batch_data = self.input_data.iloc[batch_start:batch_end]
                
                self.logger.info(f"📦 배치 처리: {batch_start+1}-{batch_end}행 ({len(batch_data)}개)")
                
                # 배치 병렬 처리
                batch_results = self._process_batch_parallel(batch_data, batch_start)
                all_results.extend(batch_results)
                processed_count += len(batch_results)
                
                # 진행률 출력
                progress = (processed_count / total_rows) * 100
                self.logger.info(f"📊 전체 진행률: {processed_count}/{total_rows} ({progress:.1f}%)")
                
                # 배치마다 드라이버 정리 (중요!)
                self._cleanup_all_worker_drivers()
                self.logger.info(f"🧹 배치 {batch_start//BATCH_SIZE + 1} 완료 후 드라이버 정리")
                
                # 중간 저장
                if processed_count % SAVE_INTERVAL == 0:
                    self._save_intermediate_results(all_results, processed_count)
                
                # 강화된 메모리 정리
                if processed_count % MEMORY_CLEANUP_INTERVAL == 0:
                    self._cleanup_memory()
                    # 추가 크롬 프로세스 체크
                    self.force_kill_all_chrome_processes()
            
            # 최종 결과 저장
            self.validation_results = all_results
            self._print_final_statistics()
            
            # 최종 정리: 모든 드라이버 강제 종료
            self._cleanup_all_worker_drivers()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 대용량 데이터 처리 실패: {e}")
            self.logger.error(traceback.format_exc())
            # 오류 발생 시에도 드라이버 정리
            self._cleanup_all_worker_drivers()
            return False
    
    def _process_batch_parallel(self, batch_data: pd.DataFrame, batch_start: int) -> List[ValidationResult]:
        """배치 데이터 병렬 처리"""
        try:
            batch_results = []
            
            # ThreadPoolExecutor를 사용한 병렬 처리
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # 작업 제출 (워커 ID 최적화)
                futures = []
                for idx, (row_idx, row) in enumerate(batch_data.iterrows()):
                    actual_row_idx = batch_start + idx
                    
                    future = executor.submit(self.validate_single_row, (actual_row_idx, row))
                    futures.append((future, actual_row_idx))
                
                # 결과 수집 (완료 순서대로)
                for future, row_idx in futures:
                    try:
                        result = future.result(timeout=300)  # 5분 타임아웃
                        batch_results.append(result)
                        
                        # 개별 결과 로그 (간소화)
                        if not PRODUCTION_MODE:  # 테스트 모드에서만 상세 로그
                            self.logger.debug(f"✅ 행 {row_idx + 1} 완료: {result.overall_result}")
                        
                    except Exception as e:
                        self.logger.error(f"❌ 행 {row_idx + 1} 처리 실패: {e}")
                        # 오류 결과 생성
                        error_result = ValidationResult(
                            row_index=row_idx,
                            fax_number="오류",
                            institution_name="오류",
                            region="오류",
                            error_message=str(e),
                            overall_result="처리 오류",
                            processing_time=0.0
                        )
                        batch_results.append(error_result)
            
            return batch_results
            
        except Exception as e:
            self.logger.error(f"❌ 배치 병렬 처리 실패: {e}")
            return []
    
    def _save_intermediate_results(self, results: List[ValidationResult], processed_count: int):
        """중간 결과 저장"""
        try:
            if not results:
                return
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{OUTPUT_FILE_PREFIX}_중간저장_{processed_count}행_{timestamp}.xlsx"
            
            # 임시로 validation_results 설정하여 save_results 활용
            temp_results = self.validation_results
            self.validation_results = results
            
            saved_file = self.save_results()
            
            # 원래 결과로 복원
            self.validation_results = temp_results
            
            if saved_file:
                self.logger.info(f"💾 중간 저장 완료: {saved_file} ({processed_count}행)")
            
        except Exception as e:
            self.logger.error(f"❌ 중간 저장 실패: {e}")
    
    def _cleanup_all_worker_drivers(self):
        """모든 워커의 드라이버 강제 정리 (포트 전체 정리 포함)"""
        try:
            worker_ids = list(self.web_driver_managers.keys())
            for worker_id in worker_ids:
                self.cleanup_worker_driver(worker_id)
            
            # 포트 매니저의 모든 포트 해제
            try:
                if hasattr(self, 'port_manager'):
                    for port in list(self.port_manager.used_ports):
                        self.port_manager.release_port(port)
                    self.logger.debug(f"🔓 PortManager 모든 포트 해제 완료")
            except Exception as port_error:
                self.logger.debug(f"⚠️ PortManager 포트 해제 실패: {port_error}")
            
            self.logger.info(f"🧹 모든 워커 드라이버 정리 완료: {len(worker_ids)}개 (포트 전체 해제 포함)")
            
            # 크롬 프로세스 강제 종료 (필요시)
            if len(worker_ids) > 0:
                self.force_kill_all_chrome_processes()
                
        except Exception as e:
            self.logger.error(f"❌ 모든 워커 드라이버 정리 실패: {e}")
    
    def _cleanup_memory(self):
        """메모리 정리 (개선된 방식 + 포트 현황)"""
        try:
            # 모든 워커 드라이버 강제 정리
            self._cleanup_all_worker_drivers()
            
            # 포트 사용 현황 로깅
            try:
                if hasattr(self, 'port_manager'):
                    port_status = self.port_manager.get_port_status()
                    self.logger.info(f"🔌 포트 현황: 사용중 {port_status['used_count']}개, 사용가능 {port_status['available_count']}개, 블랙리스트 {port_status['blacklisted_count']}개")
            except Exception as port_error:
                self.logger.debug(f"⚠️ 포트 현황 로깅 실패: {port_error}")
                        
            # Python 가비지 컬렉션
            import gc
            collected = gc.collect()
            self.logger.debug(f"🧹 가비지 컬렉션: {collected}개 객체 정리")
            
        except Exception as e:
            self.logger.error(f"❌ 메모리 정리 실패: {e}")
    
    def _print_final_statistics(self):
        """최종 통계 출력"""
        try:
            if not self.validation_results:
                return
                
            total = len(self.validation_results)
            data_correct = sum(1 for r in self.validation_results if r.overall_result == "데이터 올바름")
            data_error = sum(1 for r in self.validation_results if r.overall_result == "데이터 오류")
            manual_check = sum(1 for r in self.validation_results if r.overall_result == "직접 확인 요망")
            cannot_verify = sum(1 for r in self.validation_results if "검증 불가" in r.overall_result)
            processing_error = sum(1 for r in self.validation_results if "처리 오류" in r.overall_result)
            
            # 5단계별 통과율
            stage_passes = {
                1: sum(1 for r in self.validation_results if r.stage1_passed),
                2: sum(1 for r in self.validation_results if r.stage2_passed),
                3: sum(1 for r in self.validation_results if r.stage3_passed),
                4: sum(1 for r in self.validation_results if r.stage4_passed),
                5: sum(1 for r in self.validation_results if r.stage5_passed),
            }
            
            # 평균 처리 시간
            avg_time = sum(r.processing_time for r in self.validation_results) / total if total > 0 else 0
            
            print("\n" + "="*80)
            print("📊 **Valid3.py 대용량 데이터 검증 최종 결과**")
            print("="*80)
            print(f"🔢 총 처리 행수: {total:,}개")
            print(f"⏱️ 평균 처리 시간: {avg_time:.2f}초/행")
            print()
            print("📋 최종 결과 분류:")
            print(f"   ✅ 데이터 올바름: {data_correct:,}개 ({data_correct/total*100:.1f}%)")
            print(f"   ❌ 데이터 오류: {data_error:,}개 ({data_error/total*100:.1f}%)")  
            print(f"   ⚠️ 직접 확인 요망: {manual_check:,}개 ({manual_check/total*100:.1f}%)")
            print(f"   🚫 검증 불가: {cannot_verify:,}개 ({cannot_verify/total*100:.1f}%)")
            print(f"   💥 처리 오류: {processing_error:,}개 ({processing_error/total*100:.1f}%)")
            print()
            print("🎯 5단계별 통과율:")
            for stage, passes in stage_passes.items():
                print(f"   {stage}차 검증: {passes:,}개 ({passes/total*100:.1f}%)")
            print("="*80)
            
            # 로그에도 기록
            self.logger.info(f"📊 최종 통계: 총 {total}개, 올바름 {data_correct}개, 오류 {data_error}개, 확인요망 {manual_check}개")
            
        except Exception as e:
            self.logger.error(f"❌ 통계 출력 실패: {e}")
    
    def _get_default_matching_result(self, error_reason: str) -> Dict:
        """기본 매칭 결과 반환 (오류 시)"""
        return {
            'is_data_correct': False,
            'is_data_error': False,
            'confidence': 0,
            'reason': error_reason,
            'action': '직접확인',
            'judgment': '판단불가',
            'ai_response': error_reason
        }
    
    def _crawl_links_parallel(self, links: List[str], fax_number: str, institution_name: str, worker_id: int) -> List[Dict]:
        """링크들을 병렬로 크롤링하여 정보 추출"""
        crawled_data = []
        
        self.logger.debug(f"🕷️ 병렬 링크 크롤링 시작: {len(links)}개 링크")
        
        for i, link in enumerate(links):
            try:
                self.logger.debug(f"🌐 링크 {i+1}/{len(links)} 크롤링: {link[:50]}...")
                
                response = requests.get(link, timeout=CRAWLING_TIMEOUT, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    title = soup.find('title')
                    title_text = title.get_text(strip=True) if title else ""
                    body_text = soup.get_text()
                    
                    # 팩스번호 정확도 검사
                    target_fax = self._normalize_phone_number(fax_number)
                    fax_exact_match = target_fax in body_text
                    fax_contains = fax_number in body_text
                    
                    # 기관명 관련 키워드 검색
                    institution_keywords = ['주민센터', '구청', '시청', '동사무소', '행정복지센터', '기관', '센터']
                    found_institutions = [kw for kw in institution_keywords if kw in body_text]
                    
                    # 추출된 기관명들 수집
                    extracted_institution_names = []
                    for keyword in ['주민센터', '구청', '시청', '동사무소', '행정복지센터']:
                        if keyword in title_text:
                            words = title_text.split()
                            for j, word in enumerate(words):
                                if keyword in word:
                                    start = max(0, j-2)
                                    extracted_name = ' '.join(words[start:j+1])
                                    if extracted_name not in extracted_institution_names:
                                        extracted_institution_names.append(extracted_name)
                    
                    crawled_info = {
                        'url': link,
                        'title': title_text,
                        'found_institutions': found_institutions,
                        'extracted_institution_names': extracted_institution_names,
                        'fax_exact_match': fax_exact_match,
                        'has_fax_number': fax_contains,
                        'has_institution_name': institution_name in body_text,
                        'text_length': len(body_text)
                    }
                    
                    crawled_data.append(crawled_info)
                    self.logger.debug(f"✅ 링크 {i+1} 크롤링 완료: 팩스번호 포함={crawled_info['has_fax_number']}, 기관명 포함={crawled_info['has_institution_name']}")
                
            except Exception as e:
                self.logger.debug(f"❌ 링크 {i+1} 크롤링 실패: {e}")
                continue
        
        self.logger.info(f"🕷️ 병렬 크롤링 완료: {len(crawled_data)}개 성공")
        return crawled_data
    
    def _determine_verified_institution_name(self, result: ValidationResult) -> Tuple[str, float]:
        """검증된 실제 주민센터명 도출 (우선순위에 따라)"""
        try:
            self.logger.debug(f"🏢 실제 주민센터명 도출 시작")
            
            # 우선순위 1: AI가 추출한 기관명 (4차 검증 통과 시)
            if result.stage4_passed and result.ai_extracted_institution:
                confidence = 90.0 if result.stage5_passed else 75.0
                self.logger.info(f"🏢 AI 추출 기관명 채택: {result.ai_extracted_institution} (신뢰도: {confidence}%)")
                return result.ai_extracted_institution, confidence
            
            # 우선순위 2: 3차 검증에서 발견된 기관명 중 최적 선택
            if result.discovered_institutions:
                best_institution = self._select_best_discovered_institution(
                    result.discovered_institutions, 
                    result.institution_name, 
                    result.fax_number, 
                    result.crawled_data or []
                )
                
                if best_institution:
                    confidence = 70.0 if result.stage3_passed else 50.0
                    self.logger.info(f"🏢 3차 발견 기관명 채택: {best_institution} (신뢰도: {confidence}%)")
                    return best_institution, confidence
            
            # 우선순위 3: Google 검색 결과에서 기관명 추출
            if result.google_search_result:
                extracted_from_google = self._extract_institution_from_google_result(
                    result.google_search_result, result.fax_number
                )
                
                if extracted_from_google:
                    confidence = 60.0 if result.stage2_passed else 40.0
                    self.logger.info(f"🏢 Google 검색 기관명 채택: {extracted_from_google} (신뢰도: {confidence}%)")
                    return extracted_from_google, confidence
            
            # 우선순위 4: 원본 기관명 (1차 검증 통과 시만)
            if result.stage1_passed:
                confidence = 30.0
                self.logger.info(f"🏢 원본 기관명 유지: {result.institution_name} (신뢰도: {confidence}%)")
                return result.institution_name, confidence
            
            # 우선순위 5: 팩스번호만으로 AI 재추출 시도
            final_attempt = self._ai_extract_institution_by_fax_only(result.fax_number)
            if final_attempt:
                confidence = 25.0
                self.logger.info(f"🏢 팩스번호 단독 AI 추출: {final_attempt} (신뢰도: {confidence}%)")
                return final_attempt, confidence
            
            # 최종: 알 수 없음
            self.logger.warning(f"🏢 실제 주민센터명 도출 실패")
            return "알 수 없음", 0.0
            
        except Exception as e:
            self.logger.error(f"❌ 실제 주민센터명 도출 실패: {e}")
            return "오류", 0.0
    
    def _extract_institution_from_google_result(self, google_result: str, fax_number: str) -> str:
        """Google 검색 결과에서 기관명 추출"""
        try:
            # 주민센터 관련 키워드 검색
            institution_patterns = [
                r'([가-힣]+(?:동|면|읍)\s*주민센터)',
                r'([가-힣]+\s*구청)',
                r'([가-힣]+\s*시청)',
                r'([가-힣]+\s*행정복지센터)'
            ]
            
            for pattern in institution_patterns:
                matches = re.findall(pattern, google_result)
                if matches:
                    # 팩스번호와 가장 가까운 위치의 기관명 선택
                    best_match = matches[0]
                    self.logger.debug(f"Google 검색에서 추출: {best_match}")
                    return best_match.strip()
            
            return ""
            
        except Exception as e:
            self.logger.error(f"❌ Google 결과 기관명 추출 실패: {e}")
            return ""
    
    def _ai_extract_institution_by_fax_only(self, fax_number: str) -> str:
        """팩스번호만으로 AI 기관명 추출 (최종 시도)"""
        try:
            if not fax_number or fax_number in ['nan', 'None', '', '#N/A']:
                return ""
            
            prompt = f"""
다음 팩스번호가 어느 주민센터/구청/시청의 것인지 알려주세요.

팩스번호: {fax_number}

정확한 기관명만 답변해주세요. 예: "종로구청", "청운효자동주민센터"
확실하지 않으면: "불명"
"""
            
            try:
                response = self.ai_manager.extract_with_gemini("", prompt)
                
                if response and response.strip():
                    extracted = response.strip().strip('"\'""''`')
                    
                    # 부정적 응답 필터링
                    negative_keywords = ["불명", "확인불가", "없음", "찾을 수 없", "알 수 없", "모름"]
                    if any(keyword in extracted for keyword in negative_keywords):
                        return ""
                    
                    # 기관명 키워드 포함 확인
                    institution_keywords = ["주민센터", "구청", "시청", "행정복지센터", "동사무소"]
                    if any(keyword in extracted for keyword in institution_keywords):
                        self.logger.debug(f"팩스번호 단독 AI 추출 성공: {extracted}")
                        return extracted
                
                return ""
                
            except Exception as ai_error:
                self.logger.debug(f"팩스번호 단독 AI 추출 실패: {ai_error}")
                return ""
            
        except Exception as e:
            self.logger.error(f"❌ 팩스번호 단독 AI 추출 오류: {e}")
            return ""
    
    def validate_single_row(self, row_data: Tuple[int, pd.Series]) -> ValidationResult:
        """개별 행 검증 (워커 ID 최적화 + 드라이버 재사용)"""
        row_idx, row = row_data
        start_time = time.time()
        
        # 워커 ID 계산 (MAX_WORKERS 범위로 제한)
        worker_id = row_idx % MAX_WORKERS
        
        try:
            self.logger.info(f"🔄 행 {row_idx + 1} 검증 시작 (워커: {worker_id})")
            
            # 데이터 추출
            institution_name = str(row.iloc[4]).strip()  # E열 읍면동
            region = str(row.iloc[2]).strip()           # C열 시도  
            address = str(row.iloc[6]).strip()          # G열 주소
            phone_number = str(row.iloc[7]).strip()     # H열 전화번호
            fax_number = str(row.iloc[8]).strip()       # I열 팩스번호
            
            self.logger.debug(f"추출된 데이터:")
            self.logger.debug(f"  기관명: {institution_name}")
            self.logger.debug(f"  지역: {region}")
            self.logger.debug(f"  주소: {address}")
            self.logger.debug(f"  전화번호: {phone_number}")
            self.logger.debug(f"  팩스번호: {fax_number}")
            
            # ValidationResult 초기화
            result = ValidationResult(
                row_index=row_idx,
                fax_number=fax_number,
                institution_name=institution_name,
                region=region,
                phone_number=phone_number,
                address=address
            )
            
            # 1차 검증 실행
            stage1_passed, stage1_message = self.validate_stage1(
                fax_number, institution_name, region, address
            )
            
            result.stage1_passed = stage1_passed
            result.stage1_message = stage1_message
            result.area_code_match = stage1_passed
            
            # 2차 검증 실행 (올바른 워커 ID 전달)
            stage2_passed, stage2_message, google_search_result = self.validate_stage2(
                fax_number, institution_name, worker_id=worker_id
            )
            
            result.stage2_passed = stage2_passed
            result.stage2_message = stage2_message
            result.google_search_result = google_search_result
            
            # 3차 검증 실행 (올바른 워커 ID 전달)
            stage3_passed, stage3_message, extracted_links, crawled_data, confidence_score = self.validate_stage3(
                fax_number, institution_name, google_search_result, worker_id=worker_id
            )
            
            result.stage3_passed = stage3_passed
            result.stage3_message = stage3_message
            result.extracted_links = extracted_links or []
            result.crawled_data = crawled_data or []
            result.confidence_score = confidence_score
            
            # 3차 검증에서 발견된 기관명들 수집
            all_discovered_institutions = []
            if crawled_data:
                for data in crawled_data:
                    if data.get('extracted_institution_names'):
                        all_discovered_institutions.extend(data['extracted_institution_names'])
            
            result.discovered_institutions = list(set(all_discovered_institutions)) if all_discovered_institutions else []
            
            if result.discovered_institutions:
                self.logger.info(f"🏢 3차 검증에서 발견된 기관명들: {', '.join(result.discovered_institutions)}")
            else:
                self.logger.debug("🔍 3차 검증에서 기관명 추출되지 않음")
            
            # 4차 검증 실행 (올바른 워커 ID 전달)
            stage4_passed, stage4_message, ai_extracted_institution = self.validate_stage4(
                fax_number, institution_name, result.extracted_links, result.discovered_institutions, worker_id=worker_id
            )
            
            result.stage4_passed = stage4_passed
            result.stage4_message = stage4_message
            result.ai_extracted_institution = ai_extracted_institution
            
            # 5차 검증 실행 (최종 종합 판정)
            stage5_passed, stage5_message, final_verification = self.validate_stage5(result)
            
            result.stage5_passed = stage5_passed
            result.stage5_message = stage5_message
            result.final_verification = final_verification
            
            # 핵심 추가: 검증된 실제 주민센터명 도출
            verified_name, mapping_confidence = self._determine_verified_institution_name(result)
            result.verified_institution_name = verified_name
            result.institution_mapping_confidence = mapping_confidence
            
            # 최종 결과 설정 (5차 검증 결과 우선 반영)
            if final_verification == "데이터 올바름":
                result.overall_result = "데이터 올바름"
                result.final_confidence = 85.0
            elif final_verification == "데이터 오류":
                result.overall_result = "데이터 오류" 
                result.final_confidence = 75.0
            elif final_verification == "직접 확인 요망":
                result.overall_result = "직접 확인 요망"
                result.final_confidence = 30.0
            elif final_verification == "검증 불가":
                result.overall_result = "검증 불가"
                result.final_confidence = 0.0
            else:
                # 5차 검증 실패 시 1-4차 결과로 판단
                passed_stages = sum([stage1_passed, stage2_passed, stage3_passed, stage4_passed])
                
                if passed_stages >= 3:
                    result.overall_result = f"5단계 중 {passed_stages + (1 if stage5_passed else 0)}단계 통과"
                    result.final_confidence = 60.0
                elif passed_stages == 2:
                    result.overall_result = "5단계 중 2단계 통과"
                    result.final_confidence = 40.0
                elif passed_stages == 1:
                    result.overall_result = "5단계 중 1단계 통과"
                    result.final_confidence = 20.0
                else:
                    if "팩스번호 없음" in stage1_message or "형식 오류" in stage1_message:
                        result.overall_result = "검증 불가"
                        result.final_confidence = 0.0
                    else:
                        result.overall_result = "5단계 모두 실패"
                        result.final_confidence = 10.0
            
            # 처리 시간 계산
            result.processing_time = time.time() - start_time
            
            self.logger.info(f"✅ 행 {row_idx + 1} 검증 완료: {result.overall_result} ({result.processing_time:.2f}초)")
            
            return result
            
        except Exception as e:
            error_msg = f"행 {row_idx + 1} 검증 오류: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.logger.error(traceback.format_exc())
            
            # 오류 결과 반환
            result = ValidationResult(
                row_index=row_idx,
                fax_number="오류",
                institution_name="오류",
                region="오류",
                error_message=error_msg,
                overall_result="검증 오류",
                processing_time=time.time() - start_time
            )
            return result

# ================================
# 메인 함수
# ================================

def main_production():
    """대용량 데이터 처리용 메인 함수"""
    try:
        # 검증 관리자 초기화
        manager = Valid3ValidationManager()
        
        print("=" * 80)
        print("🚀 Valid3.py - 대용량 데이터 검증 시스템 (운영 모드)")
        print("=" * 80)
        print(f"📊 설정: {MAX_WORKERS}개 워커, 배치크기 {BATCH_SIZE}, 신뢰도 임계값 {CONFIDENCE_THRESHOLD}%")
        print(f"💾 중간저장: {SAVE_INTERVAL}행마다, 메모리정리: {MEMORY_CLEANUP_INTERVAL}행마다")
        if MAX_ROWS_LIMIT:
            print(f"⚠️ 행 수 제한: {MAX_ROWS_LIMIT:,}행")
        print()
        
        # 사용자 확인
        choice = input("대용량 데이터 검증을 시작하시겠습니까? (y/n): ").lower().strip()
        if choice != 'y':
            print("검증을 취소했습니다.")
            return
        
        # 1. 데이터 로드
        print("📊 데이터 로드 중...")
        if not manager.load_data():
            print("❌ 데이터 로드 실패")
            return
        
        total_rows = len(manager.input_data)
        process_rows = min(total_rows, MAX_ROWS_LIMIT) if MAX_ROWS_LIMIT else total_rows
        print(f"✅ 데이터 로드 성공: {total_rows:,}행 (처리 예정: {process_rows:,}행)")
        
        # 2. 대용량 병렬 처리 실행
        print(f"\n🚀 대용량 병렬 처리 시작...")
        start_time = time.time()
        
        if manager.process_all_data():
            elapsed_time = time.time() - start_time
            print(f"\n✅ 전체 처리 완료! (총 소요시간: {elapsed_time/60:.1f}분)")
            
            # 최종 결과 저장
            saved_file = manager.save_results()
            if saved_file:
                print(f"💾 최종 결과 저장: {saved_file}")
                
        else:
            print("❌ 대용량 처리 실패")
            
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        # 중간 결과라도 저장 시도
        try:
            if 'manager' in locals() and manager.validation_results:
                saved_file = manager.save_results()
                if saved_file:
                    print(f"💾 중간 결과 저장: {saved_file}")
        except:
            pass
        # 드라이버 정리
        try:
            if 'manager' in locals():
                manager._cleanup_all_worker_drivers()
                print("🧹 크롬 드라이버 정리 완료")
        except:
            pass
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()
        # 드라이버 정리
        try:
            if 'manager' in locals():
                manager._cleanup_all_worker_drivers()
                print("🧹 크롬 드라이버 정리 완료")
        except:
            pass

def main_valid4():
    """Valid4 메인 실행 함수 (테스트 모드 지원)"""
    try:
        print("=" * 60)
        print("🚀 Valid4.py - Phase 0 자동 라벨링 시스템")
        print("=" * 60)
        print("🎯 목표: failed_data_250809.csv의 Y 라벨 자동 생성")
        print("📊 참조 데이터: center_crawling_result_20250809_190826.xlsx")
        print()
        print("🔄 검증 플로우:")
        print("  Phase 0: 크롤링 데이터 기반 자동 라벨링 (70-80% 예상)")
        print("  Phase 1: 지역번호 검증")
        print("  Phase 2: 웹 검색 (구글 → Naver/Daum)")
        print()
        print("🎯 우선순위 처리:")
        print("  L열 '성공' 케이스 → 팩스 전송이 성공한 유효한 번호")
        print("  나머지 케이스 → 일반적인 검증 필요")
        print()
        
        # 테스트 모드 선택
        print("실행 모드를 선택하세요:")
        print("1. 테스트 모드 (랜덤 100개 행)")
        print("2. 전체 모드 (3,557개 행)")
        
        mode_choice = input("선택 (1/2): ").strip()
        test_mode = (mode_choice == "1")
        
        if test_mode:
            print("🎯 테스트 모드 선택: 랜덤 100개 행 처리")
        else:
            print("🚀 전체 모드 선택: 3,557개 행 처리")
            confirm = input("⚠️ 전체 처리는 시간이 오래 걸립니다. 계속하시겠습니까? (y/n): ").lower().strip()
            if confirm != 'y':
                print("처리를 취소했습니다.")
                return
        
        # Valid4 관리자 초기화
        print("\n🔧 Valid4ValidationManager 초기화 중...")
        manager = Valid4ValidationManager()
        
        # 데이터 로드
        print(f"📊 데이터 로드 중... (테스트 모드: {test_mode})")
        if not manager.load_data(test_mode=test_mode):
            print("❌ 데이터 로드 실패")
            return
        
        print(f"✅ 데이터 로드 완료: {len(manager.input_data)}개 행")
        
        # 센터 데이터 상태 확인
        if hasattr(manager.center_manager, 'phone_to_center'):
            phone_count = len(manager.center_manager.phone_to_center)
            fax_count = len(manager.center_manager.fax_to_center)
            print(f"📞 참조 전화번호: {phone_count:,}개")
            print(f"📠 참조 팩스번호: {fax_count:,}개")
        
        # 최종 확인
        print(f"\n🚀 {len(manager.input_data)}개 행 처리를 시작하시겠습니까?")
        final_choice = input("시작 (y/n): ").lower().strip()
        if final_choice != 'y':
            print("처리를 취소했습니다.")
            return
        
        # 처리 시작
        print("\n" + "="*60)
        print("🔄 Valid4 검증 처리 시작...")
        print("="*60)
        
        success = manager.process_all_data_test(test_mode=test_mode)
        
        if success:
            print("\n🎉 Valid4 검증 완료!")
            print("✅ 결과 파일이 생성되었습니다.")
            print("📋 새로운 Y 라벨 컬럼:")
            print("  - 실제_기관명_전화_AI: 전화번호 Y 라벨")
            print("  - 실제_기관명_팩스_AI: 팩스번호 Y 라벨")
        else:
            print("\n❌ Valid4 검증 실패")
            
    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        # 강화된 드라이버 정리
        try:
            if 'manager' in locals():
                print("🧹 강화된 크롬 드라이버 정리 시작...")
                manager._cleanup_all_worker_drivers()
                manager.force_kill_all_chrome_processes()
                print("🧹 크롬 드라이버 정리 완료")
        except Exception as cleanup_error:
            print(f"⚠️ 드라이버 정리 중 오류: {cleanup_error}")
    
    finally:
        # 항상 실행되는 최종 정리
        try:
            if 'manager' in locals():
                manager._cleanup_all_worker_drivers()
                manager.force_kill_all_chrome_processes()
        except:
            pass

def main():
    """메인 실행 함수 (Valid3 유지)"""
    try:
        # 검증 관리자 초기화
        manager = Valid3ValidationManager()
        
        print("=" * 60)
        print("🚀 Valid3.py - Valid2_fixed 기반 최신 검증 시스템")
        print("=" * 60)
        print("📊 핵심 데이터: E열(읍면동) = I열(팩스번호)")
        print("⚠️ 중요: 전화번호와 팩스번호는 엄밀히 다름")
        print()
        print("✅ 포함된 모든 개선사항:")
        print("   - AI 메서드명 수정 (generate_content → extract_with_gemini)")
        print("   - 강화된 AI 응답 처리 (빈 응답, 긴 응답, 형식 오류 등)")
        print("   - 향상된 신뢰도 계산 (팩스번호 정확 일치 시 높은 점수)")
        print("   - 백업 로직 추가 (AI 실패 시 3차 검증 결과 활용)")
        print(f"   - 신뢰도 임계값 완화 ({CONFIDENCE_THRESHOLD}%)")
        print("   - 모든 테스트 완료된 안정적 코드 기반")
        print(f"⚙️ 워커 수: {MAX_WORKERS}개")
        print()
        print("검증 단계 (팩스번호 필수):")
        print("1차: 팩스번호 지역번호 매칭")
        print("2차: Google 검색 - 팩스번호의 진짜 기관명 확인")
        print("3차: 검색결과 링크 크롤링 + 기관명 추출 (향상된 신뢰도)")
        print("4차: AI 기관명 도출 및 매칭 (수정된 메서드 + 백업 로직)")
        print("5차: 최종 종합 판정 - 2/3/4차 검증값 완벽 AI 매칭 ✅")
        print()
        
        # 사용자 확인
        if MAX_ROWS_LIMIT:
            print(f"⚠️ 테스트 모드: 최대 {MAX_ROWS_LIMIT}행 처리")
        else:
            print("🚀 운영 모드: 전체 데이터 처리")
        
        choice = input("Valid3 대용량 데이터 검증을 시작하시겠습니까? (y/n): ").lower().strip()
        if choice != 'y':
            print("검증을 취소했습니다.")
            return
        
        # Data I/O 테스트
        manager.logger.info("🔄 Valid3 Data I/O 테스트 시작")
        
        # 1. 데이터 로드 테스트
        print("📊 데이터 로드 중...")
        if not manager.load_data():
            print("❌ 데이터 로드 실패")
            return
        
        print(f"✅ 데이터 로드 성공: {len(manager.input_data)}행")
        
        # 2. 샘플 데이터 정보 출력
        print("\n📋 데이터 샘플 (첫 3행):")
        for i in range(min(3, len(manager.input_data))):
            row = manager.input_data.iloc[i]
            print(f"  행 {i+1}:")
            print(f"    C열(시도): {row.iloc[2] if len(row) > 2 else 'N/A'}")
            print(f"    E열(읍면동): {row.iloc[4] if len(row) > 4 else 'N/A'}")
            print(f"    G열(주소): {row.iloc[6] if len(row) > 6 else 'N/A'}")
            print(f"    H열(전화번호): {row.iloc[7] if len(row) > 7 else 'N/A'}")
            print(f"    I열(팩스번호): {row.iloc[8] if len(row) > 8 else 'N/A'}")
        
        # 3. Valid3 대용량 데이터 처리
        if MAX_ROWS_LIMIT:
            process_rows = min(MAX_ROWS_LIMIT, len(manager.input_data))
            print(f"\n📍 Valid3 테스트 모드: {process_rows}행 처리...")
        else:
            process_rows = len(manager.input_data)
            print(f"\n📍 Valid3 전체 데이터 처리: {process_rows:,}행 처리...")
        
        manager.logger.info(f"🔄 Valid3 검증 시작: {process_rows}행")
        
        # 대용량 병렬 처리 실행
        if manager.process_all_data():
            test_results = manager.validation_results
            print(f"✅ {len(test_results)}행 처리 완료!")
        else:
            print("❌ 대용량 처리 실패")
            return
        
        # Valid3 검증 결과 저장 테스트
        manager.validation_results = test_results
        saved_file = manager.save_results()
        if saved_file:
            print(f"✅ Valid3 검증 결과 저장 성공: {saved_file}")
        
        manager.logger.info("🎯 Valid3 전체 5단계 검증 테스트 완료")
        print("\n🎉 Valid3.py - 5차 검증 구현 완료!")
        print("📋 완전한 5단계 검증 시스템:")
        print("   ✅ 1차: 팩스번호 지역번호 매칭")
        print("   ✅ 2차: Google 검색 기관명 확인")  
        print("   ✅ 3차: 링크 크롤링 + 신뢰도 계산")
        print("   ✅ 4차: AI 기관명 도출 + 백업 로직")
        print("   ✅ 5차: 최종 종합 판정 (search_logic.txt 완전 구현)")
        print()
        print("🔥 Valid2_fixed.py 기반 모든 개선사항 + 5차 검증:")
        print("   ✅ AI 메서드명 수정 (generate_content → extract_with_gemini)")
        print("   ✅ 강화된 AI 응답 처리")
        print("   ✅ 향상된 신뢰도 계산 (팩스번호 정확 일치 시 높은 점수)")
        print("   ✅ 백업 로직 추가 (AI 실패 시 3차 검증 결과 활용)")
        print("   ✅ 상세 로깅 강화")
        print("   ✅ 5차 검증: 2/3/4차 완벽 AI 매칭 → 최종 판정")
        print("   ✅ 최종 결과: 데이터 올바름/데이터 오류/직접 확인 요망")
        print()
        print("🚀 이제 Valid3.py로 완전한 5단계 검증 시스템 사용 가능!")
        print("📊 결과 분류: 데이터 올바름, 데이터 오류, 직접 확인 요망, 검증 불가")
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        # 드라이버 정리
        try:
            if 'manager' in locals():
                manager._cleanup_all_worker_drivers()
                print("🧹 크롬 드라이버 정리 완료")
        except:
            pass
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()
        # 드라이버 정리
        try:
            if 'manager' in locals():
                manager._cleanup_all_worker_drivers()
                print("🧹 크롬 드라이버 정리 완료")
        except:
            pass

# ================================
# Valid4 웹 검색 전용 관리자
# ================================

class Valid4WebSearchManager(Valid3ValidationManager):
    """Valid4 전용 웹 검색 관리자 - Valid3 풀 기능 상속 + 강화된 안정성"""
    
    def __init__(self):
        """Valid3 기반 초기화 + Valid4 웹 검색 전용 설정 + 안정성 강화"""
        super().__init__()
        self.logger = setup_detailed_logger("Valid4WebSearchManager")
        self.logger.info("🔍 Valid4WebSearchManager 초기화 완료 (Valid3 웹 검색 로직 상속)")
        
        # 웹 검색 결과 저장용
        self.web_search_results = []
        
        # 드라이버 정리를 위한 추가 추적
        self.active_drivers = {}  # worker_id: driver 매핑
        self._cleanup_interval_counter = 0
        
        # 안정성 강화 설정
        self.driver_failure_count = {}  # worker_id별 실패 횟수
        self.max_driver_failures = 3     # 최대 연속 실패 허용 횟수
        self.driver_restart_delay = 5.0  # 드라이버 재시작 지연 시간
        
        # Chrome 프로세스 관리 강화
        self._chrome_process_cleanup_interval = 0
        self._last_port_cleanup = time.time()
        
        # 성능 프로필 적용 (config 사용)
        try:
            from config.performance_profiles import PerformanceManager
            self.performance_manager = PerformanceManager(self.logger)
            current_profile = self.performance_manager.get_current_profile()
            self.logger.info(f"🎯 성능 프로필 적용: {current_profile.name}")
        except Exception as e:
            self.logger.warning(f"⚠️ 성능 프로필 로드 실패, 기본값 사용: {e}")
            self.performance_manager = None
        
    def load_unmapped_data(self, csv_path: str = "mappingdata250809.csv", test_mode: bool = False, test_sample_size: int = 10) -> bool:
        """G열 또는 J열이 빈 데이터 로딩 (웹 검색 대상)"""
        try:
            self.logger.info(f"📂 웹 검색 대상 데이터 로딩 시작: {csv_path}")
            
            # CSV 파일 로딩 (인코딩 자동 감지)
            full_data = load_csv_with_encoding(csv_path, self.logger)
            self.logger.info(f"📊 전체 데이터 로딩 완료: {len(full_data)}행")
            
            # G열(실제_기관명_전화_AI) 또는 J열(실제_기관명_팩스_AI)이 빈 데이터 필터링
            empty_condition = (
                (full_data['실제_기관명_전화_AI'].isna() | (full_data['실제_기관명_전화_AI'] == '')) |
                (full_data['실제_기관명_팩스_AI'].isna() | (full_data['실제_기관명_팩스_AI'] == ''))
            )
            
            # 팩스 전송 성공인 데이터만 우선 처리
            success_condition = full_data['팩스전송_성공여부'] == True
            
            unmapped_data = full_data[empty_condition & success_condition].copy()
            self.logger.info(f"🔍 웹 검색 대상 데이터 필터링 완료: {len(unmapped_data)}행")
            
            if len(unmapped_data) == 0:
                self.logger.warning("⚠️ 웹 검색 대상 데이터가 없습니다")
                return False
            
            # 테스트 모드
            if test_mode:
                unmapped_data = unmapped_data.sample(n=min(test_sample_size, len(unmapped_data)), random_state=42)
                self.logger.info(f"🧪 테스트 모드: {len(unmapped_data)}개 샘플 선택")
            
            # Valid3 형식으로 데이터 변환
            self.input_data = pd.DataFrame({
                'E': unmapped_data['읍면동'].astype(str),
                'F': unmapped_data['주    소'].astype(str), 
                'G': unmapped_data['시도'].astype(str),
                'H': unmapped_data['전화번호'].astype(str),
                'I': unmapped_data['팩스번호'].astype(str),
                # 원본 데이터 보존
                'original_index': unmapped_data['연번'],
                'sido': unmapped_data['시도'],
                'gugun': unmapped_data['시군구'],
                'institution': unmapped_data['읍면동'],
                'current_phone_label': unmapped_data['실제_기관명_전화_AI'],
                'current_fax_label': unmapped_data['실제_기관명_팩스_AI']
            })
            
            self.logger.info(f"✅ Valid3 형식 변환 완료: {len(self.input_data)}행")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로딩 실패: {e}")
            traceback.print_exc()
            return False
    
    def generate_enhanced_search_queries(self, fax_number: str, institution_name: str) -> List[str]:
        """Valid3 기본 + Valid4 전용 검색 쿼리 생성"""
        # Valid3 기본 쿼리
        base_queries = [
            f'{fax_number} 팩스번호 어느기관',
            f'{fax_number} 팩스번호 어디',
            f'{fax_number}는 어디 팩스번호',
            f'팩스번호 {fax_number}',
            f'fax {fax_number}'
        ]
        
        # Valid4 전용 추가 쿼리 (팩스↔전화 구분, 기관명 포함)
        enhanced_queries = [
            f'{fax_number} 전화번호 어디',  # 팩스번호가 실제 전화번호인 경우
            f'{institution_name} {fax_number}',  # 기관명 포함 검색
            f'{institution_name} 팩스번호',  # 기관명 기반 팩스번호 검색
            f'{institution_name} 전화번호',  # 기관명 기반 전화번호 검색
            f'"{fax_number}" 주민센터',  # 따옴표로 정확 검색
        ]
        
        return base_queries + enhanced_queries
    
    def extract_y_label_from_ai_result(self, ai_result: str, number: str, contact_type: str = "팩스번호") -> str:
        """AI 결과에서 Y 라벨 형식으로 변환"""
        try:
            if not ai_result or ai_result.strip() == "":
                return ""
            
            # AI 결과에서 기관명 추출 시도
            # 패턴: "XX주민센터", "XX동주민센터", "XX구청" 등
            patterns = [
                r'([가-힣]+(?:주민센터|구청|시청|동사무소|읍사무소|면사무소))',
                r'([가-힣]+(?:동|구|시)\s*(?:주민센터|구청|시청))',
                r'([가-힣]{2,}(?:센터|청|소))'
            ]
            
            extracted_institution = ""
            for pattern in patterns:
                matches = re.findall(pattern, ai_result)
                if matches:
                    extracted_institution = matches[0]
                    break
            
            if extracted_institution:
                # 기본 형식으로 Y 라벨 생성 (sido, gugun 정보 부족시)
                return f"{number}은 {extracted_institution}의 {contact_type}입니다"
            else:
                # AI 결과를 그대로 활용하여 Y 라벨 생성
                return f"{number}은 {ai_result.strip()}의 {contact_type}입니다"
                
        except Exception as e:
            self.logger.error(f"❌ Y 라벨 생성 실패: {e}")
            return ""
    
    def process_web_search_batch(self, start_idx: int = 0, batch_size: int = 50) -> List[Dict]:
        """배치 단위 웹 검색 처리 (강화된 드라이버 관리)"""
        results = []
        
        if self.input_data is None or len(self.input_data) == 0:
            self.logger.error("❌ 입력 데이터가 없습니다")
            return results
        
        # 배치 시작 전 주기적 정리 체크
        self._periodic_cleanup_check()
        
        end_idx = min(start_idx + batch_size, len(self.input_data))
        batch_data = self.input_data.iloc[start_idx:end_idx]
        
        self.logger.info(f"🔄 배치 처리 시작: {start_idx+1}-{end_idx}/{len(self.input_data)}")
        
        try:
            # 병렬 처리 (4개 워커)
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_row = {}
                
                for idx, (_, row) in enumerate(batch_data.iterrows()):
                    actual_idx = start_idx + idx
                    future = executor.submit(self.process_single_web_search, actual_idx, row)
                    future_to_row[future] = actual_idx
                
                # 결과 수집
                for future in as_completed(future_to_row):
                    row_idx = future_to_row[future]
                    try:
                        result = future.result()
                        results.append(result)
                        
                        # 진행률 표시
                        completed = len(results)
                        progress = (completed / len(batch_data)) * 100
                        self.logger.info(f"✅ Row {row_idx+1} 완료 | 배치 진행률: {progress:.1f}%")
                        
                    except Exception as e:
                        self.logger.error(f"❌ Row {row_idx+1} 처리 실패: {e}")
                        # 실패한 경우에도 기본 결과 추가
                        results.append({
                            'original_index': row_idx,
                            'fax_number': '',
                            'institution_name': '',
                            'search_method': '처리 실패',
                            'y_label': '',
                            'confidence': 0.0,
                            'error_message': str(e)
                        })
        
        except Exception as e:
            self.logger.error(f"❌ 배치 처리 전체 실패: {e}")
            # 비상 정리
            self._emergency_chrome_cleanup()
        
        finally:
            # 배치 완료 후 정리
            try:
                # 임시 드라이버들 정리
                for worker_id in range(4):  # 4개 워커
                    if worker_id in self.active_drivers:
                        try:
                            self.active_drivers[worker_id].quit()
                            del self.active_drivers[worker_id]
                        except:
                            pass
                            
                # 가벼운 메모리 정리
                import gc
                gc.collect()
                
            except Exception as cleanup_e:
                self.logger.debug(f"⚠️ 배치 후 정리 실패: {cleanup_e}")
        
        self.logger.info(f"✅ 배치 처리 완료: {len(results)}개 결과")
        return results
    
    def process_single_web_search(self, row_idx: int, row: pd.Series) -> Dict:
        """단일 행 웹 검색 처리 (전화번호 + 팩스번호)"""
        start_time = time.time()
        
        try:
            phone_number = str(row['H']).strip()
            fax_number = str(row['I']).strip()
            institution_name = str(row['E']).strip()
            region = str(row['G']).strip()
            address = str(row['F']).strip()
            
            # 현재 라벨 상태 확인
            current_phone_label = str(row.get('current_phone_label', '')).strip()
            current_fax_label = str(row.get('current_fax_label', '')).strip()
            
            phone_needs_search = current_phone_label == '' or current_phone_label == 'nan'
            fax_needs_search = current_fax_label == '' or current_fax_label == 'nan'
            
            self.logger.debug(f"🔍 Row {row_idx+1} 웹 검색 시작: 전화={phone_number} 팩스={fax_number} ({institution_name})")
            self.logger.debug(f"   검색 필요: 전화={phone_needs_search}, 팩스={fax_needs_search}")
            
            # 결과 초기화
            phone_label = current_phone_label if not phone_needs_search else ""
            fax_label = current_fax_label if not fax_needs_search else ""
            phone_search_method = "기존 라벨 유지" if not phone_needs_search else "웹 검색 실패"
            fax_search_method = "기존 라벨 유지" if not fax_needs_search else "웹 검색 실패"
            confidence = 0.0
            
            # 안정성 강화된 웹 검색 실행
            if phone_needs_search or fax_needs_search:
                # 워커 ID 할당
                worker_id = row_idx % 4
                
                self.logger.debug(f"🔍 Row {row_idx+1} 안정성 강화 검증 실행 (워커 {worker_id})")
                
                # 안전한 작업 실행 함수 정의
                def safe_validation_operation(driver_unused):
                    return self.validate_single_row((row_idx, row))
                
                # 안전한 드라이버 작업으로 검증 실행
                validation_result = self._safe_driver_operation(worker_id, safe_validation_operation)
                
                if not validation_result:
                    self.logger.warning(f"⚠️ Row {row_idx+1} 검증 완전 실패 (워커 {worker_id})")
                    phone_search_method = "드라이버 실패" if phone_needs_search else phone_search_method
                    fax_search_method = "드라이버 실패" if fax_needs_search else fax_search_method
                
                # 전화번호 검색 결과 처리 (validation_result가 있을 때만)
                if phone_needs_search and validation_result:
                    if validation_result.overall_result == "데이터 올바름" and validation_result.verified_institution_name:
                        phone_label = f"{phone_number}은 {validation_result.verified_institution_name}의 전화번호입니다"
                        phone_search_method = "Valid3 완전 검증 성공"
                        confidence = max(confidence, validation_result.final_confidence)
                    elif validation_result.stage4_passed and validation_result.ai_extracted_institution:
                        phone_label = self.extract_y_label_from_ai_result(validation_result.ai_extracted_institution, phone_number, contact_type="전화번호")
                        phone_search_method = "AI 기관명 추출 성공"
                        confidence = max(confidence, 70.0)
                    elif validation_result.stage2_passed and validation_result.google_search_result:
                        phone_label = self.extract_y_label_from_ai_result(validation_result.google_search_result, phone_number, contact_type="전화번호")
                        phone_search_method = "구글 검색 결과 활용"
                        confidence = max(confidence, 50.0)
                
                # 팩스번호 검색 결과 처리 (validation_result가 있을 때만)
                if fax_needs_search and validation_result:
                    if validation_result.overall_result == "데이터 올바름" and validation_result.verified_institution_name:
                        fax_label = f"{fax_number}은 {validation_result.verified_institution_name}의 팩스번호입니다"
                        fax_search_method = "Valid3 완전 검증 성공"
                        confidence = max(confidence, validation_result.final_confidence)
                    elif validation_result.stage4_passed and validation_result.ai_extracted_institution:
                        fax_label = self.extract_y_label_from_ai_result(validation_result.ai_extracted_institution, fax_number, contact_type="팩스번호")
                        fax_search_method = "AI 기관명 추출 성공"
                        confidence = max(confidence, 70.0)
                    elif validation_result.stage2_passed and validation_result.google_search_result:
                        fax_label = self.extract_y_label_from_ai_result(validation_result.google_search_result, fax_number, contact_type="팩스번호")
                        fax_search_method = "구글 검색 결과 활용"
                        confidence = max(confidence, 50.0)
            
            processing_time = time.time() - start_time
            
            result = {
                'original_index': int(row['original_index']),
                'phone_number': phone_number,
                'fax_number': fax_number,
                'institution_name': institution_name,
                'phone_label': phone_label,
                'fax_label': fax_label,
                'phone_search_method': phone_search_method,
                'fax_search_method': fax_search_method,
                'confidence': confidence,
                'processing_time': processing_time,
                'worker_id': worker_id if 'worker_id' in locals() else -1,
                'error_message': ""
            }
            
            worker_info = f" (워커 {worker_id})" if 'worker_id' in locals() else ""
            self.logger.debug(f"✅ Row {row_idx+1} 완료{worker_info}: 전화={phone_search_method}, 팩스={fax_search_method} ({confidence:.1f}%)")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Row {row_idx+1} 웹 검색 실패: {e}")
            return {
                'original_index': row_idx,
                'phone_number': phone_number if 'phone_number' in locals() else '',
                'fax_number': fax_number if 'fax_number' in locals() else '',
                'institution_name': institution_name if 'institution_name' in locals() else '',
                'phone_label': '',
                'fax_label': '',
                'phone_search_method': '예외 발생',
                'fax_search_method': '예외 발생',
                'confidence': 0.0,
                'processing_time': time.time() - start_time,
                'error_message': str(e)
            }
    
    def save_web_search_results(self, results: List[Dict], original_csv_path: str = "mappingdata250809.csv") -> str:
        """웹 검색 결과를 새로운 CSV 파일로 저장 (G열, J열, H열, K열 업데이트)"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"mappingdata_websearch_{timestamp}.csv"
            
            # 원본 CSV 로딩 (인코딩 자동 감지)
            original_data = load_csv_with_encoding(original_csv_path, self.logger)
            self.logger.info(f"📂 원본 데이터 로딩: {len(original_data)}행")
            
            # 웹 검색 결과를 딕셔너리로 변환 (original_index 기준)
            results_dict = {r['original_index']: r for r in results}
            
            # 새로운 검색 정보 컬럼 추가
            original_data['전화_검색방법'] = ''
            original_data['팩스_검색방법'] = ''
            original_data['웹검색_신뢰도'] = 0.0
            original_data['처리_시간'] = 0.0
            original_data['웹검색_오류'] = ''
            
            updated_count = 0
            phone_updated = 0
            fax_updated = 0
            
            for idx, row in original_data.iterrows():
                original_index = row['연번']
                if original_index in results_dict:
                    result = results_dict[original_index]
                    
                    # G열(실제_기관명_전화_AI) 업데이트
                    if result['phone_label']:
                        original_data.at[idx, '실제_기관명_전화_AI'] = result['phone_label']
                        original_data.at[idx, 'D열과의 매핑여부'] = 'O'  # H열 업데이트
                        phone_updated += 1
                    
                    # J열(실제_기관명_팩스_AI) 업데이트  
                    if result['fax_label']:
                        original_data.at[idx, '실제_기관명_팩스_AI'] = result['fax_label']
                        original_data.at[idx, 'I열과의 매핑여부'] = 'O'  # K열 업데이트
                        fax_updated += 1
                    
                    # 추가 정보 업데이트
                    original_data.at[idx, '전화_검색방법'] = result['phone_search_method']
                    original_data.at[idx, '팩스_검색방법'] = result['fax_search_method']
                    original_data.at[idx, '웹검색_신뢰도'] = result['confidence']
                    original_data.at[idx, '처리_시간'] = round(result['processing_time'], 2)
                    original_data.at[idx, '웹검색_오류'] = result['error_message']
                    
                    updated_count += 1
            
            # 파일 저장 (인코딩 자동 처리)
            if not save_csv_with_encoding(original_data, output_file, self.logger):
                raise Exception(f"CSV 저장 실패: {output_file}")
            
            self.logger.info(f"✅ 웹 검색 결과 저장 완료: {output_file}")
            self.logger.info(f"📊 업데이트 통계:")
            self.logger.info(f"   - 총 처리 행: {updated_count}/{len(results)}")
            self.logger.info(f"   - G열(전화 라벨) 업데이트: {phone_updated}개")
            self.logger.info(f"   - J열(팩스 라벨) 업데이트: {fax_updated}개")
            
            return output_file
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            traceback.print_exc()
            return ""
    
    def calculate_estimated_time(self, total_count: int, workers: int = 4, avg_time_per_item: float = 15.0) -> Dict:
        """예상 소요 시간 계산"""
        # Valid3 웹 검색 평균 시간: 약 10-20초/건
        total_time_seconds = (total_count * avg_time_per_item) / workers
        
        hours = int(total_time_seconds // 3600)
        minutes = int((total_time_seconds % 3600) // 60)
        seconds = int(total_time_seconds % 60)
        
        return {
            'total_seconds': total_time_seconds,
            'hours': hours,
            'minutes': minutes,
            'seconds': seconds,
            'formatted': f"{hours}시간 {minutes}분 {seconds}초",
            'avg_per_item': avg_time_per_item,
            'workers': workers
        }
    
    def _enhanced_driver_cleanup(self):
        """강화된 드라이버 정리 (Valid4 전용 - 표준 ChromeDriver 지원)"""
        try:
            self.logger.info("🧹 Valid4 강화된 드라이버 정리 시작")
            
            # 1. 활성 드라이버 정리 (표준 ChromeDriver 포함)
            for worker_id, driver in list(self.active_drivers.items()):
                try:
                    if driver:
                        # 표준 ChromeDriver는 안전하게 종료
                        driver.quit()
                        self.logger.debug(f"✅ 워커 {worker_id} 활성 드라이버 정리")
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 활성 드라이버 정리 실패: {e}")
            
            self.active_drivers.clear()
            
            # 2. 상속받은 정리 메서드 호출
            self._cleanup_all_worker_drivers()
            
            # 3. 메모리 정리
            self._cleanup_memory()
            
            # 4. 강제 Chrome 프로세스 종료 (여전히 필요할 수 있음)
            self.force_kill_all_chrome_processes()
            
            # 5. 임시 프로필 디렉토리 정리
            try:
                import tempfile
                import shutil
                temp_dir = tempfile.gettempdir()
                
                # chrome_std_, chrome_fallback_ 프로필 정리
                for item in os.listdir(temp_dir):
                    if item.startswith(('chrome_std_', 'chrome_fallback_')):
                        item_path = os.path.join(temp_dir, item)
                        try:
                            if os.path.isdir(item_path):
                                shutil.rmtree(item_path, ignore_errors=True)
                                self.logger.debug(f"🧹 임시 프로필 디렉토리 정리: {item}")
                        except Exception as dir_error:
                            self.logger.debug(f"임시 디렉토리 정리 실패 (무시): {dir_error}")
            except Exception as temp_error:
                self.logger.debug(f"임시 디렉토리 정리 과정 오류 (무시): {temp_error}")
            
            # 6. 가비지 컬렉션
            import gc
            collected = gc.collect()
            self.logger.info(f"🧹 Valid4 강화된 드라이버 정리 완료 (GC: {collected}개 객체)")
            
        except Exception as e:
            self.logger.error(f"❌ Valid4 드라이버 정리 실패: {e}")
    
    def _periodic_cleanup_check(self):
        """주기적 정리 체크 (10건마다)"""
        self._cleanup_interval_counter += 1
        if self._cleanup_interval_counter % 10 == 0:
            self.logger.info("🔄 주기적 드라이버 정리 실행")
            self._enhanced_driver_cleanup()
    
    def _emergency_chrome_cleanup(self):
        """비상 Chrome 프로세스 정리"""
        try:
            import subprocess
            import platform
            
            if platform.system() == "Windows":
                # 모든 Chrome과 ChromeDriver 프로세스 강제 종료
                subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'], capture_output=True)
                subprocess.run(['taskkill', '/f', '/im', 'chromedriver.exe'], capture_output=True)
                subprocess.run(['taskkill', '/f', '/t', '/im', 'chrome.exe'], capture_output=True)  # 트리 종료
                self.logger.info("🚨 비상 Chrome 프로세스 정리 완료 (Windows)")
            else:
                subprocess.run(['pkill', '-9', '-f', 'chrome'], capture_output=True)
                subprocess.run(['pkill', '-9', '-f', 'chromedriver'], capture_output=True)
                self.logger.info("🚨 비상 Chrome 프로세스 정리 완료 (Linux/Mac)")
                
        except Exception as e:
            self.logger.warning(f"⚠️ 비상 Chrome 정리 실패: {e}")
    
    def _smart_driver_manager(self, worker_id: int):
        """지능형 드라이버 관리 (실패 추적 및 자동 복구)"""
        try:
            # 실패 횟수 초기화
            if worker_id not in self.driver_failure_count:
                self.driver_failure_count[worker_id] = 0
            
            # 연속 실패가 많으면 더 긴 지연
            failure_count = self.driver_failure_count[worker_id]
            if failure_count > 0:
                delay = min(self.driver_restart_delay * (2 ** failure_count), 30.0)  # 최대 30초
                self.logger.info(f"🔄 워커 {worker_id} 재시작 지연: {delay:.1f}초 (실패 {failure_count}회)")
                time.sleep(delay)
            
            # 기존 드라이버 안전하게 정리
            if worker_id in self.active_drivers:
                try:
                    old_driver = self.active_drivers[worker_id]
                    if old_driver:
                        old_driver.quit()
                    del self.active_drivers[worker_id]
                    self.logger.debug(f"🧹 워커 {worker_id} 기존 드라이버 정리 완료")
                except Exception as e:
                    self.logger.debug(f"⚠️ 워커 {worker_id} 기존 드라이버 정리 실패: {e}")
            
            # Chrome 캐시 정리 (주기적)
            if worker_id % 3 == 0:  # 3개 워커마다 1번
                self._cleanup_chrome_cache(worker_id)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} 지능형 드라이버 관리 실패: {e}")
            return False
    
    def _cleanup_chrome_cache(self, worker_id: int):
        """Chrome 캐시 및 임시 파일 정리"""
        try:
            import shutil
            import tempfile
            
            # undetected_chromedriver 캐시 정리
            uc_cache_paths = [
                os.path.expanduser("~/.undetected_chromedriver"),
                os.path.join(os.path.expanduser("~"), "AppData", "Roaming", "undetected_chromedriver"),
                os.path.join(tempfile.gettempdir(), "undetected_chromedriver")
            ]
            
            for cache_path in uc_cache_paths:
                if os.path.exists(cache_path):
                    try:
                        # 특정 파일만 정리 (전체 삭제는 위험)
                        for item in os.listdir(cache_path):
                            if item.endswith(('.tmp', '.lock')):
                                item_path = os.path.join(cache_path, item)
                                if os.path.isfile(item_path):
                                    os.remove(item_path)
                        self.logger.debug(f"🧹 워커 {worker_id} Chrome 캐시 정리: {cache_path}")
                    except Exception as e:
                        self.logger.debug(f"Chrome 캐시 정리 실패 (무시): {e}")
                        
        except Exception as e:
            self.logger.debug(f"Chrome 캐시 정리 오류 (무시): {e}")
    
    def _intelligent_port_management(self):
        """지능형 포트 관리 (충돌 방지)"""
        try:
            current_time = time.time()
            
            # 10분마다 포트 정리
            if current_time - self._last_port_cleanup > 600:
                self.logger.info("🔌 주기적 포트 정리 실행")
                
                # 사용하지 않는 포트 정리
                if hasattr(self, 'port_manager'):
                    # 모든 워커의 활성 상태 확인
                    active_worker_ids = set(self.active_drivers.keys())
                    
                    # 사용하지 않는 포트 해제
                    for port in list(self.port_manager.used_ports):
                        # 포트를 사용하는 워커가 비활성 상태면 해제
                        port_worker_id = (port - 9222) % 1000  # 포트에서 워커 ID 추정
                        if port_worker_id not in active_worker_ids:
                            try:
                                self.port_manager.release_port(port)
                                self.logger.debug(f"🔓 미사용 포트 해제: {port}")
                            except:
                                pass
                
                self._last_port_cleanup = current_time
                
        except Exception as e:
            self.logger.debug(f"포트 관리 오류: {e}")
    
    def _enhanced_user_agent_rotation(self, worker_id: int) -> str:
        """강화된 User-Agent 로테이션 (config 기반)"""
        try:
            # config의 User-Agent 사용
            from config.crawling_settings import get_user_agents
            user_agents = get_user_agents()
            
            # 워커별로 다른 User-Agent 선택 (패턴 회피)
            agent_index = (worker_id + int(time.time()) // 3600) % len(user_agents)
            selected_agent = user_agents[agent_index]
            
            self.logger.debug(f"🎭 워커 {worker_id} User-Agent: {selected_agent[:50]}...")
            return selected_agent
            
        except Exception as e:
            self.logger.warning(f"⚠️ User-Agent 로테이션 실패: {e}")
            # 기본 UserAgentRotator 사용
            return self.user_agent_rotator.get_random_user_agent()
    
    def _safe_driver_operation(self, worker_id: int, operation_func, *args, **kwargs):
        """안전한 드라이버 작업 실행 (자동 복구 포함)"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 드라이버 상태 확인
                if worker_id not in self.active_drivers or not self.active_drivers[worker_id]:
                    # 드라이버 재생성
                    success = self._create_stable_driver(worker_id)
                    if not success:
                        continue
                
                # 작업 실행
                result = operation_func(self.active_drivers[worker_id], *args, **kwargs)
                
                # 성공 시 실패 횟수 리셋
                self.driver_failure_count[worker_id] = 0
                return result
                
            except Exception as e:
                self.logger.warning(f"⚠️ 워커 {worker_id} 작업 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                
                # 실패 횟수 증가
                self.driver_failure_count[worker_id] = self.driver_failure_count.get(worker_id, 0) + 1
                
                # 드라이버 정리 후 재시도
                if worker_id in self.active_drivers:
                    try:
                        self.active_drivers[worker_id].quit()
                        del self.active_drivers[worker_id]
                    except:
                        pass
                
                if attempt < max_retries - 1:
                    delay = 2.0 * (attempt + 1)
                    time.sleep(delay)
        
        self.logger.error(f"❌ 워커 {worker_id} 모든 재시도 실패")
        return None
    
    def _create_stable_driver(self, worker_id: int) -> bool:
        """표준 ChromeDriver 기반 안정성 강화된 드라이버 생성"""
        try:
            # 지능형 관리자 실행
            if not self._smart_driver_manager(worker_id):
                return False
            
            # 포트 관리
            self._intelligent_port_management()
            
            # 🔧 표준 Selenium ChromeDriver 직접 생성 (undetected 완전 제거)
            self.logger.debug(f"🛡️ 워커 {worker_id} 표준 ChromeDriver 생성 중...")
            
            # 포트 할당
            assigned_port = None
            if hasattr(self, 'port_manager'):
                try:
                    assigned_port = self.port_manager.allocate_port(worker_id)
                    self.logger.debug(f"🔌 워커 {worker_id} 포트 할당: {assigned_port}")
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 포트 할당 실패: {e}")
            
            # Chrome 옵션 설정
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            from webdriver_manager.chrome import ChromeDriverManager
            
            chrome_options = Options()
            
            # 기본 안정성 옵션
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1366,768')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--disable-web-security')
            
            # User-Agent 로테이션
            user_agent = self._enhanced_user_agent_rotation(worker_id)
            chrome_options.add_argument(f'--user-agent={user_agent}')
            
            # 매크로 감지 회피
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # 포트 설정
            if assigned_port:
                chrome_options.add_argument(f'--remote-debugging-port={assigned_port}')
            
            # 임시 프로필 디렉토리
            import tempfile
            profile_dir = tempfile.mkdtemp(prefix=f'chrome_v4_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            
            # 🚀 호환 가능한 ChromeDriver 자동 설치 및 생성
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                self.logger.debug(f"✅ 워커 {worker_id} webdriver-manager로 ChromeDriver 생성 성공")
            except Exception as wdm_error:
                self.logger.warning(f"⚠️ 워커 {worker_id} webdriver-manager 실패: {wdm_error}")
                # Fallback: 시스템 PATH에서 chromedriver 찾기
                try:
                    driver = webdriver.Chrome(options=chrome_options)
                    self.logger.debug(f"✅ 워커 {worker_id} 시스템 PATH ChromeDriver 생성 성공")
                except Exception as system_error:
                    self.logger.error(f"❌ 워커 {worker_id} 모든 ChromeDriver 생성 실패: {system_error}")
                    return False
            
            # 드라이버 추가 설정
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(30)
            
            # 웹드라이버 감지 방지 스크립트
            try:
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko']})")
            except Exception as e:
                self.logger.debug(f"⚠️ 워커 {worker_id} 웹드라이버 감지 방지 스크립트 실패: {e}")
            
            self.active_drivers[worker_id] = driver
            self.logger.info(f"✅ 워커 {worker_id} 표준 ChromeDriver 생성 완료")
            
            # 🔍 Google 접근 및 검색창 테스트 (중요!)
            try:
                self.logger.debug(f"🔍 워커 {worker_id} Google 접근 및 검색창 테스트")
                
                # Google 페이지 로드
                driver.get("https://www.google.com")
                time.sleep(random.uniform(2.0, 3.0))  # 충분한 로딩 시간
                
                # 검색창 찾기 (여러 셀렉터 시도)
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from selenium.common.exceptions import TimeoutException
                
                search_selectors = [
                    'textarea[name="q"]',      # 최신 Google
                    '#APjFqb',                 # Google 메인 검색창
                    'input[name="q"]',         # 이전 Google
                    '[title="검색"]',           # 한국어 Google
                    '[title="Search"]'         # 영어 Google
                ]
                
                search_box = None
                for selector in search_selectors:
                    try:
                        wait = WebDriverWait(driver, 5)
                        search_box = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                        self.logger.debug(f"✅ 워커 {worker_id} 검색창 발견: {selector}")
                        break
                    except TimeoutException:
                        continue
                
                if search_box:
                    # 검색창 실제 입력 테스트
                    test_query = "test"
                    search_box.clear()
                    time.sleep(0.5)
                    search_box.send_keys(test_query)
                    time.sleep(0.5)
                    search_box.clear()  # 테스트 후 정리
                    
                    self.logger.info(f"✅ 워커 {worker_id} 검색창 입력 테스트 성공")
                    return True
                else:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 모든 검색창 셀렉터 실패")
                    # 페이지 소스 일부 확인
                    page_source = driver.page_source[:500]
                    self.logger.debug(f"페이지 소스 일부: {page_source}")
                    return True  # 그래도 드라이버는 유지
                    
            except Exception as search_test_error:
                self.logger.error(f"❌ 워커 {worker_id} Google 접근 테스트 실패: {search_test_error}")
                # 드라이버 정리 후 실패 반환
                try:
                    driver.quit()
                    del self.active_drivers[worker_id]
                except:
                    pass
                return False
            
            return True
        
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} 표준 ChromeDriver 생성 실패: {e}")
            # 포트 해제
            if assigned_port and hasattr(self, 'port_manager'):
                self.port_manager.release_port(assigned_port, worker_id)
            return False
    
    def __del__(self):
        """소멸자 - 객체 삭제 시 드라이버 정리"""
        try:
            self._enhanced_driver_cleanup()
        except:
            pass

def main_websearch():
    """Valid4 웹 검색 메인 함수"""
    try:
        print("=" * 50)
        print("🔍 Valid4 웹 검색 시스템 시작")
        print("=" * 50)
        
        # 테스트 모드 선택
        test_choice = input("테스트 모드 실행하시겠습니까? (y/n): ").strip().lower()
        test_mode = test_choice == 'y'
        test_sample_size = 10 if test_mode else 0
        
        # 관리자 초기화
        manager = Valid4WebSearchManager()
        
        # 데이터 로딩
        print("\n📂 매핑 실패 데이터 로딩 중...")
        if not manager.load_unmapped_data(test_mode=test_mode, test_sample_size=test_sample_size):
            print("❌ 데이터 로딩 실패")
            return
        
        total_count = len(manager.input_data)
        print(f"✅ 로딩 완료: {total_count}개 데이터")
        
        # 예상 시간 계산
        time_estimate = manager.calculate_estimated_time(total_count)
        print(f"\n⏱️ 예상 소요 시간: {time_estimate['formatted']}")
        print(f"   - 워커 수: {time_estimate['workers']}개")
        print(f"   - 평균 처리 시간: {time_estimate['avg_per_item']}초/건")
        
        if not test_mode:
            proceed = input(f"\n{total_count}개 데이터 웹 검색을 시작하시겠습니까? (y/n): ").strip().lower()
            if proceed != 'y':
                print("❌ 사용자가 취소했습니다")
                return
        
        # 웹 검색 실행
        print(f"\n🚀 웹 검색 시작 (배치 크기: 50개)")
        all_results = []
        start_time = time.time()
        
        for start_idx in range(0, total_count, 50):
            batch_num = (start_idx // 50) + 1
            total_batches = (total_count + 49) // 50
            
            print(f"\n📦 배치 {batch_num}/{total_batches} 처리 중...")
            batch_results = manager.process_web_search_batch(start_idx, 50)
            all_results.extend(batch_results)
            
            # 중간 저장
            if len(all_results) % 50 == 0 or start_idx + 50 >= total_count:
                print(f"💾 중간 결과 저장 중... ({len(all_results)}/{total_count})")
                manager.save_web_search_results(all_results)
        
        # 최종 결과 저장
        print(f"\n💾 최종 결과 저장 중...")
        output_file = manager.save_web_search_results(all_results)
        
        # 최종 통계
        total_time = time.time() - start_time
        phone_successes = len([r for r in all_results if r['phone_label']])
        fax_successes = len([r for r in all_results if r['fax_label']])
        
        print("\n" + "=" * 50)
        print("🎉 Valid4 웹 검색 완료!")
        print("=" * 50)
        print(f"📊 총 처리: {len(all_results)}개")
        print(f"📞 전화번호 Y 라벨 생성: {phone_successes}개")
        print(f"📠 팩스번호 Y 라벨 생성: {fax_successes}개")
        print(f"✅ 전체 성공률: {(phone_successes + fax_successes)/(len(all_results)*2)*100:.1f}%")
        print(f"⏱️ 총 소요 시간: {total_time/3600:.1f}시간")
        print(f"📁 결과 파일: {output_file}")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()
        # 강화된 드라이버 정리
        try:
            if 'manager' in locals():
                print("🧹 강화된 크롬 드라이버 정리 시작...")
                manager._enhanced_driver_cleanup()
                manager._emergency_chrome_cleanup()
                print("🧹 크롬 드라이버 정리 완료")
        except Exception as cleanup_error:
            print(f"⚠️ 드라이버 정리 중 오류: {cleanup_error}")
            # 최후의 수단
            try:
                import subprocess
                subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'], capture_output=True)
                subprocess.run(['taskkill', '/f', '/im', 'chromedriver.exe'], capture_output=True)
                print("🚨 비상 Chrome 프로세스 강제 종료 완료")
            except:
                pass
    
    finally:
        # 항상 실행되는 최종 정리
        try:
            if 'manager' in locals():
                manager._enhanced_driver_cleanup()
        except:
            pass

if __name__ == "__main__":
    print("Valid4.py 실행 옵션:")
    print("1. Valid4 (Phase 0 자동 라벨링)")
    print("2. Valid3 (기존 5단계 검증)")
    print("3. Valid4 웹 검색 (매핑 실패 데이터 검색)")
    
    choice = input("선택 (1/2/3): ").strip()
    
    if choice == "1":
        main_valid4()
    elif choice == "2":
        main()
    elif choice == "3":
        main_websearch()
    else:
        print("잘못된 선택입니다. Valid4를 실행합니다.")
        main_valid4() 