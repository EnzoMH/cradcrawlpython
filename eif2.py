#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Institution Finder v2 - 검색어 로직 개선 버전
failed_data_250715.xlsx의 H열(전화번호 기관명)과 J열(팩스번호 기관명) 채우기

개선사항:
- 자연스러운 검색어 형태로 수정 (따옴표 제거)
- 더욱 효과적인 검색 패턴 적용
- 기존 utils/config 모듈 완전 활용
- undetected_chromedriver 직접 사용
- 사용자 설정 선택권 제공
- 자동 배치 크기 계산

작성자: AI Assistant
작성일: 2025-01-16
업데이트: 사용자 설정 메뉴 및 undetected_chromedriver 직접 사용
"""

import pandas as pd
import numpy as np
import time
import random
import re
import os
import sys
import logging
import gc
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
import threading
from dataclasses import dataclass, field
import traceback

# 외부 라이브러리 imports
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

# py-cpuinfo 추가 (선택적)
try:
    import cpuinfo
    HAS_CPUINFO = True
except ImportError:
    HAS_CPUINFO = False
    print("⚠️ py-cpuinfo가 설치되지 않았습니다. 자동 감지 기능이 제한됩니다.")

# 기존 모듈들 import
from utils.phone_validator import PhoneValidator
from utils.excel_processor import ExcelProcessor
from utils.data_mapper import DataMapper
from utils.verification_engine import VerificationEngine
from config.performance_profiles import PerformanceManager, PerformanceLevel
from config.crawling_settings import CrawlingSettings

# 사용자 설정 관리 클래스
@dataclass
class UserConfig:
    """사용자 설정 데이터 클래스"""
    max_workers: int = 4
    batch_size: int = 100
    save_directory: str = "results"
    gemini_api_key: str = ""
    chrome_version_main: Optional[int] = None
    config_source: str = "manual"  # "auto", "recommended", "manual"

# 설정 메뉴 관리자
class ConfigManager:
    """설정 메뉴 관리자"""
    
    def __init__(self):
        """설정 관리자 초기화"""
        self.config = UserConfig()
        self.performance_manager = PerformanceManager()
        
        # 결과 저장 디렉토리 확인/생성
        os.makedirs("results", exist_ok=True)
        
    def show_welcome_message(self):
        """환영 메시지 출력"""
        print("=" * 80)
        print("🎯 Enhanced Institution Finder v2 - 개선된 기관명 추출 시스템")
        print("=" * 80)
        print("📞 전화번호/팩스번호로 기관명을 자동으로 찾아드립니다!")
        print("🔍 undetected_chromedriver를 사용한 고급 봇 우회 기능")
        print("⚙️  다양한 성능 프로필 지원 (저사양~고사양)")
        print()
        
    def show_system_info(self):
        """시스템 정보 표시"""
        print("📊 현재 시스템 정보:")
        print("-" * 50)
        
        # performance_manager에서 시스템 정보 가져오기
        sys_info = self.performance_manager.system_info
        
        print(f"💻 CPU: {sys_info.get('cpu_name', 'Unknown')}")
        print(f"🔧 코어/스레드: {sys_info.get('cpu_cores', 'N/A')}코어 {sys_info.get('cpu_threads', 'N/A')}스레드")
        print(f"🧠 메모리: {sys_info.get('total_memory_gb', 'N/A')}GB")
        
        # py-cpuinfo 정보 추가 (있는 경우)
        if HAS_CPUINFO:
            try:
                cpu_info = cpuinfo.get_cpu_info()
                cpu_brand = cpu_info.get('brand_raw', 'Unknown')
                if cpu_brand != sys_info.get('cpu_name', ''):
                    print(f"📝 상세 CPU: {cpu_brand}")
            except:
                pass
        
        print()

    def show_config_menu(self) -> UserConfig:
        """설정 메뉴 표시 및 사용자 선택 처리"""
        self.show_welcome_message()
        self.show_system_info()
        
        print("⚙️  설정 방식을 선택해주세요:")
        print("=" * 50)
        print("1. 🤖 자동 감지 (py-cpuinfo 기반)")
        print("2. 📋 추천 설정 (performance_profiles.py 기반)")
        print("3. ✋ 수동 설정 (직접 입력)")
        print("4. ❓ 도움말")
        print()
        
        while True:
            try:
                choice = input("선택해주세요 (1-4): ").strip()
                
                if choice == "1":
                    return self._auto_config()
                elif choice == "2":
                    return self._recommended_config()
                elif choice == "3":
                    return self._manual_config()
                elif choice == "4":
                    self._show_help()
                    continue
                else:
                    print("❌ 잘못된 선택입니다. 1-4 중에서 선택해주세요.")
                    
            except KeyboardInterrupt:
                print("\n🚫 사용자가 취소했습니다.")
                sys.exit(0)
            except Exception as e:
                print(f"❌ 입력 오류: {e}")
    
    def _auto_config(self) -> UserConfig:
        """자동 설정 (py-cpuinfo 기반)"""
        print("\n🤖 자동 감지 설정을 적용합니다...")
        
        # performance_manager에서 자동 선택된 프로필 사용
        profile = self.performance_manager.get_current_profile()
        
        self.config.max_workers = profile.max_workers
        self.config.batch_size = profile.batch_size
        self.config.config_source = "auto"
        
        print(f"✅ 자동 감지 완료!")
        print(f"   - 프로필: {profile.name}")
        print(f"   - 워커 수: {profile.max_workers}개")
        print(f"   - 배치 크기: {profile.batch_size}개")
        
        return self._finalize_config()
    
    def _recommended_config(self) -> UserConfig:
        """추천 설정 (performance_profiles.py 기반)"""
        print("\n📋 추천 설정을 선택합니다...")
        
        # 모든 프로필 표시
        profiles = {
            1: PerformanceLevel.LOW_SPEC,
            2: PerformanceLevel.MEDIUM_SPEC, 
            3: PerformanceLevel.HIGH_SPEC,
            4: PerformanceLevel.ULTRA_SPEC
        }
        
        print("사용 가능한 성능 프로필:")
        print("-" * 60)
        
        for num, level in profiles.items():
            profile = self.performance_manager.profiles[level]
            print(f"{num}. {profile.name}")
            print(f"   워커: {profile.max_workers}개 | 배치: {profile.batch_size}개 | 메모리: {profile.chrome_memory_limit}MB")
        
        # 현재 자동 선택된 프로필 표시
        current_profile = self.performance_manager.get_current_profile()
        print(f"\n🎯 시스템 분석 결과 추천: {current_profile.name}")
        
        while True:
            try:
                choice = input("\n프로필을 선택하세요 (1-4, Enter=추천사용): ").strip()
                
                if not choice:  # Enter만 누른 경우 추천 사용
                    selected_level = None
                    selected_profile = current_profile
                    break
                    
                choice_num = int(choice)
                if choice_num in profiles:
                    selected_level = profiles[choice_num]
                    selected_profile = self.performance_manager.profiles[selected_level]
                    break
                else:
                    print("❌ 1-4 중에서 선택해주세요.")
                    
            except ValueError:
                print("❌ 숫자를 입력해주세요.")
            except KeyboardInterrupt:
                print("\n🚫 취소되었습니다.")
                return self.show_config_menu()
        
        # 선택된 프로필 적용
        if selected_level:
            self.performance_manager.set_profile(selected_level)
        
        self.config.max_workers = selected_profile.max_workers
        self.config.batch_size = selected_profile.batch_size
        self.config.config_source = "recommended"
        
        print(f"\n✅ 프로필 적용 완료: {selected_profile.name}")
        
        return self._finalize_config()
    
    def _manual_config(self) -> UserConfig:
        """수동 설정"""
        print("\n✋ 수동 설정 모드입니다...")
        
        # 워커 수 설정 (2-18)
        while True:
            try:
                workers = input("워커 수를 입력하세요 (2-18, 기본값: 4): ").strip()
                if not workers:
                    self.config.max_workers = 4
                    break
                    
                worker_num = int(workers)
                if 2 <= worker_num <= 18:
                    self.config.max_workers = worker_num
                    break
                else:
                    print("❌ 워커 수는 2-18 사이여야 합니다.")
                    
            except ValueError:
                print("❌ 숫자를 입력해주세요.")
        
        print(f"✅ 워커 수: {self.config.max_workers}개")
        
        # 배치 크기는 자동 계산 또는 수동 입력
        print("\n배치 크기 설정:")
        print("1. 자동 계산 (총 데이터 수 / 워커 수)")
        print("2. 수동 입력")
        
        while True:
            try:
                batch_choice = input("선택하세요 (1-2, 기본값: 1): ").strip()
                if not batch_choice or batch_choice == "1":
                    self.config.batch_size = "auto"  # 나중에 데이터 로드 후 계산
                    print("✅ 배치 크기: 자동 계산")
                    break
                elif batch_choice == "2":
                    while True:
                        try:
                            batch_input = input("배치 크기를 입력하세요 (10-1000, 기본값: 100): ").strip()
                            if not batch_input:
                                self.config.batch_size = 100
                                break
                            
                            batch_num = int(batch_input)
                            if 10 <= batch_num <= 1000:
                                self.config.batch_size = batch_num
                                break
                            else:
                                print("❌ 배치 크기는 10-1000 사이여야 합니다.")
                        except ValueError:
                            print("❌ 숫자를 입력해주세요.")
                    print(f"✅ 배치 크기: {self.config.batch_size}개")
                    break
                else:
                    print("❌ 1 또는 2를 선택해주세요.")
            except ValueError:
                print("❌ 올바른 선택을 해주세요.")
        
        self.config.config_source = "manual"
        
        return self._finalize_config()
    
    def _finalize_config(self) -> UserConfig:
        """설정 완료 처리"""
        
        # 저장 디렉토리 설정
        print(f"\n💾 결과 파일 저장 위치:")
        save_path = input(f"저장 디렉토리 (기본값: results): ").strip()
        if save_path:
            self.config.save_directory = save_path
            os.makedirs(save_path, exist_ok=True)
        else:
            self.config.save_directory = "results"
        
        print(f"✅ 저장 위치: {self.config.save_directory}/")
        
        # Gemini API 키 설정
        print(f"\n🔑 Gemini API 키 설정:")
        
        # 환경변수 확인
        env_key = os.getenv('GEMINI_API_KEY')
        if env_key:
            print(f"✅ 환경변수에서 API 키 발견")
            self.config.gemini_api_key = env_key
        else:
            print("⚠️ GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")
            api_input = input("API 키를 직접 입력하시겠습니까? (y/N): ").strip().lower()
            
            if api_input in ['y', 'yes']:
                while True:
                    api_key = input("Gemini API 키를 입력하세요: ").strip()
                    if api_key:
                        self.config.gemini_api_key = api_key
                        # 환경변수에도 설정 (현재 세션에서 사용)
                        os.environ['GEMINI_API_KEY'] = api_key
                        print("✅ API 키 설정 완료 (환경변수 업데이트됨)")
                        break
                    else:
                        print("❌ API 키를 입력해주세요.")
            else:
                print("⚠️ API 키 없이 진행합니다. (일부 기능 제한될 수 있음)")
        
        # Chrome 버전 설정 (Chrome 138 대응)
        self.config.chrome_version_main = None  # Chrome 138 호환성을 위해 None 사용
        
        # 설정값 검증 및 fallback 적용
        self._validate_and_fix_config()
        
        # 최종 설정 확인
        print("\n" + "=" * 60)
        print("📋 최종 설정 확인")
        print("=" * 60)
        print(f"🔧 워커 수: {self.config.max_workers}개")
        print(f"📦 배치 크기: {self.config.batch_size}")
        print(f"💾 저장 위치: {self.config.save_directory}/")
        print(f"🔑 API 키: {'✅ 설정됨' if self.config.gemini_api_key else '❌ 미설정'}")
        print(f"🌐 Chrome 버전: Auto (version_main=None)")
        print(f"📊 설정 방식: {self.config.config_source}")
        print("=" * 60)
        
        confirm = input("\n계속 진행하시겠습니까? (Y/n): ").strip().lower()
        if confirm in ['', 'y', 'yes']:
            print("✅ 설정 완료! 크롤링을 시작합니다...\n")
            return self.config
        else:
            print("🔄 설정을 다시 선택합니다...\n")
            return self.show_config_menu()
    
    def _validate_and_fix_config(self):
        """설정값 검증 및 fallback 적용"""
        print("\n🔍 설정값 검증 중...")
        
        adjustments = []
        
        # 워커 수 검증
        if not isinstance(self.config.max_workers, int) or self.config.max_workers < 1 or self.config.max_workers > 20:
            original = self.config.max_workers
            self.config.max_workers = 4
            adjustments.append(f"워커 수: {original} → {self.config.max_workers} (범위: 1-20)")
        
        # 배치 크기 검증 (문자열 "auto"는 허용)
        if (self.config.batch_size != "auto" and 
            (not isinstance(self.config.batch_size, int) or self.config.batch_size < 1 or self.config.batch_size > 1000)):
            original = self.config.batch_size
            self.config.batch_size = 100
            adjustments.append(f"배치 크기: {original} → {self.config.batch_size} (범위: 1-1000 또는 'auto')")
        
        # 저장 디렉토리 검증
        if not self.config.save_directory or not isinstance(self.config.save_directory, str):
            original = self.config.save_directory
            self.config.save_directory = "results"
            adjustments.append(f"저장 디렉토리: {original} → {self.config.save_directory}")
        
        # 시스템 리소스 기반 자동 조정
        sys_info = self.performance_manager.system_info
        total_memory_gb = sys_info.get('total_memory_gb', 8)
        cpu_cores = sys_info.get('cpu_cores', 4)
        
        # 메모리 기반 워커 수 제한
        max_recommended_workers = min(18, max(2, int(total_memory_gb / 2)))
        if self.config.max_workers > max_recommended_workers:
            original = self.config.max_workers
            self.config.max_workers = max_recommended_workers
            adjustments.append(f"워커 수 메모리 제한: {original} → {self.config.max_workers} (메모리: {total_memory_gb}GB)")
        
        # CPU 기반 워커 수 추천
        cpu_recommended_workers = min(self.config.max_workers, cpu_cores * 2)
        if self.config.max_workers > cpu_recommended_workers:
            original = self.config.max_workers
            self.config.max_workers = cpu_recommended_workers
            adjustments.append(f"워커 수 CPU 제한: {original} → {self.config.max_workers} (CPU: {cpu_cores}코어)")
        
        # 조정사항 출력
        if adjustments:
            print("⚙️  설정값이 자동 조정되었습니다:")
            for adjustment in adjustments:
                print(f"   - {adjustment}")
        else:
            print("✅ 모든 설정값이 유효합니다.")
    
    def _show_help(self):
        """도움말 표시"""
        print("\n" + "=" * 60)
        print("❓ 설정 방식 도움말")
        print("=" * 60)
        print("🤖 자동 감지:")
        print("   - py-cpuinfo를 사용해 CPU 정보를 분석")
        print("   - 시스템 사양에 맞는 최적 설정 자동 적용")
        print("   - 가장 편리하지만 py-cpuinfo 설치 필요")
        print()
        print("📋 추천 설정:")
        print("   - performance_profiles.py의 프로필 중 선택")
        print("   - 저사양, 중사양, 고사양, 최고사양 4가지 옵션")
        print("   - 시스템 분석 후 추천 프로필 제안")
        print()
        print("✋ 수동 설정:")
        print("   - 워커 수: 2-18개 (동시 실행할 Chrome 인스턴스 수)")
        print("   - 배치 크기: 한 번에 처리할 데이터 수")
        print("   - 세밀한 조정 가능하지만 경험 필요")
        print()
        print("💡 권장사항:")
        print("   - 처음 사용: 자동 감지 또는 추천 설정")
        print("   - 경험자: 수동 설정으로 최적화")
        print("   - 저사양 PC: 워커 2-4개, 배치 50-100개")
        print("   - 고사양 PC: 워커 8-16개, 배치 200-500개")
        print()
        print("⚠️  주의사항:")
        print("   - 워커 수가 많을수록 메모리 사용량 증가")
        print("   - Chrome 138 사용 시 version_main=None 권장")
        print("   - API 키 없이도 기본 검색 기능 사용 가능")
        print("=" * 60)
        print()

# 로깅 설정
def setup_logging():
    """로깅 시스템 설정"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
    
    # 파일 핸들러
    file_handler = logging.FileHandler(f'enhanced_finder_v2_{timestamp}.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

# 검색 결과 클래스
@dataclass
class SearchResult:
    """개별 검색 결과"""
    row_index: int
    phone_number: str = ""
    fax_number: str = ""
    found_phone_institution: str = ""
    found_fax_institution: str = ""
    phone_success: bool = False
    fax_success: bool = False
    processing_time: float = 0.0
    error_message: str = ""
    search_queries_used: List[str] = field(default_factory=list)

# 개선된 검색 엔진
class ImprovedSearchEngine:
    """개선된 검색 엔진 - 자연스러운 검색어 적용"""
    
    def __init__(self, logger=None):
        """
        개선된 검색 엔진 초기화
        
        Args:
            logger: 로깅 객체
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # 기존 GoogleSearchEngine 활용
        # self.google_search_engine = GoogleSearchEngine(self.logger) # 이 부분은 삭제되었으므로 주석 처리
        
        # 기관명 추출 패턴
        self.institution_patterns = [
            r'([\w\s]*(?:센터|기관|청|구청|시청|군청|면사무소|읍사무소|동|주민센터|행정복지센터)[\w\s]*)',
            r'([\w\s]*(?:복지관|보건소|보건지소|병원|의원|클리닉|한의원)[\w\s]*)',
            r'([\w\s]*(?:학교|대학교|대학|학원|교육원|교육청)[\w\s]*)',
            r'([\w\s]*(?:협회|단체|재단|법인|공단|공사|회|조합)[\w\s]*)',
            r'([\w\s]*(?:교회|성당|절|사찰|종교시설)[\w\s]*)',
        ]
        
        # 제외 키워드
        self.exclude_keywords = [
            '광고', '배너', '클릭', '링크', '바로가기', '사이트맵',
            '검색결과', '네이버', '다음', '구글', '야후', '카카오',
            'COM', 'co.kr', 'www', 'http', 'https', '.com', '.kr',
            '옥션원모바일', '스팸', '홍보', '마케팅', '업체'
        ]
        
        self.logger.info("🔍 개선된 검색 엔진 초기화 완료")
    
    # 자연스러운 검색 쿼리 생성 (따옴표 제거)
    def create_natural_queries(self, number: str, number_type: str = "전화") -> List[str]:
        """
        자연스러운 검색 쿼리 생성 (따옴표 제거)
        
        Args:
            number: 전화번호 또는 팩스번호
            number_type: "전화" 또는 "팩스"
            
        Returns:
            List[str]: 우선순위별 검색 쿼리 목록
        """
        queries = []
        
        # 🎯 핵심 자연어 검색 쿼리 (최우선 - 따옴표 제거)
        if number_type == "전화":
            priority_queries = [
                f'{number} 은 어디전화번호',
                f'{number} 어디전화번호',
                f'{number} 은 어디 전화번호',
                f'{number} 어디 전화번호',
                f'{number} 전화번호 어디',
                f'{number} 는 어디전화번호',
                f'{number} 전화 어디',
            ]
        else:  # 팩스
            priority_queries = [
                f'{number} 은 어디팩스번호',
                f'{number} 어디팩스번호',
                f'{number} 은 어디 팩스번호',
                f'{number} 어디 팩스번호',
                f'{number} 팩스번호 어디',
                f'{number} 는 어디팩스번호',
                f'{number} 팩스 어디',
            ]
        
        # 우선순위 쿼리 먼저 추가
        queries.extend(priority_queries)
        
        # 🔍 정확한 매칭 검색어 (따옴표 사용)
        if number_type == "전화":
            exact_queries = [
                f'"{number}" 전화번호 기관',
                f'"{number}" 연락처 어디',
                f'"{number}" 기관명',
                f'"{number}" 전화 기관',
                f'전화번호 "{number}" 어디',
            ]
        else:  # 팩스
            exact_queries = [
                f'"{number}" 팩스번호 기관',
                f'"{number}" fax 어디',
                f'"{number}" 기관명',
                f'"{number}" 팩스 기관',
                f'팩스번호 "{number}" 어디',
            ]
        
        queries.extend(exact_queries)
        
        # 🏢 지역별 검색 강화
        area_code = number.split('-')[0] if '-' in number else number[:3]
        area_names = self._get_area_names(area_code)
        
        for area in area_names[:2]:  # 상위 2개 지역만
            if number_type == "전화":
                queries.extend([
                    f'{area} {number} 전화번호',
                    f'{number} {area} 기관',
                    f'{area} {number} 연락처',
                ])
            else:
                queries.extend([
                    f'{area} {number} 팩스번호',
                    f'{number} {area} 기관',
                    f'{area} {number} 팩스',
                ])
        
        # 🏛️ 공식 사이트 우선 검색
        official_queries = [
            f'"{number}" site:go.kr',
            f'"{number}" site:or.kr',
            f'{number} 공식 홈페이지',
            f'{number} 관공서',
        ]
        
        queries.extend(official_queries)
        
        # 📞 기관 유형별 검색
        institution_types = ['주민센터', '구청', '보건소', '복지관', '센터', '기관']
        for inst_type in institution_types[:3]:  # 상위 3개만
            if number_type == "전화":
                queries.append(f'{number} {inst_type} 전화')
            else:
                queries.append(f'{number} {inst_type} 팩스')
        
        return queries[:20]  # 상위 20개만 반환
    
    def _get_area_names(self, area_code: str) -> List[str]:
        """지역번호 기반 지역명 반환"""
        area_mapping = {
            "02": ["서울", "서울특별시", "서울시"],
            "031": ["경기", "경기도", "수원", "성남", "안양"],
            "032": ["인천", "인천광역시", "인천시"],
            "033": ["강원", "강원도", "춘천", "원주"],
            "041": ["충남", "충청남도", "천안", "아산"],
            "042": ["대전", "대전광역시", "대전시"],
            "043": ["충북", "충청북도", "청주", "충주"],
            "044": ["세종", "세종특별자치시", "세종시"],
            "051": ["부산", "부산광역시", "부산시"],
            "052": ["울산", "울산광역시", "울산시"],
            "053": ["대구", "대구광역시", "대구시"],
            "054": ["경북", "경상북도", "포항", "구미"],
            "055": ["경남", "경상남도", "창원", "마산"],
            "061": ["전남", "전라남도", "목포", "여수"],
            "062": ["광주", "광주광역시", "광주시"],
            "063": ["전북", "전라북도", "전주", "익산"],
            "064": ["제주", "제주특별자치도", "제주시"],
        }
        
        return area_mapping.get(area_code, [])
    
    def search_institution_by_number(self, driver, number: str, number_type: str = "전화") -> Optional[str]:
        """
        번호로 기관명 검색 (개선된 로직)
        
        Args:
            driver: WebDriver 인스턴스
            number: 전화번호 또는 팩스번호
            number_type: "전화" 또는 "팩스"
            
        Returns:
            Optional[str]: 발견된 기관명 또는 None
        """
        try:
            self.logger.info(f"🔍 {number_type}번호 기관명 검색 시작: {number}")
            
            # 자연스러운 검색 쿼리 생성
            search_queries = self.create_natural_queries(number, number_type)
            
            # 사용된 쿼리 로깅
            self.logger.debug(f"📝 생성된 검색어 예시: {search_queries[:3]}")
            
            # 각 쿼리별로 검색 시도 (우선순위 순)
            for query_idx, query in enumerate(search_queries[:12]):  # 상위 12개만 시도
                try:
                    self.logger.debug(f"🔎 쿼리 {query_idx + 1}: {query}")
                    
                    # 구글 검색 실행
                    page_source = self._perform_search(driver, query)
                    
                    if page_source:
                        # 기관명 추출
                        institution_name = self._extract_institution_name(page_source, number)
                        if institution_name:
                            self.logger.info(f"✅ 기관명 발견: {institution_name} (쿼리: {query})")
                            return institution_name
                    
                    # 검색 간 지연 (봇 감지 방지)
                    delay = random.uniform(1.5, 3.0)
                    # 우선순위 쿼리는 짧은 지연, 나머지는 긴 지연
                    if query_idx >= 7:  # 7번째 쿼리부터 더 긴 지연
                        delay = random.uniform(2.5, 4.0)
                    
                    time.sleep(delay)
                    
                except Exception as e:
                    self.logger.debug(f"⚠️ 쿼리 검색 실패: {query} - {e}")
                    continue
            
            self.logger.warning(f"❌ 모든 쿼리 실패: {number} ({number_type})")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ {number_type}번호 검색 오류: {number} - {e}")
            return None
    
    def _perform_search(self, driver, query: str) -> Optional[str]:
        """구글 검색 수행 (개선된 로직)"""
        try:
            # 구글 검색 페이지로 이동
            driver.get('https://www.google.com')
            time.sleep(random.uniform(1.0, 2.0))
            
            # 검색창 찾기
            search_box = WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            
            # 검색어 입력 (더 자연스럽게)
            search_box.clear()
            time.sleep(random.uniform(0.3, 0.7))
            
            # 한글자씩 입력하는 것처럼 (봇 감지 회피)
            for char in query:
                search_box.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
            
            time.sleep(random.uniform(0.5, 1.0))
            
            # 검색 실행
            search_box.send_keys(Keys.RETURN)
            
            # 검색 결과 로딩 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            
            time.sleep(random.uniform(2.0, 3.5))
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            
            # 검색 결과 확인
            if any(phrase in page_source for phrase in ["검색결과가 없습니다", "검색 결과 없음", "관련 검색결과가 없습니다"]):
                self.logger.debug(f"검색 결과 없음: {query}")
                return None
            
            # 봇 감지 확인
            if any(phrase in page_source.lower() for phrase in ["unusual traffic", "recaptcha", "suspicious activity"]):
                self.logger.warning(f"⚠️ 봇 감지 가능성 - 대기: {query}")
                time.sleep(random.uniform(8.0, 15.0))
                return None
            
            # 실제 검색이 수행되었는지 확인
            if len(page_source) < 5000:  # 너무 짧은 응답은 오류 가능성
                self.logger.debug(f"페이지 응답이 너무 짧음: {len(page_source)} bytes")
                return None
            
            return page_source
            
        except Exception as e:
            self.logger.debug(f"구글 검색 실패: {query} - {e}")
            return None
    
    def _extract_institution_name(self, page_source: str, number: str) -> Optional[str]:
        """페이지에서 기관명 추출 (강화된 로직)"""
        try:
            soup = BeautifulSoup(page_source, 'html.parser')
            text_content = soup.get_text()
            
            self.logger.debug(f"📄 페이지 텍스트 길이: {len(text_content)}자")
            
            # 1. 번호 주변 텍스트에서 기관명 찾기 (최우선)
            lines = text_content.split('\n')
            for i, line in enumerate(lines):
                if number in line:
                    self.logger.debug(f"🎯 번호 발견 라인: {line.strip()[:100]}...")
                    
                    # 주변 라인들과 함께 분석 (더 넓은 범위)
                    context_lines = lines[max(0, i-5):i+6]
                    context_text = ' '.join(context_lines)
                    
                    # 컨텍스트에서 기관명 찾기
                    institution_name = self._find_institution_in_context(context_text, number)
                    if institution_name:
                        return institution_name
            
            # 2. 제목이나 헤더에서 기관명 우선 찾기
            for tag in ['title', 'h1', 'h2', 'h3']:
                elements = soup.find_all(tag)
                for element in elements:
                    text = element.get_text().strip()
                    cleaned_name = self._clean_institution_name(text)
                    if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                        self.logger.debug(f"🏢 헤더에서 기관명 발견: {cleaned_name}")
                        return cleaned_name
            
            # 3. 전체 텍스트에서 기관명 패턴 검색
            for pattern in self.institution_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                for match in matches[:5]:  # 상위 5개만 확인
                    cleaned_name = self._clean_institution_name(match)
                    if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                        self.logger.debug(f"🏢 패턴 매칭 기관명: {cleaned_name}")
                        return cleaned_name
            
            return None
            
        except Exception as e:
            self.logger.debug(f"기관명 추출 실패: {e}")
            return None
    
    def _find_institution_in_context(self, context_text: str, number: str) -> Optional[str]:
        """컨텍스트 텍스트에서 기관명 찾기 (개선된 로직)"""
        try:
            # 주요 기관명 키워드들 (우선순위별)
            priority_keywords = ['주민센터', '행정복지센터', '구청', '시청', '군청']
            secondary_keywords = ['센터', '기관', '청', '동', '복지관', '보건소', '보건지소', '병원', '의원']
            
            # 단어 단위로 분리
            words = context_text.split()
            
            # 우선순위 키워드부터 찾기
            for keyword_list in [priority_keywords, secondary_keywords]:
                for i, word in enumerate(words):
                    for keyword in keyword_list:
                        if keyword in word:
                            # 주변 단어들과 결합하여 완전한 기관명 구성
                            start_idx = max(0, i-4)  # 더 넓은 범위
                            end_idx = min(len(words), i+5)
                            candidate = ' '.join(words[start_idx:end_idx])
                            
                            # 기관명 정제
                            cleaned_name = self._clean_institution_name(candidate)
                            if cleaned_name and self._is_valid_institution_name(cleaned_name, number):
                                return cleaned_name
            
            return None
            
        except Exception as e:
            self.logger.debug(f"컨텍스트 기관명 찾기 실패: {e}")
            return None
    
    def _clean_institution_name(self, candidate: str) -> Optional[str]:
        """기관명 후보 정제 (개선된 로직)"""
        try:
            if not candidate:
                return None
            
            # 불필요한 문자 제거 (더 정교하게)
            cleaned = re.sub(r'[^\w\s가-힣()]', ' ', candidate)
            cleaned = re.sub(r'\s+', ' ', cleaned)  # 연속 공백 제거
            cleaned = cleaned.strip()
            
            # 길이 검증
            if len(cleaned) < 3 or len(cleaned) > 60:
                return None
            
            # 숫자로만 이루어진 경우 제외
            if cleaned.isdigit():
                return None
            
            # 기관명 키워드 포함 여부 확인 (더 포괄적)
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '군청', '동', '주민센터',
                '행정복지센터', '복지관', '보건소', '보건지소', '병원', '의원', 
                '학교', '대학', '협회', '단체', '재단', '법인', '공단', '공사',
                '교회', '성당', '절', '사찰'
            ]
            
            has_keyword = any(keyword in cleaned for keyword in institution_keywords)
            if not has_keyword:
                return None
            
            # 기관명에서 핵심 부분만 추출 (키워드 중심으로)
            for keyword in institution_keywords:
                if keyword in cleaned:
                    # 키워드를 포함한 적절한 길이의 기관명 추출
                    parts = cleaned.split()
                    keyword_idx = -1
                    
                    for idx, part in enumerate(parts):
                        if keyword in part:
                            keyword_idx = idx
                            break
                    
                    if keyword_idx >= 0:
                        # 키워드 앞뒤 적절한 범위의 단어들 결합
                        start = max(0, keyword_idx - 2)
                        end = min(len(parts), keyword_idx + 3)
                        final_name = ' '.join(parts[start:end])
                        
                        if len(final_name) >= 3:
                            return final_name
            
            return cleaned if len(cleaned) >= 3 else None
            
        except Exception as e:
            self.logger.debug(f"기관명 정제 실패: {e}")
            return None
    
    def _is_valid_institution_name(self, name: str, number: str) -> bool:
        """유효한 기관명인지 검증 (강화된 로직)"""
        try:
            if not name or len(name.strip()) < 3:
                return False
            
            name = name.strip()
            
            # 제외 키워드 체크 (더 엄격하게)
            for exclude in self.exclude_keywords:
                if exclude.lower() in name.lower():
                    return False
            
            # 숫자만 있는 경우 제외
            if name.isdigit() or re.match(r'^\d+$', name):
                return False
            
            # 번호 자체가 포함된 경우 제외
            if number in name or name in number:
                return False
            
            # 너무 긴 경우 제외
            if len(name) > 60:
                return False
            
            # URL이나 이메일 형태 제외
            if any(pattern in name.lower() for pattern in ['http', 'www', '.com', '.kr', '@']):
                return False
            
            # 기관명 패턴 포함 여부 확인
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '군청', '동', '주민센터', 
                '행정복지센터', '복지관', '보건소', '보건지소', '병원', '의원',
                '학교', '대학', '협회', '단체', '재단', '법인'
            ]
            
            has_institution_keyword = any(keyword in name for keyword in institution_keywords)
            
            # 키워드가 있고, 적절한 길이인 경우만 유효
            return has_institution_keyword and 3 <= len(name) <= 60
            
        except Exception as e:
            self.logger.debug(f"기관명 검증 실패: {e}")
            return False

# 강화된 기관명 추출 메인 처리기 v2
class EnhancedInstitutionProcessor:
    """강화된 기관명 추출 메인 처리기 v2 - undetected_chromedriver 직접 사용"""
    
    def __init__(self, user_config: UserConfig):
        """
        메인 처리기 초기화
        
        Args:
            user_config: 사용자 설정 객체
        """
        self.logger = logging.getLogger(__name__)
        self.user_config = user_config
        self.max_workers = user_config.max_workers
        self.batch_size = user_config.batch_size  # "auto" 또는 숫자
        
        # 기존 모듈들 초기화
        self.performance_manager = PerformanceManager(self.logger)
        self.crawling_settings = CrawlingSettings()
        self.phone_validator = PhoneValidator(self.logger)
        self.excel_processor = ExcelProcessor(self.logger)
        self.data_mapper = DataMapper(self.logger)
        self.verification_engine = VerificationEngine()
        
        # 개선된 검색 엔진
        self.search_engine = ImprovedSearchEngine(self.logger)
        
        # 워커별 드라이버 관리
        self.worker_drivers = {}
        self.lock = threading.Lock()
        
        # Chrome 옵션 기본 설정 (performance_profiles 기반)
        self.chrome_options_base = self.performance_manager.get_chrome_options_for_profile()
        
        # 통계
        self.total_rows = 0
        self.processed_count = 0
        self.phone_success = 0
        self.fax_success = 0
        
        self.logger.info(f"🚀 개선된 기관명 추출 프로세서 v2 초기화 완료")
        self.logger.info(f"⚙️  설정: 워커 {self.max_workers}개")
        self.logger.info(f"📦 배치 크기: {self.batch_size}")
        self.logger.info(f"💾 저장 위치: {user_config.save_directory}")
        self.logger.info(f"🔍 검색어 개선: 자연스러운 형태 적용")
        self.logger.info(f"🛡️ undetected_chromedriver 직접 사용")
    
    # Excel 파일 로드 및 전처리
    def load_data(self, filepath: str) -> pd.DataFrame:
        """Excel 파일 로드 및 전처리"""
        try:
            # ExcelProcessor 활용
            success = self.excel_processor.load_excel_file(filepath)
            if not success:
                raise ValueError(f"파일 로드 실패: {filepath}")
            
            df = self.excel_processor.df
            self.logger.info(f"📊 데이터 로드 완료: {len(df)}행 × {len(df.columns)}열")
            self.logger.info(f"📋 컬럼: {list(df.columns)}")
            
            # 배치 크기 자동 계산 (데이터 로드 후)
            if self.batch_size == "auto":
                self.batch_size = self._calculate_optimal_batch_size(len(df))
                self.logger.info(f"📦 배치 크기 자동 계산: {self.batch_size}개")
            
            # 컬럼 확인 및 정보 출력
            if len(df.columns) >= 10:
                phone_col = df.columns[6]      # G열 (전화번호)
                phone_result_col = df.columns[7]  # H열 (전화번호 기관명)
                fax_col = df.columns[8]        # I열 (팩스번호)
                fax_result_col = df.columns[9]   # J열 (팩스번호 기관명)
                
                self.logger.info(f"🎯 처리 대상:")
                self.logger.info(f"   - {phone_col} (G열) → {phone_result_col} (H열)")
                self.logger.info(f"   - {fax_col} (I열) → {fax_result_col} (J열)")
                
                # 빈 값 통계
                phone_empty = df.iloc[:, 7].isna().sum()
                fax_empty = df.iloc[:, 9].isna().sum()
                total_empty = phone_empty + fax_empty
                self.logger.info(f"📈 빈 값 현황: H열 {phone_empty}개, J열 {fax_empty}개 (총 {total_empty}개)")
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def _calculate_optimal_batch_size(self, total_rows: int) -> int:
        """최적 배치 크기 자동 계산"""
        try:
            # 기본 공식: 총 데이터 수 / 워커 수
            calculated_size = max(1, total_rows // self.max_workers)
            
            # 최소/최대 제한 적용
            min_batch = 10
            max_batch = 500
            
            # 데이터 크기에 따른 조정
            if total_rows < 100:
                # 작은 데이터셋: 작은 배치
                optimal_size = min(calculated_size, 20)
            elif total_rows < 1000:
                # 중간 데이터셋: 적당한 배치
                optimal_size = min(max(calculated_size, 30), 100)
            else:
                # 큰 데이터셋: 큰 배치 (효율성)
                optimal_size = min(max(calculated_size, 50), max_batch)
            
            # 최종 제한 적용
            final_size = max(min_batch, min(optimal_size, max_batch))
            
            self.logger.info(f"📊 배치 크기 계산: {total_rows}행 ÷ {self.max_workers}워커 = {calculated_size} → 최적화: {final_size}")
            
            return final_size
            
        except Exception as e:
            self.logger.warning(f"⚠️ 배치 크기 계산 실패, 기본값 사용: {e}")
            return 100  # 기본값
    
    def _create_undetected_driver(self, worker_id: int) -> Optional[object]:
        """undetected_chromedriver 직접 생성 (performance_profiles 기반)"""
        try:
            # 워커 간 시차 두기 (봇 감지 회피)
            startup_delay = random.uniform(1.0, 3.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            # Chrome 옵션 설정
            chrome_options = uc.ChromeOptions()
            
            # performance_profiles에서 가져온 기본 옵션 적용
            for option in self.chrome_options_base:
                chrome_options.add_argument(option)
            
            # 워커별 추가 설정
            debug_port = 9222 + (worker_id * 10)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # 프로필 디렉토리 분리
            import tempfile
            profile_dir = tempfile.mkdtemp(prefix=f'uc_worker_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            
            # User-Agent 랜덤화
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            ]
            chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # 봇 감지 방지 실험적 옵션
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # undetected_chromedriver 생성 (Chrome 138 호환성)
            driver = uc.Chrome(
                options=chrome_options,
                version_main=self.user_config.chrome_version_main  # None for auto-detect
            )
            
            # 타임아웃 설정
            profile = self.performance_manager.get_current_profile()
            driver.implicitly_wait(profile.selenium_timeout)
            driver.set_page_load_timeout(profile.selenium_timeout * 2)
            
            # 웹드라이버 감지 방지 스크립트
            try:
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko']})")
                driver.execute_script("Object.defineProperty(navigator, 'platform', {get: () => 'Win32'})")
            except Exception as script_error:
                self.logger.warning(f"⚠️ 웹드라이버 감지 방지 스크립트 실패: {script_error}")
            
            self.logger.info(f"🛡️ 워커 {worker_id}: undetected_chromedriver 생성 완료 (포트: {debug_port})")
            
            return driver
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: undetected_chromedriver 생성 실패 - {e}")
            return None
    
    def process_single_row(self, row_data: Tuple[int, pd.Series], worker_id: int) -> SearchResult:
        """개별 행 처리 (전화번호와 팩스번호 모두)"""
        row_idx, row = row_data
        result = SearchResult(row_index=row_idx)
        start_time = time.time()
        
        try:
            # 컬럼 인덱스로 접근
            phone_number = str(row.iloc[6]).strip() if len(row) > 6 else ""  # G열
            fax_number = str(row.iloc[8]).strip() if len(row) > 8 else ""    # I열
            
            # 기존 결과 확인 (이미 채워진 경우 스킵)
            existing_phone_result = str(row.iloc[7]).strip() if len(row) > 7 else ""  # H열
            existing_fax_result = str(row.iloc[9]).strip() if len(row) > 9 else ""    # J열
            
            result.phone_number = phone_number
            result.fax_number = fax_number
            
            self.logger.info(f"📋 워커 {worker_id}: 행 {row_idx+1} 처리 시작")
            
            # 드라이버 가져오기
            driver = self._get_worker_driver(worker_id)
            if not driver:
                result.error_message = "드라이버 생성 실패"
                return result
            
            processed_items = []
            
            # 전화번호 처리 (H열이 비어있는 경우만)
            if (phone_number and phone_number != 'nan' and 
                (not existing_phone_result or existing_phone_result == 'nan') and
                self.phone_validator.is_valid_phone_format(phone_number)):
                
                self.logger.info(f"📞 워커 {worker_id}: 전화번호 {phone_number} 검색 시작")
                institution = self.search_engine.search_institution_by_number(driver, phone_number, "전화")
                
                if institution:
                    result.found_phone_institution = institution
                    result.phone_success = True
                    processed_items.append(f"전화({institution})")
                    self.logger.info(f"✅ 전화번호 성공: {institution}")
                else:
                    processed_items.append("전화(실패)")
                    self.logger.warning(f"⚠️ 전화번호 결과 없음")
            else:
                if existing_phone_result and existing_phone_result != 'nan':
                    processed_items.append("전화(기존)")
                else:
                    processed_items.append("전화(스킵)")
            
            # 팩스번호 처리 (J열이 비어있는 경우만)
            if (fax_number and fax_number != 'nan' and 
                (not existing_fax_result or existing_fax_result == 'nan') and
                self.phone_validator.is_valid_phone_format(fax_number)):
                
                self.logger.info(f"📠 워커 {worker_id}: 팩스번호 {fax_number} 검색 시작")
                institution = self.search_engine.search_institution_by_number(driver, fax_number, "팩스")
                
                if institution:
                    result.found_fax_institution = institution
                    result.fax_success = True
                    processed_items.append(f"팩스({institution})")
                    self.logger.info(f"✅ 팩스번호 성공: {institution}")
                else:
                    processed_items.append("팩스(실패)")
                    self.logger.warning(f"⚠️ 팩스번호 결과 없음")
            else:
                if existing_fax_result and existing_fax_result != 'nan':
                    processed_items.append("팩스(기존)")
                else:
                    processed_items.append("팩스(스킵)")
            
            result.processing_time = time.time() - start_time
            
            # 처리 결과 로깅
            self.logger.info(f"🎯 워커 {worker_id}: 행 {row_idx+1} 완료 - {', '.join(processed_items)} ({result.processing_time:.1f}초)")
            
            return result
            
        except Exception as e:
            result.error_message = str(e)
            result.processing_time = time.time() - start_time
            self.logger.error(f"❌ 워커 {worker_id}: 행 {row_idx+1} 처리 오류 - {e}")
            return result
    
    def _get_worker_driver(self, worker_id: int):
        """워커별 undetected_chromedriver 가져오기 (직접 생성 방식)"""
        # 기존 드라이버 상태 확인
        if worker_id in self.worker_drivers:
            try:
                driver = self.worker_drivers[worker_id]
                driver.current_url  # 상태 확인
                return driver
            except Exception as e:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 기존 드라이버 비정상 - {e}")
                # 비정상 드라이버 정리
                try:
                    self.worker_drivers[worker_id].quit()
                except:
                    pass
                del self.worker_drivers[worker_id]
        
        # 새 undetected_chromedriver 생성
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"🔄 워커 {worker_id}: undetected_chromedriver 생성 시도 ({attempt+1}/{max_attempts})")
                
                driver = self._create_undetected_driver(worker_id)
                if driver:
                    self.worker_drivers[worker_id] = driver
                    self.logger.info(f"✅ 워커 {worker_id}: undetected_chromedriver 할당 성공 ({attempt+1}차)")
                    return driver
                else:
                    self.logger.warning(f"⚠️ 워커 {worker_id}: 드라이버 생성 실패 ({attempt+1}차)")
                    
            except Exception as e:
                self.logger.error(f"❌ 워커 {worker_id}: 드라이버 생성 오류 ({attempt+1}차) - {e}")
            
            if attempt < max_attempts - 1:
                wait_time = (attempt + 1) * 3
                self.logger.info(f"⏱️ 워커 {worker_id}: {wait_time}초 대기 후 재시도")
                time.sleep(wait_time)
        
        self.logger.error(f"❌ 워커 {worker_id}: 모든 undetected_chromedriver 생성 시도 실패")
        return None
    
    def process_file(self, input_filepath: str) -> str:
        """파일 전체 처리 (배치별 병렬 처리)"""
        try:
            # 시스템 정보 출력
            self.performance_manager.display_performance_info()
            
            # 데이터 로드
            df = self.load_data(input_filepath)
            self.total_rows = len(df)
            
            self.logger.info(f"🚀 처리 시작: {len(df)}행")
            self.logger.info(f"⚙️  설정: 워커 {self.max_workers}개, 배치 {self.batch_size}개")
            
            # 모든 결과 저장
            all_results = {}
            
            # 배치별로 처리
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            
            for batch_start in range(0, len(df), self.batch_size):
                batch_end = min(batch_start + self.batch_size, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                
                batch_num = (batch_start // self.batch_size) + 1
                
                self.logger.info(f"📦 배치 {batch_num}/{total_batches} 처리: {batch_start+1}~{batch_end} ({len(batch_df)}개)")
                
                # 배치 내 병렬 처리
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    
                    # 워커에 작업 할당
                    for idx, (original_idx, row) in enumerate(batch_df.iterrows()):
                        worker_id = idx % self.max_workers
                        future = executor.submit(self.process_single_row, (original_idx, row), worker_id)
                        futures.append((future, original_idx))
                    
                    # 결과 수집
                    for future, row_idx in futures:
                        try:
                            result = future.result(timeout=300)  # 5분 타임아웃
                            all_results[row_idx] = result
                            
                            # 통계 업데이트
                            with self.lock:
                                self.processed_count += 1
                                if result.phone_success:
                                    self.phone_success += 1
                                if result.fax_success:
                                    self.fax_success += 1
                            
                            # 진행률 출력
                            if self.processed_count % 50 == 0:
                                progress = (self.processed_count / self.total_rows) * 100
                                self.logger.info(f"📊 진행률: {self.processed_count}/{self.total_rows} ({progress:.1f}%) - 전화:{self.phone_success}, 팩스:{self.fax_success}")
                            
                        except Exception as e:
                            self.logger.error(f"❌ 행 {row_idx+1} 결과 처리 오류: {e}")
                
                # 배치 완료 후 시스템 리소스 체크 및 조정
                current_resources = self.performance_manager.get_current_resources()
                adjustment = self.performance_manager.adjust_performance_dynamically(current_resources)
                if adjustment.get('adjusted'):
                    self.logger.info(f"⚙️  시스템 조정: {adjustment.get('reason')}")
                
                # 배치 간 휴식
                if batch_end < len(df):
                    rest_time = random.uniform(4.0, 8.0)
                    self.logger.info(f"⏱️ 배치 {batch_num} 완료 - {rest_time:.1f}초 휴식")
                    time.sleep(rest_time)
            
            # 결과를 DataFrame에 반영
            updated_count = 0
            for row_idx, result in all_results.items():
                if result.phone_success and len(df.columns) > 7:
                    df.iloc[row_idx, 7] = result.found_phone_institution  # H열
                    updated_count += 1
                if result.fax_success and len(df.columns) > 9:
                    df.iloc[row_idx, 9] = result.found_fax_institution    # J열
                    updated_count += 1
            
            self.logger.info(f"📝 총 {updated_count}개 셀 업데이트 완료")
            
            # 결과 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"enhanced_failed_data_v2_{timestamp}.xlsx"
            
            # ExcelProcessor로 저장
            save_path = os.path.join(self.user_config.save_directory, output_file)
            save_success = self.excel_processor.save_excel(df, save_path)
            if not save_success:
                # 백업 저장 방법
                df.to_excel(save_path, index=False)
                self.logger.info(f"📁 백업 방법으로 저장 완료: {save_path}")
            
            # 최종 통계 출력
            self._print_final_statistics()
            
            self.logger.info(f"�� 모든 처리 완료! 결과 파일: {save_path}")
            return save_path
            
        except Exception as e:
            self.logger.error(f"❌ 파일 처리 실패: {e}")
            self.logger.error(traceback.format_exc())
            raise
        finally:
            # 모든 드라이버 정리
            self._cleanup_drivers()
    
    def _cleanup_drivers(self):
        """모든 드라이버 정리"""
        try:
            self.logger.info("🧹 드라이버 정리 시작")
            for worker_id, driver in self.worker_drivers.items():
                try:
                    driver.quit()
                    self.logger.info(f"✅ 워커 {worker_id} 드라이버 정리 완료")
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 드라이버 정리 실패: {e}")
            
            self.worker_drivers.clear()
            gc.collect()
            self.logger.info("🧹 드라이버 정리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 드라이버 정리 오류: {e}")
    
    def _print_final_statistics(self):
        """최종 통계 출력"""
        self.logger.info("=" * 70)
        self.logger.info("📊 최종 처리 통계 (개선된 검색어 적용)")
        self.logger.info("=" * 70)
        self.logger.info(f"전체 행 수: {self.total_rows:,}")
        self.logger.info(f"처리 완료: {self.processed_count:,}")
        self.logger.info(f"전화번호 성공: {self.phone_success:,}")
        self.logger.info(f"팩스번호 성공: {self.fax_success:,}")
        
        if self.processed_count > 0:
            phone_rate = (self.phone_success / self.processed_count) * 100
            fax_rate = (self.fax_success / self.processed_count) * 100
            total_success = self.phone_success + self.fax_success
            total_attempts = self.processed_count * 2  # 전화+팩스
            overall_rate = (total_success / total_attempts) * 100
            
            self.logger.info(f"전화번호 성공률: {phone_rate:.1f}%")
            self.logger.info(f"팩스번호 성공률: {fax_rate:.1f}%") 
            self.logger.info(f"전체 성공률: {overall_rate:.1f}%")
        
        self.logger.info("🔍 주요 개선사항:")
        self.logger.info("   - 자연스러운 검색어 적용 (따옴표 제거)")
        self.logger.info("   - 더 효과적인 검색 패턴 순서")
        self.logger.info("   - 강화된 기관명 추출 로직")
        self.logger.info("=" * 70)

def main():
    """메인 실행 함수"""
    # 사용자 설정 메뉴 및 설정 관리자 초기화
    config_manager = ConfigManager()
    user_config = config_manager.show_config_menu()
    
    # 로깅 설정
    logger = setup_logging()
    
    try:
        logger.info("🎯 개선된 기관명 추출 시스템 v2 시작")
        logger.info("🔍 검색어 개선: 자연스러운 형태 (예: '02-1234-5678 은 어디전화번호')")
        
        # 입력 파일 설정
        input_file = 'rawdatafile/failed_data_250715.xlsx'
        
        # 파일 존재 확인
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_file}")
        
        # 프로세서 초기화 및 실행
        processor = EnhancedInstitutionProcessor(user_config)
        result_file = processor.process_file(input_file)
        
        logger.info(f"🎉 시스템 완료! 결과 파일: {result_file}")
        print(f"\n🎊 처리 완료! 개선된 결과를 확인하세요: {result_file}")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ 사용자에 의해 중단됨")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 시스템 오류: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 