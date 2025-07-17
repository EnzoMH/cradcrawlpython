#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Institution Finder v3 - 고급 AI 기반 기관명 추출 시스템
전화번호/팩스번호로 기관명을 찾는 차세대 크롤링 시스템

새로운 기능:
- 다중 Gemini API 키 관리 (라운드로빈 + 자동 전환)
- 고급 검색 결과 처리 (상위 5개 링크 실제 접속)
- Desktop 경로 자동 설정
- 실시간 캐시 시스템 (JSON + Excel Queue)
- 테스트 모드 (30개 랜덤 샘플)
- 향상된 텍스트 전처리 및 chunking

작성자: AI Assistant
작성일: 2025-01-16
버전: 3.0 - Advanced AI Integration
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
import json
import tempfile
import shutil
import subprocess
import zipfile
import platform
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any, Union
import threading
from dataclasses import dataclass, field
import traceback
from pathlib import Path

# 환경변수 로드
from dotenv import load_dotenv
load_dotenv()

# 외부 라이브러리 imports
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import requests

# Gemini AI - 간소화된 import
import google.generativeai as genai

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

# ================================
# 설정 및 데이터 클래스
# ================================

@dataclass
class UserConfig:
    """사용자 설정 데이터 클래스"""
    max_workers: int = 4
    batch_size: int = 100
    save_directory: str = ""  # Desktop으로 자동 설정
    execution_mode: str = "full"  # "test" or "full"
    test_sample_size: int = 30
    cache_interval: int = 50  # 50개마다 캐시 저장
    config_source: str = "manual"

@dataclass
class CacheData:
    """캐시 데이터 구조"""
    processed_rows: Dict[int, Dict] = field(default_factory=dict)
    progress: Dict[str, Any] = field(default_factory=dict)
    statistics: Dict[str, int] = field(default_factory=dict)
    timestamp: str = ""

@dataclass
class SearchResultV3:
    """고급 검색 결과"""
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
    web_sources: List[str] = field(default_factory=list)  # 실제 접속한 웹사이트들
    gemini_analysis: Dict[str, str] = field(default_factory=dict)  # AI 분석 결과

# ================================
# ChromeDriver 자동 다운로드 및 관리 시스템
# ================================

class ChromeDriverManager:
    """ChromeDriver 자동 다운로드 및 관리 클래스"""
    
    def __init__(self, logger=None):
        """
        ChromeDriver 관리자 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.current_dir = Path.cwd()
        # ChromeDriver 저장 디렉토리
        self.driver_dir = self.current_dir / "chromedriver"
        self.driver_dir.mkdir(exist_ok=True)
        
        # 플랫폼별 설정
        self.platform = platform.system().lower()
        self.architecture = platform.machine().lower()
        
        # ChromeDriver 파일명 설정
        if self.platform == "windows":
            self.driver_filename = "chromedriver.exe"
            self.platform_key = "win32" if "32" in self.architecture else "win64"
        elif self.platform == "darwin":  # macOS
            self.driver_filename = "chromedriver"
            self.platform_key = "mac-arm64" if "arm" in self.architecture else "mac-x64"
        else:  # Linux
            self.driver_filename = "chromedriver"
            self.platform_key = "linux64"
        
        self.driver_path = self.driver_dir / self.driver_filename
        
        self.logger.info(f"🔧 ChromeDriver 관리자 초기화: 플랫폼={self.platform}, 경로={self.driver_dir}")
    
    def get_chrome_version(self) -> Optional[str]:
        """설치된 Chrome 브라우저 버전 감지"""
        try:
            if self.platform == "windows":
                # Windows에서 Chrome 버전 확인
                try:
                    import winreg
                    # Chrome 레지스트리 경로들
                    registry_paths = [
                        r"SOFTWARE\Google\Chrome\BLBeacon",
                        r"SOFTWARE\Wow6432Node\Google\Chrome\BLBeacon",
                        r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\Google Chrome"
                    ]
                    
                    for path in registry_paths:
                        try:
                            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, path)
                            version, _ = winreg.QueryValueEx(key, "version")
                            winreg.CloseKey(key)
                            if version:
                                major_version = version.split('.')[0]
                                self.logger.info(f"🌐 Windows Chrome 버전 감지: {version} (메이저: {major_version})")
                                return major_version
                        except:
                            continue
                except ImportError:
                    self.logger.warning("⚠️ winreg 모듈 없음, 대체 방법 시도")
                
                # PowerShell을 통한 버전 확인 (대체 방법)
                try:
                    cmd = 'powershell "Get-ItemProperty -Path \'HKLM:\\SOFTWARE\\Google\\Chrome\\BLBeacon\' -Name version"'
                    result = subprocess.run(cmd, capture_output=True, text=True, shell=True, timeout=10)
                    if result.returncode == 0 and "version" in result.stdout:
                        for line in result.stdout.split('\n'):
                            if "version" in line and ":" in line:
                                version = line.split(':')[1].strip()
                                major_version = version.split('.')[0]
                                self.logger.info(f"🌐 PowerShell Chrome 버전 감지: {version} (메이저: {major_version})")
                                return major_version
                except Exception as e:
                    self.logger.warning(f"⚠️ PowerShell 버전 확인 실패: {e}")
                
                # 실행파일 직접 확인 (최종 대체 방법)
                chrome_paths = [
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe")
                ]
                
                for chrome_path in chrome_paths:
                    if os.path.exists(chrome_path):
                        try:
                            result = subprocess.run([chrome_path, "--version"], 
                                                  capture_output=True, text=True, timeout=10)
                            if result.returncode == 0:
                                version_text = result.stdout.strip()
                                version_match = re.search(r'(\d+)\.', version_text)
                                if version_match:
                                    major_version = version_match.group(1)
                                    self.logger.info(f"🌐 실행파일 Chrome 버전 감지: {version_text} (메이저: {major_version})")
                                    return major_version
                        except Exception as e:
                            self.logger.warning(f"⚠️ Chrome 실행파일 버전 확인 실패: {e}")
                            continue
            
            elif self.platform == "darwin":  # macOS
                try:
                    result = subprocess.run(["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "--version"],
                                          capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        version_text = result.stdout.strip()
                        version_match = re.search(r'(\d+)\.', version_text)
                        if version_match:
                            major_version = version_match.group(1)
                            self.logger.info(f"🌐 macOS Chrome 버전 감지: {version_text} (메이저: {major_version})")
                            return major_version
                except Exception as e:
                    self.logger.warning(f"⚠️ macOS Chrome 버전 확인 실패: {e}")
            
            else:  # Linux
                try:
                    # 다양한 Chrome 실행파일 경로 시도
                    chrome_commands = ["google-chrome", "chromium-browser", "chromium", "chrome"]
                    
                    for cmd in chrome_commands:
                        try:
                            result = subprocess.run([cmd, "--version"], 
                                                  capture_output=True, text=True, timeout=10)
                            if result.returncode == 0:
                                version_text = result.stdout.strip()
                                version_match = re.search(r'(\d+)\.', version_text)
                                if version_match:
                                    major_version = version_match.group(1)
                                    self.logger.info(f"🌐 Linux Chrome 버전 감지: {version_text} (메이저: {major_version})")
                                    return major_version
                        except FileNotFoundError:
                            continue
                        except Exception as e:
                            self.logger.warning(f"⚠️ Linux Chrome 명령어 {cmd} 실패: {e}")
                            continue
                except Exception as e:
                    self.logger.warning(f"⚠️ Linux Chrome 버전 확인 실패: {e}")
            
            # 기본값 반환 (현재 안정화 버전)
            self.logger.warning("⚠️ Chrome 버전 감지 실패, 기본값 120 사용")
            return "120"
            
        except Exception as e:
            self.logger.error(f"❌ Chrome 버전 감지 중 오류: {e}")
            return "120"  # 안전한 기본값
    
    def get_compatible_chromedriver_version(self, chrome_version: str) -> str:
        """Chrome 버전에 호환되는 ChromeDriver 버전 결정"""
        try:
            chrome_major = int(chrome_version)
            
            # Chrome 버전에 따른 ChromeDriver 버전 매핑
            # 참고: https://chromedriver.chromium.org/downloads
            if chrome_major >= 115:
                # Chrome 115+ 는 ChromeDriver 버전이 같음
                driver_version = chrome_version
                self.logger.info(f"🔗 Chrome {chrome_major} → ChromeDriver {driver_version}")
                return driver_version
            else:
                # 이전 버전들의 매핑
                version_mapping = {
                    114: "114.0.5735.90",
                    113: "113.0.5672.63",
                    112: "112.0.5615.49",
                    111: "111.0.5563.64",
                    110: "110.0.5481.77"
                }
                
                driver_version = version_mapping.get(chrome_major, "120.0.6099.109")  # 최신 안정화
                self.logger.info(f"🔗 Chrome {chrome_major} → ChromeDriver {driver_version} (매핑)")
                return driver_version
                
        except Exception as e:
            self.logger.warning(f"⚠️ ChromeDriver 버전 결정 실패: {e}, 기본값 사용")
            return "120.0.6099.109"
    
    def download_chromedriver(self, version: str = None) -> bool:
        """ChromeDriver 다운로드"""
        try:
            # 기존 드라이버가 있고 정상 작동하면 스킵
            if self.driver_path.exists() and self._test_existing_driver():
                self.logger.info("✅ 기존 ChromeDriver가 정상 작동 중, 다운로드 스킵")
                return True
            
            # Chrome 버전 감지
            if not version:
                chrome_version = self.get_chrome_version()
                version = self.get_compatible_chromedriver_version(chrome_version)
            
            self.logger.info(f"📥 ChromeDriver 다운로드 시작: v{version} ({self.platform_key})")
            
            # ChromeDriver 다운로드 URL 구성
            # 새로운 Chrome for Testing API 사용 (Chrome 115+)
            try:
                if int(version.split('.')[0]) >= 115:
                    # 새로운 JSON API 사용
                    api_url = f"https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"
                    response = requests.get(api_url, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    download_url = None
                    
                    # 버전별 다운로드 URL 찾기
                    for version_info in data.get('versions', []):
                        if version_info['version'].startswith(version.split('.')[0]):
                            downloads = version_info.get('downloads', {}).get('chromedriver', [])
                            for download in downloads:
                                if download['platform'] == self.platform_key:
                                    download_url = download['url']
                                    break
                            if download_url:
                                break
                    
                    if not download_url:
                        raise Exception(f"Chrome for Testing API에서 {version} 버전을 찾을 수 없음")
                
                else:
                    # 기존 방식 (Chrome 114 이하)
                    download_url = f"https://chromedriver.storage.googleapis.com/{version}/chromedriver_{self.platform_key}.zip"
                
            except Exception as e:
                self.logger.warning(f"⚠️ 정확한 버전 다운로드 실패: {e}, 최신 버전 시도")
                # 최신 안정화 버전으로 대체
                download_url = f"https://chromedriver.storage.googleapis.com/120.0.6099.109/chromedriver_{self.platform_key}.zip"
            
            # ZIP 파일 다운로드
            self.logger.info(f"📡 다운로드 중: {download_url}")
            
            response = requests.get(download_url, timeout=120)
            response.raise_for_status()
            
            # 임시 ZIP 파일 저장
            zip_path = self.driver_dir / "chromedriver.zip"
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"📦 ZIP 파일 다운로드 완료: {len(response.content)} bytes")
            
            # ZIP 파일 압축 해제
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # ZIP 내용 확인
                file_list = zip_ref.namelist()
                self.logger.debug(f"📋 ZIP 내용: {file_list}")
                
                # ChromeDriver 실행파일 찾기
                driver_file = None
                for file in file_list:
                    if self.driver_filename in file:
                        driver_file = file
                        break
                
                if not driver_file:
                    raise Exception(f"ZIP에서 {self.driver_filename}을 찾을 수 없음")
                
                # 실행파일 추출
                with zip_ref.open(driver_file) as source, open(self.driver_path, 'wb') as target:
                    target.write(source.read())
            
            # ZIP 파일 삭제
            zip_path.unlink()
            
            # 실행 권한 설정 (Unix 계열)
            if self.platform != "windows":
                os.chmod(self.driver_path, 0o755)
            
            self.logger.info(f"✅ ChromeDriver 다운로드 및 설치 완료: {self.driver_path}")
            
            # 다운로드된 드라이버 테스트
            if self._test_downloaded_driver():
                self.logger.info("🎉 다운로드된 ChromeDriver 테스트 성공")
                return True
            else:
                self.logger.warning("⚠️ 다운로드된 ChromeDriver 테스트 실패")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ ChromeDriver 다운로드 실패: {e}")
            return False
    
    def _test_existing_driver(self) -> bool:
        """기존 ChromeDriver 테스트"""
        try:
            if not self.driver_path.exists():
                return False
            
            # 간단한 버전 확인
            result = subprocess.run([str(self.driver_path), "--version"], 
                                  capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and "ChromeDriver" in result.stdout:
                self.logger.debug(f"🔍 기존 드라이버 테스트 성공: {result.stdout.strip()}")
                return True
            else:
                self.logger.warning("⚠️ 기존 드라이버 테스트 실패")
                return False
                
        except Exception as e:
            self.logger.warning(f"⚠️ 기존 드라이버 테스트 오류: {e}")
            return False
    
    def _test_downloaded_driver(self) -> bool:
        """다운로드된 ChromeDriver 테스트"""
        try:
            if not self.driver_path.exists():
                return False
            
            # 버전 확인
            result = subprocess.run([str(self.driver_path), "--version"], 
                                  capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0 and "ChromeDriver" in result.stdout:
                version_info = result.stdout.strip()
                self.logger.info(f"🎯 다운로드된 드라이버 검증: {version_info}")
                return True
            else:
                self.logger.error(f"❌ 드라이버 버전 확인 실패: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 다운로드된 드라이버 테스트 오류: {e}")
            return False
    
    def get_driver_path(self) -> Optional[str]:
        """사용 가능한 ChromeDriver 경로 반환"""
        try:
            # 먼저 다운로드 시도
            if not self.driver_path.exists():
                self.logger.info("🔄 ChromeDriver가 없음, 자동 다운로드 시도")
                if not self.download_chromedriver():
                    self.logger.error("❌ ChromeDriver 자동 다운로드 실패")
                    return None
            
            # 경로 존재 확인
            if self.driver_path.exists():
                self.logger.info(f"✅ ChromeDriver 경로 확인: {self.driver_path}")
                return str(self.driver_path)
            else:
                self.logger.error("❌ ChromeDriver 파일이 존재하지 않음")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ ChromeDriver 경로 가져오기 실패: {e}")
            return None
    
    def cleanup_driver_files(self):
        """ChromeDriver 관련 파일들 정리"""
        try:
            if self.driver_dir.exists():
                # ZIP 파일들 정리
                for zip_file in self.driver_dir.glob("*.zip"):
                    try:
                        zip_file.unlink()
                        self.logger.debug(f"🗑️ ZIP 파일 정리: {zip_file}")
                    except:
                        pass
                
                # 오래된 드라이버 파일들 정리 (백업용)
                for old_driver in self.driver_dir.glob("chromedriver_old_*"):
                    try:
                        old_driver.unlink()
                        self.logger.debug(f"🗑️ 오래된 드라이버 정리: {old_driver}")
                    except:
                        pass
                
                self.logger.info("🧹 ChromeDriver 관련 파일 정리 완료")
            
        except Exception as e:
            self.logger.warning(f"⚠️ ChromeDriver 파일 정리 실패: {e}")

# ================================
# Gemini API 관리 시스템 v2 (ai_model_manager.py 기반)
# ================================

# AI 모델 설정
AI_MODEL_CONFIG = {
    "temperature": 0.1,
    "top_p": 0.8,
    "top_k": 40,
    "max_output_tokens": 2048,
}

class GeminiAPIManager:
    """AI 모델 관리 클래스 - 4개의 Gemini API 키 지원 (ai_model_manager.py 기반)"""
    
    def __init__(self, logger=None):
        """
        AI 모델 관리자 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.gemini_models = []
        self.gemini_config = AI_MODEL_CONFIG
        self.current_model_index = 0
        self.setup_models()
    
    def setup_models(self):
        """4개의 AI 모델 초기화"""
        try:
            # API 키들 가져오기
            api_keys = {
                'GEMINI_1': os.getenv('GEMINI_API_KEY'),
                'GEMINI_2': os.getenv('GEMINI_API_KEY_2'),
                'GEMINI_3': os.getenv('GEMINI_API_KEY_3'),
                'GEMINI_4': os.getenv('GEMINI_API_KEY_4')
            }
            
            # 최소 하나의 API 키는 있어야 함
            valid_keys = {k: v for k, v in api_keys.items() if v}
            if not valid_keys:
                raise ValueError("GEMINI_API_KEY, GEMINI_API_KEY_2, GEMINI_API_KEY_3, 또는 GEMINI_API_KEY_4 환경 변수 중 최소 하나는 설정되어야 합니다.")
            
            # 각 API 키에 대해 모델 초기화
            for model_name, api_key in valid_keys.items():
                try:
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel(
                        "gemini-2.0-flash-lite-001",
                        generation_config=self.gemini_config
                    )
                    
                    self.gemini_models.append({
                        'model': model,
                        'api_key': api_key[:10] + "...",
                        'name': model_name,
                        'failures': 0
                    })
                    
                    self.logger.info(f"🤖 {model_name} 모델 초기화 성공")
                    
                except Exception as e:
                    self.logger.error(f"❌ {model_name} 모델 초기화 실패: {e}")
                    continue
            
            if not self.gemini_models:
                raise ValueError("사용 가능한 Gemini 모델이 없습니다.")
            
            self.logger.info(f"🎉 총 {len(self.gemini_models)}개의 Gemini 모델 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ AI 모델 초기화 실패: {e}")
            raise
    
    def get_next_model(self) -> Optional[Dict]:
        """다음 사용 가능한 모델 선택 (라운드 로빈 방식)"""
        if not self.gemini_models:
            return None
        
        # 실패 횟수가 적은 모델 우선 선택
        available_models = [m for m in self.gemini_models if m['failures'] < 3]
        if not available_models:
            # 모든 모델이 실패한 경우 실패 횟수 리셋
            for model in self.gemini_models:
                model['failures'] = 0
            available_models = self.gemini_models
        
        # 라운드 로빈 방식으로 선택
        model = available_models[self.current_model_index % len(available_models)]
        self.current_model_index = (self.current_model_index + 1) % len(available_models)
        
        return model
    
    def extract_with_gemini(self, text_content: str, prompt_template: str) -> str:
        """
        Gemini API를 통한 정보 추출 (다중 모델 지원)
        
        Args:
            text_content: 분석할 텍스트 내용
            prompt_template: 프롬프트 템플릿 ({text_content} 플레이스홀더 포함)
            
        Returns:
            str: AI 응답 결과
        """
        if not self.gemini_models:
            return "오류: 사용 가능한 모델이 없습니다."
        
        # 모든 모델을 시도해볼 수 있도록 최대 시도 횟수 설정
        max_attempts = len(self.gemini_models)
        
        for attempt in range(max_attempts):
            current_model = self.get_next_model()
            if not current_model:
                continue
            
            try:
                # 텍스트 길이 제한 (Gemini API 제한)
                max_length = 32000
                if len(text_content) > max_length:
                    front_portion = int(max_length * 0.67)
                    back_portion = max_length - front_portion
                    text_content = text_content[:front_portion] + "\n... (중략) ...\n" + text_content[-back_portion:]
                
                # 프롬프트 생성
                prompt = prompt_template.format(text_content=text_content)
                
                # 현재 모델로 API 호출
                response = current_model['model'].generate_content(prompt)
                result_text = response.text
                
                # 성공 시 로그 출력
                self.logger.info(f"✅ {current_model['name']} API 성공 - 응답 (일부): {result_text[:200]}...")
                
                return result_text
                
            except Exception as e:
                # 실패 시 다음 모델로 시도
                current_model['failures'] += 1
                self.logger.warning(f"⚠️ {current_model['name']} API 실패 (시도 {attempt + 1}/{max_attempts}): {str(e)}")
                
                if attempt < max_attempts - 1:
                    self.logger.info(f"🔄 다음 모델로 재시도 중...")
                    continue
                else:
                    self.logger.error(f"❌ 모든 Gemini 모델 실패")
                    return f"오류: 모든 API 호출 실패 - 마지막 오류: {str(e)}"
        
        return "오류: 모든 모델 시도 실패"
    
    def call_gemini_api(self, prompt: str, max_retries: int = 3) -> Tuple[str, str]:
        """Gemini API 호출 (extract_with_gemini 래퍼)"""
        try:
            # 간단한 프롬프트 템플릿 사용
            result = self.extract_with_gemini("", prompt)
            
            if result and not result.startswith("오류"):
                # 사용된 모델 이름 반환 (간단화)
                return "SUCCESS", result
            else:
                return "FAILED", result
                
        except Exception as e:
            self.logger.error(f"❌ API 호출 오류: {e}")
            return "FAILED", str(e)
    
    def get_model_status(self) -> str:
        """모델 상태 정보 반환"""
        if not self.gemini_models:
            return "❌ 사용 가능한 모델 없음"
        
        status_info = []
        for model in self.gemini_models:
            status = "✅ 정상" if model['failures'] < 3 else "❌ 실패"
            status_info.append(f"{model['name']}: {status} (실패: {model['failures']}회)")
        
        return " | ".join(status_info)
    
    def reset_failures(self):
        """모든 모델의 실패 횟수 초기화"""
        for model in self.gemini_models:
            model['failures'] = 0
        self.logger.info("🔄 모든 모델의 실패 횟수 초기화 완료")
    
    def get_available_models_count(self) -> int:
        """사용 가능한 모델 수 반환"""
        return len([m for m in self.gemini_models if m['failures'] < 3])
    
    def is_available(self) -> bool:
        """사용 가능한 모델이 있는지 확인"""
        return self.get_available_models_count() > 0
    
    def get_statistics(self) -> Dict[str, Any]:
        """API 사용 통계 반환"""
        return {
            "total_keys": len(self.gemini_models),
            "active_keys": self.get_available_models_count(),
            "failed_keys": [m['name'] for m in self.gemini_models if m['failures'] >= 3],
            "current_model_index": self.current_model_index,
            "model_status": self.get_model_status()
        }

# ================================
# 캐시 관리 시스템
# ================================

class CacheManager:
    """실시간 캐시 관리 시스템 - JSON + Excel Queue"""
    
    def __init__(self, save_directory: str, cache_interval: int = 50, logger=None):
        """캐시 관리자 초기화"""
        self.logger = logger or logging.getLogger(__name__)
        self.save_directory = Path(save_directory)
        self.cache_interval = cache_interval
        self.cache_data = CacheData()
        
        # 캐시 파일 경로
        self.json_cache_path = self.save_directory / "cache_progress.json"
        self.excel_queue_dir = self.save_directory / "cache_excel"
        
        # 디렉토리 생성
        self.save_directory.mkdir(exist_ok=True)
        self.excel_queue_dir.mkdir(exist_ok=True)
        
        # 기존 캐시 로드
        self._load_existing_cache()
        
        self.logger.info(f"📋 캐시 관리자 초기화: {self.cache_interval}개마다 저장")
    
    def _load_existing_cache(self):
        """기존 캐시 데이터 로드"""
        try:
            if self.json_cache_path.exists():
                with open(self.json_cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cache_data = CacheData(**data)
                self.logger.info(f"📂 기존 캐시 로드: {len(self.cache_data.processed_rows)}개 행")
            else:
                self.logger.info("📂 새로운 캐시 세션 시작")
        except Exception as e:
            self.logger.warning(f"⚠️ 캐시 로드 실패, 새로 시작: {e}")
            self.cache_data = CacheData()
    
    def add_result(self, row_index: int, result: SearchResultV3):
        """검색 결과를 캐시에 추가"""
        try:
            # 결과를 딕셔너리로 변환
            result_dict = {
                "phone_number": result.phone_number,
                "fax_number": result.fax_number,
                "found_phone_institution": result.found_phone_institution,
                "found_fax_institution": result.found_fax_institution,
                "phone_success": result.phone_success,
                "fax_success": result.fax_success,
                "processing_time": result.processing_time,
                "error_message": result.error_message,
                "web_sources": result.web_sources,
                "gemini_analysis": result.gemini_analysis,
                "timestamp": datetime.now().isoformat()
            }
            
            self.cache_data.processed_rows[row_index] = result_dict
            
            # 통계 업데이트
            if not self.cache_data.statistics:
                self.cache_data.statistics = {"processed": 0, "phone_success": 0, "fax_success": 0}
            
            self.cache_data.statistics["processed"] += 1
            if result.phone_success:
                self.cache_data.statistics["phone_success"] += 1
            if result.fax_success:
                self.cache_data.statistics["fax_success"] += 1
            
            # 캐시 간격마다 저장
            if len(self.cache_data.processed_rows) % self.cache_interval == 0:
                self.save_cache()
                self.create_excel_checkpoint()
            
        except Exception as e:
            self.logger.error(f"❌ 캐시 추가 실패: {e}")
    
    def save_cache(self):
        """JSON 캐시 저장"""
        try:
            self.cache_data.timestamp = datetime.now().isoformat()
            self.cache_data.progress = {
                "total_processed": len(self.cache_data.processed_rows),
                "last_update": self.cache_data.timestamp
            }
            
            # dataclass를 dict로 변환
            cache_dict = {
                "processed_rows": self.cache_data.processed_rows,
                "progress": self.cache_data.progress,
                "statistics": self.cache_data.statistics,
                "timestamp": self.cache_data.timestamp
            }
            
            with open(self.json_cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_dict, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"💾 JSON 캐시 저장: {len(self.cache_data.processed_rows)}개 행")
            
        except Exception as e:
            self.logger.error(f"❌ JSON 캐시 저장 실패: {e}")
    
    def create_excel_checkpoint(self):
        """Excel 체크포인트 생성 (Queue 방식)"""
        try:
            processed_count = len(self.cache_data.processed_rows)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 새 Excel 파일명
            excel_filename = f"checkpoint_{processed_count:04d}_{timestamp}.xlsx"
            excel_path = self.excel_queue_dir / excel_filename
            
            # 캐시 데이터를 DataFrame으로 변환
            rows_data = []
            for row_idx, data in self.cache_data.processed_rows.items():
                row_data = {"row_index": row_idx}
                row_data.update(data)
                rows_data.append(row_data)
            
            if rows_data:
                df = pd.DataFrame(rows_data)
                df.to_excel(excel_path, index=False)
                self.logger.info(f"📊 Excel 체크포인트 생성: {excel_filename}")
                
                # 이전 체크포인트 삭제 (Queue 관리)
                self._cleanup_old_checkpoints()
            
        except Exception as e:
            self.logger.error(f"❌ Excel 체크포인트 생성 실패: {e}")
    
    def _cleanup_old_checkpoints(self):
        """이전 체크포인트 파일들 정리 (최신 2개만 유지)"""
        try:
            excel_files = list(self.excel_queue_dir.glob("checkpoint_*.xlsx"))
            if len(excel_files) > 2:
                # 파일명으로 정렬 (타임스탬프 기준)
                excel_files.sort()
                
                # 오래된 파일들 삭제
                for old_file in excel_files[:-2]:
                    old_file.unlink()
                    self.logger.info(f"🗑️ 이전 체크포인트 삭제: {old_file.name}")
        
        except Exception as e:
            self.logger.warning(f"⚠️ 체크포인트 정리 실패: {e}")
    
    def get_processed_rows(self) -> List[int]:
        """처리된 행 번호 목록 반환"""
        return list(self.cache_data.processed_rows.keys())
    
    def get_statistics(self) -> Dict[str, Any]:
        """캐시 통계 반환"""
        return {
            "processed_count": len(self.cache_data.processed_rows),
            "statistics": self.cache_data.statistics,
            "last_update": self.cache_data.timestamp,
            "cache_file": str(self.json_cache_path),
            "excel_checkpoints": len(list(self.excel_queue_dir.glob("checkpoint_*.xlsx")))
        }

# ================================
# 설정 관리자 v3
# ================================

class ConfigManagerV3:
    """고급 설정 관리자 v3"""
    
    def __init__(self):
        """설정 관리자 초기화"""
        self.config = UserConfig()
        self.performance_manager = PerformanceManager()
        
        # Desktop 경로 자동 설정
        self._setup_desktop_path()
        
    def _setup_desktop_path(self):
        """Desktop 경로 자동 감지 및 설정"""
        try:
            # 플랫폼별 Desktop 경로 감지
            if os.name == 'nt':  # Windows
                desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            else:  # Linux/Mac
                desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            
            # 경로 존재 확인
            if os.path.exists(desktop_path):
                self.config.save_directory = desktop_path
                print(f"💻 Desktop 경로 자동 설정: {desktop_path}")
            else:
                # 대체 경로 설정
                self.config.save_directory = os.path.expanduser("~")
                print(f"💻 홈 디렉토리로 설정: {self.config.save_directory}")
                
        except Exception as e:
            print(f"⚠️ Desktop 경로 설정 실패, 현재 디렉토리 사용: {e}")
            self.config.save_directory = os.getcwd()
    
    def show_execution_menu(self) -> UserConfig:
        """실행 모드 선택 메뉴"""
        print("=" * 80)
        print("🎯 Enhanced Institution Finder v3 - AI 기반 고급 기관명 추출")
        print("=" * 80)
        print("🚀 새로운 기능:")
        print("  ✨ 다중 Gemini API 키 지원 (자동 로드밸런싱)")
        print("  🔍 고급 검색 (상위 5개 웹사이트 실제 접속)")
        print("  💾 실시간 캐시 시스템 (중단 후 재시작 가능)")
        print("  📊 Desktop 자동 저장")
        print()
        
        # 시스템 정보 표시
        self._show_system_info()
        
        print("📋 실행 모드를 선택하세요:")
        print("-" * 50)
        print("1. 🧪 테스트 모드 (30개 랜덤 샘플)")
        print("2. 🔄 전체 크롤링 (모든 데이터)")
        print("3. ❓ 도움말")
        print()
        
        while True:
            try:
                choice = input("선택하세요 (1-3): ").strip()
                
                if choice == "1":
                    return self._setup_test_mode()
                elif choice == "2":
                    return self._setup_full_mode()
                elif choice == "3":
                    self._show_help()
                    continue
                else:
                    print("❌ 1-3 중에서 선택해주세요.")
                    
            except KeyboardInterrupt:
                print("\n🚫 사용자가 취소했습니다.")
                sys.exit(0)
    
    def _show_system_info(self):
        """시스템 정보 표시"""
        print("📊 시스템 정보:")
        print("-" * 30)
        
        sys_info = self.performance_manager.system_info
        print(f"💻 CPU: {sys_info.get('cpu_cores', 'N/A')}코어")
        print(f"🧠 메모리: {sys_info.get('total_memory_gb', 'N/A')}GB")
        print(f"💾 저장위치: {self.config.save_directory}")
        
        # API 키 확인 (간단한 환경변수 체크)
        api_keys_count = sum(1 for i in range(1, 5) 
                           if os.getenv('GEMINI_API_KEY' if i == 1 else f'GEMINI_API_KEY_{i}'))
        print(f"🔑 Gemini API 키: {api_keys_count}개 감지")
        print()
    
    def _setup_test_mode(self) -> UserConfig:
        """테스트 모드 설정"""
        print("\n🧪 테스트 모드 설정")
        print("-" * 40)
        
        self.config.execution_mode = "test"
        
        # 샘플 크기 설정
        while True:
            try:
                sample_input = input("테스트 샘플 수 (기본값: 30): ").strip()
                if not sample_input:
                    self.config.test_sample_size = 30
                    break
                
                sample_size = int(sample_input)
                if 10 <= sample_size <= 100:
                    self.config.test_sample_size = sample_size
                    break
                else:
                    print("❌ 샘플 수는 10-100 사이여야 합니다.")
            except ValueError:
                print("❌ 숫자를 입력해주세요.")
        
        # 기본 성능 설정 (테스트용)
        self.config.max_workers = 2
        self.config.batch_size = 10
        self.config.cache_interval = 10
        
        print(f"✅ 테스트 모드 설정 완료")
        print(f"   - 샘플: {self.config.test_sample_size}개")
        print(f"   - 워커: {self.config.max_workers}개")
        print(f"   - 캐시: {self.config.cache_interval}개마다")
        
        return self._finalize_config()
    
    def _setup_full_mode(self) -> UserConfig:
        """전체 크롤링 모드 설정"""
        print("\n🔄 전체 크롤링 모드 설정")
        print("-" * 40)
        
        self.config.execution_mode = "full"
        
        # 성능 프로필 선택 (기존 로직 활용)
        profile = self.performance_manager.get_current_profile()
        self.config.max_workers = profile.max_workers
        self.config.batch_size = profile.batch_size
        self.config.cache_interval = 50
        
        print(f"✅ 전체 모드 설정 완료")
        print(f"   - 프로필: {profile.name}")
        print(f"   - 워커: {self.config.max_workers}개")
        print(f"   - 배치: {self.config.batch_size}개")
        print(f"   - 캐시: {self.config.cache_interval}개마다")
        
        return self._finalize_config()
    
    def _finalize_config(self) -> UserConfig:
        """설정 최종 확인"""
        print("\n" + "=" * 60)
        print("📋 최종 설정 확인")
        print("=" * 60)
        print(f"🎯 실행 모드: {self.config.execution_mode}")
        if self.config.execution_mode == "test":
            print(f"🧪 테스트 샘플: {self.config.test_sample_size}개")
        print(f"🔧 워커 수: {self.config.max_workers}개")
        print(f"📦 배치 크기: {self.config.batch_size}개")
        print(f"💾 저장 위치: {self.config.save_directory}")
        print(f"📋 캐시 간격: {self.config.cache_interval}개마다")
        
        # API 키 상태 확인 (환경변수 기반)
        api_keys = []
        for i in range(1, 5):
            key = os.getenv('GEMINI_API_KEY' if i == 1 else f'GEMINI_API_KEY_{i}')
            if key:
                api_keys.append(f"GEMINI_{i}")
        
        print(f"🔑 API 키: {len(api_keys)}개 ({', '.join(api_keys)})")
        print("=" * 60)
        
        confirm = input("\n계속 진행하시겠습니까? (Y/n): ").strip().lower()
        if confirm in ['', 'y', 'yes']:
            print("✅ 설정 완료! 시스템을 시작합니다...\n")
            return self.config
        else:
            print("🔄 설정을 다시 선택합니다...\n")
            return self.show_execution_menu()
    
    def _show_help(self):
        """도움말 표시"""
        print("\n" + "=" * 60)
        print("❓ Enhanced Institution Finder v3 도움말")
        print("=" * 60)
        print("🧪 테스트 모드:")
        print("   - 전체 데이터에서 랜덤하게 선택된 샘플만 처리")
        print("   - 빠른 성능 확인 및 시스템 테스트용")
        print("   - H열, J열이 비어있는 행들 중에서 선택")
        print()
        print("🔄 전체 크롤링:")
        print("   - 모든 데이터를 처리")
        print("   - 시스템 사양에 맞는 최적화된 설정 자동 적용")
        print("   - 중간 저장 기능으로 안전한 장시간 실행")
        print()
        print("🆕 v3 새로운 기능:")
        print("   - 🔑 다중 API 키: 4개 Gemini API 키 자동 관리")
        print("   - 🔍 고급 검색: 실제 웹사이트 접속하여 정보 수집")
        print("   - 🤖 AI 분석: Gemini를 통한 기관명 정확도 향상")
        print("   - 💾 실시간 캐시: 중단 후 재시작 가능")
        print("   - 📊 Desktop 저장: 결과를 Desktop에 자동 저장")
        print()
        print("⚠️ 주의사항:")
        print("   - .env 파일에 GEMINI_API_KEY~GEMINI_API_KEY_4 설정 필요")
        print("   - 인터넷 연결 및 충분한 메모리 필요")
        print("   - 장시간 실행시 시스템 리소스 모니터링 권장")
        print("=" * 60)
        print()

# ================================
# 로깅 설정
# ================================

def setup_logging_v3():
    """로깅 시스템 설정 v3"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
    
    # 파일 핸들러
    file_handler = logging.FileHandler(f'eif3_advanced_{timestamp}.log', encoding='utf-8')
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

# ================================
# 고급 검색 엔진 v3
# ================================

class AdvancedSearchEngineV3:
    """고급 검색 엔진 v3 - 실제 웹사이트 접속 및 AI 통합"""
    
    def __init__(self, gemini_manager: GeminiAPIManager, logger=None):
        """고급 검색 엔진 초기화"""
        self.logger = logger or logging.getLogger(__name__)
        self.gemini_manager = gemini_manager
        
        # 검색 설정
        self.max_search_results = 5  # 상위 5개 결과
        self.max_retries = 3
        self.request_delay = (2.0, 4.0)  # 요청 간 지연
        
        # 텍스트 처리 설정
        self.max_tokens = 2048  # Gemini 입력 토큰 제한
        self.chunk_overlap = 100  # 청크 간 겹침
        
        # 기관명 추출 패턴 (강화)
        self.institution_patterns = [
            r'([\w\s]*(?:주민센터|행정복지센터|동주민센터)[\w\s]*)',
            r'([\w\s]*(?:구청|시청|군청|면사무소|읍사무소)[\w\s]*)',
            r'([\w\s]*(?:센터|기관|청|동|복지관|보건소|보건지소)[\w\s]*)',
            r'([\w\s]*(?:병원|의원|클리닉|한의원|치과)[\w\s]*)',
            r'([\w\s]*(?:학교|대학교|대학|학원|교육원|교육청)[\w\s]*)',
            r'([\w\s]*(?:협회|단체|재단|법인|공단|공사|회|조합)[\w\s]*)',
            r'([\w\s]*(?:교회|성당|절|사찰|종교시설)[\w\s]*)',
        ]
        
        # 제외 키워드 (확장)
        self.exclude_keywords = [
            '광고', '배너', '클릭', '링크', '바로가기', '사이트맵', '검색결과',
            '네이버', '다음', '구글', '야후', '카카오', 'COM', 'co.kr', 'www',
            'http', 'https', '.com', '.kr', '옥션원모바일', '스팸', '홍보',
            '마케팅', '업체', '쇼핑', '온라인', '인터넷', '웹사이트', '홈페이지',
            '카페', '블로그', '게시판', '댓글', '리뷰', 'review', 'blog'
        ]
        
        self.logger.info("🔍 고급 검색 엔진 v3 초기화 완료")
    
    def create_enhanced_search_queries(self, number: str, number_type: str = "전화") -> List[str]:
        """향상된 검색 쿼리 생성 (자연어 + 정확한 매칭)"""
        queries = []
        
        # 🎯 최우선 자연어 검색어 (따옴표 제거)
        if number_type == "전화":
            priority_queries = [
                f'{number} 은 어디전화번호',
                f'{number} 어디전화번호',
                f'{number} 전화번호 어디',
                f'{number} 연락처 기관',
                f'{number} 기관 전화',
            ]
        else:  # 팩스
            priority_queries = [
                f'{number} 은 어디팩스번호',
                f'{number} 어디팩스번호',
                f'{number} 팩스번호 어디',
                f'{number} 팩스 기관',
                f'{number} 기관 팩스',
            ]
        
        queries.extend(priority_queries)
        
        # 🔍 정확한 매칭 검색어 (따옴표 사용)
        if number_type == "전화":
            exact_queries = [
                f'"{number}" 전화번호 기관',
                f'"{number}" 연락처',
                f'"{number}" 기관명',
                f'전화번호 "{number}"',
            ]
        else:
            exact_queries = [
                f'"{number}" 팩스번호 기관',
                f'"{number}" fax',
                f'"{number}" 기관명',
                f'팩스번호 "{number}"',
            ]
        
        queries.extend(exact_queries)
        
        # 🏢 지역별 검색
        area_code = number.split('-')[0] if '-' in number else number[:3]
        area_names = self._get_area_names(area_code)
        
        for area in area_names[:2]:
            queries.extend([
                f'{area} {number} {number_type}번호',
                f'{number} {area} 기관',
            ])
        
        # 🏛️ 공식 사이트 우선 검색
        queries.extend([
            f'"{number}" site:go.kr',
            f'"{number}" site:or.kr',
            f'{number} 관공서',
        ])
        
        return queries[:12]  # 상위 12개만 반환
    
    def _get_area_names(self, area_code: str) -> List[str]:
        """지역번호 기반 지역명 반환"""
        area_mapping = {
            "02": ["서울", "서울특별시"],
            "031": ["경기", "경기도"],
            "032": ["인천", "인천광역시"],
            "033": ["강원", "강원도"],
            "041": ["충남", "충청남도"],
            "042": ["대전", "대전광역시"],
            "043": ["충북", "충청북도"],
            "044": ["세종", "세종특별자치시"],
            "051": ["부산", "부산광역시"],
            "052": ["울산", "울산광역시"],
            "053": ["대구", "대구광역시"],
            "054": ["경북", "경상북도"],
            "055": ["경남", "경상남도"],
            "061": ["전남", "전라남도"],
            "062": ["광주", "광주광역시"],
            "063": ["전북", "전라북도"],
            "064": ["제주", "제주특별자치도"],
        }
        return area_mapping.get(area_code, [])
    
    def search_with_advanced_ai(self, driver, number: str, number_type: str = "전화") -> SearchResultV3:
        """고급 AI 기반 검색 (스니펫 기반 + Gemini 분석)"""
        result = SearchResultV3(row_index=0)  # row_index는 외부에서 설정
        result.phone_number = number if number_type == "전화" else ""
        result.fax_number = number if number_type == "팩스" else ""
        
        try:
            self.logger.info(f"🚀 고급 AI 검색 시작: {number} ({number_type})")
            
            # 1단계: 구글 검색 및 스니펫 수집
            search_queries = self.create_enhanced_search_queries(number, number_type)
            collected_snippets = []
            
            for query in search_queries[:5]:  # 상위 5개 쿼리만
                try:
                    snippets = self._extract_search_result_snippets(driver, query)
                    collected_snippets.extend(snippets)
                    
                    # 충분한 스니펫이 수집되면 중단
                    if len(collected_snippets) >= self.max_search_results:
                        break
                        
                    # 검색 간 지연
                    time.sleep(random.uniform(*self.request_delay))
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ 쿼리 검색 실패: {query} - {e}")
                    continue
            
            # 중복 제거 및 상위 5개 선택 (URL 기준으로 중복 제거)
            unique_snippets = []
            seen_urls = set()
            for snippet in collected_snippets:
                url = snippet.get('url', '')
                if url not in seen_urls:
                    unique_snippets.append(snippet)
                    seen_urls.add(url)
                    if len(unique_snippets) >= self.max_search_results:
                        break
            
            self.logger.info(f"📝 수집된 스니펫: {len(unique_snippets)}개")
            
            # 스니펫 정보를 로그에 상세히 출력
            for i, snippet in enumerate(unique_snippets):
                self.logger.info(f"📝 스니펫 {i+1}: 제목={snippet.get('title', '')[:50]}..., 내용={snippet.get('snippet', '')[:50]}...")
            
            # 2단계: 스니펫 전처리 및 통합
            if unique_snippets:
                processed_snippets = self._preprocess_snippets(unique_snippets, number)
                
                # 3단계: Gemini AI 분석 (스니펫 기반)
                institution_candidates = []
                
                # 스니펫들을 청크로 나누어 AI 분석
                snippet_chunks = self._create_snippet_chunks(processed_snippets)
                
                for chunk_idx, snippet_chunk in enumerate(snippet_chunks):
                    try:
                        self.logger.info(f"🤖 AI 분석 {chunk_idx+1}/{len(snippet_chunks)} (스니펫 {len(snippet_chunk)}개)")
                        
                        # Gemini 프롬프트 템플릿 생성 (스니펫 전용)
                        prompt_template = self._create_snippet_gemini_prompt_template(number, number_type)
                        
                        # 스니펫 텍스트 결합
                        combined_snippet_text = self._combine_snippets_for_ai(snippet_chunk)
                        
                        # Gemini API 호출
                        ai_response = self.gemini_manager.extract_with_gemini(combined_snippet_text, prompt_template)
                        
                        if ai_response and not ai_response.startswith("오류"):
                            institution_name = self._extract_institution_from_ai_response(ai_response)
                            if institution_name:
                                institution_candidates.append({
                                    "name": institution_name,
                                    "confidence": self._calculate_confidence(institution_name, number),
                                    "source": f"스니펫_청크_{chunk_idx+1}",
                                    "api_key": "GEMINI_AI"
                                })
                        
                        # AI 호출 간 지연
                        time.sleep(random.uniform(1.0, 2.0))
                        
                    except Exception as e:
                        self.logger.warning(f"⚠️ AI 분석 실패 (청크 {chunk_idx+1}): {e}")
                        continue
                
                # 4단계: 최종 결과 선정
                if institution_candidates:
                    # 신뢰도 순으로 정렬
                    institution_candidates.sort(key=lambda x: x["confidence"], reverse=True)
                    best_candidate = institution_candidates[0]
                    
                    # 결과 저장
                    if number_type == "전화":
                        result.found_phone_institution = best_candidate["name"]
                        result.phone_success = True
                    else:
                        result.found_fax_institution = best_candidate["name"]
                        result.fax_success = True
                    
                    # Gemini 분석 결과 저장
                    result.gemini_analysis = {
                        "best_candidate": best_candidate,
                        "all_candidates": institution_candidates,
                        "total_chunks": len(snippet_chunks),
                        "successful_analyses": len([c for c in institution_candidates if c["name"]]),
                        "processed_snippets": len(processed_snippets)
                    }
                    
                    # 처리된 스니펫 정보도 저장
                    result.web_sources = [s.get('url', '') for s in unique_snippets[:5]]  # 상위 5개 URL
                    
                    self.logger.info(f"🎯 AI 분석 성공: {best_candidate['name']} (신뢰도: {best_candidate['confidence']:.2f})")
                else:
                    self.logger.warning(f"⚠️ AI 분석에서 유효한 기관명을 찾지 못함")
            else:
                self.logger.warning(f"⚠️ 스니펫 수집 실패로 AI 분석 불가")
            
            return result
            
        except Exception as e:
            result.error_message = str(e)
            self.logger.error(f"❌ 고급 AI 검색 오류: {e}")
            return result
    

    
    def _is_valid_result_link(self, url: str) -> bool:
        """유효한 검색 결과 링크인지 확인"""
        if not url or not url.startswith('http'):
            return False
        
        # 제외할 URL 패턴
        exclude_patterns = [
            'google.com', 'youtube.com', 'facebook.com', 'instagram.com',
            'twitter.com', 'tiktok.com', 'maps.google', 'translate.google',
            'images.google', 'webcache.googleusercontent.com',
            'accounts.google', 'support.google', 'policies.google'
        ]
        
        for pattern in exclude_patterns:
            if pattern in url.lower():
                return False
        
        return True
    

    

    

    

    
    def _extract_institution_from_ai_response(self, ai_response: str) -> Optional[str]:
        """AI 응답에서 기관명 추출"""
        try:
            response = ai_response.strip()
            
            # "찾을 수 없음" 등의 실패 응답 확인
            failure_keywords = ['찾을 수 없음', '없음', '확인할 수 없음', '불명', '미상', '정보 없음']
            if any(keyword in response for keyword in failure_keywords):
                return None
            
            # 기관명 키워드 포함 여부 확인
            institution_keywords = [
                '센터', '기관', '청', '구청', '시청', '군청', '동', '주민센터',
                '행정복지센터', '복지관', '보건소', '보건지소', '병원', '의원',
                '학교', '대학', '협회', '단체', '재단', '법인'
            ]
            
            if any(keyword in response for keyword in institution_keywords):
                # 첫 번째 라인만 사용 (추가 설명 제거)
                first_line = response.split('\n')[0].strip()
                
                # 불필요한 문구 제거
                cleaned = re.sub(r'^(답변:|결과:|기관명:|기관:)', '', first_line).strip()
                cleaned = re.sub(r'["]', '', cleaned).strip()
                
                if len(cleaned) >= 3 and len(cleaned) <= 50:
                    return cleaned
            
            return None
            
        except Exception as e:
            self.logger.warning(f"⚠️ AI 응답 파싱 실패: {e}")
            return None
    
    def _calculate_confidence(self, institution_name: str, number: str) -> float:
        """기관명의 신뢰도 계산"""
        try:
            confidence = 0.5  # 기본 점수
            
            # 기관명 키워드별 가중치
            keyword_weights = {
                '주민센터': 0.9, '행정복지센터': 0.9, '동주민센터': 0.9,
                '구청': 0.8, '시청': 0.8, '군청': 0.8,
                '보건소': 0.7, '복지관': 0.7, '센터': 0.6,
                '병원': 0.6, '의원': 0.5, '학교': 0.5
            }
            
            for keyword, weight in keyword_weights.items():
                if keyword in institution_name:
                    confidence = max(confidence, weight)
                    break
            
            # 길이 기반 보정
            if 5 <= len(institution_name) <= 20:
                confidence += 0.1
            elif len(institution_name) > 30:
                confidence -= 0.2
            
            # 특수문자나 숫자가 많으면 감점
            special_count = len(re.findall(r'[^\w\s가-힣]', institution_name))
            if special_count > 2:
                confidence -= 0.2
            
            return max(0.0, min(1.0, confidence))
            
        except Exception as e:
            self.logger.warning(f"⚠️ 신뢰도 계산 실패: {e}")
            return 0.5

    def _extract_search_result_snippets(self, driver, query: str) -> List[Dict]:
        """구글 검색 결과에서 스니펫 데이터 추출 (안정화된 단순 버전)"""
        try:
            self.logger.debug(f"🔍 검색 시작: {query[:50]}...")
            
            # 1단계: 기본 구글 검색 수행
            try:
                # 구글 검색 페이지로 이동
                driver.get('https://www.google.com')
                time.sleep(random.uniform(1.5, 2.5))
                
                # 검색창 찾기 (단순한 셀렉터 사용)
                search_box = None
                search_selectors = ['input[name="q"]', 'textarea[name="q"]']
                
                for selector in search_selectors:
                    try:
                        search_box = WebDriverWait(driver, 8).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        break
                    except:
                        continue
                
                if not search_box:
                    self.logger.warning(f"⚠️ 검색창을 찾을 수 없음")
                    return []
                
                # 검색어 입력 (단순화)
                search_box.clear()
                time.sleep(0.5)
                search_box.send_keys(query)
                time.sleep(0.8)
                search_box.send_keys(Keys.RETURN)
                
                # 결과 로딩 대기 (단순화)
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, 'search'))
                    )
                    time.sleep(random.uniform(2.0, 3.0))
                except:
                    self.logger.warning(f"⚠️ 검색 결과 로딩 실패")
                    return []
                
            except Exception as search_error:
                self.logger.warning(f"⚠️ 구글 검색 수행 실패: {search_error}")
                return []
            
            # 2단계: 검색 결과에서 스니펫 추출 (단순화)
            snippet_data = []
            
            try:
                # 가장 기본적인 셀렉터만 사용
                result_elements = driver.find_elements(By.CSS_SELECTOR, '.g')
                
                if not result_elements:
                    # 대체 셀렉터 시도
                    result_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-ved]')
                
                if not result_elements:
                    self.logger.warning(f"⚠️ 검색 결과 요소를 찾을 수 없음")
                    return []
                
                self.logger.debug(f"✅ 검색 결과 요소 발견: {len(result_elements)}개")
                
                # 각 결과에서 기본 정보만 추출
                for idx, element in enumerate(result_elements[:self.max_search_results]):
                    try:
                        snippet_info = {
                            'index': idx,
                            'title': '',
                            'snippet': '',
                            'text_content': '',
                            'url': ''
                        }
                        
                        # 제목 추출 (가장 기본적인 방법)
                        try:
                            title_element = element.find_element(By.CSS_SELECTOR, 'h3')
                            snippet_info['title'] = title_element.text.strip()
                        except:
                            pass
                        
                        # URL 추출
                        try:
                            url_element = element.find_element(By.CSS_SELECTOR, 'a[href*="http"]')
                            href = url_element.get_attribute('href')
                            if href and 'google.com' not in href:
                                snippet_info['url'] = href
                        except:
                            pass
                        
                        # 스니펫 텍스트 추출 (가장 기본적인 방법)
                        try:
                            # 여러 스니펫 셀렉터 시도
                            snippet_selectors = ['.VwiC3b', '.s3v9rd', '.st']
                            for selector in snippet_selectors:
                                try:
                                    snippet_element = element.find_element(By.CSS_SELECTOR, selector)
                                    snippet_info['snippet'] = snippet_element.text.strip()
                                    break
                                except:
                                    continue
                        except:
                            pass
                        
                        # 전체 텍스트
                        snippet_info['text_content'] = element.text.strip()
                        
                        # 유효한 데이터가 있으면 추가
                        if snippet_info['title'] or snippet_info['snippet'] or snippet_info['text_content']:
                            snippet_data.append(snippet_info)
                            
                    except Exception as element_error:
                        self.logger.debug(f"요소 {idx} 처리 실패: {element_error}")
                        continue
                
            except Exception as extract_error:
                self.logger.warning(f"⚠️ 스니펫 추출 오류: {extract_error}")
            
            self.logger.info(f"📋 추출된 스니펫: {len(snippet_data)}개 (쿼리: {query[:30]}...)")
            
            # 스니펫 정보 로그 (디버깅용)
            for i, snippet in enumerate(snippet_data):
                title = snippet.get('title', '')[:30]
                content = snippet.get('snippet', '')[:30] 
                self.logger.debug(f"  스니펫 {i+1}: 제목='{title}...', 내용='{content}...'")
            
            return snippet_data
            
        except Exception as e:
            self.logger.warning(f"⚠️ 스니펫 추출 전체 실패: {query} - {e}")
            return []

    def _extract_snippet_from_element(self, element, idx: int) -> Optional[Dict]:
        """개별 검색 결과 요소에서 스니펫 데이터 추출"""
        try:
            snippet_info = {
                'index': idx,
                'title': '',
                'snippet': '',
                'text_content': '',
                'url': ''
            }
            
            # 제목 추출 (여러 셀렉터 시도)
            title_selectors = [
                'h3',
                'h3 a', 
                '.LC20lb',
                '.DKV0Md',
                '[role="heading"]'
            ]
            
            for selector in title_selectors:
                try:
                    title_element = element.find_element(By.CSS_SELECTOR, selector)
                    if title_element and title_element.text.strip():
                        snippet_info['title'] = title_element.text.strip()
                        break
                except:
                    continue
            
            # URL 추출 (여러 셀렉터 시도)
            url_selectors = [
                'h3 a',
                'a[href*="http"]:not([href*="google.com"])',
                '.yuRUbf a'
            ]
            
            for selector in url_selectors:
                try:
                    url_element = element.find_element(By.CSS_SELECTOR, selector)
                    if url_element:
                        href = url_element.get_attribute('href')
                        if href and self._is_valid_result_link(href):
                            snippet_info['url'] = href
                            break
                except:
                    continue
            
            # 스니펫 텍스트 추출 (여러 셀렉터 시도)
            snippet_selectors = [
                '.VwiC3b',      # 일반적인 스니펫
                '.s3v9rd',      # 대체 스니펫
                '.st',          # 구버전 스니펫
                'span[data-ved]', # 새로운 구조
                '.lEBKkf'       # 최신 구조
            ]
            
            for selector in snippet_selectors:
                try:
                    snippet_element = element.find_element(By.CSS_SELECTOR, selector)
                    if snippet_element and snippet_element.text.strip():
                        snippet_info['snippet'] = snippet_element.text.strip()
                        break
                except:
                    continue
            
            # 전체 텍스트 내용
            snippet_info['text_content'] = element.text.strip()
            
            # 🔍 유효성 검증: 제목이나 스니펫 중 하나는 있어야 함
            if snippet_info['title'] or snippet_info['snippet']:
                return snippet_info
            
            return None
            
        except Exception as e:
            self.logger.debug(f"스니펫 데이터 추출 오류: {e}")
            return None

# ================================
# 메인 처리기 v3
# ================================

class EnhancedInstitutionProcessorV3:
    """고급 기관명 추출 프로세서 v3 (스니펫 기반 + 안정성 강화)"""
    
    def __init__(self, user_config: UserConfig):
        """메인 처리기 초기화"""
        self.logger = logging.getLogger(__name__)
        self.user_config = user_config
        
        # 기본 설정
        self.max_workers = user_config.max_workers
        self.batch_size = user_config.batch_size
        self.execution_mode = user_config.execution_mode
        
        # 관리자들 초기화
        self.gemini_manager = GeminiAPIManager(self.logger)
        self.cache_manager = CacheManager(user_config.save_directory, user_config.cache_interval, self.logger)
        self.performance_manager = PerformanceManager(self.logger)
        # ChromeDriver 자동 다운로드 및 관리
        self.chromedriver_manager = ChromeDriverManager(self.logger)
        
        # 기존 유틸리티들
        self.phone_validator = PhoneValidator(self.logger)
        self.excel_processor = ExcelProcessor(self.logger)
        self.data_mapper = DataMapper(self.logger)
        self.verification_engine = VerificationEngine()
        
        # 고급 검색 엔진
        self.search_engine = AdvancedSearchEngineV3(self.gemini_manager, self.logger)
        
        # 드라이버 관리
        self.worker_drivers = {}
        self.lock = threading.Lock()
        
        # 포트 관리 (봇 우회 핵심)
        self.used_ports = set()
        self.base_port = 9222
        
        # 중간 결과 저장을 위한 설정 (acrawl_i5.py 방식)
        self.save_interval = 10  # 10개 처리마다 중간 저장
        self.intermediate_results = []
        self.processed_count = 0
        
        # 저장 통계
        self.save_stats = {
            'total_batches': 0,
            'successful_saves': 0,
            'failed_saves': 0,
            'intermediate_saves': 0,
            'last_save_time': None
        }
        
        # 기존 통계
        self.total_rows = 0
        self.phone_success = 0
        self.fax_success = 0
        self.ai_analysis_count = 0
        
        self.logger.info(f"🚀 고급 AI 기반 처리기 v3 초기화 완료")
        self.logger.info(f"⚙️  설정: {self.execution_mode} 모드, 워커 {self.max_workers}개")

    def save_intermediate_results(self, results: List[Dict], batch_idx: int, force_save: bool = False) -> bool:
        """
        중간 결과 저장 (acrawl_i5.py 방식)
        
        Args:
            results: 저장할 결과 리스트
            batch_idx: 배치 인덱스
            force_save: 강제 저장 여부
            
        Returns:
            bool: 저장 성공 여부
        """
        try:
            # 저장 조건 확인
            should_save = (
                force_save or 
                len(results) >= self.save_interval or
                batch_idx % 5 == 0  # 5배치마다 저장
            )
            
            if not should_save:
                return True
                
            if not results:
                return True
                
            self.logger.info(f"💾 중간 결과 저장 시작: {len(results)}개 (배치 {batch_idx})")
            
            # 타임스탬프와 파일명 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            filename = os.path.join(desktop_path, 
                f"eif3_중간결과_배치{batch_idx:03d}_{timestamp}.xlsx")
            
            # DataFrame 생성 및 저장 (acrawl_i5.py 방식)
            df_result = pd.DataFrame(results)
            
            # 최신 pandas 버전 호환성을 위해 ExcelWriter 사용
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False)
                
            self.save_stats['intermediate_saves'] += 1
            self.save_stats['successful_saves'] += 1
            self.save_stats['last_save_time'] = datetime.now()
            
            self.logger.info(f"✅ 중간 결과 저장 완료: {filename}")
            self.logger.info(f"📊 저장된 데이터: {len(results)}행 × {len(df_result.columns)}열")
            
            return True
            
        except Exception as e:
            self.save_stats['failed_saves'] += 1
            self.logger.error(f"❌ 중간 결과 저장 실패: {e}")
            return False

    def save_final_results(self, all_results: List[Dict], mode: str = "full") -> str:
        """
        최종 결과 저장 (acrawl_i5.py 방식)
        
        Args:
            all_results: 전체 결과 리스트
            mode: 처리 모드
            
        Returns:
            str: 저장된 파일 경로
        """
        try:
            if not all_results:
                self.logger.warning("⚠️ 저장할 결과가 없습니다")
                return ""
                
            self.logger.info(f"💾 최종 결과 저장 시작: {len(all_results)}개")
            
            # 타임스탬프와 파일명 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            filename = os.path.join(desktop_path, 
                f"eif3_최종결과_{mode}모드_{timestamp}.xlsx")
            
            # DataFrame 생성
            df_result = pd.DataFrame(all_results)
            
            # 컬럼 순서 정리 (중요한 컬럼을 앞쪽에)
            priority_columns = [
                'row_index', 'phone_number', 'fax_number', 
                'found_phone_institution', 'found_fax_institution',
                'phone_success', 'fax_success',
                'processing_time', 'worker_id'
            ]
            
            # 존재하는 컬럼만 우선순위에 포함
            existing_priority = [col for col in priority_columns if col in df_result.columns]
            other_columns = [col for col in df_result.columns if col not in existing_priority]
            final_column_order = existing_priority + other_columns
            
            df_result = df_result[final_column_order]
            
            # Excel 저장 (acrawl_i5.py 방식과 동일)
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False)
                
            self.save_stats['successful_saves'] += 1
            self.save_stats['last_save_time'] = datetime.now()
            
            # 저장 통계 로그
            success_count = len([r for r in all_results if r.get('phone_success') or r.get('fax_success')])
            success_rate = (success_count / len(all_results)) * 100 if all_results else 0
            
            self.logger.info(f"✅ 최종 결과 저장 완료: {filename}")
            self.logger.info(f"📊 저장 통계:")
            self.logger.info(f"   - 전체 데이터: {len(all_results):,}개")
            self.logger.info(f"   - 성공 추출: {success_count:,}개 ({success_rate:.1f}%)")
            self.logger.info(f"   - 컬럼 수: {len(df_result.columns)}개")
            self.logger.info(f"   - 중간 저장: {self.save_stats['intermediate_saves']}회")
            
            return filename
            
        except Exception as e:
            self.save_stats['failed_saves'] += 1
            self.logger.error(f"❌ 최종 결과 저장 실패: {e}")
            return ""

    def save_error_recovery_data(self, partial_results: List[Dict], error_info: str) -> str:
        """
        오류 복구용 부분 결과 저장
        
        Args:
            partial_results: 부분 결과 리스트
            error_info: 오류 정보
            
        Returns:
            str: 저장된 파일 경로
        """
        try:
            if not partial_results:
                return ""
                
            self.logger.info(f"🛡️ 오류 복구 데이터 저장: {len(partial_results)}개")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            filename = os.path.join(desktop_path, 
                f"eif3_복구데이터_{timestamp}.xlsx")
            
            # 오류 정보 추가
            for result in partial_results:
                result['recovery_info'] = f"오류 발생: {error_info}"
                result['recovery_time'] = timestamp
                
            df_result = pd.DataFrame(partial_results)
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False)
                
            self.logger.info(f"✅ 복구 데이터 저장 완료: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"❌ 복구 데이터 저장 실패: {e}")
            return ""

    def get_save_statistics(self) -> Dict:
        """저장 통계 반환"""
        stats = self.save_stats.copy()
        stats['save_success_rate'] = (
            stats['successful_saves'] / max(stats['successful_saves'] + stats['failed_saves'], 1) * 100
        )
        return stats

    def load_and_prepare_data(self, filepath: str) -> pd.DataFrame:
        """데이터 로드 및 전처리"""
        try:
            # Excel 파일 로드
            success = self.excel_processor.load_excel_file(filepath)
            if not success:
                raise ValueError(f"파일 로드 실패: {filepath}")
            
            df = self.excel_processor.df
            self.logger.info(f"📊 데이터 로드 완료: {len(df)}행 × {len(df.columns)}열")
            
            # 실행 모드에 따른 데이터 준비
            if self.execution_mode == "test":
                df = self._create_test_sample(df)
                self.logger.info(f"🧪 테스트 샘플 생성: {len(df)}행")
            
            # 배치 크기 자동 계산
            if isinstance(self.batch_size, str) and self.batch_size == "auto":
                self.batch_size = self._calculate_optimal_batch_size(len(df))
                self.logger.info(f"📦 배치 크기 자동 계산: {self.batch_size}개")
            
            self.total_rows = len(df)
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def _create_test_sample(self, df: pd.DataFrame) -> pd.DataFrame:
        """테스트용 랜덤 샘플 생성 (조건부)"""
        try:
            # H열(전화번호 기관명)과 J열(팩스번호 기관명)이 비어있는 행들 필터링
            if len(df.columns) >= 10:
                # H열(인덱스 7)과 J열(인덱스 9)이 비어있는 조건
                phone_empty = df.iloc[:, 7].isna() | (df.iloc[:, 7] == '') | (df.iloc[:, 7] == 'nan')
                fax_empty = df.iloc[:, 9].isna() | (df.iloc[:, 9] == '') | (df.iloc[:, 9] == 'nan')
                
                # 둘 중 하나라도 비어있는 행들
                empty_rows = phone_empty | fax_empty
                candidate_df = df[empty_rows]
                
                self.logger.info(f"🔍 처리 대상 후보: {len(candidate_df)}행 (전체 {len(df)}행 중)")
                
                if len(candidate_df) == 0:
                    self.logger.warning("⚠️ 처리할 빈 행이 없습니다. 전체 데이터에서 샘플링")
                    candidate_df = df
            else:
                self.logger.warning("⚠️ 컬럼 수가 부족합니다. 전체 데이터에서 샘플링")
                candidate_df = df
            
            # 샘플 크기 조정
            sample_size = min(self.user_config.test_sample_size, len(candidate_df))
            
            # 랜덤 샘플링
            if len(candidate_df) > sample_size:
                sampled_df = candidate_df.sample(n=sample_size, random_state=42)
                sampled_df = sampled_df.sort_index()  # 원래 순서 유지
            else:
                sampled_df = candidate_df
            
            self.logger.info(f"✅ 테스트 샘플 선정: {len(sampled_df)}행")
            return sampled_df
            
        except Exception as e:
            self.logger.error(f"❌ 테스트 샘플 생성 실패: {e}")
            # 기본 샘플링
            sample_size = min(self.user_config.test_sample_size, len(df))
            return df.sample(n=sample_size, random_state=42).sort_index()
    
    def _calculate_optimal_batch_size(self, total_rows: int) -> int:
        """최적 배치 크기 계산"""
        try:
            # 기본 공식: 총 데이터 수 / 워커 수
            calculated_size = max(1, total_rows // self.max_workers)
            
            # 실행 모드별 조정
            if self.execution_mode == "test":
                # 테스트 모드: 작은 배치 (빠른 확인)
                optimal_size = min(calculated_size, 10)
            else:
                # 전체 모드: 데이터 크기에 따른 최적화
                if total_rows < 100:
                    optimal_size = min(calculated_size, 20)
                elif total_rows < 1000:
                    optimal_size = min(max(calculated_size, 30), 100)
                else:
                    optimal_size = min(max(calculated_size, 50), 200)
            
            # 최종 제한 적용
            final_size = max(5, min(optimal_size, 500))
            
            self.logger.info(f"📊 배치 크기 계산: {total_rows}행 ÷ {self.max_workers}워커 = {calculated_size} → 최적화: {final_size}")
            return final_size
            
        except Exception as e:
            self.logger.warning(f"⚠️ 배치 크기 계산 실패, 기본값 사용: {e}")
            return 20 if self.execution_mode == "test" else 50
    
    def _create_selenium_driver(self, worker_id: int) -> Optional[object]:
        """Chrome 138 호환성 강화 - web_driver_manager.py 기반 봇 우회 드라이버"""
        try:
            # 워커 간 시차 두기 (봇 감지 회피 + 파일 충돌 방지)
            startup_delay = random.uniform(0.5, 2.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            self.logger.info(f"🚀 워커 {worker_id}: Chrome 138 호환 드라이버 생성 시작 (지연: {startup_delay:.1f}초)")
            
            # undetected_chromedriver 캐시 정리 (Chrome 138 호환성 개선)
            self._cleanup_uc_cache(worker_id)
            
            # undetected_chromedriver 옵션 설정 (web_driver_manager.py 완전 적용)
            chrome_options = uc.ChromeOptions()
            
            # 🛡️ 기본 봇 우회 옵션 (Chrome 138 검증됨)
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
            
            # 🔧 Chrome 138 호환성 및 안정성 (검증됨)
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
            
            # 💾 메모리 최적화 (web_driver_manager.py 검증됨)
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=256')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
            chrome_options.add_argument('--disable-background-mode')
            
            # 🌐 포트 분배 (봇 우회 핵심) - web_driver_manager.py 방식
            debug_port = self._get_available_port(worker_id)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # 🎭 User-Agent 랜덤화 (봇 감지 회피) - Chrome 138 호환성
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            ]
            chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # 🔐 Chrome 138 호환성 주의: excludeSwitches 옵션 조건부 적용
            try:
                # Chrome 138에서 문제가 될 수 있으므로 안전하게 적용
                chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                chrome_options.add_experimental_option('useAutomationExtension', False)
            except Exception as exp_error:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 실험적 옵션 적용 실패 (Chrome 138 호환성): {exp_error}")
            
            # 📁 프로필 디렉토리 분리 (워커별) - web_driver_manager.py 방식
            profile_dir = tempfile.mkdtemp(prefix=f'eif3_uc_worker_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            
            self.logger.info(f"🔧 워커 {worker_id}: UC 옵션 설정 완료 (포트: {debug_port})")
            
            # 🎯 Chrome 138 호환성 핵심: version_main=None + undetected_chromedriver
            try:
                driver = uc.Chrome(
                    options=chrome_options, 
                    version_main=None,  # 🔑 Chrome 138 자동 호환성 핵심!
                    driver_executable_path=None  # UC가 자동으로 관리
                )
            except Exception as uc_error:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 1차 UC 드라이버 생성 실패, 재시도: {uc_error}")
                # 재시도 with 다른 설정 (web_driver_manager.py 방식)
                time.sleep(random.uniform(1.0, 3.0))
                driver = uc.Chrome(options=chrome_options, version_main=None)
            
            # 타임아웃 설정 (web_driver_manager.py 기반)
            driver.implicitly_wait(8)
            driver.set_page_load_timeout(15)
            driver.set_script_timeout(10)
            
            # 🛡️ 웹드라이버 감지 방지 스크립트 (Chrome 138 호환성 강화)
            try:
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko']})")
                driver.execute_script("Object.defineProperty(navigator, 'permissions', {get: () => undefined})")
            except Exception as script_error:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 감지 방지 스크립트 실패 (무시): {script_error}")
            
            # 드라이버 검증 및 봇 감지 처리
            try:
                driver.get("https://www.google.com")
                time.sleep(random.uniform(3.0, 5.0))
                
                # 봇 감지 화면 확인 및 수동 처리
                if self._check_and_handle_bot_detection(driver, worker_id):
                    self.logger.info(f"🤖 워커 {worker_id}: 봇 감지 화면 수동 처리 완료")
                
                # Google 페이지 검증
                current_title = driver.title.lower()
                if "google" in current_title or "구글" in current_title:
                    self.logger.info(f"✅ 워커 {worker_id}: Chrome 138 호환 드라이버 생성 완료 (포트: {debug_port})")
                    return driver
                else:
                    self.logger.warning(f"⚠️ 워커 {worker_id}: 예상과 다른 페이지: {driver.title}")
                    # 봇 감지 가능성 재확인
                    if self._check_and_handle_bot_detection(driver, worker_id):
                        return driver
                    else:
                        raise Exception("Google 페이지 로딩 실패 - fallback 시도 필요")
                        
            except Exception as test_error:
                self.logger.warning(f"⚠️ 워커 {worker_id}: UC 드라이버 테스트 실패: {test_error}")
                try:
                    driver.quit()
                except:
                    pass
                
                # 🔄 Fallback 전략 시도 (web_driver_manager.py 완전 적용)
                return self._create_fallback_driver(worker_id)
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: Chrome 138 호환 드라이버 생성 실패 - {e}")
            # 🔄 Fallback 전략 시도
            return self._create_fallback_driver(worker_id)
    
    def _get_available_port(self, worker_id: int = 0) -> int:
        """사용 가능한 포트 번호 생성 (web_driver_manager.py 스타일)"""
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
    
    def _cleanup_uc_cache(self, worker_id: int):
        """undetected_chromedriver 캐시 정리 (Chrome 138 호환성 개선) - web_driver_manager.py 완전 적용"""
        try:
            # undetected_chromedriver 캐시 디렉토리 (web_driver_manager.py 방식)
            uc_cache_dirs = [
                os.path.expanduser("~/.undetected_chromedriver"),
                os.path.join(os.path.expanduser("~"), "AppData", "Roaming", "undetected_chromedriver"),
                os.path.join(os.path.expanduser("~"), "appdata", "roaming", "undetected_chromedriver"),
                # 추가 캐시 경로
                os.path.join(tempfile.gettempdir(), "undetected_chromedriver"),
                os.path.join(os.getcwd(), ".undetected_chromedriver")
            ]
            
            for cache_dir in uc_cache_dirs:
                if os.path.exists(cache_dir):
                    try:
                        # 워커별로 다른 시간에 정리 (충돌 방지) - web_driver_manager.py 방식
                        if worker_id % 3 == 0:  # 3개 워커마다 1번씩만
                            self.logger.debug(f"🧹 워커 {worker_id}: UC 캐시 정리 - {cache_dir}")
                            
                            # 특정 파일들만 안전하게 삭제 (web_driver_manager.py 검증됨)
                            for item in os.listdir(cache_dir):
                                item_path = os.path.join(cache_dir, item)
                                # 안전한 파일만 삭제
                                if item.endswith(('.exe', '.tmp', '.lock', '.log')):
                                    try:
                                        if os.path.isfile(item_path):
                                            os.remove(item_path)
                                            self.logger.debug(f"🗑️ UC 캐시 파일 삭제: {item}")
                                    except Exception as file_error:
                                        self.logger.debug(f"UC 캐시 파일 삭제 실패 (무시): {file_error}")
                                        pass
                                # 빈 디렉토리 정리
                                elif os.path.isdir(item_path) and not os.listdir(item_path):
                                    try:
                                        os.rmdir(item_path)
                                        self.logger.debug(f"🗑️ UC 빈 디렉토리 삭제: {item}")
                                    except Exception as dir_error:
                                        self.logger.debug(f"UC 디렉토리 삭제 실패 (무시): {dir_error}")
                                        pass
                                        
                    except Exception as cleanup_error:
                        self.logger.debug(f"UC 캐시 정리 실패 (무시): {cleanup_error}")
                        pass
        
        except Exception as e:
            self.logger.debug(f"UC 캐시 정리 과정 오류 (무시): {e}")
            # 캐시 정리 실패는 치명적이지 않으므로 무시
    
    def _create_fallback_driver(self, worker_id: int = 0):
        """안전한 fallback 드라이버 생성 (web_driver_manager.py 완전 적용)"""
        try:
            self.logger.warning(f"🔄 워커 {worker_id} fallback 드라이버 생성 시도 - web_driver_manager.py 방식")
            
            # 재시도 간 대기 (web_driver_manager.py 검증됨)
            time.sleep(random.uniform(3.0, 6.0))
            
            # web_driver_manager.py의 3단계 전략 시도
            strategies = [
                self._try_minimal_chrome,
                self._try_headless_chrome, 
                self._try_basic_chrome
            ]
            
            for strategy_idx, strategy in enumerate(strategies):
                try:
                    self.logger.info(f"🔧 워커 {worker_id} 전략 {strategy_idx + 1} 시도: {strategy.__name__}")
                    driver = strategy(worker_id)
                    if driver:
                        # 드라이버 검증 (web_driver_manager.py 방식)
                        try:
                            driver.get("https://www.google.com")
                            time.sleep(random.uniform(2.0, 4.0))
                            
                            current_title = driver.title.lower()
                            if "google" in current_title or "구글" in current_title:
                                self.logger.info(f"✅ 워커 {worker_id} 전략 {strategy_idx + 1} 성공 및 검증 완료")
                                return driver
                            else:
                                self.logger.warning(f"⚠️ 워커 {worker_id} 전략 {strategy_idx + 1} 검증 실패")
                                driver.quit()
                                continue
                        except Exception as verify_error:
                            self.logger.warning(f"⚠️ 워커 {worker_id} 전략 {strategy_idx + 1} 검증 오류: {verify_error}")
                            try:
                                driver.quit()
                            except:
                                pass
                            continue
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 전략 {strategy_idx + 1} 실패: {e}")
                    
                    # 전략 간 대기 (web_driver_manager.py 방식)
                    if strategy_idx < len(strategies) - 1:
                        wait_time = random.uniform(2.0, 4.0)
                        self.logger.info(f"⏱️ 워커 {worker_id}: 다음 전략까지 {wait_time:.1f}초 대기")
                        time.sleep(wait_time)
                    continue
            
            self.logger.error(f"❌ 워커 {worker_id} 모든 fallback 전략 실패")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id} fallback 드라이버 생성 실패: {e}")
            return None
    
    def _try_minimal_chrome(self, worker_id: int):
        """최소 옵션 Chrome 시도 (Chrome 138 호환성 우선) - web_driver_manager.py 완전 적용"""
        try:
            chrome_options = uc.ChromeOptions()
            
            # 절대 최소 옵션 (Chrome 138 검증됨) - web_driver_manager.py 방식
            minimal_options = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1366,768',
                '--disable-logging',
                '--log-level=3',
                '--disable-extensions'
            ]
            
            for option in minimal_options:
                chrome_options.add_argument(option)
            
            # 안전한 포트 (web_driver_manager.py 방식)
            port = 9222 + worker_id + 15000
            chrome_options.add_argument(f'--remote-debugging-port={port}')
            
            # Chrome 138 핵심: version_main=None (web_driver_manager.py 검증됨)
            driver = uc.Chrome(options=chrome_options, version_main=None)
            driver.implicitly_wait(15)
            driver.set_page_load_timeout(30)
            
            self.logger.info(f"✅ 워커 {worker_id}: 최소 옵션 Chrome 전략 성공")
            return driver
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: 최소 옵션 Chrome 전략 실패: {e}")
            raise
    
    def _try_headless_chrome(self, worker_id: int):
        """헤드리스 Chrome 시도 (Chrome 138 호환성) - web_driver_manager.py 완전 적용"""
        try:
            chrome_options = uc.ChromeOptions()
            
            # 헤드리스 모드로 더 안전하게 (web_driver_manager.py 검증됨)
            headless_options = [
                '--headless',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1366,768',
                '--disable-logging',
                '--log-level=3'
            ]
            
            for option in headless_options:
                chrome_options.add_argument(option)
            
            # 안전한 포트 (web_driver_manager.py 방식)
            port = 9222 + worker_id + 20000
            chrome_options.add_argument(f'--remote-debugging-port={port}')
            
            # Chrome 138 핵심: version_main=None (web_driver_manager.py 검증됨)
            driver = uc.Chrome(options=chrome_options, version_main=None)
            driver.implicitly_wait(20)
            driver.set_page_load_timeout(40)
            
            self.logger.info(f"✅ 워커 {worker_id}: 헤드리스 Chrome 전략 성공")
            return driver
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: 헤드리스 Chrome 전략 실패: {e}")
            raise
    
    def _try_basic_chrome(self, worker_id: int):
        """기본 Chrome 시도 (최후의 수단, Chrome 138 호환성) - web_driver_manager.py 완전 적용"""
        try:
            chrome_options = uc.ChromeOptions()
            
            # 기본 설정만 (web_driver_manager.py 검증됨)
            basic_options = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--window-size=800,600'
            ]
            
            for option in basic_options:
                chrome_options.add_argument(option)
            
            # 안전한 포트 (web_driver_manager.py 방식)
            port = 9222 + worker_id + 25000  
            chrome_options.add_argument(f'--remote-debugging-port={port}')
            
            # 실험적 옵션 없이, Chrome 138 핵심: version_main=None (web_driver_manager.py 검증됨)
            driver = uc.Chrome(options=chrome_options, version_main=None)
            driver.implicitly_wait(30)
            driver.set_page_load_timeout(60)
            
            self.logger.info(f"✅ 워커 {worker_id}: 기본 Chrome 전략 성공")
            return driver
            
        except Exception as e:
            self.logger.warning(f"⚠️ 워커 {worker_id}: 기본 Chrome 전략 실패: {e}")
            raise
    
    def _check_and_handle_bot_detection(self, driver, worker_id: int) -> bool:
        """봇 감지 화면 확인 및 수동 처리 대기 (Chrome 138 호환성 강화)"""
        try:
            # 봇 감지 관련 요소들 확인 (Chrome 138 확장 패턴)
            bot_detection_selectors = [
                # Google 봇 감지 화면 (Chrome 138 업데이트)
                "div[data-google-abuse-prevention-form]",
                "div[data-g-captcha]", 
                "#captcha-form",
                ".g-recaptcha",
                "iframe[src*='recaptcha']",
                "div[id*='captcha']",
                "div[class*='captcha']",
                "div[class*='challenge']",
                "div[id*='challenge']",
                "form[action*='sorry']",
                "div[class*='sorry']",
                
                # 일반적인 봇 감지 문구 (XPath - Chrome 138 호환성)
                "//div[contains(text(), '로봇이 아님을 확인')]",
                "//div[contains(text(), 'unusual traffic')]",
                "//div[contains(text(), 'automated requests')]",
                "//div[contains(text(), 'verify that you')]",
                "//span[contains(text(), '로봇이 아닙니다')]",
                "//h1[contains(text(), 'Before you continue')]",
                "//div[contains(text(), 'not a robot')]",
                "//div[contains(text(), 'prove you are human')]",
                "//div[contains(text(), 'security check')]",
                "//div[contains(text(), '보안 검사')]",
                
                # Chrome 138 새로운 패턴
                "div[jsname*='captcha']",
                "div[data-ved*='captcha']",
                ".VfPpkd-dgl2Hf-ppHlrf-sM5MNb",  # Google Material Design 봇 감지
                "div[role='dialog'][aria-label*='captcha']"
            ]
            
            # 봇 감지 화면 체크 (Chrome 138 안전성 강화)
            bot_detected = False
            detected_element = None
            
            for selector in bot_detection_selectors:
                try:
                    if selector.startswith("//"):
                        # XPath 셀렉터 (Chrome 138 호환성)
                        try:
                            elements = driver.find_elements(By.XPATH, selector)
                        except Exception as xpath_error:
                            self.logger.debug(f"XPath 실행 실패 (무시): {xpath_error}")
                            continue
                    else:
                        # CSS 셀렉터 (Chrome 138 호환성)
                        try:
                            elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        except Exception as css_error:
                            self.logger.debug(f"CSS 셀렉터 실행 실패 (무시): {css_error}")
                            continue
                    
                    if elements and len(elements) > 0:
                        # 요소가 실제로 보이는지 확인 (Chrome 138 안정성)
                        try:
                            visible_elements = [e for e in elements if e.is_displayed()]
                            if visible_elements:
                                bot_detected = True
                                detected_element = selector
                                break
                        except Exception as visibility_error:
                            self.logger.debug(f"가시성 확인 실패 (무시): {visibility_error}")
                            # 보이지 않더라도 요소가 있으면 감지로 처리
                            bot_detected = True
                            detected_element = selector
                            break
                        
                except Exception as selector_error:
                    self.logger.debug(f"셀렉터 {selector} 처리 실패 (무시): {selector_error}")
                    continue
            
            # 페이지 제목으로도 확인 (Chrome 138 확장)
            try:
                current_title = driver.title.lower()
                title_keywords = [
                    "captcha", "robot", "verification", "보안", "확인", 
                    "unusual traffic", "automated", "sorry", "challenge",
                    "prove you", "human", "security check", "봇 감지"
                ]
                
                if any(keyword in current_title for keyword in title_keywords):
                    bot_detected = True
                    detected_element = f"title: {driver.title}"
                    
            except Exception as title_error:
                self.logger.debug(f"제목 확인 실패 (무시): {title_error}")
            
            # URL 패턴으로도 확인 (Chrome 138 확장)
            try:
                current_url = driver.current_url.lower()
                url_keywords = [
                    "sorry.google", "captcha", "recaptcha", "challenge",
                    "verify", "unusual", "automated", "security"
                ]
                
                if any(keyword in current_url for keyword in url_keywords):
                    bot_detected = True
                    detected_element = f"url: {driver.current_url}"
                    
            except Exception as url_error:
                self.logger.debug(f"URL 확인 실패 (무시): {url_error}")
            
            # 페이지 소스 검사 (Chrome 138 추가 안전장치)
            if not bot_detected:
                try:
                    page_source = driver.page_source.lower()
                    source_keywords = [
                        "g-recaptcha", "captcha", "robot", "unusual traffic",
                        "automated requests", "verify that you", "prove you",
                        "security check", "challenge", "sorry.google"
                    ]
                    
                    if any(keyword in page_source for keyword in source_keywords):
                        bot_detected = True
                        detected_element = "page_source_analysis"
                        
                except Exception as source_error:
                    self.logger.debug(f"페이지 소스 확인 실패 (무시): {source_error}")
            
            if bot_detected:
                self.logger.warning(f"🤖 워커 {worker_id}: 봇 감지 화면 발견 - {detected_element}")
                
                # 현재 상태 정보 수집 (Chrome 138 디버깅)
                try:
                    current_url = driver.current_url
                    current_title = driver.title
                    self.logger.warning(f"📍 현재 URL: {current_url}")
                    self.logger.warning(f"📄 현재 제목: {current_title}")
                except Exception as info_error:
                    self.logger.debug(f"상태 정보 수집 실패: {info_error}")
                
                # 사용자에게 수동 처리 요청 (Chrome 138 개선된 안내)
                print(f"\n" + "="*80)
                print(f"🤖 Chrome 138 봇 감지 화면 발견! (워커 {worker_id})")
                print(f"📍 URL: {getattr(driver, 'current_url', 'N/A')}")
                print(f"🔍 감지된 요소: {detected_element}")
                print("="*80)
                print("👤 수동 작업 필요:")
                print("   1. 브라우저 창에서 로봇 검증을 완료하세요")
                print("   2. CAPTCHA나 '로봇이 아닙니다' 확인을 통과하세요")
                print("   3. reCAPTCHA 이미지 선택이나 체크박스를 클릭하세요")
                print("   4. Google 메인 페이지가 정상적으로 나타나면")
                print("   5. 이 창으로 돌아와서 Enter 키를 누르세요")
                print("="*80)
                print("⚠️ Chrome 138 환경에서는 봇 감지가 더 엄격할 수 있습니다.")
                print("   시간을 두고 천천히 진행해주세요.")
                print("="*80)
                
                # 사용자 입력 대기 (Chrome 138 안정성)
                try:
                    input("👆 로봇 검증 완료 후 Enter 키를 누르세요...")
                    
                    # 검증 완료 후 페이지 재확인 (Chrome 138 검증)
                    time.sleep(3)  # 페이지 안정화 대기
                    
                    # 다시 봇 감지 확인 (재귀 호출 방지를 위한 간단 확인)
                    try:
                        verification_passed = True
                        
                        # 간단한 봇 감지 재확인
                        simple_selectors = ["div[data-g-captcha]", ".g-recaptcha", "#captcha-form"]
                        for simple_selector in simple_selectors:
                            try:
                                still_detected = driver.find_elements(By.CSS_SELECTOR, simple_selector)
                                if still_detected:
                                    verification_passed = False
                                    break
                            except:
                                continue
                        
                        # Google 페이지 확인
                        current_title = driver.title.lower()
                        current_url = driver.current_url.lower()
                        
                        if verification_passed and ("google" in current_title or "google" in current_url):
                            self.logger.info(f"✅ 워커 {worker_id}: 봇 검증 수동 처리 성공")
                            print(f"✅ 봇 검증 완료! (워커 {worker_id})")
                            return True
                        else:
                            self.logger.warning(f"⚠️ 워커 {worker_id}: 아직 봇 검증이 완료되지 않은 것 같습니다")
                            print(f"⚠️ 아직 봇 검증이 완료되지 않았습니다.")
                            print(f"   현재 제목: {getattr(driver, 'title', 'N/A')}")
                            print(f"   현재 URL: {getattr(driver, 'current_url', 'N/A')}")
                            
                            # 한 번 더 기회 제공
                            retry = input("다시 시도하시겠습니까? (y/n): ").strip().lower()
                            if retry == 'y':
                                return self._check_and_handle_bot_detection(driver, worker_id)
                        
                    except Exception as verification_error:
                        self.logger.warning(f"⚠️ 워커 {worker_id}: 검증 상태 확인 실패: {verification_error}")
                        
                except KeyboardInterrupt:
                    self.logger.warning(f"⚠️ 워커 {worker_id}: 사용자가 봇 검증을 취소했습니다")
                    print(f"\n⚠️ 봇 검증이 취소되었습니다. (워커 {worker_id})")
                    return False
                except Exception as input_error:
                    self.logger.warning(f"⚠️ 워커 {worker_id}: 사용자 입력 처리 실패: {input_error}")
                    return False
                
            return not bot_detected  # 봇 감지되지 않으면 True 반환
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 봇 감지 확인 중 오류 - {e}")
            # 오류 발생시 안전하게 통과로 처리
            return True
    
    def _get_worker_driver(self, worker_id: int):
        """워커별 드라이버 가져오기 (개선된 버전)"""
        # 기존 드라이버 상태 확인
        if worker_id in self.worker_drivers:
            try:
                driver = self.worker_drivers[worker_id]
                # 더 안전한 상태 확인
                current_url = driver.current_url
                if current_url and "data:" not in current_url:  # 정상 상태 확인
                    self.logger.debug(f"🔄 워커 {worker_id}: 기존 드라이버 재사용")
                    return driver
                else:
                    raise Exception("드라이버 상태 비정상")
            except Exception as e:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 기존 드라이버 비정상, 교체 필요: {e}")
                # 비정상 드라이버 안전하게 제거
                try:
                    self.worker_drivers[worker_id].quit()
                except:
                    pass
                del self.worker_drivers[worker_id]
        
        # 새 드라이버 생성 (재시도 로직 개선)
        max_attempts = 5  # 재시도 횟수 증가
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"🔧 워커 {worker_id}: 새 드라이버 생성 시도 {attempt + 1}/{max_attempts}")
                
                # 시도 간 대기 시간 증가
                if attempt > 0:
                    wait_time = min(10, (attempt * 3) + random.uniform(1, 3))
                    self.logger.info(f"⏱️ 워커 {worker_id}: {wait_time:.1f}초 대기 후 재시도")
                    time.sleep(wait_time)
                
                # 드라이버 생성 (Selenium WebDriver 사용)
                driver = self._create_selenium_driver(worker_id)
                if driver:
                    with self.lock:  # 스레드 안전성
                        self.worker_drivers[worker_id] = driver
                    self.logger.info(f"✅ 워커 {worker_id}: 새 드라이버 생성 성공")
                    return driver
                else:
                    raise Exception("드라이버 생성 반환값 None")
                    
            except Exception as e:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 드라이버 생성 실패 시도 {attempt+1}/{max_attempts}: {e}")
                
                # 마지막 시도가 아니면 계속
                if attempt < max_attempts - 1:
                    continue
                else:
                    self.logger.error(f"❌ 워커 {worker_id}: 모든 드라이버 생성 시도 실패")
        
        return None
    
    def process_single_row(self, row_data: Tuple[int, pd.Series], worker_id: int) -> SearchResultV3:
        """개별 행 처리 (고급 AI 검색)"""
        row_idx, row = row_data
        result = SearchResultV3(row_index=row_idx)
        start_time = time.time()
        
        try:
            # 데이터 추출
            phone_number = str(row.iloc[6]).strip() if len(row) > 6 else ""
            fax_number = str(row.iloc[8]).strip() if len(row) > 8 else ""
            
            # 기존 결과 확인
            existing_phone_result = str(row.iloc[7]).strip() if len(row) > 7 else ""
            existing_fax_result = str(row.iloc[9]).strip() if len(row) > 9 else ""
            
            result.phone_number = phone_number
            result.fax_number = fax_number
            
            self.logger.info(f"🔄 워커 {worker_id}: 행 {row_idx+1} 처리 시작")
            
            # 드라이버 가져오기 (대체 방안 포함)
            driver = self._get_worker_driver(worker_id)
            if not driver:
                self.logger.warning(f"⚠️ 워커 {worker_id}: 드라이버 없음, 대체 방법 시도")
                # 드라이버 없이 기본 검증만 수행
                result = self._process_without_driver(result, phone_number, fax_number, existing_phone_result, existing_fax_result)
                return result
            
            processed_items = []
            
            # 전화번호 처리 (고급 AI 검색)
            if (phone_number and phone_number not in ['nan', 'None', ''] and 
                (not existing_phone_result or existing_phone_result in ['nan', 'None', '']) and
                self.phone_validator.is_valid_phone_format(phone_number)):
                
                self.logger.info(f"📞 워커 {worker_id}: 전화번호 {phone_number} 고급 AI 검색")
                
                try:
                    phone_result = self.search_engine.search_with_advanced_ai(driver, phone_number, "전화")
                    
                    if phone_result.phone_success:
                        result.found_phone_institution = phone_result.found_phone_institution
                        result.phone_success = True
                        result.gemini_analysis.update(phone_result.gemini_analysis)
                        result.web_sources.extend(phone_result.web_sources)
                        processed_items.append(f"전화({phone_result.found_phone_institution})")
                        self.ai_analysis_count += 1
                    else:
                        processed_items.append("전화(AI실패)")
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 전화번호 AI 검색 오류: {e}")
                    processed_items.append("전화(오류)")
            else:
                if existing_phone_result and existing_phone_result not in ['nan', 'None', '']:
                    processed_items.append("전화(기존)")
                else:
                    processed_items.append("전화(스킵)")
            
            # 팩스번호 처리 (고급 AI 검색)
            if (fax_number and fax_number not in ['nan', 'None', ''] and 
                (not existing_fax_result or existing_fax_result in ['nan', 'None', '']) and
                self.phone_validator.is_valid_phone_format(fax_number)):
                
                self.logger.info(f"📠 워커 {worker_id}: 팩스번호 {fax_number} 고급 AI 검색")
                
                try:
                    fax_result = self.search_engine.search_with_advanced_ai(driver, fax_number, "팩스")
                    
                    if fax_result.fax_success:
                        result.found_fax_institution = fax_result.found_fax_institution
                        result.fax_success = True
                        result.gemini_analysis.update(fax_result.gemini_analysis)
                        result.web_sources.extend(fax_result.web_sources)
                        processed_items.append(f"팩스({fax_result.found_fax_institution})")
                        self.ai_analysis_count += 1
                    else:
                        processed_items.append("팩스(AI실패)")
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 팩스번호 AI 검색 오류: {e}")
                    processed_items.append("팩스(오류)")
            else:
                if existing_fax_result and existing_fax_result not in ['nan', 'None', '']:
                    processed_items.append("팩스(기존)")
                else:
                    processed_items.append("팩스(스킵)")
            
            result.processing_time = time.time() - start_time
            
            # 캐시에 결과 추가
            self.cache_manager.add_result(row_idx, result)
            
            # 통계 업데이트
            with self.lock:
                self.processed_count += 1
                if result.phone_success:
                    self.phone_success += 1
                if result.fax_success:
                    self.fax_success += 1
            
            self.logger.info(f"✅ 워커 {worker_id}: 행 {row_idx+1} 완료 - {', '.join(processed_items)} ({result.processing_time:.1f}초)")
            
            return result
            
        except Exception as e:
            result.error_message = str(e)
            result.processing_time = time.time() - start_time
            self.logger.error(f"❌ 워커 {worker_id}: 행 {row_idx+1} 처리 오류 - {e}")
            return result
    
    def process_file(self, input_filepath: str) -> str:
        """파일 전체 처리 (AI 기반 + 안정성 강화)"""
        try:
            # 시스템 정보 출력
            self.performance_manager.display_performance_info()
            
            # Gemini API 상태 확인
            api_stats = self.gemini_manager.get_statistics()
            self.logger.info(f"🔑 Gemini API 상태: {api_stats['active_keys']}/{api_stats['total_keys']}개 키 활성")
            
            # 데이터 로드 및 준비
            df = self.load_and_prepare_data(input_filepath)
            
            self.logger.info(f"🚀 AI 기반 처리 시작: {len(df)}행 ({self.execution_mode} 모드)")
            self.logger.info(f"⚙️  설정: 워커 {self.max_workers}개, 배치 {self.batch_size}개")
            
            # 모든 결과 저장 (acrawl_i5.py 방식)
            all_results = {}
            intermediate_results_list = []  # 중간 저장용 리스트
            
            # 배치별 처리
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            self.save_stats['total_batches'] = total_batches
            
            for batch_start in range(0, len(df), self.batch_size):
                batch_end = min(batch_start + self.batch_size, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                
                batch_num = (batch_start // self.batch_size) + 1
                
                self.logger.info(f"📦 배치 {batch_num}/{total_batches} 처리: {batch_start+1}~{batch_end} ({len(batch_df)}개)")
                
                # 배치 결과 저장용
                batch_results = []
                
                try:
                    # 배치 내 병렬 처리
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = []
                        
                        for idx, (original_idx, row) in enumerate(batch_df.iterrows()):
                            worker_id = idx % self.max_workers
                            future = executor.submit(self.process_single_row, (original_idx, row), worker_id)
                            futures.append((future, original_idx))
                        
                        # 결과 수집
                        for future, row_idx in futures:
                            try:
                                result = future.result(timeout=600)  # 10분 타임아웃 (AI 처리 고려)
                                all_results[row_idx] = result
                                
                                # 배치 결과에 추가 (중간 저장용)
                                result_dict = {
                                    'row_index': row_idx + 1,  # 1-based 인덱스
                                    'phone_number': result.phone_number,
                                    'fax_number': result.fax_number,
                                    'found_phone_institution': result.found_phone_institution,
                                    'found_fax_institution': result.found_fax_institution,
                                    'phone_success': result.phone_success,
                                    'fax_success': result.fax_success,
                                    'processing_time': result.processing_time,
                                    'error_message': result.error_message,
                                    'worker_id': getattr(result, 'worker_id', 'unknown'),
                                    'batch_number': batch_num,
                                    'processed_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                batch_results.append(result_dict)
                                intermediate_results_list.append(result_dict)
                                
                                self.processed_count += 1
                                
                                # 성공 통계 업데이트
                                if result.phone_success:
                                    self.phone_success += 1
                                if result.fax_success:
                                    self.fax_success += 1
                                    
                                # 진행률 출력
                                if self.processed_count % 10 == 0:
                                    progress = (self.processed_count / len(df)) * 100
                                    self.logger.info(f"📊 진행률: {self.processed_count}/{len(df)} ({progress:.1f}%) - AI분석:{self.ai_analysis_count}, 성공(전화:{self.phone_success}, 팩스:{self.fax_success})")
                                
                            except Exception as e:
                                self.logger.error(f"❌ 행 {row_idx+1} 결과 처리 오류: {e}")
                                
                                # 오류 결과도 저장 (복구용)
                                error_result = {
                                    'row_index': row_idx + 1,
                                    'phone_number': '',
                                    'fax_number': '',
                                    'found_phone_institution': '',
                                    'found_fax_institution': '',
                                    'phone_success': False,
                                    'fax_success': False,
                                    'processing_time': 0,
                                    'error_message': str(e),
                                    'worker_id': 'unknown',
                                    'batch_number': batch_num,
                                    'processed_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                batch_results.append(error_result)
                                intermediate_results_list.append(error_result)
                    
                    # 배치 완료 후 중간 저장 (acrawl_i5.py 방식)
                    try:
                        self.save_intermediate_results(batch_results, batch_num)
                        self.logger.info(f"✅ 배치 {batch_num} 중간 저장 완료: {len(batch_results)}개")
                    except Exception as save_error:
                        self.logger.warning(f"⚠️ 배치 {batch_num} 중간 저장 실패: {save_error}")
                    
                except Exception as batch_error:
                    self.logger.error(f"❌ 배치 {batch_num} 처리 실패: {batch_error}")
                    
                    # 오류 복구 데이터 저장
                    try:
                        if intermediate_results_list:
                            recovery_file = self.save_error_recovery_data(
                                intermediate_results_list, 
                                f"배치 {batch_num} 처리 실패: {batch_error}"
                            )
                            self.logger.info(f"🛡️ 복구 데이터 저장: {recovery_file}")
                    except Exception as recovery_error:
                        self.logger.error(f"❌ 복구 데이터 저장 실패: {recovery_error}")
                
                # 배치 간 휴식 (AI API 제한 고려)
                if batch_end < len(df):
                    rest_time = random.uniform(5.0, 10.0)
                    self.logger.info(f"⏱️ 배치 {batch_num} 완료 - {rest_time:.1f}초 휴식 (API 제한 고려)")
                    time.sleep(rest_time)
            
            # 결과를 DataFrame에 반영
            updated_count = 0
            for row_idx, result in all_results.items():
                try:
                    if result.phone_success and len(df.columns) > 7:
                        df.iloc[df.index.get_loc(row_idx), 7] = result.found_phone_institution
                        updated_count += 1
                    if result.fax_success and len(df.columns) > 9:
                        df.iloc[df.index.get_loc(row_idx), 9] = result.found_fax_institution
                        updated_count += 1
                except Exception as update_error:
                    self.logger.warning(f"⚠️ 행 {row_idx+1} DataFrame 업데이트 실패: {update_error}")
            
            # 최종 캐시 저장
            try:
                self.cache_manager.save_cache()
                self.cache_manager.create_excel_checkpoint()
            except Exception as cache_error:
                self.logger.warning(f"⚠️ 캐시 저장 실패: {cache_error}")
            
            self.logger.info(f"📝 총 {updated_count}개 셀 업데이트 완료")
            
            # 최종 결과 저장 (acrawl_i5.py 방식)
            try:
                mode_suffix = "test" if self.execution_mode == "test" else "full"
                final_file = self.save_final_results(intermediate_results_list, mode_suffix)
                
                if final_file:
                    self.logger.info(f"✅ 최종 결과 저장 성공: {final_file}")
                    return final_file
                else:
                    # 백업 저장 (기존 방식)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_file = f"eif3_backup_{mode_suffix}_{timestamp}.xlsx"
                    save_path = os.path.join(self.user_config.save_directory, backup_file)
                    df.to_excel(save_path, index=False)
                    self.logger.warning(f"⚠️ 최종 저장 실패, 백업 저장: {save_path}")
                    return save_path
                    
            except Exception as final_save_error:
                self.logger.error(f"❌ 최종 저장 실패: {final_save_error}")
                
                # 긴급 백업 저장
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                emergency_file = f"eif3_emergency_{timestamp}.xlsx"
                df.to_excel(emergency_file, index=False)
                self.logger.info(f"🚨 긴급 백업 저장: {emergency_file}")
                return emergency_file
            
        except Exception as e:
            self.logger.error(f"❌ 파일 처리 실패: {e}")
            self.logger.error(traceback.format_exc())
            raise
        finally:
            # 모든 드라이버 정리
            self._cleanup_drivers()
    
    def _process_without_driver(self, result: SearchResultV3, phone_number: str, fax_number: str, 
                              existing_phone_result: str, existing_fax_result: str) -> SearchResultV3:
        """드라이버 없이 기본 처리 (대체 방안)"""
        try:
            self.logger.info("🔧 드라이버 없음 - 기본 패턴 매칭 시도")
            
            # 전화번호 기본 처리
            if (phone_number and phone_number not in ['nan', 'None', ''] and 
                self.phone_validator.is_valid_phone_format(phone_number)):
                
                # 지역번호 기반 간단한 추정
                area_institution = self._guess_institution_by_area_code(phone_number)
                if area_institution:
                    result.found_phone_institution = f"[추정] {area_institution}"
                    result.phone_success = True
                    self.logger.info(f"📞 전화번호 지역 추정: {area_institution}")
                else:
                    result.found_phone_institution = "[검색 필요]"
            
            # 팩스번호 기본 처리
            if (fax_number and fax_number not in ['nan', 'None', ''] and 
                self.phone_validator.is_valid_phone_format(fax_number)):
                
                # 지역번호 기반 간단한 추정
                area_institution = self._guess_institution_by_area_code(fax_number)
                if area_institution:
                    result.found_fax_institution = f"[추정] {area_institution}"
                    result.fax_success = True
                    self.logger.info(f"📠 팩스번호 지역 추정: {area_institution}")
                else:
                    result.found_fax_institution = "[검색 필요]"
            
            result.error_message = "드라이버 없음 - 기본 처리 완료"
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 드라이버 없는 기본 처리 실패: {e}")
            result.error_message = f"드라이버 없음 및 기본 처리 실패: {e}"
            return result
    
    def _guess_institution_by_area_code(self, number: str) -> Optional[str]:
        """지역번호 기반 기관 추정"""
        try:
            # 지역번호 추출
            if '-' in number:
                area_code = number.split('-')[0]
            else:
                area_code = number[:3] if len(number) >= 3 else number
            
            # 지역별 기관 추정 매핑
            area_mapping = {
                "02": "서울시 관련 기관",
                "031": "경기도 관련 기관", 
                "032": "인천시 관련 기관",
                "033": "강원도 관련 기관",
                "041": "충남 관련 기관",
                "042": "대전시 관련 기관",
                "043": "충북 관련 기관",
                "044": "세종시 관련 기관",
                "051": "부산시 관련 기관",
                "052": "울산시 관련 기관",
                "053": "대구시 관련 기관",
                "054": "경북 관련 기관",
                "055": "경남 관련 기관",
                "061": "전남 관련 기관",
                "062": "광주시 관련 기관",
                "063": "전북 관련 기관",
                "064": "제주도 관련 기관"
            }
            
            return area_mapping.get(area_code)
            
        except Exception as e:
            self.logger.warning(f"⚠️ 지역번호 추정 실패: {e}")
            return None

    def _cleanup_drivers(self):
        """모든 드라이버 정리 (web_driver_manager.py 완전 적용)"""
        try:
            self.logger.info("🧹 드라이버 정리 시작 - web_driver_manager.py 방식")
            
            # 각 워커 드라이버 개별 정리 (web_driver_manager.py 방식)
            for worker_id, driver in list(self.worker_drivers.items()):
                try:
                    # 드라이버 상태 확인 후 안전한 종료
                    if driver:
                        try:
                            # 브라우저 캐시 정리 (web_driver_manager.py 방식)
                            driver.execute_script("window.localStorage.clear();")
                            driver.execute_script("window.sessionStorage.clear();")
                            self.logger.debug(f"✅ 워커 {worker_id} 브라우저 캐시 정리 완료")
                        except Exception as cache_error:
                            self.logger.debug(f"⚠️ 워커 {worker_id} 캐시 정리 실패 (무시): {cache_error}")
                        
                        # 브라우저 종료
                        driver.quit()
                        self.logger.info(f"✅ 워커 {worker_id} 드라이버 정리 완료")
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 워커 {worker_id} 드라이버 정리 실패: {e}")
                finally:
                    # 딕셔너리에서 제거
                    if worker_id in self.worker_drivers:
                        del self.worker_drivers[worker_id]
            
            # 완전 초기화
            self.worker_drivers.clear()
            
            # 임시 파일 정리 시도 (web_driver_manager.py 개선된 방식)
            try:
                temp_base = tempfile.gettempdir()
                cleanup_patterns = [
                    'eif3_uc_worker_*',
                    'chrome_worker_*',
                    'scoped_dir*_chromedriver',
                    'undetected_chromedriver*'
                ]
                
                for pattern in cleanup_patterns:
                    try:
                        for item in os.listdir(temp_base):
                            if any(item.startswith(p.replace('*', '')) for p in cleanup_patterns):
                                temp_path = os.path.join(temp_base, item)
                                try:
                                    if os.path.isdir(temp_path):
                                        shutil.rmtree(temp_path, ignore_errors=True)
                                        self.logger.debug(f"🗑️ 임시 디렉토리 정리: {item}")
                                    elif os.path.isfile(temp_path):
                                        os.remove(temp_path)
                                        self.logger.debug(f"🗑️ 임시 파일 정리: {item}")
                                except Exception as item_error:
                                    self.logger.debug(f"임시 파일 정리 실패 (무시): {item_error}")
                                    pass
                    except Exception as pattern_error:
                        self.logger.debug(f"패턴 {pattern} 정리 실패 (무시): {pattern_error}")
                        pass
                        
            except Exception as temp_error:
                self.logger.warning(f"⚠️ 임시 파일 정리 실패: {temp_error}")
            
            # ChromeDriver 관련 파일 정리 (web_driver_manager.py 방식)
            try:
                self.chromedriver_manager.cleanup_driver_files()
            except Exception as chromedriver_error:
                self.logger.warning(f"⚠️ ChromeDriver 파일 정리 실패: {chromedriver_error}")
            
            # 포트 사용 목록 정리
            try:
                self.used_ports.clear()
                self.logger.debug("🔌 포트 사용 목록 정리 완료")
            except Exception as port_error:
                self.logger.debug(f"포트 목록 정리 실패 (무시): {port_error}")
            
            # 메모리 정리 (web_driver_manager.py 방식)
            gc.collect()
            
            # 시스템 캐시 정리 (Linux 환경) - web_driver_manager.py 방식
            if os.name == 'posix':
                try:
                    os.system('sync')
                    self.logger.debug("🔄 시스템 캐시 동기화 완료")
                except Exception as sync_error:
                    self.logger.debug(f"시스템 캐시 동기화 실패 (무시): {sync_error}")
            
            self.logger.info("🧹 드라이버 정리 완료 - web_driver_manager.py 방식 적용")
            
        except Exception as e:
            self.logger.error(f"❌ 드라이버 정리 오류: {e}")
            # 강제 정리 시도
            try:
                self.worker_drivers.clear()
                gc.collect()
                self.logger.info("🔧 강제 드라이버 정리 완료")
            except Exception as force_error:
                self.logger.error(f"❌ 강제 정리도 실패: {force_error}")

    # ================================
    # 스니펫 기반 처리 메서드들
    # ================================

    def _preprocess_snippets(self, snippets: List[Dict], number: str) -> List[Dict]:
        """수집된 스니펫들을 전처리"""
        try:
            processed_snippets = []
            
            for snippet in snippets:
                try:
                    # 스니펫 정리 및 강화
                    processed_snippet = {
                        'title': self._clean_snippet_text(snippet.get('title', '')),
                        'snippet': self._clean_snippet_text(snippet.get('snippet', '')),
                        'text_content': self._clean_snippet_text(snippet.get('text_content', '')),
                        'url': snippet.get('url', ''),
                        'index': snippet.get('index', 0),
                        'relevance_score': self._calculate_snippet_relevance(snippet, number)
                    }
                    
                    # 최소 유효성 검증
                    if (processed_snippet['title'] or processed_snippet['snippet']) and processed_snippet['relevance_score'] > 0:
                        processed_snippets.append(processed_snippet)
                        
                except Exception as e:
                    self.logger.debug(f"스니펫 전처리 실패 (무시): {e}")
                    continue
            
            # 관련성 점수로 정렬
            processed_snippets.sort(key=lambda x: x['relevance_score'], reverse=True)
            
            self.logger.debug(f"📝 스니펫 전처리 완료: {len(processed_snippets)}개")
            return processed_snippets
            
        except Exception as e:
            self.logger.warning(f"⚠️ 스니펫 전처리 오류: {e}")
            return snippets  # 원본 반환

    def _clean_snippet_text(self, text: str) -> str:
        """스니펫 텍스트 정리"""
        if not text:
            return ""
        
        try:
            # 불필요한 문자 제거
            text = re.sub(r'[\r\n\t]+', ' ', text)
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()
            
            # HTML 태그 제거 (혹시 남아있을 경우)
            text = re.sub(r'<[^>]+>', '', text)
            
            # 특수 문자 정리
            text = re.sub(r'[^\w\s가-힣\d\-\(\)\.\,\:]', ' ', text)
            text = re.sub(r'\s+', ' ', text)
            
            return text.strip()
            
        except Exception as e:
            self.logger.debug(f"텍스트 정리 실패: {e}")
            return text

    def _calculate_snippet_relevance(self, snippet: Dict, number: str) -> float:
        """스니펫의 관련성 점수 계산"""
        try:
            score = 0.0
            
            title = snippet.get('title', '').lower()
            snippet_text = snippet.get('snippet', '').lower()
            full_text = snippet.get('text_content', '').lower()
            
            # 전화번호 직접 포함 여부 (높은 점수)
            if number in full_text:
                score += 5.0
            
            # 기관 관련 키워드 포함 여부
            institution_keywords = ['센터', '청', '구청', '시청', '동', '면', '읍', '주민센터', '사무소', '기관']
            for keyword in institution_keywords:
                if keyword in title:
                    score += 2.0
                if keyword in snippet_text:
                    score += 1.5
                if keyword in full_text:
                    score += 1.0
            
            # 연락처 관련 키워드
            contact_keywords = ['전화', '연락처', '번호', '팩스', 'tel', 'fax', '연락']
            for keyword in contact_keywords:
                if keyword in snippet_text:
                    score += 1.0
                if keyword in full_text:
                    score += 0.5
            
            # URL 품질 점수 (공식 사이트일 가능성)
            url = snippet.get('url', '').lower()
            if any(domain in url for domain in ['.go.kr', '.or.kr']):
                score += 3.0
            elif any(domain in url for domain in ['.com', '.net']):
                score += 1.0
            
            # 제목의 품질 점수
            if title and len(title) > 5:
                score += 1.0
            
            # 스니펫 길이 점수 (적당한 길이가 좋음)
            snippet_length = len(snippet_text)
            if 20 <= snippet_length <= 200:
                score += 1.0
            elif snippet_length > 200:
                score += 0.5
            
            return score
            
        except Exception as e:
            self.logger.debug(f"관련성 점수 계산 실패: {e}")
            return 1.0  # 기본 점수

    def _create_snippet_chunks(self, processed_snippets: List[Dict]) -> List[List[Dict]]:
        """스니펫들을 AI 분석용 청크로 분할"""
        try:
            # 청크 크기 설정 (스니펫 기준)
            chunk_size = 3  # 한 번에 3개 스니펫씩 처리
            
            chunks = []
            for i in range(0, len(processed_snippets), chunk_size):
                chunk = processed_snippets[i:i + chunk_size]
                if chunk:  # 빈 청크 방지
                    chunks.append(chunk)
            
            self.logger.debug(f"📦 스니펫 청킹 완료: {len(chunks)}개 청크")
            return chunks
            
        except Exception as e:
            self.logger.warning(f"⚠️ 스니펫 청킹 실패: {e}")
            return [processed_snippets]  # 전체를 하나의 청크로

    def _combine_snippets_for_ai(self, snippet_chunk: List[Dict]) -> str:
        """AI 분석을 위해 스니펫들을 텍스트로 결합"""
        try:
            combined_text = ""
            
            for i, snippet in enumerate(snippet_chunk, 1):
                title = snippet.get('title', '')
                snippet_text = snippet.get('snippet', '')
                url = snippet.get('url', '')
                
                chunk_text = f"\n=== 검색 결과 {i} ===\n"
                if title:
                    chunk_text += f"제목: {title}\n"
                if snippet_text:
                    chunk_text += f"내용: {snippet_text}\n"
                if url:
                    chunk_text += f"출처: {url}\n"
                
                combined_text += chunk_text
            
            return combined_text.strip()
            
        except Exception as e:
            self.logger.warning(f"⚠️ 스니펫 결합 실패: {e}")
            return ""

    def _create_snippet_gemini_prompt_template(self, number: str, number_type: str) -> str:
        """스니펫 분석용 Gemini 프롬프트 템플릿 생성"""
        try:
            prompt = f"""
다음은 '{number}' {number_type}번호와 관련된 Google 검색 결과 스니펫들입니다.
이 검색 결과들을 분석하여 해당 번호를 사용하는 정확한 기관명을 찾아주세요.

**분석 지침:**
1. 제공된 검색 결과 스니펫에서 {number}번호와 연관된 기관명을 찾으세요
2. 공식적이고 정확한 기관명을 우선시하세요
3. 약칭보다는 정식명칭을 선호하세요
4. 불확실한 경우 가장 신뢰도가 높은 결과를 선택하세요

**응답 형식:**
기관명: [찾은 기관의 정식명칭]
신뢰도: [높음/중간/낮음]
근거: [판단 근거를 간단히 설명]

**주의사항:**
- 기관명이 명확하지 않으면 "기관명을 찾을 수 없음"이라고 답하세요
- 추측이나 가정은 피하고 스니펫 내용에 근거해서만 답하세요
- 번호가 일치하지 않으면 해당 사실을 명시하세요

**검색 결과 스니펫:**
"""
            return prompt.strip()
            
        except Exception as e:
            self.logger.warning(f"⚠️ 스니펫 프롬프트 생성 실패: {e}")
            return "다음 검색 결과에서 기관명을 찾아주세요:"
    
    def _print_final_statistics(self):
        """최종 통계 출력"""
        self.logger.info("=" * 80)
        self.logger.info("📊 Enhanced Institution Finder v3 - 최종 통계")
        self.logger.info("=" * 80)
        self.logger.info(f"실행 모드: {self.execution_mode}")
        self.logger.info(f"전체 행 수: {self.total_rows:,}")
        self.logger.info(f"처리 완료: {self.processed_count:,}")
        self.logger.info(f"전화번호 성공: {self.phone_success:,}")
        self.logger.info(f"팩스번호 성공: {self.fax_success:,}")
        self.logger.info(f"AI 분석 횟수: {self.ai_analysis_count:,}")
        
        if self.processed_count > 0:
            phone_rate = (self.phone_success / self.processed_count) * 100
            fax_rate = (self.fax_success / self.processed_count) * 100
            total_success = self.phone_success + self.fax_success
            total_attempts = self.processed_count * 2
            overall_rate = (total_success / total_attempts) * 100
            
            self.logger.info(f"전화번호 성공률: {phone_rate:.1f}%")
            self.logger.info(f"팩스번호 성공률: {fax_rate:.1f}%")
            self.logger.info(f"전체 성공률: {overall_rate:.1f}%")
        
        self.logger.info("🆕 v3 핵심 기능:")
        self.logger.info("   - 🤖 Gemini AI 기반 고급 분석")
        self.logger.info("   - 🔍 실제 웹사이트 접속 및 내용 분석")
        self.logger.info("   - 🔑 다중 API 키 자동 관리")
        self.logger.info("   - 💾 실시간 캐시 및 복구 시스템")
        self.logger.info("   - 📊 Desktop 자동 저장")
        self.logger.info("=" * 80)

def main():
    """메인 실행 함수 v3"""
    # 로깅 설정
    logger = setup_logging_v3()
    
    try:
        logger.info("🚀 Enhanced Institution Finder v3 시작")
        logger.info("🎯 AI 기반 고급 기관명 추출 시스템")
        
        # 설정 관리자 초기화 및 실행 모드 선택
        config_manager = ConfigManagerV3()
        user_config = config_manager.show_execution_menu()
        
        # 입력 파일 확인
        input_file = 'rawdatafile/failed_data_250715.xlsx'
        
        if not os.path.exists(input_file):
            logger.error(f"❌ 입력 파일을 찾을 수 없습니다: {input_file}")
            print(f"❌ 파일이 없습니다: {input_file}")
            
            # 사용 가능한 파일 목록 표시
            rawdata_dir = 'rawdatafile'
            if os.path.exists(rawdata_dir):
                files = [f for f in os.listdir(rawdata_dir) if f.endswith(('.xlsx', '.csv'))]
                if files:
                    print(f"\n📁 사용 가능한 파일들 ({rawdata_dir}/):")
                    for i, file in enumerate(files, 1):
                        print(f"   {i}. {file}")
                    
                    try:
                        choice = input(f"\n파일을 선택하세요 (1-{len(files)}): ").strip()
                        if choice.isdigit() and 1 <= int(choice) <= len(files):
                            input_file = os.path.join(rawdata_dir, files[int(choice)-1])
                            logger.info(f"📄 선택된 파일: {input_file}")
                        else:
                            raise ValueError("잘못된 선택")
                    except:
                        logger.error("❌ 파일 선택 실패")
                        sys.exit(1)
            else:
                sys.exit(1)
        
        # 메인 처리기 초기화 및 실행
        processor = EnhancedInstitutionProcessorV3(user_config)
        result_file = processor.process_file(input_file)
        
        logger.info(f"🎉 시스템 완료! 결과 파일: {result_file}")
        print(f"\n🎊 Enhanced Institution Finder v3 완료!")
        print(f"📁 결과 파일: {result_file}")
        print(f"🎯 AI 기반 고급 분석이 적용되었습니다.")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ 사용자에 의해 중단됨")
        print("\n⚠️ 작업이 중단되었습니다. 캐시 파일에서 진행상황을 확인할 수 있습니다.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 시스템 오류: {e}")
        logger.error(traceback.format_exc())
        print(f"\n❌ 오류가 발생했습니다: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 