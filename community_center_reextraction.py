#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주민센터 연락처 재추출 시스템
- 기존 추출 실패 데이터 재처리
- 검증 컬럼 기반 선별적 재추출
- 강화된 검증 시스템 적용
- Headless 모드 지원
- 18개 워커 병렬 처리 최적화
"""

import os
import re
import time
import json
import logging
import pandas as pd
import traceback
import psutil
import threading
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

# Selenium 관련
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import undetected_chromedriver as uc
import random

# AI 관련
import google.generativeai as genai
from dotenv import load_dotenv

# 한국 지역번호 매핑
KOREAN_AREA_CODES = {
    "02": "서울", 
    "031": "경기", 
    "032": "인천", 
    "033": "강원",
    "041": "충남", 
    "042": "대전", 
    "043": "충북", 
    "044": "세종",
    "051": "부산", 
    "052": "울산", 
    "053": "대구", 
    "054": "경북", 
    "055": "경남",
    "061": "전남", 
    "062": "광주", 
    "063": "전북", 
    "064": "제주",
    "070": "인터넷전화", 
    "010": "핸드폰", 
    "017": "핸드폰"
}

# 중간 저장 단위 설정
INTERMEDIATE_SAVE_INTERVAL = 30

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('community_center_reextraction.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# AI 모델 설정
AI_MODEL_CONFIG = {
    "temperature": 0.1,
    "top_p": 0.8,
    "top_k": 40,
    "max_output_tokens": 2048,
}

# ===== 재추출 전용 워커 함수들 =====

# 재추출용 WebDriver 생성
def create_reextraction_worker_driver(worker_id: int):
    """재추출용 WebDriver 생성"""
    try:
        import undetected_chromedriver as uc
        import random
        import time
        
        # 워커 간 시차 두기
        startup_delay = random.uniform(0.5, 1.5) * worker_id
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
        
        # Headless 모드 설정
        if globals().get('HEADLESS_MODE', True):
            chrome_options.add_argument('--headless')
        
        # 재추출 최적화 옵션
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
        
        # 메모리 최적화 (재추출용)
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--max_old_space_size=256')
        chrome_options.add_argument('--aggressive-cache-discard')
        chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
        
        # 안전한 포트 설정
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
        
        # 재추출 최적화 타임아웃
        driver.implicitly_wait(8)
        driver.set_page_load_timeout(15)
        
        # 웹드라이버 감지 방지
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print(f"🔧 재추출 워커 {worker_id}: WebDriver 생성 완료 (포트: {debug_port})")
        
        return driver
        
    except Exception as e:
        print(f"❌ 재추출 워커 {worker_id} WebDriver 생성 오류: {e}")
        return None

# 강화된 전화번호 정규화 (재추출용)
def normalize_phone_reextraction(phone: str) -> str:
    """강화된 전화번호 정규화 (재추출용)"""
    if not phone:
        return ""
    
    # 숫자만 추출
    digits = re.sub(r'[^\d]', '', phone)
    if not digits:
        return ""
    
    # 길이 검증
    if len(digits) < 8 or len(digits) > 11:
        return ""
    
    # 지역번호별 정규화
    if digits.startswith('02'):
        # 서울 (02-XXXX-XXXX)
        if len(digits) == 9:
            return f"02-{digits[2:5]}-{digits[5:]}"
        elif len(digits) == 10:
            return f"02-{digits[2:6]}-{digits[6:]}"
    elif digits.startswith('0'):
        # 지역번호 (0XX-XXX-XXXX 또는 0XX-XXXX-XXXX)
        if len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11:
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    elif digits.startswith('070'):
        # 인터넷 전화 (070-XXXX-XXXX)
        if len(digits) == 11:
            return f"070-{digits[3:7]}-{digits[7:]}"
    elif digits.startswith('1'):
        # 단축번호 (1XXX-XXXX)
        if len(digits) == 8:
            return f"{digits[:4]}-{digits[4:]}"
    
    return ""

# 강화된 전화번호 형식 검사 (재추출용)
def is_valid_phone_format_reextraction(phone: str) -> bool:
    """강화된 전화번호 형식 검사 (재추출용)"""
    try:
        if not phone:
            return False
        
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) < 8 or len(digits) > 11:
            return False
        
        # 지역번호 추출
        if digits.startswith('02'):
            area_code = '02'
            if len(digits) not in [9, 10]:
                return False
        elif digits.startswith('0'):
            area_code = digits[:3]
            if len(digits) not in [10, 11]:
                return False
        elif digits.startswith('070'):
            area_code = '070'
            if len(digits) != 11:
                return False
        elif digits.startswith('1'):
            # 단축번호
            if len(digits) not in [8, 9]:
                return False
            return True
        else:
            return False
        
        # KOREAN_AREA_CODES에서 유효한 지역번호인지 확인
        if area_code not in KOREAN_AREA_CODES:
            return False
        
        return True
        
    except Exception:
        return False

# 강화된 지역 일치성 검사 (재추출용)
def is_regional_match_reextraction(phone: str, sido: str) -> bool:
    """강화된 지역 일치성 검사 (재추출용)"""
    try:
        if not phone or not sido:
            return True
        
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) < 8:
            return False
        
        # 지역번호 추출
        if digits.startswith('02'):
            area_code = '02'
        elif digits.startswith('0'):
            area_code = digits[:3]
        elif digits.startswith('070'):
            area_code = '070'
        elif digits.startswith('1'):
            # 단축번호는 지역 구분이 없으므로 허용
            return True
        else:
            return False
        
        # KOREAN_AREA_CODES에서 지역 확인
        phone_region = KOREAN_AREA_CODES.get(area_code, "")
        if not phone_region:
            return False
        
        # 특별 처리: 인터넷전화와 핸드폰은 지역 구분이 없으므로 허용
        if phone_region in ["인터넷전화", "핸드폰"]:
            return True
        
        # 지역 매칭 검사
        sido_normalized = sido.replace("특별시", "").replace("광역시", "").replace("특별자치도", "").replace("도", "").replace("시", "")
        
        # 정확한 지역 매칭
        region_matches = {
            "서울": ["서울"],
            "경기": ["경기"],
            "인천": ["인천"],
            "강원": ["강원"],
            "충남": ["충남", "충청남도"],
            "대전": ["대전"],
            "충북": ["충북", "충청북도"],
            "세종": ["세종"],
            "부산": ["부산"],
            "울산": ["울산"],
            "대구": ["대구"],
            "경북": ["경북", "경상북도"],
            "경남": ["경남", "경상남도"],
            "전남": ["전남", "전라남도"],
            "광주": ["광주"],
            "전북": ["전북", "전라북도"],
            "제주": ["제주"]
        }
        
        expected_regions = region_matches.get(phone_region, [phone_region])
        
        for expected_region in expected_regions:
            if expected_region in sido_normalized or sido_normalized in expected_region:
                return True
        
        return False
        
    except Exception:
        return False

# 재추출 전용 구글 검색 (전화번호)
def search_phone_number_reextraction(driver, query: str, phone_patterns: List[str]):
    """재추출 전용 구글 검색 (전화번호)"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.keys import Keys
        from selenium.common.exceptions import TimeoutException, WebDriverException
        from bs4 import BeautifulSoup
        import time
        import random
        import re
        
        # 재추출 최적화 지연
        delay = random.uniform(0.5, 1.2)
        time.sleep(delay)
        
        # 재시도 로직 (재추출용 - 더 빠른 처리)
        max_retries = 2
        for retry in range(max_retries):
            try:
                # 구글 검색 페이지로 이동
                driver.get('https://www.google.com')
                
                # 짧은 대기 시간
                time.sleep(random.uniform(0.8, 1.5))
                
                # 검색창 찾기
                search_box = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.NAME, 'q'))
                )
                
                # 검색어 입력 (재추출용 - 더 빠른 입력)
                search_box.clear()
                search_box.send_keys(query)
                search_box.send_keys(Keys.RETURN)
                
                # 결과 페이지 로딩 대기
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.ID, 'search'))
                )
                
                # 짧은 대기 시간
                time.sleep(random.uniform(0.8, 1.5))
                
                # 페이지 소스 가져오기
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                # 전화번호 추출
                phone_number = None
                for pattern in phone_patterns:
                    matches = re.findall(pattern, text_content, re.IGNORECASE)
                    for match in matches:
                        normalized = normalize_phone_reextraction(match)
                        if is_valid_phone_format_reextraction(normalized):
                            phone_number = normalized
                            break
                    if phone_number:
                        break
                
                return phone_number
                
            except (TimeoutException, WebDriverException) as e:
                if retry < max_retries - 1:
                    wait_time = random.uniform(3, 6)
                    print(f"⚠️ 재추출 전화번호 검색 실패 (재시도 {retry + 1}/{max_retries}), {wait_time:.1f}초 후 재시도: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        return None
        
    except Exception as e:
        print(f"❌ 재추출 전화번호 구글 검색 오류: {e}")
        time.sleep(random.uniform(3, 6))
        return None

# 재추출 전용 구글 검색 (팩스번호)
def search_fax_number_reextraction(driver, query: str, fax_patterns: List[str]):
    """재추출 전용 구글 검색 (팩스번호)"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.keys import Keys
        from selenium.common.exceptions import TimeoutException, WebDriverException
        from bs4 import BeautifulSoup
        import time
        import random
        import re
        
        # 재추출 최적화 지연
        delay = random.uniform(0.5, 1.2)
        time.sleep(delay)
        
        # 재시도 로직 (재추출용)
        max_retries = 2
        for retry in range(max_retries):
            try:
                # 구글 검색 페이지로 이동
                driver.get('https://www.google.com')
                
                # 짧은 대기 시간
                time.sleep(random.uniform(0.8, 1.5))
                
                # 검색창 찾기
                search_box = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.NAME, 'q'))
                )
                
                # 검색어 입력
                search_box.clear()
                search_box.send_keys(query)
                search_box.send_keys(Keys.RETURN)
                
                # 결과 페이지 로딩 대기
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.ID, 'search'))
                )
                
                # 짧은 대기 시간
                time.sleep(random.uniform(0.8, 1.5))
                
                # 페이지 소스 가져오기
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                # 팩스번호 추출
                fax_number = None
                for pattern in fax_patterns:
                    matches = re.findall(pattern, text_content, re.IGNORECASE)
                    for match in matches:
                        normalized = normalize_phone_reextraction(match)
                        if is_valid_phone_format_reextraction(normalized):
                            fax_number = normalized
                            break
                    if fax_number:
                        break
                
                return fax_number
                
            except (TimeoutException, WebDriverException) as e:
                if retry < max_retries - 1:
                    wait_time = random.uniform(3, 6)
                    print(f"⚠️ 재추출 팩스번호 검색 실패 (재시도 {retry + 1}/{max_retries}), {wait_time:.1f}초 후 재시도: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        return None
        
    except Exception as e:
        print(f"❌ 재추출 팩스번호 구글 검색 오류: {e}")
        time.sleep(random.uniform(3, 6))
        return None

# 재추출 전용 역검색 검증
def reverse_search_validation_reextraction(driver, phone_number: str, institution_name: str, sido: str) -> Dict[str, Any]:
    """재추출 전용 역검색 검증 (단순화)"""
    try:
        if not phone_number or not institution_name:
            return {'is_valid': False, 'reason': '전화번호 또는 기관명이 없음', 'confidence': 0.0}
        
        # 단순한 역검색 쿼리 (재추출용)
        query = f"{phone_number}"
        
        try:
            # 구글 검색 실행
            driver.get('https://www.google.com')
            time.sleep(random.uniform(0.8, 1.5))
            
            # 검색창 찾기
            search_box = WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            
            # 검색어 입력
            search_box.clear()
            search_box.send_keys(query)
            search_box.send_keys(Keys.RETURN)
            
            # 결과 페이지 대기
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            
            time.sleep(random.uniform(0.8, 1.5))
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            text_content = soup.get_text()
            
            # 기관명 매칭 확인 (단순화)
            confidence = calculate_institution_match_confidence_reextraction(text_content, institution_name, sido)
            
            # 재추출용 낮은 임계값 (더 관대한 검증)
            if confidence >= 0.4:
                return {
                    'is_valid': True,
                    'reason': f"역검색 기관명 매칭 확인",
                    'confidence': confidence
                }
            else:
                return {
                    'is_valid': False,
                    'reason': f'역검색에서 기관명 매칭 실패 (신뢰도: {confidence:.2f})',
                    'confidence': confidence
                }
            
        except Exception as e:
            return {'is_valid': False, 'reason': f'역검색 중 오류 발생: {str(e)}', 'confidence': 0.0}
        
    except Exception as e:
        return {'is_valid': False, 'reason': f'역검색 중 오류 발생: {str(e)}', 'confidence': 0.0}

# 재추출용 기관명 매칭 신뢰도 계산 (단순화)
def calculate_institution_match_confidence_reextraction(text_content: str, institution_name: str, sido: str) -> float:
    """재추출용 기관명 매칭 신뢰도 계산 (단순화)"""
    try:
        if not text_content or not institution_name:
            return 0.0
        
        confidence = 0.0
        
        # 기관명 정규화
        institution_normalized = institution_name.replace(sido, "").strip()
        institution_keywords = [
            institution_normalized,
            institution_normalized.replace("주민센터", ""),
            institution_normalized.replace("행정복지센터", ""),
            institution_normalized.replace("행정센터", "")
        ]
        
        # 시도 정보 확인
        if sido in text_content:
            confidence += 0.3
        
        # 기관명 키워드 매칭
        for keyword in institution_keywords:
            if keyword and keyword.strip():
                if keyword.strip() in text_content:
                    confidence += 0.5
                    break
        
        # 주민센터 관련 키워드 확인
        center_keywords = ["주민센터", "행정복지센터", "행정센터"]
        for keyword in center_keywords:
            if keyword in text_content:
                confidence += 0.2
                break
        
        return min(confidence, 1.0)
        
    except Exception:
        return 0.0

# 재추출 전용 청크 처리 함수
def process_reextraction_chunk(chunk_df: pd.DataFrame, worker_id: int, phone_patterns: List[str], fax_patterns: List[str]) -> List[Dict]:
    """재추출 전용 청크 처리 함수"""
    import pandas as pd
    import re
    import time
    import random
    
    results = []
    driver = None
    
    try:
        driver = create_reextraction_worker_driver(worker_id)
        if not driver:
            return results
        
        print(f"🔧 재추출 워커 {worker_id}: 처리 시작 ({len(chunk_df)}개)")
        
        for idx, row in chunk_df.iterrows():
            sido = row.get('시도', '')
            sigungu = row.get('시군구', '')
            name = row.get('읍면동', '')
            address = row.get('주    소', '')
            existing_phone = str(row.get('전화번호', '')).strip()
            existing_fax = str(row.get('팩스번호', '')).strip()
            
            # 검증 컬럼 확인
            phone_verification = str(row.get('전화번호 검증', '')).strip()
            fax_verification = str(row.get('팩스번호 검증', '')).strip()
            
            if not name:
                continue
            
            try:
                print(f"🔄 재추출 워커 {worker_id}: 처리 중 - {sido} {name}")
                
                # 정규화된 검색 쿼리 생성
                normalized_name = f"{sido} {name}"
                if "주민센터" not in name:
                    normalized_name += " 주민센터"
                
                new_phone = existing_phone
                new_fax = existing_fax
                
                # 전화번호 재추출 조건 확인
                need_phone_reextraction = (
                    not existing_phone or 
                    existing_phone == '' or 
                    phone_verification == 'FAIL' or
                    phone_verification == ''
                )
                
                # 팩스번호 재추출 조건 확인
                need_fax_reextraction = (
                    not existing_fax or 
                    existing_fax == '' or 
                    fax_verification == 'FAIL' or
                    fax_verification == ''
                )
                
                # 🔍 전화번호 재추출
                if need_phone_reextraction:
                    phone_search_queries = [
                        f"{normalized_name} 전화번호",
                        f"{normalized_name} 연락처",
                        f"{name} 전화번호"
                    ]
                    
                    for query_idx, phone_search_query in enumerate(phone_search_queries):
                        print(f"🔍 재추출 워커 {worker_id}: 전화번호 검색 {query_idx+1}/{len(phone_search_queries)} - {phone_search_query}")
                        phone_number = search_phone_number_reextraction(driver, phone_search_query, phone_patterns)
                        
                        if phone_number and is_valid_phone_format_reextraction(phone_number) and is_regional_match_reextraction(phone_number, sido):
                            # 역검색 검증 (재추출용 - 더 관대한 검증)
                            reverse_validation = reverse_search_validation_reextraction(driver, phone_number, name, sido)
                            
                            if reverse_validation['is_valid']:
                                new_phone = phone_number
                                print(f"✅ 재추출 워커 {worker_id}: 전화번호 재추출 성공 - {name} -> {new_phone}")
                                break
                            else:
                                print(f"❌ 재추출 워커 {worker_id}: 전화번호 역검색 실패 - {name} -> {phone_number}")
                        else:
                            print(f"❌ 재추출 워커 {worker_id}: 전화번호 검색 실패 - {name}")
                        
                        # 검색 간격
                        time.sleep(random.uniform(0.5, 1.0))
                
                # 🔍 팩스번호 재추출
                if need_fax_reextraction:
                    fax_search_queries = [
                        f"{normalized_name} 팩스번호",
                        f"{normalized_name} 팩스",
                        f"{name} 팩스번호"
                    ]
                    
                    for query_idx, fax_search_query in enumerate(fax_search_queries):
                        print(f"🔍 재추출 워커 {worker_id}: 팩스번호 검색 {query_idx+1}/{len(fax_search_queries)} - {fax_search_query}")
                        fax_number = search_fax_number_reextraction(driver, fax_search_query, fax_patterns)
                        
                        if fax_number and is_valid_phone_format_reextraction(fax_number) and is_regional_match_reextraction(fax_number, sido):
                            # 역검색 검증 (재추출용)
                            reverse_validation = reverse_search_validation_reextraction(driver, fax_number, name, sido)
                            
                            if reverse_validation['is_valid']:
                                new_fax = fax_number
                                print(f"✅ 재추출 워커 {worker_id}: 팩스번호 재추출 성공 - {name} -> {new_fax}")
                                break
                            else:
                                print(f"❌ 재추출 워커 {worker_id}: 팩스번호 역검색 실패 - {name} -> {fax_number}")
                        else:
                            print(f"❌ 재추출 워커 {worker_id}: 팩스번호 검색 실패 - {name}")
                        
                        # 검색 간격
                        time.sleep(random.uniform(0.5, 1.0))
                
                # 중복 번호 처리
                if new_phone and new_fax and new_phone == new_fax:
                    print(f"⚠️ 재추출 워커 {worker_id}: 전화번호와 팩스번호가 동일함 - {name} -> {new_phone}")
                    new_fax = ""  # 팩스번호 제거
                
                results.append({
                    'index': idx,
                    'name': f"{sido} {name}",
                    'phone': new_phone or '',
                    'fax': new_fax or '',
                    'phone_reextracted': new_phone != existing_phone,
                    'fax_reextracted': new_fax != existing_fax
                })
                
                # 안전한 랜덤 지연
                delay = random.uniform(0.8, 1.5)
                time.sleep(delay)
                
            except Exception as e:
                print(f"❌ 재추출 워커 {worker_id}: 처리 오류 - {name}: {e}")
                
                # 에러 발생 시 기존 데이터 유지
                results.append({
                    'index': idx,
                    'name': f"{sido} {name}",
                    'phone': existing_phone,
                    'fax': existing_fax,
                    'phone_reextracted': False,
                    'fax_reextracted': False
                })
                
                # 에러 발생 시 더 긴 대기
                error_delay = random.uniform(2.0, 4.0)
                time.sleep(error_delay)
                continue
        
        print(f"🎉 재추출 워커 {worker_id}: 처리 완료 ({len(results)}개)")
        
    except Exception as e:
        print(f"❌ 재추출 워커 {worker_id}: 프로세스 오류: {e}")
    finally:
        if driver:
            driver.quit()
    
    return results

# 재추출 성능 모니터링 클래스
class ReextractionMonitor:
    """재추출 성능 모니터링 클래스"""
    
    def __init__(self):
        """재추출 모니터 초기화"""
        self.stats = {
            'total_processed': 0,
            'phone_reextracted': 0,
            'fax_reextracted': 0,
            'phone_failed': 0,
            'fax_failed': 0,
            'both_success': 0,
            'both_failed': 0,
            'start_time': datetime.now()
        }
    
    def record_result(self, phone_reextracted: bool, fax_reextracted: bool, phone_success: bool, fax_success: bool):
        """재추출 결과 기록"""
        self.stats['total_processed'] += 1
        
        if phone_reextracted and phone_success:
            self.stats['phone_reextracted'] += 1
        elif phone_reextracted and not phone_success:
            self.stats['phone_failed'] += 1
        
        if fax_reextracted and fax_success:
            self.stats['fax_reextracted'] += 1
        elif fax_reextracted and not fax_success:
            self.stats['fax_failed'] += 1
        
        if phone_success and fax_success:
            self.stats['both_success'] += 1
        elif not phone_success and not fax_success:
            self.stats['both_failed'] += 1
    
    def print_statistics(self):
        """재추출 통계 출력"""
        stats = self.stats
        elapsed_time = datetime.now() - stats['start_time']
        
        print("\n" + "="*60)
        print("📊 재추출 시스템 성능 통계")
        print("="*60)
        
        print(f"⏱️  총 처리 시간: {elapsed_time}")
        print(f"📈 총 처리 건수: {stats['total_processed']}")
        
        if stats['total_processed'] > 0:
            print(f"\n📞 전화번호 재추출:")
            print(f"   성공: {stats['phone_reextracted']} ({stats['phone_reextracted']/stats['total_processed']*100:.1f}%)")
            print(f"   실패: {stats['phone_failed']}")
            
            print(f"\n📠 팩스번호 재추출:")
            print(f"   성공: {stats['fax_reextracted']} ({stats['fax_reextracted']/stats['total_processed']*100:.1f}%)")
            print(f"   실패: {stats['fax_failed']}")
            
            print(f"\n🎯 종합 결과:")
            print(f"   전화+팩스 모두 성공: {stats['both_success']} ({stats['both_success']/stats['total_processed']*100:.1f}%)")
            print(f"   전화+팩스 모두 실패: {stats['both_failed']} ({stats['both_failed']/stats['total_processed']*100:.1f}%)")
        
        print("="*60)

# 전역 재추출 모니터 인스턴스
reextraction_monitor = ReextractionMonitor()

# 주민센터 재추출 메인 클래스
class CommunityCenterReextractor:
    """주민센터 연락처 재추출 메인 클래스"""
    
    def __init__(self, excel_path: str):
        """
        재추출기 초기화
        
        Args:
            excel_path: 재추출 대상 Excel 파일 경로
        """
        self.excel_path = excel_path
        self.logger = logging.getLogger(__name__)
        
        # 환경 변수 로드
        load_dotenv()
        
        # 데이터 로드
        self.df = None
        self._load_data()
        
        # 결과 저장용
        self.results = []
        self.processed_count = 0
        self.reextracted_count = 0
        self.start_time = datetime.now()
        
        # 중간 저장 카운터
        self.intermediate_save_counter = 0
        
        # 시스템 모니터링용
        self.process = psutil.Process()
        self.monitoring_active = False
        self.monitoring_thread = None
        self.system_stats = {
            'cpu_percent': 0,
            'memory_mb': 0,
            'memory_percent': 0
        }
        
        # 재추출 최적화 멀티프로세싱 설정
        cpu_count = multiprocessing.cpu_count()
        
        # Headless 모드에 따른 워커 수 동적 조정
        if globals().get('HEADLESS_MODE', True):
            self.max_workers = 18  # Headless 모드: 18개 워커
            self.chunk_size = 10   # 재추출 최적화 청크
        else:
            self.max_workers = 12  # GUI 모드: 12개 워커
            self.chunk_size = 8    # 안정적인 청크 크기
        
        # 재추출 최적화 요청 간격
        if globals().get('HEADLESS_MODE', True):
            self.request_delay_min = 0.5  # 더 빠른 재추출
            self.request_delay_max = 1.0  # 더 빠른 재추출
        else:
            self.request_delay_min = 0.8  # 안정적인 재추출
            self.request_delay_max = 1.5  # 안정적인 재추출
        
        # 재추출용 정규식 패턴
        self.phone_patterns = [
            r'전화[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'tel[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'T[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'연락처[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
        ]
        
        self.fax_patterns = [
            r'팩스[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'F[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'전송[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*팩스',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*fax',
        ]
        
        # 시스템 모니터링 시작
        self._start_system_monitoring()
        
        headless_status = "Headless" if globals().get('HEADLESS_MODE', True) else "GUI"
        self.logger.info(f"🚀 CommunityCenterReextractor 초기화 완료")
        self.logger.info(f"🔧 {headless_status} 모드 - 워커: {self.max_workers}개, 청크: {self.chunk_size}개")
        self.logger.info(f"⚡ 재추출 요청 간격: {self.request_delay_min}~{self.request_delay_max}초")
        self.logger.info(f"🔧 재추출 최적화 설정 적용")
    
    def _load_data(self):
        """Excel 데이터 로드 (재추출 대상)"""
        try:
            # Excel 파일 읽기
            self.df = pd.read_excel(self.excel_path)
            
            self.logger.info(f"📊 재추출 대상 데이터 로드 완료: {len(self.df)}개 주민센터")
            
            # 필수 컬럼 확인
            required_columns = ['연번', '시도', '시군구', '읍면동', '우편번호', '주    소', '전화번호', '팩스번호']
            missing_columns = [col for col in required_columns if col not in self.df.columns]
            
            if missing_columns:
                self.logger.error(f"❌ 필수 컬럼 누락: {missing_columns}")
                raise ValueError(f"필수 컬럼이 누락되었습니다: {missing_columns}")
            
            # 새로운 컬럼들이 없으면 추가
            new_columns = ['전화번호 검증', '팩스번호 검증', '전화번호 재추출', '팩스번호 재추출']
            for col in new_columns:
                if col not in self.df.columns:
                    self.df[col] = ''
            
            # 데이터 전처리
            self.df = self.df.dropna(subset=['읍면동'])
            
            # 재추출 대상 통계
            total_count = len(self.df)
            phone_missing = len(self.df[self.df['전화번호'].isna() | (self.df['전화번호'] == '')])
            fax_missing = len(self.df[self.df['팩스번호'].isna() | (self.df['팩스번호'] == '')])
            phone_verification_fail = len(self.df[self.df['전화번호 검증'] == 'FAIL'])
            fax_verification_fail = len(self.df[self.df['팩스번호 검증'] == 'FAIL'])
            
            self.logger.info(f"✅ 재추출 대상 데이터 전처리 완료: {total_count}개 주민센터")
            self.logger.info(f"📊 재추출 대상 통계:")
            self.logger.info(f"   전화번호 누락: {phone_missing}개")
            self.logger.info(f"   팩스번호 누락: {fax_missing}개")
            self.logger.info(f"   전화번호 검증 실패: {phone_verification_fail}개")
            self.logger.info(f"   팩스번호 검증 실패: {fax_verification_fail}개")
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def _start_system_monitoring(self):
        """시스템 모니터링 시작"""
        try:
            self.monitoring_active = True
            self.monitoring_thread = threading.Thread(target=self._monitor_system, daemon=True)
            self.monitoring_thread.start()
            self.logger.info("📊 시스템 모니터링 시작")
        except Exception as e:
            self.logger.error(f"❌ 시스템 모니터링 시작 오류: {e}")
    
    def _monitor_system(self):
        """시스템 리소스 모니터링"""
        while self.monitoring_active:
            try:
                cpu_percent = self.process.cpu_percent()
                memory_info = self.process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                
                system_memory = psutil.virtual_memory()
                memory_percent = (memory_info.rss / system_memory.total) * 100
                
                self.system_stats.update({
                    'cpu_percent': cpu_percent,
                    'memory_mb': memory_mb,
                    'memory_percent': memory_percent
                })
                
                time.sleep(30)
                
            except Exception as e:
                self.logger.error(f"❌ 시스템 모니터링 오류: {e}")
                time.sleep(30)
    
    def _cleanup(self):
        """정리 작업"""
        try:
            self.monitoring_active = False
            if self.monitoring_thread:
                self.monitoring_thread.join(timeout=1)
            
            self.logger.info("🧹 시스템 정리 완료")
        except Exception as e:
            self.logger.error(f"❌ 정리 작업 오류: {e}")
    
    def run_reextraction(self):
        """전체 재추출 프로세스 실행"""
        try:
            self.logger.info("🎯 주민센터 연락처 재추출 시작")
            
            # 1단계: 병렬 재추출
            self.logger.info(f"🔄 1단계: 병렬 재추출 ({self.max_workers}개 워커)")
            self._reextract_contacts_parallel()
            
            # 2단계: 최종 결과 저장
            self.logger.info("💾 2단계: 최종 결과 저장 (Excel 형식)")
            result_path = self._save_results()
            
            self.logger.info("🎉 전체 재추출 프로세스 완료")
            
        except KeyboardInterrupt:
            self.logger.info("⚠️ 사용자 중단 요청 감지")
            self._save_intermediate_results("사용자중단저장")
            raise
        except Exception as e:
            self.logger.error(f"❌ 재추출 프로세스 실패: {e}")
            self._save_intermediate_results("오류발생저장")
            raise
        finally:
            self._cleanup()
    
    def _reextract_contacts_parallel(self):
        """병렬 재추출 처리"""
        total_rows = len(self.df)
        
        if total_rows == 0:
            self.logger.info("📞 재추출할 데이터가 없습니다.")
            return
        
        # 데이터를 워커 수만큼 분할
        chunks = self._split_dataframe(self.df, self.max_workers)
        
        self.logger.info(f"🔄 재추출 병렬 처리 시작: {total_rows}개 데이터를 {len(chunks)}개 프로세스로 처리")
        
        # 멀티프로세싱으로 병렬 처리
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for i, chunk in enumerate(chunks):
                future = executor.submit(
                    process_reextraction_chunk,
                    chunk,
                    i,
                    self.phone_patterns,
                    self.fax_patterns
                )
                futures.append(future)
            
            # 결과 수집
            for future in as_completed(futures):
                try:
                    results = future.result()
                    self._merge_reextraction_results(results)
                except Exception as e:
                    self.logger.error(f"❌ 재추출 프로세스 오류: {e}")
        
        # 중간 저장
        self._save_intermediate_results("재추출완료")
        self.logger.info("🔄 재추출 병렬 처리 완료")
    
    def _split_dataframe(self, df: pd.DataFrame, num_chunks: int) -> List[pd.DataFrame]:
        """데이터프레임을 균등하게 분할"""
        chunk_size = max(1, len(df) // num_chunks)
        chunks = []
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            if i == num_chunks - 1:
                end_idx = len(df)
            else:
                end_idx = (i + 1) * chunk_size
            
            if start_idx < len(df):
                chunk = df.iloc[start_idx:end_idx].copy()
                chunks.append(chunk)
        
        return chunks
    
    def _merge_reextraction_results(self, results: List[Dict]):
        """재추출 결과를 메인 데이터프레임에 병합"""
        try:
            for result in results:
                idx = result['index']
                phone = result.get('phone', '')
                fax = result.get('fax', '')
                phone_reextracted = result.get('phone_reextracted', False)
                fax_reextracted = result.get('fax_reextracted', False)
                name = result.get('name', 'Unknown')
                
                # 기존 데이터 확인
                existing_phone = str(self.df.at[idx, '전화번호']).strip()
                existing_fax = str(self.df.at[idx, '팩스번호']).strip()
                
                # 재추출 결과 업데이트
                if phone and phone.strip():
                    self.df.at[idx, '전화번호'] = phone
                    if phone_reextracted:
                        self.df.at[idx, '전화번호 재추출'] = 'SUCCESS'
                        self.logger.info(f"✅ 전화번호 재추출 성공: {name} -> {phone}")
                    else:
                        self.df.at[idx, '전화번호 재추출'] = 'UNCHANGED'
                else:
                    if phone_reextracted:
                        self.df.at[idx, '전화번호 재추출'] = 'FAIL'
                        self.logger.info(f"❌ 전화번호 재추출 실패: {name}")
                
                if fax and fax.strip():
                    self.df.at[idx, '팩스번호'] = fax
                    if fax_reextracted:
                        self.df.at[idx, '팩스번호 재추출'] = 'SUCCESS'
                        self.logger.info(f"✅ 팩스번호 재추출 성공: {name} -> {fax}")
                    else:
                        self.df.at[idx, '팩스번호 재추출'] = 'UNCHANGED'
                else:
                    if fax_reextracted:
                        self.df.at[idx, '팩스번호 재추출'] = 'FAIL'
                        self.logger.info(f"❌ 팩스번호 재추출 실패: {name}")
                
                # 모니터링 기록
                phone_success = phone and phone.strip() and phone_reextracted
                fax_success = fax and fax.strip() and fax_reextracted
                reextraction_monitor.record_result(phone_reextracted, fax_reextracted, phone_success, fax_success)
                
                self.processed_count += 1
                if phone_reextracted or fax_reextracted:
                    self.reextracted_count += 1
                
                self.intermediate_save_counter += 1
                
                # 진행률 표시
                progress_percent = (self.processed_count / len(self.df)) * 100
                self.logger.info(f"📊 진행률: {self.processed_count}/{len(self.df)} ({progress_percent:.1f}%) - 재추출: {self.reextracted_count}개")
                
                # 30개 단위로 중간 저장
                if self.intermediate_save_counter >= INTERMEDIATE_SAVE_INTERVAL:
                    self._save_intermediate_results(f"재추출중간저장_{self.processed_count}개처리")
                    self.intermediate_save_counter = 0
                    self.logger.info(f"💾 중간 저장 완료: {self.processed_count}개 처리됨")
                
        except Exception as e:
            self.logger.error(f"❌ 재추출 결과 병합 오류: {e}")
    
    def _save_results(self) -> str:
        """최종 결과 저장 (Excel 형식)"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(self.excel_path))[0]
            result_filename = f"{base_name}_재추출완료_{timestamp}.xlsx"
            result_path = os.path.join(os.path.dirname(self.excel_path), result_filename)
            
            self.df.to_excel(result_path, index=False)
            
            # 통계 정보
            total_count = len(self.df)
            phone_reextracted = len(self.df[self.df['전화번호 재추출'] == 'SUCCESS'])
            fax_reextracted = len(self.df[self.df['팩스번호 재추출'] == 'SUCCESS'])
            phone_failed = len(self.df[self.df['전화번호 재추출'] == 'FAIL'])
            fax_failed = len(self.df[self.df['팩스번호 재추출'] == 'FAIL'])
            
            self.logger.info(f"💾 재추출 최종 결과 저장 완료 (Excel): {result_path}")
            self.logger.info(f"📊 재추출 최종 통계:")
            self.logger.info(f"  - 전체 처리 주민센터 수: {total_count}")
            self.logger.info(f"  - 전화번호 재추출 성공: {phone_reextracted} ({phone_reextracted/total_count*100:.1f}%)")
            self.logger.info(f"  - 팩스번호 재추출 성공: {fax_reextracted} ({fax_reextracted/total_count*100:.1f}%)")
            self.logger.info(f"  - 전화번호 재추출 실패: {phone_failed} ({phone_failed/total_count*100:.1f}%)")
            self.logger.info(f"  - 팩스번호 재추출 실패: {fax_failed} ({fax_failed/total_count*100:.1f}%)")
            self.logger.info(f"  - 총 재추출 성공: {self.reextracted_count}")
            
            return result_path
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 오류: {e}")
            raise
    
    def _save_intermediate_results(self, suffix: str = "중간저장"):
        """중간 결과 저장 (Excel 형식)"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(self.excel_path))[0]
            result_filename = f"{base_name}_{suffix}_{timestamp}.xlsx"
            result_path = os.path.join(os.path.dirname(self.excel_path), result_filename)
            
            self.df.to_excel(result_path, index=False)
            
            self.logger.info(f"💾 중간 저장 완료 (Excel): {result_path}")
            
            return result_path
            
        except Exception as e:
            self.logger.error(f"❌ 중간 저장 오류: {e}")
            return None

# 메인 실행 함수 (재추출 전용)
def main():
    """메인 실행 함수 (재추출 전용)"""
    try:
        print("🚀 주민센터 연락처 재추출 시스템 시작")
        print("=" * 60)
        
        # Headless 모드 선택
        print("\n🔧 브라우저 모드 선택:")
        print("1. Headless 모드 (권장) - CPU/메모리 사용량 낮음, 브라우저 창 안 보임")
        print("2. GUI 모드 - 브라우저 창 보임, CPU/메모리 사용량 높음")
        
        while True:
            choice = input("\n선택하세요 (1 또는 2, 기본값: 1): ").strip()
            if choice == "" or choice == "1":
                globals()['HEADLESS_MODE'] = True
                print("✅ Headless 모드로 실행합니다 (CPU/메모리 최적화)")
                break
            elif choice == "2":
                globals()['HEADLESS_MODE'] = False
                print("✅ GUI 모드로 실행합니다 (브라우저 창 표시)")
                break
            else:
                print("❌ 잘못된 선택입니다. 1 또는 2를 입력하세요.")
        
        # 워커 수 조정 (Headless 모드에 따라)
        if globals()['HEADLESS_MODE']:
            print(f"🔧 Headless 모드: 18개 워커로 최적화")
        else:
            print(f"🔧 GUI 모드: 12개 워커로 안정화")
        
        # 재추출 Excel 파일 경로 설정
        excel_path = r"C:\Users\MyoengHo Shin\pjt\cradcrawlpython\rawdatafile\failed_data_250714.xlsx"
        
        # 파일 존재 확인
        if not os.path.exists(excel_path):
            print(f"❌ 재추출 Excel 파일을 찾을 수 없습니다: {excel_path}")
            return
        
        print(f"📁 재추출 대상 파일 경로: {excel_path}")
        
        # 재추출기 초기화 및 실행
        reextractor = CommunityCenterReextractor(excel_path)
        reextractor.run_reextraction()
        
        print("=" * 60)
        print("✅ 주민센터 연락처 재추출 완료!")
        
        # 재추출 시스템 성능 통계 출력
        reextraction_monitor.print_statistics()
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        # 중단 시에도 통계 출력
        reextraction_monitor.print_statistics()
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        import traceback
        traceback.print_exc()
        # 오류 시에도 통계 출력
        reextraction_monitor.print_statistics()

if __name__ == "__main__":
    main() 