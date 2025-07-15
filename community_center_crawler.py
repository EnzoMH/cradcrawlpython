#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
개선된 주민센터 전화번호/팩스번호 추출 시스템
- 행정안전부 읍면동 하부행정기관 현황 CSV 파일 처리
- 전화번호 기반 지역 매핑
- 기관명 자동 정규화
- 12개 워커 병렬 처리 최적화 (AMD Ryzen 5 3600 환경)
- 엄격한 유효성 검사
- 50개 단위 중간 저장
- Excel 형식 출력
- 이메일 기능 제거
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

# 한국 지역번호 매핑 (하드코딩)
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
INTERMEDIATE_SAVE_INTERVAL = 50

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('community_center_crawler_improved.log', encoding='utf-8'),
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

# ===== 병렬 처리 워커 함수들 =====

# 개선된 워커용 WebDriver 생성 (과부하 방지)
def create_improved_worker_driver(worker_id: int):
    """개선된 워커용 WebDriver 생성 (과부하 방지)"""
    try:
        import undetected_chromedriver as uc
        import random
        import time
        
        # 워커 간 시차 두기
        startup_delay = random.uniform(1.0, 3.0) * worker_id
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
        
        # Headless 모드 설정 (전역 변수로 제어)
        if globals().get('HEADLESS_MODE', True):
            chrome_options.add_argument('--headless')
        
        # 🛡️ 리소스 절약 옵션 (12개 워커 최적화)
        chrome_options.add_argument('--disable-images')  # 이미지 로딩 비활성화
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
        
        # 메모리 제한 (12개 워커에 맞게 조정)
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--max_old_space_size=256')  # 더 작은 메모리 할당
        chrome_options.add_argument('--aggressive-cache-discard')
        chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
        
        # 안전한 포트 설정 (9222 + worker_id * 10)
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
        
        # 타임아웃 설정 (12개 워커에 맞게 최적화)
        driver.implicitly_wait(10)  # 더 짧은 대기 시간
        driver.set_page_load_timeout(20)  # 더 짧은 페이지 로드 시간
        
        # 웹드라이버 감지 방지
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print(f"🔧 워커 {worker_id}: WebDriver 생성 완료 (포트: {debug_port})")
        
        return driver
        
    except Exception as e:
        print(f"❌ 워커 {worker_id} WebDriver 생성 오류: {e}")
        return None
    
# 주소에서 지역 정보 추출
def get_region_from_address(address: str) -> str:
    """주소에서 지역 정보 추출"""
    if not address:
        return ""
    
    region_patterns = [
        r'(강원특별자치도|강원도)\s+(\S+시|\S+군)',
        r'(서울특별시|서울시|서울)\s+(\S+구)',
        r'(경기도|경기)\s+(\S+시|\S+군)',
        r'(인천광역시|인천시|인천)\s+(\S+구)',
        r'(충청남도|충남)\s+(\S+시|\S+군)',
        r'(충청북도|충북)\s+(\S+시|\S+군)',
        r'(전라남도|전남)\s+(\S+시|\S+군)',
        r'(전라북도|전북)\s+(\S+시|\S+군)',
        r'(경상남도|경남)\s+(\S+시|\S+군)',
        r'(경상북도|경북)\s+(\S+시|\S+군)',
        r'(부산광역시|부산시|부산)\s+(\S+구)',
        r'(대구광역시|대구시|대구)\s+(\S+구)',
        r'(광주광역시|광주시|광주)\s+(\S+구)',
        r'(대전광역시|대전시|대전)\s+(\S+구)',
        r'(울산광역시|울산시|울산)\s+(\S+구)',
        r'(제주특별자치도|제주도|제주)\s+(\S+시)',
        r'(세종특별자치시|세종시|세종)',
    ]
    
    for pattern in region_patterns:
        match = re.search(pattern, address)
        if match:
            return match.group(1)
    
    return ""

# 주민센터명 정규화 (시군구 제거)
def normalize_center_name(sido: str, name: str) -> str:
    """주민센터명 정규화 (시군구 제거)"""
    if not name:
        return name
    
    name = name.strip()
    
    # 이미 "주민센터", "행정복지센터" 등이 포함되어 있는 경우 그대로 사용
    if any(keyword in name for keyword in ["주민센터", "행정복지센터", "행정센터"]):
        return f"{sido} {name}"
    
    # 기본적으로 "주민센터" 추가
    return f"{sido} {name} 주민센터"

# 개선된 연락처 추출 청크 처리 (미추출 데이터 전용)
def process_improved_contact_extraction(chunk_df: pd.DataFrame, worker_id: int, phone_patterns: List[str], fax_patterns: List[str], area_codes: Dict) -> List[Dict]:
    """개선된 연락처 추출 청크 처리 (미추출 데이터 전용)"""
    import pandas as pd
    import re
    import time
    import random
    
    results = []
    driver = None
    
    try:
        driver = create_improved_worker_driver(worker_id)
        if not driver:
            return results
        
        print(f"🔧 워커 {worker_id}: 미추출 데이터 연락처 추출 시작 ({len(chunk_df)}개)")
        
        for idx, row in chunk_df.iterrows():
            sido = row.get('시도', '')
            sigungu = row.get('시군구', '')
            name = row.get('읍면동', '')
            address = row.get('주    소', '')
            existing_phone = str(row.get('전화번호', '')).strip()
            existing_fax = str(row.get('팩스번호', '')).strip()
            
            if not name:
                continue
            
            try:
                print(f"📞 워커 {worker_id}: 연락처 검색 - {sido} {name}")
                print(f"📋 워커 {worker_id}: 기존 데이터 - 전화:{existing_phone}, 팩스:{existing_fax}")
                
                # 정규화된 검색 쿼리 생성 (시군구 제거)
                normalized_name = normalize_center_name(sido, name)
                
                valid_phone = existing_phone if existing_phone else None
                valid_fax = existing_fax if existing_fax else None
                
                # 🔍 1. 전화번호 검색 (기존 전화번호가 없는 경우만)
                if not existing_phone:
                    phone_search_queries = [
                        f"{normalized_name} 전화번호",
                        f"{normalized_name} 연락처",
                        f"{normalized_name} 대표전화",
                        f"{sido} {name} 전화번호",
                        f"{name} 전화번호"
                    ]
                    
                    for query_idx, phone_search_query in enumerate(phone_search_queries):
                        print(f"🔍 워커 {worker_id}: 전화번호 검색쿼리 {query_idx+1}/{len(phone_search_queries)} - {phone_search_query}")
                        phone_number = search_phone_number(driver, phone_search_query, phone_patterns)
                        
                        if phone_number and is_valid_phone_format_simple(phone_number) and is_regional_match_simple(phone_number, sido):
                            # 역검색 검증
                            reverse_validation = reverse_search_validation(driver, phone_number, name, sido)
                            validation_monitor.record_reverse_search(
                                reverse_validation['is_valid'], 
                                reverse_validation['reason'], 
                                reverse_validation['confidence']
                            )
                            
                            if reverse_validation['is_valid']:
                            valid_phone = phone_number
                                validation_monitor.record_phone_validation(True)
                                print(f"✅ 워커 {worker_id}: 전화번호 발견 및 역검색 검증 성공 (쿼리 {query_idx+1}) - {name} -> {valid_phone}")
                                print(f"   검증 신뢰도: {reverse_validation['confidence']:.2f}")
                            break
                            else:
                                validation_monitor.record_phone_validation(False, reverse_validation['reason'])
                                print(f"❌ 워커 {worker_id}: 전화번호 역검색 검증 실패 (쿼리 {query_idx+1}) - {name} -> {phone_number}")
                                print(f"   사유: {reverse_validation['reason']}")
                        else:
                            print(f"❌ 워커 {worker_id}: 전화번호 검색 실패 (쿼리 {query_idx+1}) - {name}")
                            if query_idx < len(phone_search_queries) - 1:
                                # 다음 쿼리 시도 전 짧은 대기
                                time.sleep(random.uniform(0.5, 1.0))
                    
                    if not valid_phone:
                        print(f"❌ 워커 {worker_id}: 모든 전화번호 검색 실패 - {name}")
                else:
                    print(f"⏭️ 워커 {worker_id}: 전화번호 이미 존재 - {name} -> {existing_phone}")
                
                # 🔍 2. 팩스번호 검색 (기존 팩스번호가 없는 경우만)
                if not existing_fax:
                    fax_search_queries = [
                        f"{normalized_name} 팩스번호",
                        f"{normalized_name} 팩스",
                        f"{normalized_name} fax",
                        f"{sido} {name} 팩스번호",
                        f"{name} 팩스번호"
                    ]
                    
                    for query_idx, fax_search_query in enumerate(fax_search_queries):
                        print(f"🔍 워커 {worker_id}: 팩스번호 검색쿼리 {query_idx+1}/{len(fax_search_queries)} - {fax_search_query}")
                        fax_number = search_fax_number(driver, fax_search_query, fax_patterns)
                        
                        if fax_number and is_valid_phone_format_simple(fax_number) and is_regional_match_simple(fax_number, sido):
                            # 역검색 검증
                            reverse_validation = reverse_search_validation(driver, fax_number, name, sido, search_type="팩스")
                            validation_monitor.record_reverse_search(
                                reverse_validation['is_valid'], 
                                reverse_validation['reason'], 
                                reverse_validation['confidence']
                            )
                            
                            if reverse_validation['is_valid']:
                            valid_fax = fax_number
                                print(f"✅ 워커 {worker_id}: 팩스번호 발견 및 역검색 검증 성공 (쿼리 {query_idx+1}) - {name} -> {valid_fax}")
                                print(f"   검증 신뢰도: {reverse_validation['confidence']:.2f}")
                            break
                            else:
                                print(f"❌ 워커 {worker_id}: 팩스번호 역검색 검증 실패 (쿼리 {query_idx+1}) - {name} -> {fax_number}")
                                print(f"   사유: {reverse_validation['reason']}")
                        else:
                            print(f"❌ 워커 {worker_id}: 팩스번호 검색 실패 (쿼리 {query_idx+1}) - {name}")
                            if query_idx < len(fax_search_queries) - 1:
                                # 다음 쿼리 시도 전 짧은 대기
                                time.sleep(random.uniform(0.5, 1.0))
                    
                    if not valid_fax:
                        print(f"❌ 워커 {worker_id}: 모든 팩스번호 검색 실패 - {name}")
                else:
                    print(f"⏭️ 워커 {worker_id}: 팩스번호 이미 존재 - {name} -> {existing_fax}")
                
                # 🚨 팩스번호 검증 로직 (전화번호와의 유사성 기준)
                if valid_phone and valid_fax:
                    fax_validation_result = validate_fax_number(valid_fax, valid_phone, sido, name)
                    validation_monitor.record_fax_validation(
                        fax_validation_result['is_valid'], 
                        fax_validation_result['reason'], 
                        fax_validation_result['confidence']
                    )
                    
                    if not fax_validation_result['is_valid']:
                        print(f"⚠️ 워커 {worker_id}: 팩스번호 검증 실패 - {name}")
                        print(f"   사유: {fax_validation_result['reason']}")
                        print(f"   전화번호: {valid_phone}, 팩스번호: {valid_fax}")
                        valid_fax = None  # 검증 실패 시 팩스번호 제거
                    else:
                        print(f"✅ 워커 {worker_id}: 팩스번호 검증 성공 - {name} -> {valid_fax}")
                elif valid_fax and not valid_phone:
                    # 전화번호가 없는 경우 팩스번호만으로 검증
                    standalone_valid = is_valid_phone_format_simple(valid_fax) and is_regional_match_simple(valid_fax, sido)
                    validation_monitor.record_fax_validation(
                        standalone_valid, 
                        "팩스번호 단독 검증" if not standalone_valid else ""
                    )
                    
                    if not standalone_valid:
                        print(f"⚠️ 워커 {worker_id}: 팩스번호 단독 검증 실패 - {name} -> {valid_fax}")
                        valid_fax = None
                
                results.append({
                    'index': idx,
                    'name': f"{sido} {name}",
                    'phone': valid_phone or '',
                    'fax': valid_fax or ''
                })
                
                # 안전한 랜덤 지연
                delay = random.uniform(1.0, 2.0)
                time.sleep(delay)
                
            except Exception as e:
                print(f"❌ 워커 {worker_id}: 연락처 검색 오류 - {name}: {e}")
                
                # 에러 발생 시 더 긴 대기
                error_delay = random.uniform(3.0, 5.0)
                print(f"⏳ 워커 {worker_id}: 에러 발생으로 {error_delay:.1f}초 대기...")
                time.sleep(error_delay)
                
                results.append({
                    'index': idx,
                    'name': f"{sido} {name}",
                    'phone': existing_phone,
                    'fax': existing_fax
                })
                continue
        
        print(f"🎉 워커 {worker_id}: 미추출 데이터 연락처 추출 완료 ({len(results)}개)")
        
    except Exception as e:
        print(f"❌ 워커 {worker_id}: 연락처 추출 프로세스 오류: {e}")
    finally:
        if driver:
            driver.quit()
    
    return results

# 전화번호 전용 구글 검색
def search_phone_number(driver, query: str, phone_patterns: List[str]):
    """전화번호 전용 구글 검색"""
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
        
        # 안전한 랜덤 지연
        delay = random.uniform(1.0, 3.0)
        time.sleep(delay)
        
        # 재시도 로직
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 구글 검색 페이지로 이동
                driver.get('https://www.google.com')
                
                # 추가 대기 시간
                time.sleep(random.uniform(1.5, 2.5))
                
                # 검색창 찾기
                search_box = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.NAME, 'q'))
                )
                
                # 검색어 입력
                search_box.clear()
                for char in query:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.03, 0.08))
                
                # 검색 실행
                search_box.send_keys(Keys.RETURN)
                
                # 결과 페이지 로딩 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'search'))
                )
                
                # 추가 대기 시간
                time.sleep(random.uniform(1.0, 2.0))
                
                # 페이지 소스 가져오기
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                # 전화번호 추출
                phone_number = None
                for pattern in phone_patterns:
                    matches = re.findall(pattern, text_content, re.IGNORECASE)
                    for match in matches:
                        normalized = normalize_phone_simple(match)
                        if is_valid_phone_format_simple(normalized):
                            phone_number = normalized
                            break
                    if phone_number:
                        break
                
                return phone_number
                
            except (TimeoutException, WebDriverException) as e:
                if retry < max_retries - 1:
                    wait_time = random.uniform(5, 10)
                    print(f"⚠️ 전화번호 검색 실패 (재시도 {retry + 1}/{max_retries}), {wait_time:.1f}초 후 재시도: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        return None
        
    except Exception as e:
        print(f"❌ 전화번호 구글 검색 오류: {e}")
        time.sleep(random.uniform(5, 10))
        return None

# 팩스번호 전용 구글 검색
def search_fax_number(driver, query: str, fax_patterns: List[str]):  
    """팩스번호 전용 구글 검색"""
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
        
        # 안전한 랜덤 지연
        delay = random.uniform(1.0, 3.0)
        time.sleep(delay)
        
        # 재시도 로직
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 구글 검색 페이지로 이동
                driver.get('https://www.google.com')
                
                # 추가 대기 시간
                time.sleep(random.uniform(1.5, 2.5))
                
                # 검색창 찾기
                search_box = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.NAME, 'q'))
                )
                
                # 검색어 입력
                search_box.clear()
                for char in query:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.03, 0.08))
                
                # 검색 실행
                search_box.send_keys(Keys.RETURN)
                
                # 결과 페이지 로딩 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'search'))
                )
                
                # 추가 대기 시간
                time.sleep(random.uniform(1.0, 2.0))
                
                # 페이지 소스 가져오기
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                # 팩스번호 추출
                fax_number = None
                for pattern in fax_patterns:
                    matches = re.findall(pattern, text_content, re.IGNORECASE)
                    for match in matches:
                        normalized = normalize_phone_simple(match)
                        if is_valid_phone_format_simple(normalized):
                            fax_number = normalized
                            break
                    if fax_number:
                        break
                
                return fax_number
                
            except (TimeoutException, WebDriverException) as e:
                if retry < max_retries - 1:
                    wait_time = random.uniform(5, 10)
                    print(f"⚠️ 팩스번호 검색 실패 (재시도 {retry + 1}/{max_retries}), {wait_time:.1f}초 후 재시도: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        return None
        
    except Exception as e:
        print(f"❌ 팩스번호 구글 검색 오류: {e}")
        time.sleep(random.uniform(5, 10))
        return None

# 강화된 전화번호 정규화
def normalize_phone_simple(phone: str) -> str:
    """강화된 전화번호 정규화"""
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

# 강화된 전화번호 형식 검사
def is_valid_phone_format_simple(phone: str) -> bool:
    """강화된 전화번호 형식 검사 (KOREAN_AREA_CODES 활용)"""
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
            # 단축번호 (1588, 1599 등)
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

# 강화된 지역 일치성 검사
def is_regional_match_simple(phone: str, sido: str) -> bool:
    """강화된 지역 일치성 검사 (KOREAN_AREA_CODES 활용)"""
    try:
        if not phone or not sido:
            return True  # 데이터가 없으면 허용
        
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
        
        # 지역 매칭 검사 (더 엄격한 검사)
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

# 팩스번호 검증 함수 (전화번호와의 유사성 기준)
def validate_fax_number(fax: str, phone: str, sido: str, institution_name: str) -> Dict[str, Any]:
    """
    팩스번호 검증 (전화번호와의 유사성 기준)
    
    Args:
        fax: 팩스번호
        phone: 전화번호
        sido: 시도
        institution_name: 기관명
        
    Returns:
        Dict: 검증 결과 {'is_valid': bool, 'reason': str, 'confidence': float}
    """
    try:
        if not fax or not phone:
            return {'is_valid': False, 'reason': '전화번호 또는 팩스번호가 없음', 'confidence': 0.0}
        
        # 1. 기본 형식 검증
        if not is_valid_phone_format_simple(fax):
            return {'is_valid': False, 'reason': '팩스번호 형식이 올바르지 않음', 'confidence': 0.0}
        
        # 2. 지역 일치성 검증
        if not is_regional_match_simple(fax, sido):
            return {'is_valid': False, 'reason': '팩스번호가 해당 지역과 일치하지 않음', 'confidence': 0.0}
        
        # 3. 전화번호와 동일한지 확인
        if fax == phone:
            return {'is_valid': False, 'reason': '팩스번호가 전화번호와 동일함', 'confidence': 0.0}
        
        # 4. 전화번호와의 유사성 검증
        similarity_score = calculate_phone_similarity(fax, phone)
        
        # 5. 지역번호 일치성 확인
        fax_digits = re.sub(r'[^\d]', '', fax)
        phone_digits = re.sub(r'[^\d]', '', phone)
        
        # 지역번호 추출
        if fax_digits.startswith('02'):
            fax_area_code = '02'
        elif fax_digits.startswith('0'):
            fax_area_code = fax_digits[:3]
        else:
            fax_area_code = fax_digits[:3]
        
        if phone_digits.startswith('02'):
            phone_area_code = '02'
        elif phone_digits.startswith('0'):
            phone_area_code = phone_digits[:3]
        else:
            phone_area_code = phone_digits[:3]
        
        # 지역번호가 다른 경우 의심스러움
        if fax_area_code != phone_area_code:
            # 인접 지역인지 확인
            adjacent_regions = get_adjacent_regions(phone_area_code)
            if fax_area_code not in adjacent_regions:
                return {
                    'is_valid': False, 
                    'reason': f'팩스번호 지역번호({fax_area_code})가 전화번호 지역번호({phone_area_code})와 다름', 
                    'confidence': 0.2
                }
        
        # 6. 유사성 점수 기반 검증
        confidence = similarity_score
        
        # 지역번호가 같으면 신뢰도 증가
        if fax_area_code == phone_area_code:
            confidence += 0.3
        
        # 번호 패턴 유사성 확인
        if has_similar_pattern(fax_digits, phone_digits):
            confidence += 0.2
        
        # 최종 판정
        if confidence >= 0.6:
            return {'is_valid': True, 'reason': '검증 통과', 'confidence': confidence}
        else:
            return {
                'is_valid': False, 
                'reason': f'전화번호와 유사성이 낮음 (신뢰도: {confidence:.2f})', 
                'confidence': confidence
            }
        
    except Exception as e:
        return {'is_valid': False, 'reason': f'검증 중 오류 발생: {str(e)}', 'confidence': 0.0}

def calculate_phone_similarity(fax: str, phone: str) -> float:
    """전화번호와 팩스번호의 유사성 계산"""
    try:
        fax_digits = re.sub(r'[^\d]', '', fax)
        phone_digits = re.sub(r'[^\d]', '', phone)
        
        if not fax_digits or not phone_digits:
            return 0.0
        
        # 지역번호 제거하고 비교
        if fax_digits.startswith('02'):
            fax_local = fax_digits[2:]
        elif fax_digits.startswith('0'):
            fax_local = fax_digits[3:]
        else:
            fax_local = fax_digits
        
        if phone_digits.startswith('02'):
            phone_local = phone_digits[2:]
        elif phone_digits.startswith('0'):
            phone_local = phone_digits[3:]
        else:
            phone_local = phone_digits
        
        # 연속된 동일 숫자 개수 확인
        common_digits = 0
        min_length = min(len(fax_local), len(phone_local))
        
        for i in range(min_length):
            if fax_local[i] == phone_local[i]:
                common_digits += 1
            else:
                break
        
        # 유사성 점수 계산
        similarity = common_digits / max(len(fax_local), len(phone_local))
        
        return similarity
        
    except Exception:
        return 0.0

def get_adjacent_regions(area_code: str) -> List[str]:
    """인접 지역 코드 반환"""
    adjacent_map = {
        '02': ['031', '032'],  # 서울 - 경기, 인천
        '031': ['02', '032', '033', '041', '043'],  # 경기 - 서울, 인천, 강원, 충남, 충북
        '032': ['02', '031'],  # 인천 - 서울, 경기
        '033': ['031', '043'],  # 강원 - 경기, 충북
        '041': ['031', '042', '043'],  # 충남 - 경기, 대전, 충북
        '042': ['041', '043'],  # 대전 - 충남, 충북
        '043': ['031', '033', '041', '042'],  # 충북 - 경기, 강원, 충남, 대전
        '051': ['052', '055'],  # 부산 - 울산, 경남
        '052': ['051', '053', '054', '055'],  # 울산 - 부산, 대구, 경북, 경남
        '053': ['052', '054', '055'],  # 대구 - 울산, 경북, 경남
        '054': ['052', '053', '055'],  # 경북 - 울산, 대구, 경남
        '055': ['051', '052', '053', '054'],  # 경남 - 부산, 울산, 대구, 경북
        '061': ['062', '063'],  # 전남 - 광주, 전북
        '062': ['061', '063'],  # 광주 - 전남, 전북
        '063': ['061', '062'],  # 전북 - 전남, 광주
        '064': []  # 제주 - 인접 지역 없음
    }
    
    return adjacent_map.get(area_code, [])

def has_similar_pattern(fax_digits: str, phone_digits: str) -> bool:
    """번호 패턴 유사성 확인"""
    try:
        # 뒷자리 4자리 비교
        if len(fax_digits) >= 4 and len(phone_digits) >= 4:
            fax_suffix = fax_digits[-4:]
            phone_suffix = phone_digits[-4:]
            
            # 뒷자리가 연속된 경우 (예: 1234, 1235)
            if abs(int(fax_suffix) - int(phone_suffix)) <= 10:
        return True 
        
        # 중간 자리 패턴 비교
        if len(fax_digits) >= 7 and len(phone_digits) >= 7:
            fax_middle = fax_digits[-7:-4]
            phone_middle = phone_digits[-7:-4]
            
            if fax_middle == phone_middle:
                return True
        
        return False
        
    except Exception:
        return False

# 역검색 검증 함수 (번호로 기관명 확인)
def reverse_search_validation(driver, phone_number: str, institution_name: str, sido: str, search_type: str = "전화") -> Dict[str, Any]:
    """
    전화번호/팩스번호를 역검색하여 기관명과 일치하는지 확인
    
    Args:
        driver: WebDriver 인스턴스
        phone_number: 검색할 전화번호/팩스번호
        institution_name: 기관명
        sido: 시도
        search_type: 검색 타입 ("전화" 또는 "팩스")
        
    Returns:
        Dict: 검증 결과 {'is_valid': bool, 'reason': str, 'confidence': float}
    """
    try:
        if not phone_number or not institution_name:
            return {'is_valid': False, 'reason': '전화번호 또는 기관명이 없음', 'confidence': 0.0}
        
        # 역검색 쿼리 생성 (단순화)
        reverse_queries = [
            f"{phone_number}"
        ]
        
        max_confidence = 0.0
        best_match_reason = ""
        
        for query in reverse_queries:
            try:
                # 구글 검색 실행
                driver.get('https://www.google.com')
                time.sleep(random.uniform(1.0, 2.0))
                
                # 검색창 찾기
                search_box = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.NAME, 'q'))
                )
                
                # 검색어 입력
                search_box.clear()
                search_box.send_keys(query)
                search_box.send_keys(Keys.RETURN)
                
                # 결과 페이지 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'search'))
                )
                
                time.sleep(random.uniform(1.0, 2.0))
                
                # 페이지 소스 가져오기
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                # 기관명 매칭 확인
                confidence = calculate_institution_match_confidence(text_content, institution_name, sido)
                
                if confidence > max_confidence:
                    max_confidence = confidence
                    best_match_reason = f"역검색 쿼리 '{query}'에서 기관명 매칭 확인"
                
                # 높은 신뢰도면 바로 성공 처리
                if confidence >= 0.7:
                    return {
                        'is_valid': True,
                        'reason': best_match_reason,
                        'confidence': confidence
                    }
                
                # 검색 간격
                time.sleep(random.uniform(1.0, 2.0))
                
            except Exception as e:
                print(f"⚠️ 역검색 쿼리 '{query}' 실행 중 오류: {e}")
                continue
        
        # 최종 판정
        if max_confidence >= 0.5:
            return {
                'is_valid': True,
                'reason': best_match_reason,
                'confidence': max_confidence
            }
        else:
            return {
                'is_valid': False,
                'reason': f'역검색에서 기관명 매칭 실패 (최대 신뢰도: {max_confidence:.2f})',
                'confidence': max_confidence
            }
        
    except Exception as e:
        return {'is_valid': False, 'reason': f'역검색 중 오류 발생: {str(e)}', 'confidence': 0.0}

def calculate_institution_match_confidence(text_content: str, institution_name: str, sido: str) -> float:
    """텍스트 내용에서 기관명 매칭 신뢰도 계산"""
    try:
        if not text_content or not institution_name:
            return 0.0
        
        text_lower = text_content.lower()
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
            confidence += 0.2
        
        # 기관명 키워드 매칭
        for keyword in institution_keywords:
            if keyword and keyword.strip():
                if keyword.strip() in text_content:
                    confidence += 0.4
                    break
        
        # 주민센터 관련 키워드 확인
        center_keywords = ["주민센터", "행정복지센터", "행정센터", "동사무소", "구청", "시청"]
        for keyword in center_keywords:
            if keyword in text_content:
                confidence += 0.2
                break
        
        # 주소 관련 키워드 확인
        address_keywords = ["주소", "위치", "찾아오시는길", "오시는길"]
        for keyword in address_keywords:
            if keyword in text_content:
                confidence += 0.1
                break
        
        # 연락처 관련 키워드 확인
        contact_keywords = ["전화", "연락처", "팩스", "문의"]
        for keyword in contact_keywords:
            if keyword in text_content:
                confidence += 0.1
                break
        
        return min(confidence, 1.0)  # 최대 1.0으로 제한
        
    except Exception:
        return 0.0

# 검증 시스템 성능 모니터링 클래스
class ValidationMonitor:
    """검증 시스템 성능 모니터링"""
    
    def __init__(self):
        """검증 모니터 초기화"""
        self.stats = {
            'total_phone_validations': 0,
            'successful_phone_validations': 0,
            'failed_phone_validations': 0,
            'total_fax_validations': 0,
            'successful_fax_validations': 0,
            'failed_fax_validations': 0,
            'total_reverse_searches': 0,
            'successful_reverse_searches': 0,
            'failed_reverse_searches': 0,
            'validation_reasons': {},
            'confidence_scores': []
        }
    
    def record_phone_validation(self, success: bool, reason: str = ""):
        """전화번호 검증 결과 기록"""
        self.stats['total_phone_validations'] += 1
        if success:
            self.stats['successful_phone_validations'] += 1
        else:
            self.stats['failed_phone_validations'] += 1
            if reason:
                self.stats['validation_reasons'][reason] = self.stats['validation_reasons'].get(reason, 0) + 1
    
    def record_fax_validation(self, success: bool, reason: str = "", confidence: float = 0.0):
        """팩스번호 검증 결과 기록"""
        self.stats['total_fax_validations'] += 1
        if success:
            self.stats['successful_fax_validations'] += 1
        else:
            self.stats['failed_fax_validations'] += 1
            if reason:
                self.stats['validation_reasons'][reason] = self.stats['validation_reasons'].get(reason, 0) + 1
        
        if confidence > 0:
            self.stats['confidence_scores'].append(confidence)
    
    def record_reverse_search(self, success: bool, reason: str = "", confidence: float = 0.0):
        """역검색 결과 기록"""
        self.stats['total_reverse_searches'] += 1
        if success:
            self.stats['successful_reverse_searches'] += 1
        else:
            self.stats['failed_reverse_searches'] += 1
            if reason:
                self.stats['validation_reasons'][reason] = self.stats['validation_reasons'].get(reason, 0) + 1
        
        if confidence > 0:
            self.stats['confidence_scores'].append(confidence)
    
    def get_statistics(self) -> Dict[str, Any]:
        """검증 통계 반환"""
        stats = self.stats.copy()
        
        # 성공률 계산
        if stats['total_phone_validations'] > 0:
            stats['phone_success_rate'] = stats['successful_phone_validations'] / stats['total_phone_validations']
        else:
            stats['phone_success_rate'] = 0.0
        
        if stats['total_fax_validations'] > 0:
            stats['fax_success_rate'] = stats['successful_fax_validations'] / stats['total_fax_validations']
        else:
            stats['fax_success_rate'] = 0.0
        
        if stats['total_reverse_searches'] > 0:
            stats['reverse_search_success_rate'] = stats['successful_reverse_searches'] / stats['total_reverse_searches']
        else:
            stats['reverse_search_success_rate'] = 0.0
        
        # 평균 신뢰도 계산
        if stats['confidence_scores']:
            stats['average_confidence'] = sum(stats['confidence_scores']) / len(stats['confidence_scores'])
        else:
            stats['average_confidence'] = 0.0
        
        return stats
    
    def print_statistics(self):
        """검증 통계 출력"""
        stats = self.get_statistics()
        
        print("\n" + "="*60)
        print("🔍 강화된 검증 시스템 성능 통계")
        print("="*60)
        
        print(f"📞 전화번호 검증:")
        print(f"   총 검증 횟수: {stats['total_phone_validations']}")
        print(f"   성공: {stats['successful_phone_validations']} ({stats['phone_success_rate']:.1%})")
        print(f"   실패: {stats['failed_phone_validations']}")
        
        print(f"\n📠 팩스번호 검증:")
        print(f"   총 검증 횟수: {stats['total_fax_validations']}")
        print(f"   성공: {stats['successful_fax_validations']} ({stats['fax_success_rate']:.1%})")
        print(f"   실패: {stats['failed_fax_validations']}")
        
        print(f"\n🔄 역검색 검증:")
        print(f"   총 검증 횟수: {stats['total_reverse_searches']}")
        print(f"   성공: {stats['successful_reverse_searches']} ({stats['reverse_search_success_rate']:.1%})")
        print(f"   실패: {stats['failed_reverse_searches']}")
        
        print(f"\n📊 신뢰도 분석:")
        print(f"   평균 신뢰도: {stats['average_confidence']:.2f}")
        print(f"   신뢰도 샘플 수: {len(stats['confidence_scores'])}")
        
        if stats['validation_reasons']:
            print(f"\n❌ 주요 실패 사유:")
            sorted_reasons = sorted(stats['validation_reasons'].items(), key=lambda x: x[1], reverse=True)
            for reason, count in sorted_reasons[:5]:
                print(f"   {reason}: {count}회")
        
        print("="*60)

# 전역 검증 모니터 인스턴스
validation_monitor = ValidationMonitor()

# AI 모델 관리 클래스 - 4개의 Gemini API 키 지원
class AIModelManager:
    """AI 모델 관리 클래스 - 4개의 Gemini API 키 지원"""
    
    def __init__(self):
        self.gemini_models = []
        self.gemini_config = None
        self.current_model_index = 0
        self.setup_models()
    
    def setup_models(self):
        """4개의 AI 모델 초기화"""
        try:
            # 첫 번째 API 키
            api_key_1 = os.getenv('GEMINI_API_KEY')
            # 두 번째 API 키
            api_key_2 = os.getenv('GEMINI_API_KEY_2')
            # 세 번째 API 키
            api_key_3 = os.getenv('GEMINI_API_KEY_3')
            # 네 번째 API 키
            api_key_4 = os.getenv('GEMINI_API_KEY_4')
            
            if not api_key_1 and not api_key_2 and not api_key_3 and not api_key_4:
                raise ValueError("GEMINI_API_KEY, GEMINI_API_KEY_2, GEMINI_API_KEY_3, 또는 GEMINI_API_KEY_4 환경 변수가 설정되지 않았습니다.")
            
            self.gemini_config = AI_MODEL_CONFIG
            
            # 첫 번째 모델 설정
            if api_key_1:
                try:
                    import google.generativeai as genai
                    genai.configure(api_key=api_key_1)
                    model_1 = genai.GenerativeModel(
                        "gemini-1.5-flash",
                        generation_config=self.gemini_config
                    )
                    self.gemini_models.append({
                        'model': model_1,
                        'api_key': api_key_1[:10] + "...",
                        'name': 'GEMINI_1',
                        'failures': 0
                    })
                    logging.getLogger(__name__).info("🤖 Gemini AI 모델 1 초기화 성공")
                except Exception as e:
                    logging.getLogger(__name__).error(f"❌ Gemini 모델 1 초기화 실패: {e}")
            
            # 두 번째 모델 설정
            if api_key_2:
                try:
                    import google.generativeai as genai
                    genai.configure(api_key=api_key_2)
                    model_2 = genai.GenerativeModel(
                        "gemini-1.5-flash",
                        generation_config=self.gemini_config
                    )
                    self.gemini_models.append({
                        'model': model_2,
                        'api_key': api_key_2[:10] + "...",
                        'name': 'GEMINI_2',
                        'failures': 0
                    })
                    logging.getLogger(__name__).info("🤖 Gemini AI 모델 2 초기화 성공")
                except Exception as e:
                    logging.getLogger(__name__).error(f"❌ Gemini 모델 2 초기화 실패: {e}")
            
            # 세 번째 모델 설정
            if api_key_3:
                try:
                    import google.generativeai as genai
                    genai.configure(api_key=api_key_3)
                    model_3 = genai.GenerativeModel(
                        "gemini-1.5-flash",
                        generation_config=self.gemini_config
                    )
                    self.gemini_models.append({
                        'model': model_3,
                        'api_key': api_key_3[:10] + "...",
                        'name': 'GEMINI_3',
                        'failures': 0
                    })
                    logging.getLogger(__name__).info("🤖 Gemini AI 모델 3 초기화 성공")
                except Exception as e:
                    logging.getLogger(__name__).error(f"❌ Gemini 모델 3 초기화 실패: {e}")
            
            # 네 번째 모델 설정
            if api_key_4:
                try:
                    import google.generativeai as genai
                    genai.configure(api_key=api_key_4)
                    model_4 = genai.GenerativeModel(
                        "gemini-1.5-flash",
                        generation_config=self.gemini_config
                    )
                    self.gemini_models.append({
                        'model': model_4,
                        'api_key': api_key_4[:10] + "...",
                        'name': 'GEMINI_4',
                        'failures': 0
                    })
                    logging.getLogger(__name__).info("🤖 Gemini AI 모델 4 초기화 성공")
                except Exception as e:
                    logging.getLogger(__name__).error(f"❌ Gemini 모델 4 초기화 실패: {e}")
            
            if not self.gemini_models:
                raise ValueError("사용 가능한 Gemini 모델이 없습니다.")
            
            logging.getLogger(__name__).info(f"🎉 총 {len(self.gemini_models)}개의 Gemini 모델 초기화 완료")
            
        except Exception as e:
            logging.getLogger(__name__).error(f"❌ AI 모델 초기화 실패: {e}")
            raise
    
    def get_next_model(self):
        """다음 사용 가능한 모델 선택"""
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
        """Gemini API를 통한 정보 추출 (다중 모델 지원)"""
        if not self.gemini_models:
            return "오류: 사용 가능한 모델이 없습니다."
        
        # 모든 모델을 시도해볼 수 있도록 최대 시도 횟수 설정
        max_attempts = len(self.gemini_models)
        
        for attempt in range(max_attempts):
            current_model = self.get_next_model()
            if not current_model:
                continue
            
            try:
                # 텍스트 길이 제한
                max_length = 32000
                if len(text_content) > max_length:
                    front_portion = int(max_length * 0.67)
                    back_portion = max_length - front_portion
                    text_content = text_content[:front_portion] + "\n... (중략) ...\n" + text_content[-back_portion:]
                
                prompt = prompt_template.format(text_content=text_content)
                
                # 현재 모델로 API 호출
                response = current_model['model'].generate_content(prompt)
                result_text = response.text
                
                # 성공 시 로그 출력
                logger = logging.getLogger(__name__)
                logger.info(f"✅ {current_model['name']} API 성공 - 응답 (일부): {result_text[:200]}...")
                
                return result_text
                
            except Exception as e:
                # 실패 시 다음 모델로 시도
                current_model['failures'] += 1
                logger = logging.getLogger(__name__)
                logger.warning(f"⚠️ {current_model['name']} API 실패 (시도 {attempt + 1}/{max_attempts}): {str(e)}")
                
                if attempt < max_attempts - 1:
                    logger.info(f"🔄 다음 모델로 재시도 중...")
                    continue
                else:
                    logger.error(f"❌ 모든 Gemini 모델 실패")
                    return f"오류: 모든 API 호출 실패 - 마지막 오류: {str(e)}"
        
        return "오류: 모든 모델 시도 실패"
    
    def get_model_status(self) -> str:
        """모델 상태 정보 반환"""
        if not self.gemini_models:
            return "❌ 사용 가능한 모델 없음"
        
        status_info = []
        for model in self.gemini_models:
            status = "✅ 정상" if model['failures'] < 3 else "❌ 실패"
            status_info.append(f"{model['name']}: {status} (실패: {model['failures']}회)")
        
        return " | ".join(status_info)

# 개선된 주민센터 연락처 추출 봇
class ImprovedCommunityCenterCrawler:
    """개선된 주민센터 연락처 추출 봇"""
    
    def __init__(self, csv_path: str, use_ai: bool = True):
        """
        초기화
        
        Args:
            csv_path: 원본 CSV 파일 경로
            use_ai: AI 기능 사용 여부
        """
        self.csv_path = csv_path
        self.use_ai = use_ai
        self.logger = logging.getLogger(__name__)
        
        # 환경 변수 로드
        load_dotenv()
        
        # AI 모델 초기화
        self.ai_model_manager = None
        if self.use_ai:
            self._initialize_ai()
        
        # WebDriver 초기화
        self.driver = None
        self._initialize_webdriver()
        
        # 데이터 로드
        self.df = None
        self._load_data()
        
        # 결과 저장용
        self.results = []
        self.processed_count = 0
        self.success_count = 0
        self.invalid_count = 0
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
        
        # 🚀 멀티프로세싱 설정 (동적 워커 수 조정)
        # AMD Ryzen 5 3600 (6코어 12스레드) 환경에 최적화
        cpu_count = multiprocessing.cpu_count()
        
        # Headless 모드에 따른 워커 수 동적 조정
        if globals().get('HEADLESS_MODE', True):
            self.max_workers = 18  # Headless 모드: 18개 워커 (메모리 효율적)
            self.chunk_size = 12   # 더 큰 청크로 효율성 향상
        else:
            self.max_workers = 12  # GUI 모드: 12개 워커 (안정성 우선)
            self.chunk_size = 8    # 안정적인 청크 크기
        
        # 요청 간격 설정 (초) - 워커 수에 맞게 최적화
        if globals().get('HEADLESS_MODE', True):
            self.request_delay_min = 0.8  # Headless: 더 빠른 요청
            self.request_delay_max = 1.5  # Headless: 더 빠른 요청
        else:
            self.request_delay_min = 1.0  # GUI: 안정적인 요청
            self.request_delay_max = 2.0  # GUI: 안정적인 요청
        
        # 에러 발생 시 대기 시간 (초) - 단축
        self.error_wait_time = 5
        
        # 전화번호/팩스번호 정규식 패턴
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
        self.logger.info(f"🚀 ImprovedCommunityCenterCrawler 초기화 완료")
        self.logger.info(f"🔧 {headless_status} 모드 - 워커: {self.max_workers}개, 청크: {self.chunk_size}개")
        self.logger.info(f"⚡ 요청 간격: {self.request_delay_min}~{self.request_delay_max}초")
        self.logger.info(f"🔧 AMD Ryzen 5 3600 (6코어 12스레드) 환경에 최적화된 설정 적용")
    
    def _initialize_ai(self):
        """AI 모델 초기화"""
        try:
            self.ai_model_manager = AIModelManager()
            self.logger.info("🤖 AI 모델 관리자 초기화 완료")
            # 모델 상태 로그
            status = self.ai_model_manager.get_model_status()
            self.logger.info(f"🔍 AI 모델 상태: {status}")
        except Exception as e:
            self.logger.error(f"❌ AI 모델 초기화 실패: {e}")
            self.use_ai = False
    
    def _initialize_webdriver(self):
        """WebDriver 초기화"""
        try:
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
            
            # Headless 모드 설정 (전역 변수로 제어)
            if globals().get('HEADLESS_MODE', True):
                chrome_options.add_argument('--headless')
            
            # 리소스 절약 옵션
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
            
            # 메모리 제한
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=512')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
            
            self.driver = uc.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(20)
            
            # 메모리 관리를 위한 초기 가비지 컬렉션
            import gc
            gc.collect()
            
            self.logger.info("🌐 WebDriver 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 초기화 실패: {e}")
            raise
    
    def _load_data(self):
        """CSV 데이터 로드 (미추출 데이터 전용)"""
        try:
            # CSV 파일 읽기 (인코딩 문제 해결)
            try:
                self.df = pd.read_csv(self.csv_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    self.df = pd.read_csv(self.csv_path, encoding='cp949')
                except UnicodeDecodeError:
                    self.df = pd.read_csv(self.csv_path, encoding='euc-kr')
            
            self.logger.info(f"📊 미추출 데이터 로드 완료: {len(self.df)}개 주민센터")
            
            # 컬럼 확인 및 정리
            required_columns = ['연번', '시도', '시군구', '읍면동', '우편번호', '주    소', '전화번호', '팩스번호']
            missing_columns = [col for col in required_columns if col not in self.df.columns]
            
            if missing_columns:
                self.logger.error(f"❌ 필수 컬럼 누락: {missing_columns}")
                raise ValueError(f"필수 컬럼이 누락되었습니다: {missing_columns}")
            
            # 전화번호, 팩스번호 컬럼이 이미 존재하므로 NaN 값을 빈 문자열로 변환
            self.df['전화번호'] = self.df['전화번호'].fillna('')
            self.df['팩스번호'] = self.df['팩스번호'].fillna('')
            
            # 데이터 전처리
            self.df = self.df.dropna(subset=['읍면동'])  # 읍면동이 없는 행 제거
            
            # 미추출 데이터 통계
            total_count = len(self.df)
            phone_missing = len(self.df[self.df['전화번호'].str.strip() == ''])
            fax_missing = len(self.df[self.df['팩스번호'].str.strip() == ''])
            both_missing = len(self.df[(self.df['전화번호'].str.strip() == '') & (self.df['팩스번호'].str.strip() == '')])
            
            self.logger.info(f"✅ 미추출 데이터 전처리 완료: {total_count}개 주민센터")
            self.logger.info(f"📊 미추출 통계 - 전화번호 누락: {phone_missing}개, 팩스번호 누락: {fax_missing}개, 둘 다 누락: {both_missing}개")
            
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
        overload_count = 0
        
        while self.monitoring_active:
            try:
                cpu_percent = self.process.cpu_percent()
                memory_info = self.process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                
                system_memory = psutil.virtual_memory()
                memory_percent = (memory_info.rss / system_memory.total) * 100
                
                # 전체 시스템 리소스 확인
                system_cpu = psutil.cpu_percent(interval=1)
                system_memory_percent = system_memory.percent
                
                self.system_stats.update({
                    'cpu_percent': cpu_percent,
                    'memory_mb': memory_mb,
                    'memory_percent': memory_percent,
                    'system_cpu': system_cpu,
                    'system_memory': system_memory_percent
                })
                
                # 과부하 감지
                if system_cpu > 80 or system_memory_percent > 90:
                    overload_count += 1
                    if overload_count >= 2:
                        self.logger.warning(f"🚨 시스템 과부하 감지! CPU: {system_cpu:.1f}%, 메모리: {system_memory_percent:.1f}%")
                        self.logger.warning("⏳ 시스템 안정화를 위해 30초 대기...")
                        time.sleep(30)
                        overload_count = 0
                else:
                    overload_count = 0
                
                # 프로세스 과부하 체크
                if cpu_percent > 70 or memory_percent > 30:
                    self.logger.warning(f"⚠️ 프로세스 리소스 높음 - CPU: {cpu_percent:.1f}%, 메모리: {memory_percent:.1f}%")
                
                time.sleep(30)
                
            except Exception as e:
                self.logger.error(f"❌ 시스템 모니터링 오류: {e}")
                time.sleep(30)
    
    def _log_system_stats(self, stage: str):
        """시스템 통계 로깅"""
        try:
            stats = self.system_stats
            self.logger.info(f"📊 [{stage}] CPU: {stats['cpu_percent']:.1f}%, "
                           f"메모리: {stats['memory_mb']:.1f}MB ({stats['memory_percent']:.1f}%)")
        except Exception as e:
            self.logger.error(f"❌ 시스템 통계 로깅 오류: {e}")
    
    def _cleanup_memory(self):
        """메모리 정리"""
        try:
            import gc
            gc.collect()
            
            if self.driver:
                # 브라우저 캐시 정리
                self.driver.execute_script("window.localStorage.clear();")
                self.driver.execute_script("window.sessionStorage.clear();")
                
            # 임시 데이터 정리
            self.results = []
            
        except Exception as e:
            self.logger.error(f"❌ 메모리 정리 실패: {e}")
    
    def _cleanup(self):
        """정리 작업"""
        try:
            self.monitoring_active = False
            if self.monitoring_thread:
                self.monitoring_thread.join(timeout=1)
            
            if self.driver:
                self.driver.quit()
                self.logger.info("🧹 WebDriver 정리 완료")
                
            self.logger.info("🧹 시스템 정리 완료")
        except Exception as e:
            self.logger.error(f"❌ 정리 작업 오류: {e}")
    
    def run_extraction(self):
        """전체 추출 프로세스 실행"""
        try:
            self.logger.info("🎯 주민센터 연락처 추출 시작")
            self._log_system_stats("프로세스 시작")
            
            # 1단계: 병렬 연락처 추출
            self.logger.info(f"📞 1단계: 병렬 연락처 추출 ({self.max_workers}개 워커)")
            self._extract_contacts_parallel()
            self._log_system_stats("1단계 완료")
            
            # 2단계: 최종 결과 저장
            self.logger.info("💾 2단계: 최종 결과 저장 (Excel 형식)")
            result_path = self._save_results()
            self._log_system_stats("결과 저장 완료")
            
            self.logger.info("🎉 전체 추출 프로세스 완료")
            
        except KeyboardInterrupt:
            self.logger.info("⚠️ 사용자 중단 요청 감지")
            self._save_intermediate_results("사용자중단저장")
            raise
        except Exception as e:
            self.logger.error(f"❌ 추출 프로세스 실패: {e}")
            self._save_intermediate_results("오류발생저장")
            raise
        finally:
            self._cleanup()
    
    def _extract_contacts_parallel(self):
        """병렬 연락처 추출 (미추출 데이터 전용)"""
        # 미추출 데이터는 모든 행을 처리하되, 각 행에서 누락된 번호만 검색
        total_rows = len(self.df)
        
        if total_rows == 0:
            self.logger.info("📞 연락처 추출할 데이터가 없습니다.")
            return
        
        # 데이터를 워커 수만큼 분할
        chunks = self._split_dataframe(self.df, self.max_workers)
        
        self.logger.info(f"📞 미추출 데이터 병렬 처리 시작: {total_rows}개 데이터를 {len(chunks)}개 프로세스로 처리")
        
        # 멀티프로세싱으로 병렬 처리
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for i, chunk in enumerate(chunks):
                future = executor.submit(
                    process_improved_contact_extraction,
                    chunk,
                    i,
                    self.phone_patterns,
                    self.fax_patterns,
                    KOREAN_AREA_CODES
                )
                futures.append(future)
            
            # 결과 수집
            for future in as_completed(futures):
                try:
                    results = future.result()
                    self._merge_extraction_results(results)
                except Exception as e:
                    self.logger.error(f"❌ 연락처 추출 프로세스 오류: {e}")
        
        # 중간 저장
        self._save_intermediate_results("미추출데이터처리_완료")
        self.logger.info("📞 미추출 데이터 병렬 처리 완료")
    
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
    
    def _merge_extraction_results(self, results: List[Dict]):
        """추출 결과를 메인 데이터프레임에 병합 (미추출 데이터 전용)"""
        try:
            for result in results:
                idx = result['index']
                phone = result.get('phone', '')
                fax = result.get('fax', '')
                name = result.get('name', 'Unknown')
                
                # 기존 데이터 확인
                existing_phone = str(self.df.at[idx, '전화번호']).strip()
                existing_fax = str(self.df.at[idx, '팩스번호']).strip()
                
                # 전화번호 업데이트 (기존에 없었고 새로 찾은 경우)
                if phone and phone.strip() and not existing_phone:
                    self.df.at[idx, '전화번호'] = phone
                    self.success_count += 1
                    self.logger.info(f"✅ 전화번호 신규 발견: {name} -> {phone}")
                elif phone and phone.strip() and existing_phone:
                    # 기존 번호와 다른 경우 로그만 출력
                    if phone != existing_phone:
                        self.logger.info(f"🔄 전화번호 변경 감지: {name} -> 기존:{existing_phone}, 신규:{phone}")
                    self.df.at[idx, '전화번호'] = phone
                
                # 팩스번호 업데이트 (기존에 없었고 새로 찾은 경우)
                if fax and fax.strip() and not existing_fax:
                    self.df.at[idx, '팩스번호'] = fax
                    self.success_count += 1
                    self.logger.info(f"✅ 팩스번호 신규 발견: {name} -> {fax}")
                elif fax and fax.strip() and existing_fax:
                    # 기존 번호와 다른 경우 로그만 출력
                    if fax != existing_fax:
                        self.logger.info(f"🔄 팩스번호 변경 감지: {name} -> 기존:{existing_fax}, 신규:{fax}")
                    self.df.at[idx, '팩스번호'] = fax
                
                # 둘 다 찾지 못한 경우
                if (not phone or not phone.strip()) and (not fax or not fax.strip()) and not existing_phone and not existing_fax:
                    self.invalid_count += 1
                    self.logger.info(f"❌ 연락처 검색 실패: {name}")
                
                self.processed_count += 1
                self.intermediate_save_counter += 1
                
                # 진행률 표시
                total_count = len(self.df)
                progress_percent = (self.processed_count / total_count) * 100
                self.logger.info(f"📊 진행률: {self.processed_count}/{total_count} ({progress_percent:.1f}%) - 성공: {self.success_count}개, 실패: {self.invalid_count}개")
                
                # 50개 단위로 중간 저장
                if self.intermediate_save_counter >= INTERMEDIATE_SAVE_INTERVAL:
                    self._save_intermediate_results(f"미추출데이터중간저장_{self.processed_count}개처리")
                    self.intermediate_save_counter = 0
                    self.logger.info(f"💾 중간 저장 완료: {self.processed_count}개 처리됨")
                
        except Exception as e:
            self.logger.error(f"❌ 결과 병합 오류: {e}")
    
    def _save_results(self) -> str:
        """최종 결과 저장 (Excel 형식) - 미추출 데이터 전용"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(self.csv_path))[0]
            result_filename = f"{base_name}_추출완료_{timestamp}.xlsx"
            result_path = os.path.join(os.path.dirname(self.csv_path), result_filename)
            
            self.df.to_excel(result_path, index=False)
            
            # 통계 정보
            total_count = len(self.df)
            phone_count = len(self.df[self.df['전화번호'].notna() & (self.df['전화번호'].str.strip() != '')])
            fax_count = len(self.df[self.df['팩스번호'].notna() & (self.df['팩스번호'].str.strip() != '')])
            phone_missing = total_count - phone_count
            fax_missing = total_count - fax_count
            both_complete = len(self.df[(self.df['전화번호'].notna() & (self.df['전화번호'].str.strip() != '')) & 
                                       (self.df['팩스번호'].notna() & (self.df['팩스번호'].str.strip() != ''))])
            
            self.logger.info(f"💾 미추출 데이터 최종 결과 저장 완료 (Excel): {result_path}")
            self.logger.info(f"📊 미추출 데이터 처리 최종 통계:")
            self.logger.info(f"  - 전체 미추출 주민센터 수: {total_count}")
            self.logger.info(f"  - 전화번호 확보: {phone_count} ({phone_count/total_count*100:.1f}%)")
            self.logger.info(f"  - 팩스번호 확보: {fax_count} ({fax_count/total_count*100:.1f}%)")
            self.logger.info(f"  - 전화번호 여전히 미확보: {phone_missing} ({phone_missing/total_count*100:.1f}%)")
            self.logger.info(f"  - 팩스번호 여전히 미확보: {fax_missing} ({fax_missing/total_count*100:.1f}%)")
            self.logger.info(f"  - 전화+팩스 모두 확보: {both_complete} ({both_complete/total_count*100:.1f}%)")
            self.logger.info(f"  - 처리된 기관 수: {self.processed_count}")
            self.logger.info(f"  - 신규 추출 성공: {self.success_count}")
            self.logger.info(f"  - 검색 실패: {self.invalid_count}")
            
            return result_path
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 오류: {e}")
            raise
    
    def _save_intermediate_results(self, suffix: str = "중간저장"):
        """중간 결과 저장 (Excel 형식)"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(self.csv_path))[0]
            result_filename = f"{base_name}_{suffix}_{timestamp}.xlsx"
            result_path = os.path.join(os.path.dirname(self.csv_path), result_filename)
            
            self.df.to_excel(result_path, index=False)
            
            total_count = len(self.df)
            phone_count = len(self.df[self.df['전화번호'].notna() & (self.df['전화번호'] != '')])
            fax_count = len(self.df[self.df['팩스번호'].notna() & (self.df['팩스번호'] != '')])
            
            self.logger.info(f"💾 중간 저장 완료 (Excel): {result_path}")
            self.logger.info(f"📊 현재 통계 - 전체: {total_count}, 전화: {phone_count}, 팩스: {fax_count}")
            
            return result_path
            
        except Exception as e:
            self.logger.error(f"❌ 중간 저장 오류: {e}")
            return None 
    

 

# 메인 실행 함수 (미추출 데이터 전용)
def main():
    """메인 실행 함수 (미추출 데이터 전용)"""
    try:
        print("🚀 주민센터 미추출 데이터 연락처 추출 시스템 시작")
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
        
        # 미추출 CSV 파일 경로 설정
        csv_path = r"C:\Users\MyoengHo Shin\pjt\cradcrawlpython\rawdatafile\행정안전부_읍면동 하부행정기관 현황_20240731.csv"
        
        # 파일 존재 확인
        if not os.path.exists(csv_path):
            print(f"❌ 미추출 CSV 파일을 찾을 수 없습니다: {csv_path}")
            return
        
        print(f"📁 미추출 데이터 파일 경로: {csv_path}")
        
        # 봇 초기화 및 실행
        bot = ImprovedCommunityCenterCrawler(csv_path, use_ai=True)
        bot.run_extraction()
        
        print("=" * 60)
        print("✅ 주민센터 미추출 데이터 연락처 추출 완료!")
        
        # 강화된 검증 시스템 성능 통계 출력
        validation_monitor.print_statistics()
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        # 중단 시에도 통계 출력
        validation_monitor.print_statistics()
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        import traceback
        traceback.print_exc()
        # 오류 시에도 통계 출력
        validation_monitor.print_statistics()

if __name__ == "__main__":
    main() 