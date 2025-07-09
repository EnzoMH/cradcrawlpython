#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
저사양 환경 최적화된 교회 크롤러
- Intel Celeron G1610 (2코어) 환경 최적화
- 단일 프로세스 처리
- 메모리 사용량 최소화
- Chrome 브라우저 최적화
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
from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# Selenium 관련
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import undetected_chromedriver as uc
import random

# AI 관련
import google.generativeai as genai
from dotenv import load_dotenv

# 한국 지역번호 매핑
KOREAN_AREA_CODES = {
    "02": "서울", "031": "경기", "032": "인천", "033": "강원",
    "041": "충남", "042": "대전", "043": "충북", "044": "세종",
    "051": "부산", "052": "울산", "053": "대구", "054": "경북", 
    "055": "경남", "061": "전남", "062": "광주", "063": "전북", 
    "064": "제주", "070": "인터넷전화", "010": "핸드폰"
}

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('church_crawler_optimized.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class LowSpecChurchCrawler:
    """저사양 환경 최적화된 교회 크롤러"""
    
    def __init__(self, excel_path: str):
        self.excel_path = excel_path
        self.logger = logging.getLogger(__name__)
        
        # 환경 변수 로드
        load_dotenv()
        
        # Gemini API 설정
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        if self.gemini_api_key:
            genai.configure(api_key=self.gemini_api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash')
            self.use_ai = True
            self.logger.info("🤖 Gemini AI 모델 초기화 성공")
        else:
            self.use_ai = False
            self.logger.warning("⚠️ Gemini API 키가 없어 AI 기능 비활성화")
        
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
        self.start_time = datetime.now()
        
        # 저사양 환경 설정
        self.request_delay_min = 3.0  # 최소 3초
        self.request_delay_max = 5.0  # 최대 5초
        self.memory_cleanup_interval = 20  # 20개마다 메모리 정리
        
        # 팩스번호 정규식 패턴
        self.fax_patterns = [
            r'팩스[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'F[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'전송[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*팩스',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*fax',
        ]
        
        # 시스템 모니터링 시작
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(target=self._monitor_system, daemon=True)
        self.monitoring_thread.start()
        
        self.logger.info("🚀 저사양 최적화된 교회 크롤러 초기화 완료")
    
    def _initialize_webdriver(self):
        """저사양 환경 최적화된 WebDriver 초기화"""
        try:
            chrome_options = uc.ChromeOptions()
            
            # 기본 옵션
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=800,600')  # 작은 윈도우
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            
            # 🛡️ 저사양 환경 메모리/CPU 최적화
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-images')  # 이미지 로딩 비활성화
            chrome_options.add_argument('--disable-javascript')  # 자바스크립트 비활성화
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
            chrome_options.add_argument('--disk-cache-size=1')  # 디스크 캐시 최소화
            chrome_options.add_argument('--media-cache-size=1')  # 미디어 캐시 최소화
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--disable-application-cache')
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=128')  # JS 힙 크기 제한
            chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
            
            self.driver = uc.Chrome(options=chrome_options)
            self.driver.implicitly_wait(8)  # 단축
            self.driver.set_page_load_timeout(15)  # 페이지 로드 타임아웃 단축
            
            # 메모리 관리를 위한 초기 가비지 컬렉션
            import gc
            gc.collect()
            
            self.logger.info("🌐 저사양 최적화된 WebDriver 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 초기화 실패: {e}")
            raise
    
    def _load_data(self):
        """Excel 데이터 로드"""
        try:
            # academy.xlsx 파일 읽기
            if not os.path.exists(self.excel_path):
                # church_crawler.py가 있는 경우 academy.xlsx 찾기
                self.excel_path = 'academy.xlsx'
            
            self.df = pd.read_excel(self.excel_path)
            self.logger.info(f"📊 데이터 로드 완료: {len(self.df)}개 교회")
            
            # 컬럼명 확인 및 정규화
            if '기관명' in self.df.columns:
                self.df = self.df.rename(columns={
                    '기관명': 'name',
                    '주소': 'address', 
                    '전화번호': 'phone',
                    '팩스번호': 'fax',
                    '홈페이지': 'homepage'
                })
            
            # 누락된 컬럼 추가
            for col in ['name', 'address', 'phone', 'fax', 'homepage']:
                if col not in self.df.columns:
                    self.df[col] = ''
            
            # NaN 값 처리
            self.df = self.df.fillna('')
            
            self.logger.info("✅ 데이터 전처리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def run_extraction(self):
        """전체 추출 프로세스 실행 (단일 프로세스)"""
        try:
            self.logger.info("🎯 저사양 최적화 교회 데이터 크롤링 시작")
            
            # 홈페이지가 없는 교회들을 우선 처리 (구글 검색)
            self._process_churches_without_homepage()
            
            # 홈페이지가 있는 교회들 처리
            self._process_churches_with_homepage()
            
            # 결과 저장
            result_path = self._save_results()
            
            self.logger.info("🎉 전체 크롤링 프로세스 완료")
            return result_path
            
        except KeyboardInterrupt:
            self.logger.info("⚠️ 사용자 중단 요청 감지")
            self._save_intermediate_results("사용자중단저장")
            raise
        except Exception as e:
            self.logger.error(f"❌ 크롤링 프로세스 실패: {e}")
            self._save_intermediate_results("오류발생저장")
            raise
        finally:
            self._cleanup()
    
    def _process_churches_without_homepage(self):
        """홈페이지가 없는 교회들 처리 (구글 검색)"""
        no_homepage_rows = self.df[
            (self.df['fax'].isna() | (self.df['fax'] == '')) & 
            (self.df['homepage'].isna() | (self.df['homepage'] == ''))
        ]
        
        self.logger.info(f"📞 홈페이지 없는 교회 구글 검색: {len(no_homepage_rows)}개")
        
        for idx, row in no_homepage_rows.iterrows():
            try:
                name = str(row['name']).strip()
                address = str(row['address']).strip()
                phone = str(row['phone']).strip()
                
                if not name:
                    continue
                
                self.logger.info(f"🔍 구글 검색: {name}")
                
                # 구글 검색으로 팩스번호 찾기
                fax_number = self._search_google_for_fax(name, address)
                
                if fax_number and self._validate_fax_number(fax_number, phone, address, name):
                    self.df.at[idx, 'fax'] = fax_number
                    self.success_count += 1
                    self.logger.info(f"[구글 검색 성공] {name} -> {fax_number}")
                else:
                    self.logger.info(f"[구글 검색 실패] {name} -> 팩스번호 없음")
                
                self.processed_count += 1
                
                # 메모리 정리
                if self.processed_count % self.memory_cleanup_interval == 0:
                    self._cleanup_memory()
                
                # 요청 간격
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                self.logger.error(f"❌ 구글 검색 오류: {name} - {e}")
                continue
    
    def _process_churches_with_homepage(self):
        """홈페이지가 있는 교회들 처리"""
        homepage_rows = self.df[
            (self.df['fax'].isna() | (self.df['fax'] == '')) & 
            (self.df['homepage'].notna() & (self.df['homepage'] != ''))
        ]
        
        self.logger.info(f"🔍 홈페이지 크롤링: {len(homepage_rows)}개")
        
        for idx, row in homepage_rows.iterrows():
            try:
                name = str(row['name']).strip()
                homepage = str(row['homepage']).strip()
                phone = str(row['phone']).strip()
                address = str(row['address']).strip()
                
                if not name or not homepage:
                    continue
                
                self.logger.info(f"🔍 홈페이지 크롤링: {name} -> {homepage}")
                
                # 홈페이지 크롤링
                page_data = self._crawl_homepage(homepage)
                fax_number = None
                
                if page_data:
                    # HTML에서 직접 팩스번호 추출
                    fax_numbers = self._extract_fax_from_html(page_data['html'])
                    for fax in fax_numbers:
                        if self._validate_fax_number(fax, phone, address, name):
                            fax_number = fax
                            break
                    
                    # AI로 추가 추출 시도
                    if not fax_number and self.use_ai:
                        ai_fax = self._extract_fax_with_ai(name, page_data)
                        if ai_fax and self._validate_fax_number(ai_fax, phone, address, name):
                            fax_number = ai_fax
                
                if fax_number:
                    self.df.at[idx, 'fax'] = fax_number
                    self.success_count += 1
                    self.logger.info(f"[홈페이지 성공] {name} -> {fax_number}")
                else:
                    self.logger.info(f"[홈페이지 실패] {name} -> 팩스번호 없음")
                
                self.processed_count += 1
                
                # 메모리 정리
                if self.processed_count % self.memory_cleanup_interval == 0:
                    self._cleanup_memory()
                
                # 요청 간격
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                self.logger.error(f"❌ 홈페이지 크롤링 오류: {name} - {e}")
                continue
    
    def _search_google_for_fax(self, name: str, address: str) -> Optional[str]:
        """구글 검색으로 팩스번호 찾기"""
        try:
            # 검색 쿼리 생성
            region = self._get_region_from_address(address)
            if region:
                search_query = f"{region} {name} 팩스번호"
            else:
                search_query = f"{name} 교회 팩스번호"
            
            # 구글 검색
            self.driver.get('https://www.google.com')
            time.sleep(2)
            
            # 검색창 찾기
            search_box = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            
            # 검색어 입력
            search_box.clear()
            search_box.send_keys(search_query)
            search_box.send_keys(Keys.RETURN)
            
            # 결과 대기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            time.sleep(2)
            
            # 페이지 소스에서 팩스번호 추출
            page_source = self.driver.page_source
            fax_numbers = self._extract_fax_from_html(page_source)
            
            # 유효한 팩스번호 반환
            for fax in fax_numbers:
                if self._is_valid_phone_format(fax):
                    return fax
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 구글 검색 실패: {search_query} - {e}")
            return None
    
    def _crawl_homepage(self, url: str) -> Optional[Dict[str, Any]]:
        """홈페이지 크롤링"""
        try:
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url
            
            self.driver.get(url)
            time.sleep(3)
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            text_content = soup.get_text(separator=' ', strip=True)
            
            return {
                'url': url,
                'html': page_source,
                'text_content': text_content
            }
            
        except Exception as e:
            self.logger.error(f"❌ 홈페이지 크롤링 오류: {url} - {e}")
            return None
    
    def _extract_fax_from_html(self, html_content: str) -> List[str]:
        """HTML에서 팩스번호 추출"""
        try:
            fax_numbers = []
            for pattern in self.fax_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    normalized = self._normalize_phone_number(match)
                    if normalized and normalized not in fax_numbers:
                        fax_numbers.append(normalized)
            
            return fax_numbers
            
        except Exception as e:
            self.logger.error(f"❌ HTML 팩스번호 추출 오류: {e}")
            return []
    
    def _extract_fax_with_ai(self, name: str, page_data: Dict[str, Any]) -> Optional[str]:
        """AI를 통한 팩스번호 추출"""
        if not self.use_ai:
            return None
        
        try:
            prompt = f"""
'{name}' 교회의 홈페이지에서 팩스번호를 찾아주세요.

홈페이지 내용:
{page_data.get('text_content', '')[:3000]}

요청:
이 교회의 팩스번호를 찾아서 다음 형식으로만 응답해주세요:
- 팩스번호가 있으면: 팩스번호만 (예: 02-1234-5678)
- 팩스번호가 없으면: "없음"
"""
            
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            if "없음" in result.lower():
                return None
            
            # 팩스번호 패턴 추출
            for pattern in self.fax_patterns:
                matches = re.findall(pattern, result, re.IGNORECASE)
                for match in matches:
                    normalized = self._normalize_phone_number(match)
                    if self._is_valid_phone_format(normalized):
                        return normalized
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ AI 팩스번호 추출 오류: {name} - {e}")
            return None
    
    def _validate_fax_number(self, fax_number: str, phone_number: str, address: str, name: str) -> bool:
        """팩스번호 유효성 검증 (완화된 버전)"""
        try:
            if not fax_number:
                return False
            
            normalized_fax = self._normalize_phone_number(fax_number)
            
            # 1. 형식 검증
            if not self._is_valid_phone_format(normalized_fax):
                return False
            
            # 2. 전화번호와 동일한지 확인 (동일해도 허용)
            if phone_number:
                normalized_phone = self._normalize_phone_number(phone_number)
                if normalized_fax == normalized_phone:
                    self.logger.info(f"[동일번호 허용] {name}: {normalized_fax}")
                    return True  # 동일한 번호도 허용
            
            # 3. 지역 일치성 검사 (완화)
            if address:
                fax_region = self._get_region_from_phone(normalized_fax)
                address_region = self._get_region_from_address(address)
                if fax_region and address_region and fax_region != address_region:
                    self.logger.info(f"[지역불일치 허용] {name}: 팩스={fax_region}, 주소={address_region}")
                    # 지역 불일치도 허용
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 팩스번호 검증 오류: {name} - {e}")
            return False
    
    def _normalize_phone_number(self, phone: str) -> str:
        """전화번호 정규화"""
        if not phone:
            return ""
        
        numbers = re.findall(r'\d+', str(phone))
        if not numbers:
            return phone
        
        if len(numbers) >= 3:
            return f"{numbers[0]}-{numbers[1]}-{numbers[2]}"
        elif len(numbers) == 2:
            return f"{numbers[0]}-{numbers[1]}"
        else:
            return numbers[0]
    
    def _is_valid_phone_format(self, phone: str) -> bool:
        """전화번호 형식 유효성 검사"""
        try:
            if not phone:
                return False
            
            digits = re.sub(r'[^\d]', '', phone)
            if len(digits) < 8 or len(digits) > 11:
                return False
            
            # 유효한 지역번호 패턴
            valid_patterns = [
                r'^02\d{7,8}$',  # 서울
                r'^0[3-6]\d{7,8}$',  # 지역번호
                r'^070\d{7,8}$',  # 인터넷전화
                r'^1[5-9]\d{6,7}$',  # 특수번호
                r'^080\d{7,8}$',  # 무료전화
            ]
            
            for pattern in valid_patterns:
                if re.match(pattern, digits):
                    return True
            
            return False
            
        except Exception:
            return False
    
    def _get_region_from_phone(self, phone: str) -> str:
        """전화번호에서 지역 정보 추출"""
        try:
            digits = re.sub(r'[^\d]', '', phone)
            if len(digits) >= 10:
                area_code = digits[:2] if digits.startswith('02') else digits[:3]
            else:
                area_code = digits[:2] if digits.startswith('02') else digits[:3]
            
            return KOREAN_AREA_CODES.get(area_code, "")
        except:
            return ""
    
    def _get_region_from_address(self, address: str) -> str:
        """주소에서 지역 정보 추출"""
        if not address:
            return ""
        
        for region in KOREAN_AREA_CODES.values():
            if region in address and region not in ["핸드폰", "인터넷전화"]:
                return region
        
        return ""
    
    def _cleanup_memory(self):
        """메모리 정리"""
        try:
            import gc
            gc.collect()
            
            if self.driver:
                # 브라우저 캐시 정리
                try:
                    self.driver.execute_script("window.localStorage.clear();")
                    self.driver.execute_script("window.sessionStorage.clear();")
                except:
                    pass
            
            self.logger.info(f"🧹 메모리 정리 완료 (처리: {self.processed_count})")
            
        except Exception as e:
            self.logger.error(f"❌ 메모리 정리 실패: {e}")
    
    def _monitor_system(self):
        """시스템 모니터링 (저사양 환경용)"""
        while self.monitoring_active:
            try:
                # CPU/메모리 사용률 확인
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                
                # 🚨 저사양 환경 임계값
                if cpu_percent > 70 or memory.percent > 90:
                    self.logger.warning(f"🚨 시스템 과부하! CPU: {cpu_percent:.1f}%, 메모리: {memory.percent:.1f}%")
                    self.logger.warning("⏳ 시스템 안정화를 위해 30초 대기...")
                    time.sleep(30)
                
                time.sleep(30)  # 30초마다 체크
                
            except Exception as e:
                self.logger.error(f"❌ 시스템 모니터링 오류: {e}")
                time.sleep(30)
    
    def _save_results(self) -> str:
        """결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_filename = f"church_data_optimized_{timestamp}.xlsx"
            
            self.df.to_excel(result_filename, index=False)
            
            # 통계 정보
            total_count = len(self.df)
            fax_count = len(self.df[self.df['fax'].notna() & (self.df['fax'] != '')])
            
            self.logger.info(f"💾 결과 저장 완료: {result_filename}")
            self.logger.info(f"📊 최종 통계:")
            self.logger.info(f"  - 전체 교회 수: {total_count}")
            self.logger.info(f"  - 팩스번호 보유: {fax_count} ({fax_count/total_count*100:.1f}%)")
            self.logger.info(f"  - 처리된 교회 수: {self.processed_count}")
            self.logger.info(f"  - 성공 추출 수: {self.success_count}")
            
            return result_filename
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 오류: {e}")
            raise
    
    def _save_intermediate_results(self, suffix: str):
        """중간 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"church_data_{suffix}_{timestamp}.xlsx"
            
            self.df.to_excel(filename, index=False)
            self.logger.info(f"💾 중간 저장 완료: {filename}")
            
        except Exception as e:
            self.logger.error(f"❌ 중간 저장 오류: {e}")
    
    def _cleanup(self):
        """정리 작업"""
        try:
            self.monitoring_active = False
            
            if self.driver:
                self.driver.quit()
                self.logger.info("🧹 WebDriver 정리 완료")
            
            self.logger.info("🧹 시스템 정리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 정리 작업 오류: {e}")


def main():
    """메인 실행 함수"""
    try:
        print("🚀 저사양 최적화 교회 크롤러 시작")
        print("=" * 50)
        
        # 크롤러 초기화 및 실행
        crawler = LowSpecChurchCrawler("academy.xlsx")
        result_path = crawler.run_extraction()
        
        print("=" * 50)
        print(f"✅ 크롤링 완료! 결과: {result_path}")
        
        # 완료 후 PC 종료 (옵션)
        print("작업이 완료되었습니다. 1분 후 PC가 종료됩니다.")
        time.sleep(60)
        os.system("shutdown /s /t 0")
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main() 