#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Intel Core i5-4210M 환경 최적화된 교회 크롤러
- Intel Core i5-4210M (2코어 4스레드) 환경 최적화
- 멀티프로세싱 처리 (4개 워커)
- 메모리 사용량 관리
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
import multiprocessing
from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from queue import Empty
from concurrent.futures import ThreadPoolExecutor, as_completed

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
def setup_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'church_crawler_{name}.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(name)

class I5ChurchCrawler:
    """Intel i5-4210M 환경 최적화된 교회 크롤러"""
    
    def __init__(self, excel_path: str, worker_id: int = 0):
        self.excel_path = excel_path
        self.worker_id = worker_id
        self.logger = setup_logger(f"worker_{worker_id}")
        
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
        
        # 📊 실시간 통계 시스템
        self.total_records = 0  # 전체 레코드 수
        self.phone_extracted = 0  # 전화번호 추출 성공
        self.fax_extracted = 0    # 팩스번호 추출 성공
        self.homepage_extracted = 0  # 홈페이지 추출 성공
        self.current_phase = "초기화"  # 현재 단계
        self.current_region = ""      # 현재 처리 지역
        self.statistics_update_interval = 100  # 100개마다 통계 업데이트
        
        # i5-4210M 환경 설정
        self.request_delay_min = 2.0  # 최소 2초 (성능 향상)
        self.request_delay_max = 4.0  # 최대 4초
        self.memory_cleanup_interval = 30  # 30개마다 메모리 정리 (성능 향상)
        self.max_threads = 2  # 워커당 2개 스레드 사용
        
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
        
        self.logger.info(f"🚀 Worker {worker_id} 초기화 완료")
    
    def _initialize_webdriver(self):
        """i5-4210M 환경 최적화된 WebDriver 초기화"""
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
            
            # 메모리 최적화 (i5-4210M은 더 많은 메모리 사용 가능)
            chrome_options.add_argument('--disk-cache-size=32768')  # 32MB 캐시
            chrome_options.add_argument('--media-cache-size=32768')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--disable-application-cache')
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=256')  # JS 힙 크기 증가
            
            self.driver = uc.Chrome(options=chrome_options)
            self.driver.implicitly_wait(5)  # 응답성 향상
            self.driver.set_page_load_timeout(10)  # 타임아웃 단축
            
            # 메모리 관리
            import gc
            gc.collect()
            
            self.logger.info("🌐 WebDriver 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 초기화 실패: {e}")
            raise

    def _load_data(self):
        """Excel 데이터 로드"""
        try:
            if not os.path.exists(self.excel_path):
                self.excel_path = 'academy2.xlsx'
            
            self.df = pd.read_excel(self.excel_path)
            self.logger.info(f"📊 데이터 로드 완료: {len(self.df)}개 학원/교습소")
            
            # 🔄 새로운 컬럼 구조 대응: 기관명 | 위치 | 주소 | 전화번호 | 팩스번호 | 홈페이지
            expected_columns = ['기관명', '위치', '주소', '전화번호', '팩스번호', '홈페이지']
            
            # 컬럼명 정규화
            if '기관명' in self.df.columns:
                self.df = self.df.rename(columns={
                    '기관명': 'name',
                    '위치': 'location',
                    '주소': 'address', 
                    '전화번호': 'phone',
                    '팩스번호': 'fax',
                    '홈페이지': 'homepage'
                })
            
            # 누락된 컬럼 추가
            for col in ['name', 'location', 'address', 'phone', 'fax', 'homepage']:
                if col not in self.df.columns:
                    self.df[col] = ''
            
            # NaN 값 처리
            self.df = self.df.fillna('')
            
            # 📊 지역별 데이터 분포 확인
            self._analyze_region_distribution()
            
            # 전체 레코드 수 설정
            self.total_records = len(self.df)
            
            self.logger.info("✅ 데이터 전처리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            raise

    def _analyze_region_distribution(self):
        """지역별 데이터 분포 분석"""
        try:
            # 지역별 카운트
            region_counts = {}
            seoul_count = 0
            gyeonggi_count = 0
            incheon_count = 0
            
            # 🔄 실제 데이터 기반 지역 분류
            seoul_districts = ['강남구', '강동구', '강북구', '강서구', '관악구', '광진구', '구로구', '금천구', 
                              '노원구', '도봉구', '동대문구', '동작구', '마포구', '서대문구', '서초구', '성동구', 
                              '성북구', '송파구', '양천구', '영등포구', '용산구', '은평구', '종로구', '중구', '중랑구']
            
            gyeonggi_cities = ['화성시', '용인시', '수원시', '성남시', '고양시', '남양주시', '김포시', '부천시', 
                              '안양시', '의정부시', '시흥시', '파주시', '이천시', '안산시', '광명시', '평택시', 
                              '하남시', '오산시', '구리시', '안성시', '포천시', '양주시', '여주시', '동두천시', 
                              '과천시', '군포시', '의왕시', '연천군', '가평군', '양평군']
            
            incheon_districts = ['서구', '남동구', '연수구', '부평구', '계양구', '미추홀구', '동구', '중구', 
                                '강화군', '옹진군']
            
            for idx, row in self.df.iterrows():
                location = str(row.get('location', '')).strip()
                
                if location in seoul_districts:
                    seoul_count += 1
                elif location in gyeonggi_cities:
                    gyeonggi_count += 1
                elif location in incheon_districts:
                    incheon_count += 1
            
            # 📊 분포 정보 로깅
            self.logger.info(f"📍 지역별 데이터 분포:")
            self.logger.info(f"   - 서울: {seoul_count:,}개")
            self.logger.info(f"   - 경기도: {gyeonggi_count:,}개")
            self.logger.info(f"   - 인천: {incheon_count:,}개")
            self.logger.info(f"   - 전체: {len(self.df):,}개")
            
            # 지역별 인덱스 범위 저장
            self.region_ranges = {
                'seoul': {'start': 0, 'end': seoul_count, 'count': seoul_count},
                'gyeonggi': {'start': seoul_count, 'end': seoul_count + gyeonggi_count, 'count': gyeonggi_count},
                'incheon': {'start': seoul_count + gyeonggi_count, 'end': seoul_count + gyeonggi_count + incheon_count, 'count': incheon_count}
            }
            
        except Exception as e:
            self.logger.error(f"❌ 지역별 분포 분석 실패: {e}")
            # 기본값 설정
            self.region_ranges = {
                'seoul': {'start': 0, 'end': 8395, 'count': 8395},
                'gyeonggi': {'start': 8395, 'end': 27795, 'count': 19400},
                'incheon': {'start': 27795, 'end': 31414, 'count': 3619}
            }

    def process_region_phone_extraction(self, region_name: str) -> List[Dict]:
        """지역별 전화번호 추출 처리"""
        try:
            self.current_phase = "전화번호추출"
            self.current_region = region_name
            
            # 지역별 청크 설정 가져오기
            chunk_config = self.get_region_chunk_config()
            if region_name not in chunk_config:
                raise ValueError(f"지원하지 않는 지역: {region_name}")
            
            chunk_size = chunk_config[region_name]['chunk_size']
            total_count = chunk_config[region_name]['total_count']
            
            self.logger.info(f"🚀 {region_name} 지역 전화번호 추출 시작 (총 {total_count:,}개, {chunk_size:,}개씩 처리)")
            
            # 지역 데이터를 청크로 분할
            chunks = self.split_region_data_by_chunks(region_name, chunk_size)
            all_results = []
            
            for chunk_idx, chunk in enumerate(chunks):
                chunk_results = []
                
                self.logger.info(f"📦 청크 {chunk_idx + 1}/{len(chunks)} 처리 중...")
                
                # 청크 내 각 행 처리
                for idx, row in chunk.iterrows():
                    result = self._process_single_academy_phone(row)
                    if result:
                        chunk_results.append(result)
                
                # 중간 저장
                if chunk_results:
                    self._save_region_intermediate_results(region_name, chunk_idx + 1, chunk_results)
                    all_results.extend(chunk_results)
                
                # 이전 중간 파일 삭제 (현재 청크 제외)
                if chunk_idx > 0:
                    self._cleanup_intermediate_files(region_name)
            
            # 지역별 최종 결과 저장
            if all_results:
                self._save_region_final_results(region_name, all_results)
                # 모든 중간 파일 삭제
                self._cleanup_intermediate_files(region_name)
            
            self.logger.info(f"✅ {region_name} 지역 전화번호 추출 완료: {len(all_results):,}개 처리")
            return all_results
            
        except Exception as e:
            self.logger.error(f"지역별 전화번호 추출 실패 ({region_name}): {e}")
            return []

    def process_region_fax_extraction(self, region_name: str, phone_data: List[Dict]) -> List[Dict]:
        """지역별 팩스번호 추출 처리"""
        try:
            self.current_phase = "팩스번호추출"
            self.current_region = region_name
            
            # 전화번호가 있는 데이터만 필터링
            phone_available_data = [row for row in phone_data if row.get('phone')]
            
            if not phone_available_data:
                self.logger.info(f"⚠️ {region_name} 지역: 전화번호가 있는 데이터 없음")
                return phone_data
            
            self.logger.info(f"🚀 {region_name} 지역 팩스번호 추출 시작 (전화번호 있는 {len(phone_available_data):,}개 대상)")
            
            # 청크 설정
            chunk_config = self.get_region_chunk_config()
            chunk_size = chunk_config[region_name]['chunk_size']
            
            all_results = []
            processed_count = 0
            
            # 청크 단위로 처리
            for i in range(0, len(phone_available_data), chunk_size):
                chunk_data = phone_available_data[i:i + chunk_size]
                chunk_results = []
                
                for row_dict in chunk_data:
                    # Dict를 Series로 변환
                    row = pd.Series(row_dict)
                    result = self._process_single_academy_fax(row)
                    if result:
                        chunk_results.append(result)
                
                processed_count += len(chunk_data)
                
                # 중간 저장
                if chunk_results:
                    self._save_region_intermediate_results(region_name, i // chunk_size + 1, chunk_results)
                    all_results.extend(chunk_results)
                
                self.logger.info(f"📦 팩스번호 추출 진행: {processed_count:,}/{len(phone_available_data):,}")
            
            # 지역별 최종 결과 저장
            if all_results:
                self._save_region_final_results(region_name, all_results)
                self._cleanup_intermediate_files(region_name)
            
            self.logger.info(f"✅ {region_name} 지역 팩스번호 추출 완료")
            return all_results
            
        except Exception as e:
            self.logger.error(f"지역별 팩스번호 추출 실패 ({region_name}): {e}")
            return phone_data

    def process_region_homepage_extraction(self, region_name: str, existing_data: List[Dict]) -> List[Dict]:
        """지역별 홈페이지 추출 처리"""
        try:
            self.current_phase = "홈페이지추출"
            self.current_region = region_name
            
            self.logger.info(f"🚀 {region_name} 지역 홈페이지 추출 시작 ({len(existing_data):,}개 대상)")
            
            # 청크 설정
            chunk_config = self.get_region_chunk_config()
            chunk_size = chunk_config[region_name]['chunk_size']
            
            all_results = []
            processed_count = 0
            
            # 청크 단위로 처리
            for i in range(0, len(existing_data), chunk_size):
                chunk_data = existing_data[i:i + chunk_size]
                chunk_results = []
                
                for row_dict in chunk_data:
                    # Dict를 Series로 변환
                    row = pd.Series(row_dict)
                    result = self._process_single_academy_homepage(row)
                    if result:
                        chunk_results.append(result)
                
                processed_count += len(chunk_data)
                
                # 중간 저장
                if chunk_results:
                    self._save_region_intermediate_results(region_name, i // chunk_size + 1, chunk_results)
                    all_results.extend(chunk_results)
                
                self.logger.info(f"📦 홈페이지 추출 진행: {processed_count:,}/{len(existing_data):,}")
            
            # 지역별 최종 결과 저장
            if all_results:
                self._save_region_final_results(region_name, all_results)
                self._cleanup_intermediate_files(region_name)
            
            self.logger.info(f"✅ {region_name} 지역 홈페이지 추출 완료")
            return all_results
            
        except Exception as e:
            self.logger.error(f"지역별 홈페이지 추출 실패 ({region_name}): {e}")
            return existing_data

    def get_region_data(self, region_name: str) -> pd.DataFrame:
        """특정 지역의 데이터 반환"""
        try:
            if region_name not in self.region_ranges:
                raise ValueError(f"지원하지 않는 지역: {region_name}")
            
            range_info = self.region_ranges[region_name]
            start_idx = range_info['start']
            end_idx = range_info['end']
            
            return self.df.iloc[start_idx:end_idx].copy()
            
        except Exception as e:
            self.logger.error(f"지역 데이터 추출 실패 ({region_name}): {e}")
            return pd.DataFrame()

    def split_region_data_by_chunks(self, region_name: str, chunk_size: int) -> List[pd.DataFrame]:
        """지역 데이터를 청크 단위로 분할"""
        try:
            region_df = self.get_region_data(region_name)
            if region_df.empty:
                return []
            
            chunks = []
            total_rows = len(region_df)
            
            for i in range(0, total_rows, chunk_size):
                end_idx = min(i + chunk_size, total_rows)
                chunk = region_df.iloc[i:end_idx].copy()
                chunks.append(chunk)
            
            self.logger.info(f"📦 {region_name} 지역 데이터 분할 완료: {len(chunks)}개 청크")
            return chunks
            
        except Exception as e:
            self.logger.error(f"지역 데이터 분할 실패 ({region_name}): {e}")
            return []

    def get_region_chunk_config(self) -> Dict[str, Dict[str, int]]:
        """지역별 청크 설정 반환"""
        return {
            'seoul': {
                'chunk_size': 2000,
                'total_count': self.region_ranges['seoul']['count']
            },
            'gyeonggi': {
                'chunk_size': 4000,
                'total_count': self.region_ranges['gyeonggi']['count']
            },
            'incheon': {
                'chunk_size': 1000,
                'total_count': self.region_ranges['incheon']['count']
            }
        }

    def process_chunk(self, chunk):
        """청크 단위 처리 (멀티프로세싱용)"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = []
            
            for idx, row in chunk.iterrows():
                if not row['name'].strip():
                    continue
                    
                future = executor.submit(self._process_single_church, row)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    self.logger.error(f"처리 중 오류 발생: {e}")
        
        return results

    def _process_single_church(self, row):
        """단일 교회 처리"""
        try:
            name = str(row['name']).strip()
            location = str(row['location']).strip()
            address = str(row['address']).strip()
            phone = str(row['phone']).strip()
            homepage = str(row['homepage']).strip()
            
            result = {
                'name': name,
                'location': location,
                'address': address,
                'phone': phone,
                'fax': '',
                'homepage': homepage
            }
            
            # 홈페이지가 있는 경우
            if homepage:
                page_data = self._crawl_homepage(homepage)
                if page_data:
                    fax_numbers = self._extract_fax_from_html(page_data['html'])
                    if fax_numbers:
                        for fax in fax_numbers:
                            if self._validate_fax_number(fax, phone, address, name):
                                result['fax'] = fax
                                break
                    
                    # AI 추출 시도
                    if not result['fax'] and self.use_ai:
                        ai_fax = self._extract_fax_with_ai(name, page_data)
                        if ai_fax and self._validate_fax_number(ai_fax, phone, address, name):
                            result['fax'] = ai_fax
            
            # 홈페이지가 없거나 팩스를 찾지 못한 경우
            if not result['fax']:
                google_fax = self._search_google_for_fax(name, location, address)
                if google_fax and self._validate_fax_number(google_fax, phone, address, name):
                    result['fax'] = google_fax
            
            self.processed_count += 1
            if result['fax']:
                self.success_count += 1
            
            # 📊 통계 업데이트
            self._update_statistics(result)
            
            # 메모리 정리
            if self.processed_count % self.memory_cleanup_interval == 0:
                self._cleanup_memory()
            
            return result
            
        except Exception as e:
            self.logger.error(f"교회 처리 중 오류 발생 ({name}): {e}")
            return None

    def _process_single_academy_phone(self, row):
        """단일 학원/교습소 전화번호 처리"""
        try:
            name = str(row['name']).strip()
            location = str(row['location']).strip()
            address = str(row['address']).strip()
            
            result = {
                'name': name,
                'location': location,
                'address': address,
                'phone': '',
                'fax': '',
                'homepage': ''
            }
            
            # 전화번호 추출
            phone_number = self._search_google_for_phone(name, location, address)
            if phone_number:
                result['phone'] = phone_number
            
            self.processed_count += 1
            if result['phone']:
                self.phone_extracted += 1
            
            # 통계 업데이트
            if self.processed_count % self.statistics_update_interval == 0:
                self._display_realtime_statistics()
            
            # 메모리 정리
            if self.processed_count % self.memory_cleanup_interval == 0:
                self._cleanup_memory()
            
            return result
            
        except Exception as e:
            self.logger.error(f"전화번호 처리 중 오류 발생 ({name}): {e}")
            return None

    def _process_single_academy_fax(self, row):
        """단일 학원/교습소 팩스번호 처리"""
        try:
            name = str(row['name']).strip()
            location = str(row['location']).strip()
            address = str(row['address']).strip()
            phone = str(row['phone']).strip()
            
            result = row.to_dict()  # 기존 데이터 유지
            
            # 팩스번호 추출 (전화번호가 있는 경우만)
            if phone:
                fax_number = self._search_google_for_fax(name, location, address)
                if fax_number and self._validate_fax_number(fax_number, phone, address, name):
                    result['fax'] = fax_number
            
            self.processed_count += 1
            if result.get('fax'):
                self.fax_extracted += 1
            
            # 통계 업데이트
            if self.processed_count % self.statistics_update_interval == 0:
                self._display_realtime_statistics()
            
            # 메모리 정리
            if self.processed_count % self.memory_cleanup_interval == 0:
                self._cleanup_memory()
            
            return result
            
        except Exception as e:
            self.logger.error(f"팩스번호 처리 중 오류 발생 ({name}): {e}")
            return None

    def _process_single_academy_homepage(self, row):
        """단일 학원/교습소 홈페이지 처리"""
        try:
            name = str(row['name']).strip()
            location = str(row['location']).strip()
            address = str(row['address']).strip()
            
            result = row.to_dict()  # 기존 데이터 유지
            
            # 홈페이지 추출
            homepage_url = self._search_google_for_homepage(name, location, address)
            if homepage_url:
                result['homepage'] = homepage_url
            
            self.processed_count += 1
            if result.get('homepage'):
                self.homepage_extracted += 1
            
            # 통계 업데이트
            if self.processed_count % self.statistics_update_interval == 0:
                self._display_realtime_statistics()
            
            # 메모리 정리
            if self.processed_count % self.memory_cleanup_interval == 0:
                self._cleanup_memory()
            
            return result
            
        except Exception as e:
            self.logger.error(f"홈페이지 처리 중 오류 발생 ({name}): {e}")
            return None

    def _search_google_for_fax(self, name: str, location: str, address: str) -> Optional[str]:
        """구글 검색으로 팩스번호 찾기"""
        try:
            # 🔄 새로운 검색어 형식: "위치 + 기관명 + 팩스번호"
            normalized_location = self._normalize_location(location)
            search_query = f"{normalized_location} {name} 팩스번호"
            
            self.driver.get(f"https://www.google.com/search?q={search_query}")
            
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 검색 결과에서 팩스번호 패턴 찾기
            text_content = soup.get_text()
            for pattern in self.fax_patterns:
                matches = re.finditer(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    fax_number = match.group(1)
                    if self._is_valid_phone_format(fax_number):
                        return self._normalize_phone_number(fax_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"구글 검색 중 오류 발생: {e}")
            return None

    def _search_google_for_phone(self, name: str, location: str, address: str) -> Optional[str]:
        """구글 검색으로 전화번호 찾기"""
        try:
            # 🔄 전화번호 검색어 형식: "위치 + 기관명 + 전화번호"
            normalized_location = self._normalize_location(location)
            search_query = f"{normalized_location} {name} 전화번호"
            
            self.driver.get(f"https://www.google.com/search?q={search_query}")
            
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 전화번호 패턴 정의
            phone_patterns = [
                r'전화[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'tel[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'T[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'연락처[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',  # 기본 전화번호 패턴
            ]
            
            # 검색 결과에서 전화번호 패턴 찾기
            text_content = soup.get_text()
            for pattern in phone_patterns:
                matches = re.finditer(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    phone_number = match.group(1) if match.groups() else match.group(0)
                    if self._is_valid_phone_format(phone_number):
                        return self._normalize_phone_number(phone_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"전화번호 구글 검색 중 오류 발생: {e}")
            return None

    def _search_google_for_homepage(self, name: str, location: str, address: str) -> Optional[str]:
        """구글 검색으로 홈페이지 찾기"""
        try:
            # 🔄 홈페이지 검색어 형식: "위치 + 기관명 + 홈페이지"
            normalized_location = self._normalize_location(location)
            search_query = f"{normalized_location} {name} 홈페이지"
            
            self.driver.get(f"https://www.google.com/search?q={search_query}")
            
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 홈페이지 URL 패턴 찾기
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                if any(platform in href.lower() for platform in ['http', 'www']):
                    # 구글 검색 결과 링크 필터링
                    if not any(exclude in href.lower() for exclude in ['google.com', 'youtube.com', 'facebook.com/tr']):
                        # 다양한 플랫폼 지원
                        if any(platform in href.lower() for platform in [
                            'daum.cafe', 'naver.blog', 'naver.modoo', 'instagram.com',
                            'cafe.naver.com', 'blog.naver.com', 'modoo.at'
                        ]):
                            return href
                        # 일반 웹사이트 URL
                        elif href.startswith('http') and '.' in href:
                            return href
            
            return None
            
        except Exception as e:
            self.logger.error(f"홈페이지 구글 검색 중 오류 발생: {e}")
            return None

    def _crawl_homepage(self, url: str) -> Optional[Dict[str, Any]]:
        """홈페이지 크롤링"""
        try:
            self.driver.get(url)
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            return {
                'url': url,
                'html': self.driver.page_source,
                'text': BeautifulSoup(self.driver.page_source, 'html.parser').get_text()
            }
            
        except Exception as e:
            self.logger.error(f"홈페이지 크롤링 중 오류 발생 ({url}): {e}")
            return None

    def _extract_fax_from_html(self, html_content: str) -> List[str]:
        """HTML에서 팩스번호 추출"""
        fax_numbers = []
        for pattern in self.fax_patterns:
            matches = re.finditer(pattern, html_content, re.IGNORECASE)
            for match in matches:
                fax_number = match.group(1)
                if self._is_valid_phone_format(fax_number):
                    normalized = self._normalize_phone_number(fax_number)
                    if normalized not in fax_numbers:
                        fax_numbers.append(normalized)
        return fax_numbers

    def _extract_fax_with_ai(self, name: str, page_data: Dict[str, Any]) -> Optional[str]:
        """AI를 사용하여 팩스번호 추출"""
        if not self.use_ai:
            return None
            
        try:
            prompt = f"""
            다음 텍스트에서 '{name}' 학원/교습소의 팩스번호를 찾아주세요.
            형식: 지역번호-국번-번호 (예: 02-1234-5678)
            응답은 팩스번호만 작성해주세요.
            
            텍스트:
            {page_data['text'][:3000]}  # 텍스트 길이 제한
            """
            
            response = self.model.generate_content(prompt)
            if response and response.text:
                fax_number = response.text.strip()
                if self._is_valid_phone_format(fax_number):
                    return self._normalize_phone_number(fax_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"AI 추출 중 오류 발생: {e}")
            return None

    def _validate_fax_number(self, fax_number: str, phone_number: str, address: str, name: str) -> bool:
        """팩스번호 유효성 검증"""
        try:
            if not self._is_valid_phone_format(fax_number):
                return False
            
            # 전화번호와 동일한지 확인
            if fax_number == phone_number:
                return False
            
            # 지역 일치 여부 확인
            fax_region = self._get_region_from_phone(fax_number)
            phone_region = self._get_region_from_phone(phone_number)
            address_region = self._get_region_from_address(address)
            
            # 1. 팩스번호와 전화번호의 지역이 같은 경우
            if fax_region and phone_region and fax_region == phone_region:
                return True
            
            # 2. 팩스번호와 주소의 지역이 같은 경우
            if fax_region and address_region and fax_region == address_region:
                return True
            
            # 3. 인터넷 팩스(070)는 허용
            if fax_number.startswith('070'):
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"팩스번호 검증 중 오류 발생: {e}")
            return False

    def _normalize_phone_number(self, phone: str) -> str:
        """전화번호 정규화"""
        # 숫자만 추출
        numbers = re.sub(r'[^0-9]', '', phone)
        
        # 길이별 처리
        if len(numbers) == 7:  # 지역번호 없는 경우
            return f"02-{numbers[:3]}-{numbers[3:]}"
        elif len(numbers) == 8:  # 지역번호 없는 경우 (4-4)
            return f"02-{numbers[:4]}-{numbers[4:]}"
        elif len(numbers) in [9, 10, 11]:  # 지역번호 있는 경우
            if numbers.startswith('02'):
                return f"02-{numbers[2:-4]}-{numbers[-4:]}"
            else:
                return f"{numbers[:3]}-{numbers[3:-4]}-{numbers[-4:]}"
        
        return phone

    def _is_valid_phone_format(self, phone: str) -> bool:
        """전화번호 형식 검증"""
        # 기본 정규식 패턴
        patterns = [
            r'^\d{2,3}-\d{3,4}-\d{4}$',  # 02-123-4567 or 031-123-4567
            r'^\d{2,3}\d{3,4}\d{4}$',    # 0212345678 or 02123456789
            r'^\d{2,3} \d{3,4} \d{4}$',  # 02 123 4567 or 031 123 4567
        ]
        
        # 숫자만 추출
        numbers = re.sub(r'[^0-9]', '', phone)
        
        # 길이 체크
        if not (7 <= len(numbers) <= 11):
            return False
            
        # 지역번호 체크
        area_code = numbers[:2] if numbers.startswith('02') else numbers[:3]
        if area_code not in KOREAN_AREA_CODES:
            return False
        
        # 패턴 매칭
        normalized = self._normalize_phone_number(phone)
        return any(re.match(pattern, normalized) for pattern in patterns)

    def _get_region_from_phone(self, phone: str) -> str:
        """전화번호에서 지역 추출"""
        if not phone:
            return ""
            
        numbers = re.sub(r'[^0-9]', '', phone)
        if numbers.startswith('02'):
            return KOREAN_AREA_CODES.get('02', '')
        else:
            area_code = numbers[:3]
            return KOREAN_AREA_CODES.get(area_code, '')

    def _get_region_from_address(self, address: str) -> str:
        """주소에서 지역 추출"""
        if not address:
            return ""
            
        # 시도 단위 매칭
        regions = {
            '서울': '서울', '경기': '경기', '인천': '인천', '강원': '강원',
            '충남': '충남', '대전': '대전', '충북': '충북', '세종': '세종',
            '부산': '부산', '울산': '울산', '대구': '대구', '경북': '경북',
            '경남': '경남', '전남': '전남', '광주': '광주', '전북': '전북',
            '제주': '제주'
        }
        
        for region, value in regions.items():
            if region in address:
                return value
        
        return ""

    def _normalize_location(self, location: str) -> str:
        """위치 정규화 (검색어 최적화)"""
        if not location:
            return ""
        
        location = location.strip()
        
        # 🔄 위치별 정규화 규칙
        if '서울' in location:
            # 서울: 그대로 사용
            return location
        elif '경기' in location:
            # 경기도: "시" 제거 (용인시 → 용인)
            location = location.replace('경기도 ', '')
            if location.endswith('시'):
                location = location[:-1]
            return location
        elif '인천' in location:
            # 인천: "인천광역시" → "인천" (인천광역시 연수구 → 인천 연수구)
            location = location.replace('인천광역시 ', '인천 ')
            return location
        
        return location

    def _display_realtime_statistics(self):
        """실시간 통계 표시"""
        try:
            # 경과 시간 계산
            elapsed_time = datetime.now() - self.start_time
            elapsed_minutes = elapsed_time.total_seconds() / 60
            
            # 처리 속도 계산
            if elapsed_minutes > 0:
                processing_speed = self.processed_count / elapsed_minutes
                estimated_total_time = self.total_records / processing_speed if processing_speed > 0 else 0
                remaining_time = estimated_total_time - elapsed_minutes
            else:
                processing_speed = 0
                remaining_time = 0
            
            # 📊 실시간 통계 출력
            print("\n" + "="*60)
            print("🔍 실시간 진행 상황")
            print("="*60)
            print(f"📍 현재 작업: {self.current_phase} ({self.current_region})")
            print(f"📊 전화번호: {self.phone_extracted:,} / {self.total_records:,} ({self.phone_extracted/self.total_records*100:.1f}%) {'✅' if self.phone_extracted > 0 else '⏳'}")
            print(f"📊 팩스번호: {self.fax_extracted:,} / {self.total_records:,} ({self.fax_extracted/self.total_records*100:.1f}%) {'✅' if self.fax_extracted > 0 else '⏳'}")
            print(f"📊 홈페이지: {self.homepage_extracted:,} / {self.total_records:,} ({self.homepage_extracted/self.total_records*100:.1f}%) {'✅' if self.homepage_extracted > 0 else '⏳'}")
            print(f"📊 전체 처리: {self.processed_count:,} / {self.total_records:,} ({self.processed_count/self.total_records*100:.1f}%)")
            print(f"⏱️ 경과시간: {elapsed_minutes:.1f}분")
            print(f"🚀 처리속도: {processing_speed:.1f}개/분")
            if remaining_time > 0:
                print(f"⏰ 예상 완료: {remaining_time:.1f}분 후")
            print("="*60)
            
        except Exception as e:
            self.logger.error(f"통계 표시 중 오류 발생: {e}")

    def _update_statistics(self, result: dict):
        """통계 업데이트"""
        try:
            if result:
                if result.get('phone'):
                    self.phone_extracted += 1
                if result.get('fax'):
                    self.fax_extracted += 1
                if result.get('homepage'):
                    self.homepage_extracted += 1
            
            # 100개마다 통계 표시
            if self.processed_count % self.statistics_update_interval == 0:
                self._display_realtime_statistics()
                
        except Exception as e:
            self.logger.error(f"통계 업데이트 중 오류 발생: {e}")

    def _cleanup_memory(self):
        """메모리 정리"""
        try:
            # 브라우저 캐시 정리
            self.driver.execute_script("window.localStorage.clear();")
            self.driver.execute_script("window.sessionStorage.clear();")
            
            # 파이썬 가비지 컬렉션
            import gc
            gc.collect()
            
            # 시스템 캐시 정리 (Linux)
            if os.name == 'posix':
                os.system('sync')
            
        except Exception as e:
            self.logger.error(f"메모리 정리 중 오류 발생: {e}")

    def _monitor_system(self):
        """시스템 리소스 모니터링"""
        while self.monitoring_active:
            try:
                process = psutil.Process(os.getpid())
                memory_usage = process.memory_info().rss / 1024 / 1024  # MB
                cpu_percent = process.cpu_percent()
                
                self.logger.info(f"시스템 상태 - CPU: {cpu_percent:.1f}%, "
                               f"메모리: {memory_usage:.1f}MB, "
                               f"처리: {self.processed_count}개, "
                               f"성공: {self.success_count}개")
                
                time.sleep(30)  # 30초마다 갱신
                
            except Exception as e:
                self.logger.error(f"모니터링 중 오류 발생: {e}")
                time.sleep(60)

    def _save_results(self) -> str:
        """결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # 바탕화면 경로 자동 감지
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            filename = os.path.join(desktop_path, f"학원교습소-전화번호추출_worker{self.worker_id}_{timestamp}.xlsx")
            
            df_result = pd.DataFrame(self.results)
            df_result.to_excel(filename, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"✅ 결과 저장 완료: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"결과 저장 중 오류 발생: {e}")
            return self._save_intermediate_results("오류")

    def _save_intermediate_results(self, suffix: str):
        """중간 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # 바탕화면 경로 자동 감지
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            filename = os.path.join(desktop_path, f"학원교습소-전화번호추출_worker{self.worker_id}_{suffix}_{timestamp}.xlsx")
            
            df_result = pd.DataFrame(self.results)
            df_result.to_excel(filename, index=False, encoding='utf-8-sig')
            
            return filename
            
        except Exception as e:
            self.logger.error(f"중간 결과 저장 중 오류 발생: {e}")
            return None

    def _save_region_intermediate_results(self, region_name: str, chunk_num: int, results: List[Dict]):
        """지역별 중간 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            
            # 파일명 형식: 학원교습소-전화번호추출-서울-중간저장-2000개_YYYYMMDD_HHMMSS.xlsx
            filename = os.path.join(desktop_path, 
                f"학원교습소-{self.current_phase}-{region_name}-중간저장-{len(results)}개_{timestamp}.xlsx")
            
            df_result = pd.DataFrame(results)
            df_result.to_excel(filename, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"💾 중간 저장 완료: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"지역별 중간 결과 저장 중 오류 발생: {e}")
            return None

    def _save_region_final_results(self, region_name: str, results: List[Dict]):
        """지역별 최종 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            
            # 파일명 형식: 학원교습소-전화번호추출-서울-완료_YYYYMMDD_HHMMSS.xlsx
            filename = os.path.join(desktop_path, 
                f"학원교습소-{self.current_phase}-{region_name}-완료_{timestamp}.xlsx")
            
            df_result = pd.DataFrame(results)
            df_result.to_excel(filename, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"✅ 지역 완료 저장: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"지역별 최종 결과 저장 중 오류 발생: {e}")
            return None

    def _cleanup_intermediate_files(self, region_name: str, keep_final: bool = True):
        """중간 파일 정리"""
        try:
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            
            # 중간 저장 파일 패턴 검색
            import glob
            pattern = os.path.join(desktop_path, f"학원교습소-{self.current_phase}-{region_name}-중간저장-*.xlsx")
            intermediate_files = glob.glob(pattern)
            
            deleted_count = 0
            for file_path in intermediate_files:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                    self.logger.info(f"🗑️ 중간 파일 삭제: {os.path.basename(file_path)}")
                except Exception as e:
                    self.logger.error(f"파일 삭제 실패: {file_path} - {e}")
            
            if deleted_count > 0:
                self.logger.info(f"🧹 {region_name} 지역 중간 파일 {deleted_count}개 정리 완료")
            
        except Exception as e:
            self.logger.error(f"중간 파일 정리 중 오류 발생: {e}")

    def _cleanup(self):
        """리소스 정리"""
        try:
            self.monitoring_active = False
            if self.monitoring_thread.is_alive():
                self.monitoring_thread.join(timeout=3)
            
            if self.driver:
                self.driver.quit()
            
            self.logger.info("🧹 리소스 정리 완료")
            
        except Exception as e:
            self.logger.error(f"리소스 정리 중 오류 발생: {e}")

def split_dataframe(df, n_chunks):
    """데이터프레임을 n개의 청크로 분할"""
    chunk_size = len(df) // n_chunks
    chunks = []
    for i in range(n_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < n_chunks - 1 else len(df)
        chunks.append(df.iloc[start_idx:end_idx].copy())
    return chunks

def worker_process(excel_path: str, chunk_df: pd.DataFrame, worker_id: int):
    """워커 프로세스"""
    crawler = I5ChurchCrawler(excel_path, worker_id)
    try:
        results = crawler.process_chunk(chunk_df)
        return results
    finally:
        crawler._cleanup()

def main():
    """메인 함수"""
    try:
        # 🔧 멀티프로세싱 설정 (쉽게 수정 가능)
        # 숫자만 변경하면 프로세스 수 조정 가능
        n_processes = 4  # Intel i5-4210M 환경 최적화 (2코어 4스레드)
        
        # 엑셀 파일 경로
        excel_path = 'academy2.xlsx'
        
        print("🚀 학원교습소 데이터 크롤링 시작!")
        print("="*60)
        
        # 크롤러 초기화
        crawler = I5ChurchCrawler(excel_path, worker_id=0)
        
        try:
            # 📊 전체 통계 초기화
            all_phone_results = []
            all_fax_results = []
            all_homepage_results = []
            
            # 🔄 Phase 1: 전화번호 추출
            print("\n🔄 Phase 1: 전화번호 추출")
            print("="*60)
            
            # 서울 지역 전화번호 추출
            seoul_phone_results = crawler.process_region_phone_extraction('seoul')
            all_phone_results.extend(seoul_phone_results)
            
            # 경기도 지역 전화번호 추출
            gyeonggi_phone_results = crawler.process_region_phone_extraction('gyeonggi')
            all_phone_results.extend(gyeonggi_phone_results)
            
            # 인천 지역 전화번호 추출
            incheon_phone_results = crawler.process_region_phone_extraction('incheon')
            all_phone_results.extend(incheon_phone_results)
            
            # 전화번호 추출 완료 저장
            if all_phone_results:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
                phone_filename = os.path.join(desktop_path, f"학원데이터교습소_전화번호추출완료_{timestamp}.xlsx")
                pd.DataFrame(all_phone_results).to_excel(phone_filename, index=False, encoding='utf-8-sig')
                print(f"✅ 전화번호 추출 완료: {phone_filename}")
            
            # 🔄 Phase 2: 팩스번호 추출
            print("\n🔄 Phase 2: 팩스번호 추출")
            print("="*60)
            
            # 서울 지역 팩스번호 추출
            seoul_fax_results = crawler.process_region_fax_extraction('seoul', seoul_phone_results)
            all_fax_results.extend(seoul_fax_results)
            
            # 경기도 지역 팩스번호 추출
            gyeonggi_fax_results = crawler.process_region_fax_extraction('gyeonggi', gyeonggi_phone_results)
            all_fax_results.extend(gyeonggi_fax_results)
            
            # 인천 지역 팩스번호 추출
            incheon_fax_results = crawler.process_region_fax_extraction('incheon', incheon_phone_results)
            all_fax_results.extend(incheon_fax_results)
            
            # 팩스번호 추출 완료 저장
            if all_fax_results:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                fax_filename = os.path.join(desktop_path, f"학원데이터교습소_팩스번호추출완료_{timestamp}.xlsx")
                pd.DataFrame(all_fax_results).to_excel(fax_filename, index=False, encoding='utf-8-sig')
                print(f"✅ 팩스번호 추출 완료: {fax_filename}")
            
            # 🔄 Phase 3: 홈페이지 추출
            print("\n🔄 Phase 3: 홈페이지 추출")
            print("="*60)
            
            # 서울 지역 홈페이지 추출
            seoul_homepage_results = crawler.process_region_homepage_extraction('seoul', seoul_fax_results)
            all_homepage_results.extend(seoul_homepage_results)
            
            # 경기도 지역 홈페이지 추출
            gyeonggi_homepage_results = crawler.process_region_homepage_extraction('gyeonggi', gyeonggi_fax_results)
            all_homepage_results.extend(gyeonggi_homepage_results)
            
            # 인천 지역 홈페이지 추출
            incheon_homepage_results = crawler.process_region_homepage_extraction('incheon', incheon_fax_results)
            all_homepage_results.extend(incheon_homepage_results)
            
            # 홈페이지 추출 완료 저장
            if all_homepage_results:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                homepage_filename = os.path.join(desktop_path, f"학원데이터교습소_홈페이지추출완료_{timestamp}.xlsx")
                pd.DataFrame(all_homepage_results).to_excel(homepage_filename, index=False, encoding='utf-8-sig')
                print(f"✅ 홈페이지 추출 완료: {homepage_filename}")
            
            # 🔄 Phase 4: 전체 데이터 병합 (검증미완)
            print("\n🔄 Phase 4: 전체 데이터 병합")
            print("="*60)
            
            if all_homepage_results:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                final_filename = os.path.join(desktop_path, f"학원데이터교습소_전체데이터(검증미완)_추출완료_{timestamp}.xlsx")
                pd.DataFrame(all_homepage_results).to_excel(final_filename, index=False, encoding='utf-8-sig')
                print(f"✅ 전체 데이터 병합 완료: {final_filename}")
            
            # 📊 최종 통계 출력
            print("\n📊 최종 통계")
            print("="*60)
            print(f"📞 전화번호 추출: {len([r for r in all_homepage_results if r.get('phone')]):,}개")
            print(f"📠 팩스번호 추출: {len([r for r in all_homepage_results if r.get('fax')]):,}개")
            print(f"🌐 홈페이지 추출: {len([r for r in all_homepage_results if r.get('homepage')]):,}개")
            print(f"📊 전체 처리: {len(all_homepage_results):,}개")
            
            print("\n🎉 모든 단계 완료!")
            
        finally:
            crawler._cleanup()
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 