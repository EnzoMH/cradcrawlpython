#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import random
import logging
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup
import google.generativeai as genai
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class InfoExtractor:
    """정보 추출 클래스"""
    
    def __init__(self, web_driver_manager, logger=None):
        """
        정보 추출기 초기화
        
        Args:
            web_driver_manager: WebDriver 관리자 객체
            logger: 로깅 객체 (기본값: None)
        """
        self.web_driver_manager = web_driver_manager
        self.logger = logger or logging.getLogger(__name__)
        
        # 딜레이 설정
        self.request_delay_min = 1.0
        self.request_delay_max = 2.0
        
        # AI 설정
        self.use_ai = False
        self.model = None
        
        # 팩스번호 정규식 패턴
        self.fax_patterns = [
            r'팩스[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'F[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'전송[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*팩스',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*fax',
        ]
        
        # 한국 지역번호 매핑
        self.area_codes = {
            "02": "서울", "031": "경기", "032": "인천", "033": "강원",
            "041": "충남", "042": "대전", "043": "충북", "044": "세종",
            "051": "부산", "052": "울산", "053": "대구", "054": "경북", 
            "055": "경남", "061": "전남", "062": "광주", "063": "전북", 
            "064": "제주", "070": "인터넷전화", "010": "핸드폰"
        }
    
    def initialize_ai(self, api_key: str):
        """
        AI 초기화
        
        Args:
            api_key: Gemini API 키
        """
        try:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash')
            self.use_ai = True
            self.logger.info("🤖 Gemini AI 모델 초기화 성공")
        except Exception as e:
            self.logger.error(f"❌ AI 초기화 실패: {e}")
            self.use_ai = False
    
    def search_google_for_phone(self, name: str, location: str, address: str) -> Optional[str]:
        """
        구글 검색으로 전화번호 찾기
        
        Args:
            name: 기관명
            location: 위치
            address: 주소
            
        Returns:
            Optional[str]: 전화번호
        """
        try:
            normalized_location = self._normalize_location(location)
            search_query = f"{normalized_location} {name} 전화번호"
            
            driver = self.web_driver_manager.get_driver()
            driver.get(f"https://www.google.com/search?q={search_query}")
            
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 전화번호 패턴
            phone_patterns = [
                r'전화[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'tel[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'T[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'연락처[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
                r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            ]
            
            text_content = soup.get_text()
            for pattern in phone_patterns:
                matches = re.finditer(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    phone_number = match.group(1) if match.groups() else match.group(0)
                    if self._is_valid_phone_format(phone_number):
                        return self._normalize_phone_number(phone_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"전화번호 검색 실패: {e}")
            return None
    
    def search_google_for_fax(self, name: str, location: str, address: str) -> Optional[str]:
        """
        구글 검색으로 팩스번호 찾기
        
        Args:
            name: 기관명
            location: 위치
            address: 주소
            
        Returns:
            Optional[str]: 팩스번호
        """
        try:
            normalized_location = self._normalize_location(location)
            search_query = f"{normalized_location} {name} 팩스번호"
            
            driver = self.web_driver_manager.get_driver()
            driver.get(f"https://www.google.com/search?q={search_query}")
            
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            text_content = soup.get_text()
            for pattern in self.fax_patterns:
                matches = re.finditer(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    fax_number = match.group(1)
                    if self._is_valid_phone_format(fax_number):
                        return self._normalize_phone_number(fax_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"팩스번호 검색 실패: {e}")
            return None
    
    def search_google_for_homepage(self, name: str, location: str, address: str) -> Optional[str]:
        """
        구글 검색으로 홈페이지 찾기
        
        Args:
            name: 기관명
            location: 위치
            address: 주소
            
        Returns:
            Optional[str]: 홈페이지 URL
        """
        try:
            normalized_location = self._normalize_location(location)
            search_query = f"{normalized_location} {name} 홈페이지"
            
            driver = self.web_driver_manager.get_driver()
            driver.get(f"https://www.google.com/search?q={search_query}")
            
            time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                if any(platform in href.lower() for platform in ['http', 'www']):
                    if not any(exclude in href.lower() for exclude in ['google.com', 'youtube.com', 'facebook.com/tr']):
                        if any(platform in href.lower() for platform in [
                            'daum.cafe', 'naver.blog', 'naver.modoo', 'instagram.com',
                            'cafe.naver.com', 'blog.naver.com', 'modoo.at'
                        ]):
                            return href
                        elif href.startswith('http') and '.' in href:
                            return href
            
            return None
            
        except Exception as e:
            self.logger.error(f"홈페이지 검색 실패: {e}")
            return None
    
    def extract_fax_from_html(self, html_content: str) -> List[str]:
        """
        HTML에서 팩스번호 추출
        
        Args:
            html_content: HTML 내용
            
        Returns:
            List[str]: 팩스번호 리스트
        """
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
    
    def extract_fax_with_ai(self, name: str, page_data: Dict[str, Any]) -> Optional[str]:
        """
        AI를 사용하여 팩스번호 추출
        
        Args:
            name: 기관명
            page_data: 페이지 데이터
            
        Returns:
            Optional[str]: 팩스번호
        """
        if not self.use_ai:
            return None
            
        try:
            prompt = f"""
            다음 텍스트에서 '{name}' 학원/교습소의 팩스번호를 찾아주세요.
            형식: 지역번호-국번-번호 (예: 02-1234-5678)
            응답은 팩스번호만 작성해주세요.
            
            텍스트:
            {page_data['text'][:3000]}
            """
            
            response = self.model.generate_content(prompt)
            if response and response.text:
                fax_number = response.text.strip()
                if self._is_valid_phone_format(fax_number):
                    return self._normalize_phone_number(fax_number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"AI 추출 실패: {e}")
            return None
    
    def _normalize_location(self, location: str) -> str:
        """
        위치 정규화
        
        Args:
            location: 위치
            
        Returns:
            str: 정규화된 위치
        """
        if not location:
            return ""
        
        location = location.strip()
        
        if '서울' in location:
            return location
        elif '경기' in location:
            location = location.replace('경기도 ', '')
            if location.endswith('시'):
                location = location[:-1]
            return location
        elif '인천' in location:
            location = location.replace('인천광역시 ', '인천 ')
            return location
        
        return location
    
    def _normalize_phone_number(self, phone: str) -> str:
        """
        전화번호 정규화
        
        Args:
            phone: 전화번호
            
        Returns:
            str: 정규화된 전화번호
        """
        numbers = re.sub(r'[^0-9]', '', phone)
        
        if len(numbers) == 7:
            return f"02-{numbers[:3]}-{numbers[3:]}"
        elif len(numbers) == 8:
            return f"02-{numbers[:4]}-{numbers[4:]}"
        elif len(numbers) in [9, 10, 11]:
            if numbers.startswith('02'):
                return f"02-{numbers[2:-4]}-{numbers[-4:]}"
            else:
                return f"{numbers[:3]}-{numbers[3:-4]}-{numbers[-4:]}"
        
        return phone
    
    def _is_valid_phone_format(self, phone: str) -> bool:
        """
        전화번호 형식 검증
        
        Args:
            phone: 전화번호
            
        Returns:
            bool: 유효성 여부
        """
        patterns = [
            r'^\d{2,3}-\d{3,4}-\d{4}$',
            r'^\d{2,3}\d{3,4}\d{4}$',
            r'^\d{2,3} \d{3,4} \d{4}$',
        ]
        
        numbers = re.sub(r'[^0-9]', '', phone)
        
        if not (7 <= len(numbers) <= 11):
            return False
            
        area_code = numbers[:2] if numbers.startswith('02') else numbers[:3]
        if area_code not in self.area_codes:
            return False
        
        normalized = self._normalize_phone_number(phone)
        return any(re.match(pattern, normalized) for pattern in patterns)
    
    def _get_region_from_phone(self, phone: str) -> str:
        """
        전화번호에서 지역 추출
        
        Args:
            phone: 전화번호
            
        Returns:
            str: 지역명
        """
        if not phone:
            return ""
            
        numbers = re.sub(r'[^0-9]', '', phone)
        if numbers.startswith('02'):
            return self.area_codes.get('02', '')
        else:
            area_code = numbers[:3]
            return self.area_codes.get(area_code, '') 