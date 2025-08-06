#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
홈페이지 크롤링 엔진 클래스
"""

import time
import random
import logging
import re
from typing import Optional, Dict, List
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests

class HomepageCrawler:
    """홈페이지 크롤링 엔진 클래스"""
    
    def __init__(self, logger=None):
        """
        홈페이지 크롤러 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # 팩스번호 정규식 패턴
        self.fax_patterns = [
            r'팩스[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'fax[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'F[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'전송[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*팩스',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*fax',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}).*전송',
        ]
        
        # 전화번호 정규식 패턴
        self.phone_patterns = [
            r'전화[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'tel[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'연락처[\s:：]*(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
            r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})',
        ]
        
        # 제외할 패턴들
        self.exclude_patterns = [
            r'^\d{4}$',  # 4자리 숫자
            r'^\d{1,3}$',  # 1-3자리 숫자
            r'^\d{4}-\d{2}-\d{2}$',  # 날짜 형식
            r'^\d{6}$',  # 6자리 숫자
        ]
        
        # 요청 헤더
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    
    def crawl_homepage(self, url: str, use_selenium: bool = False, driver=None) -> Optional[Dict]:
        """
        홈페이지 크롤링
        
        Args:
            url: 크롤링할 URL
            use_selenium: Selenium 사용 여부
            driver: WebDriver 인스턴스 (Selenium 사용시)
            
        Returns:
            Optional[Dict]: 크롤링 결과 데이터
        """
        try:
            if use_selenium and driver:
                return self._crawl_with_selenium(url, driver)
            else:
                return self._crawl_with_requests(url)
                
        except Exception as e:
            self.logger.error(f"❌ 홈페이지 크롤링 실패: {url} - {e}")
            return None
    
    def _crawl_with_selenium(self, url: str, driver) -> Optional[Dict]:
        """Selenium을 사용한 크롤링"""
        try:
            # 페이지 로드
            driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            
            # 페이지 제목
            title = driver.title
            
            # 텍스트 내용 추출
            body_text = ""
            try:
                body_element = driver.find_element(By.TAG_NAME, "body")
                body_text = body_element.text
            except:
                pass
            
            result = {
                'url': url,
                'title': title,
                'html_content': page_source,
                'text_content': body_text,
                'method': 'selenium'
            }
            
            self.logger.info(f"🌐 Selenium 크롤링 완료: {url}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Selenium 크롤링 실패: {url} - {e}")
            return None
    
    def _crawl_with_requests(self, url: str) -> Optional[Dict]:
        """Requests를 사용한 크롤링"""
        try:
            # HTTP 요청
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            # 인코딩 설정
            response.encoding = response.apparent_encoding
            
            # BeautifulSoup으로 파싱
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 제목 추출
            title = soup.title.string if soup.title else ""
            
            # 텍스트 내용 추출
            text_content = soup.get_text()
            
            result = {
                'url': url,
                'title': title.strip() if title else "",
                'html_content': response.text,
                'text_content': text_content,
                'method': 'requests'
            }
            
            self.logger.info(f"🌐 Requests 크롤링 완료: {url}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Requests 크롤링 실패: {url} - {e}")
            return None
    
    def extract_fax_from_html(self, html_content: str) -> List[str]:
        """HTML 내용에서 팩스번호 추출"""
        try:
            if not html_content:
                return []
            
            # BeautifulSoup으로 파싱
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 스크립트와 스타일 태그 제거
            for script in soup(["script", "style"]):
                script.decompose()
            
            # 텍스트 추출
            text_content = soup.get_text()
            
            return self._extract_fax_from_text(text_content)
            
        except Exception as e:
            self.logger.error(f"❌ HTML 팩스번호 추출 실패: {e}")
            return []
    
    def extract_phone_from_html(self, html_content: str) -> List[str]:
        """HTML 내용에서 전화번호 추출"""
        try:
            if not html_content:
                return []
            
            # BeautifulSoup으로 파싱
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 스크립트와 스타일 태그 제거
            for script in soup(["script", "style"]):
                script.decompose()
            
            # 텍스트 추출
            text_content = soup.get_text()
            
            return self._extract_phone_from_text(text_content)
            
        except Exception as e:
            self.logger.error(f"❌ HTML 전화번호 추출 실패: {e}")
            return []
    
    def _extract_fax_from_text(self, text: str) -> List[str]:
        """텍스트에서 팩스번호 추출"""
        try:
            if not text:
                return []
            
            fax_numbers = []
            
            for pattern in self.fax_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        fax_number = match[0] if match[0] else match[1] if len(match) > 1 else ""
                    else:
                        fax_number = match
                    
                    if fax_number and self._is_valid_number(fax_number):
                        fax_numbers.append(fax_number)
            
            # 중복 제거
            return list(set(fax_numbers))
            
        except Exception as e:
            self.logger.error(f"❌ 텍스트 팩스번호 추출 실패: {e}")
            return []
    
    def _extract_phone_from_text(self, text: str) -> List[str]:
        """텍스트에서 전화번호 추출"""
        try:
            if not text:
                return []
            
            phone_numbers = []
            
            for pattern in self.phone_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        phone_number = match[0] if match[0] else match[1] if len(match) > 1 else ""
                    else:
                        phone_number = match
                    
                    if phone_number and self._is_valid_number(phone_number):
                        phone_numbers.append(phone_number)
            
            # 중복 제거
            return list(set(phone_numbers))
            
        except Exception as e:
            self.logger.error(f"❌ 텍스트 전화번호 추출 실패: {e}")
            return []
    
    def _is_valid_number(self, number: str) -> bool:
        """번호 유효성 검사"""
        try:
            # 숫자만 추출
            digits = re.sub(r'[^\d]', '', number)
            
            # 길이 체크
            if len(digits) < 8 or len(digits) > 11:
                return False
            
            # 제외 패턴 체크
            for pattern in self.exclude_patterns:
                if re.match(pattern, digits):
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 번호 유효성 검사 실패: {number} - {e}")
            return False
    
    def extract_contact_info(self, page_data: Dict) -> Dict:
        """페이지 데이터에서 연락처 정보 추출"""
        try:
            result = {
                'fax_numbers': [],
                'phone_numbers': [],
                'email_addresses': [],
                'addresses': []
            }
            
            if not page_data:
                return result
            
            # HTML 내용에서 추출
            if 'html_content' in page_data:
                result['fax_numbers'] = self.extract_fax_from_html(page_data['html_content'])
                result['phone_numbers'] = self.extract_phone_from_html(page_data['html_content'])
                result['email_addresses'] = self._extract_emails_from_html(page_data['html_content'])
                result['addresses'] = self._extract_addresses_from_html(page_data['html_content'])
            
            # 텍스트 내용에서 추가 추출
            if 'text_content' in page_data:
                text_fax = self._extract_fax_from_text(page_data['text_content'])
                text_phone = self._extract_phone_from_text(page_data['text_content'])
                
                # 중복 제거하며 병합
                result['fax_numbers'] = list(set(result['fax_numbers'] + text_fax))
                result['phone_numbers'] = list(set(result['phone_numbers'] + text_phone))
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 연락처 정보 추출 실패: {e}")
            return {
                'fax_numbers': [],
                'phone_numbers': [],
                'email_addresses': [],
                'addresses': []
            }
    
    def _extract_emails_from_html(self, html_content: str) -> List[str]:
        """HTML에서 이메일 주소 추출"""
        try:
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, html_content)
            return list(set(emails))
        except Exception as e:
            self.logger.error(f"❌ 이메일 추출 실패: {e}")
            return []
    
    def _extract_addresses_from_html(self, html_content: str) -> List[str]:
        """HTML에서 주소 추출"""
        try:
            # 간단한 주소 패턴 (시/도 + 시/군/구 포함)
            address_patterns = [
                r'[서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충북|충남|전북|전남|경북|경남|제주].*?[시|군|구].*?[동|읍|면]',
                r'\d{5}.*?[시|군|구].*?[동|읍|면]',
            ]
            
            addresses = []
            soup = BeautifulSoup(html_content, 'html.parser')
            text_content = soup.get_text()
            
            for pattern in address_patterns:
                matches = re.findall(pattern, text_content)
                addresses.extend(matches)
            
            return list(set(addresses))
            
        except Exception as e:
            self.logger.error(f"❌ 주소 추출 실패: {e}")
            return []
    
    def get_page_summary(self, page_data: Dict) -> Dict:
        """페이지 요약 정보 생성"""
        try:
            if not page_data:
                return {}
            
            # 텍스트 길이
            text_length = len(page_data.get('text_content', ''))
            
            # HTML 길이
            html_length = len(page_data.get('html_content', ''))
            
            # 연락처 정보
            contact_info = self.extract_contact_info(page_data)
            
            return {
                'url': page_data.get('url', ''),
                'title': page_data.get('title', ''),
                'text_length': text_length,
                'html_length': html_length,
                'fax_count': len(contact_info['fax_numbers']),
                'phone_count': len(contact_info['phone_numbers']),
                'email_count': len(contact_info['email_addresses']),
                'address_count': len(contact_info['addresses']),
                'method': page_data.get('method', 'unknown')
            }
            
        except Exception as e:
            self.logger.error(f"❌ 페이지 요약 생성 실패: {e}")
            return {}


# 전역 함수들 (기존 코드와의 호환성을 위해)
def crawl_homepage_simple(url: str):
    """간단한 홈페이지 크롤링 (호환성 함수)"""
    crawler = HomepageCrawler()
    return crawler.crawl_homepage(url)

def extract_fax_from_html_simple(html_content: str):
    """간단한 HTML 팩스번호 추출 (호환성 함수)"""
    crawler = HomepageCrawler()
    return crawler.extract_fax_from_html(html_content) 