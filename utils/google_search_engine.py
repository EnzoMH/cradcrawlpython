#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
구글 검색 엔진 클래스
"""

import time
import random
import logging
import re
from typing import Optional, List, Dict
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class GoogleSearchEngine:
    """구글 검색 엔진 클래스"""
    
    def __init__(self, logger=None):
        """
        구글 검색 엔진 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # 구글 검색 URL
        self.search_url = "https://www.google.com/search"
        
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
        
        # 검색 결과 선택자
        self.result_selectors = [
            'div.g',
            'div[data-ved]',
            '.rc',
            '.tF2Cxc'
        ]
        
        # 링크 선택자
        self.link_selectors = [
            'h3 a',
            'a[href*="http"]',
            '.yuRUbf a'
        ]
    
    def search_google(self, driver, query: str, max_results: int = 5) -> List[Dict]:
        """
        구글 검색 실행
        
        Args:
            driver: WebDriver 인스턴스
            query: 검색 쿼리
            max_results: 최대 결과 수
            
        Returns:
            List[Dict]: 검색 결과 목록
        """
        try:
            # 검색 페이지로 이동
            search_params = f"?q={query.replace(' ', '+')}"
            driver.get(self.search_url + search_params)
            
            # 검색 결과 로딩 대기
            time.sleep(random.uniform(2, 4))
            
            # 검색 결과 파싱
            results = self._parse_search_results(driver, max_results)
            
            self.logger.info(f"🔍 구글 검색 완료: '{query}' - {len(results)}개 결과")
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 구글 검색 실패: {query} - {e}")
            return []
    
    def _parse_search_results(self, driver, max_results: int) -> List[Dict]:
        """검색 결과 파싱"""
        results = []
        
        try:
            # 검색 결과 요소 찾기
            result_elements = []
            for selector in self.result_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        result_elements = elements[:max_results]
                        break
                except:
                    continue
            
            if not result_elements:
                self.logger.warning("⚠️ 검색 결과 요소를 찾을 수 없습니다")
                return []
            
            # 각 결과 처리
            for i, element in enumerate(result_elements):
                if i >= max_results:
                    break
                
                try:
                    result_data = self._extract_result_data(element)
                    if result_data:
                        results.append(result_data)
                        
                except Exception as e:
                    self.logger.debug(f"결과 {i+1} 처리 중 오류: {e}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 검색 결과 파싱 실패: {e}")
            return []
    
    def _extract_result_data(self, element) -> Optional[Dict]:
        """개별 검색 결과 데이터 추출"""
        try:
            result_data = {
                'title': '',
                'url': '',
                'snippet': '',
                'text_content': ''
            }
            
            # 제목과 URL 추출
            link_element = None
            for selector in self.link_selectors:
                try:
                    link_element = element.find_element(By.CSS_SELECTOR, selector)
                    if link_element:
                        break
                except:
                    continue
            
            if link_element:
                result_data['title'] = link_element.text.strip()
                result_data['url'] = link_element.get_attribute('href')
            
            # 스니펫 추출
            try:
                snippet_selectors = [
                    '.VwiC3b',
                    '.s3v9rd',
                    '.st',
                    'span[data-ved]'
                ]
                
                for selector in snippet_selectors:
                    try:
                        snippet_element = element.find_element(By.CSS_SELECTOR, selector)
                        if snippet_element:
                            result_data['snippet'] = snippet_element.text.strip()
                            break
                    except:
                        continue
                        
            except:
                pass
            
            # 전체 텍스트 내용
            result_data['text_content'] = element.text.strip()
            
            # 유효한 결과인지 확인
            if result_data['title'] and result_data['url']:
                return result_data
            
            return None
            
        except Exception as e:
            self.logger.debug(f"결과 데이터 추출 오류: {e}")
            return None
    
    def search_for_fax(self, driver, org_name: str, address: str = "", additional_info: str = "") -> List[str]:
        """
        팩스번호 검색
        
        Args:
            driver: WebDriver 인스턴스
            org_name: 기관명
            address: 주소 (선택사항)
            additional_info: 추가 정보 (선택사항)
            
        Returns:
            List[str]: 발견된 팩스번호 목록
        """
        try:
            # 검색 쿼리 생성
            query_parts = [org_name, "팩스", "fax"]
            if address:
                query_parts.append(address.split()[0])  # 주소의 첫 번째 부분만
            if additional_info:
                query_parts.append(additional_info)
            
            query = " ".join(query_parts)
            
            # 구글 검색 실행
            search_results = self.search_google(driver, query, max_results=3)
            
            # 팩스번호 추출
            fax_numbers = []
            for result in search_results:
                found_fax = self._extract_fax_from_text(result['text_content'])
                fax_numbers.extend(found_fax)
                
                found_fax_snippet = self._extract_fax_from_text(result['snippet'])
                fax_numbers.extend(found_fax_snippet)
            
            # 중복 제거
            unique_fax = list(set(fax_numbers))
            
            if unique_fax:
                self.logger.info(f"📠 팩스번호 발견: {org_name} - {unique_fax}")
            
            return unique_fax
            
        except Exception as e:
            self.logger.error(f"❌ 팩스번호 검색 실패: {org_name} - {e}")
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
                    
                    if fax_number:
                        # 숫자만 추출하여 정규화
                        normalized = re.sub(r'[^\d]', '', fax_number)
                        if len(normalized) >= 8:  # 최소 8자리
                            fax_numbers.append(fax_number)
            
            return fax_numbers
            
        except Exception as e:
            self.logger.error(f"❌ 팩스번호 추출 오류: {e}")
            return []
    
    def search_for_phone(self, driver, org_name: str, address: str = "") -> List[str]:
        """
        전화번호 검색
        
        Args:
            driver: WebDriver 인스턴스
            org_name: 기관명
            address: 주소 (선택사항)
            
        Returns:
            List[str]: 발견된 전화번호 목록
        """
        try:
            # 검색 쿼리 생성
            query_parts = [org_name, "전화번호", "연락처"]
            if address:
                query_parts.append(address.split()[0])
            
            query = " ".join(query_parts)
            
            # 구글 검색 실행
            search_results = self.search_google(driver, query, max_results=3)
            
            # 전화번호 추출
            phone_numbers = []
            phone_pattern = r'(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})'
            
            for result in search_results:
                text_content = result['text_content'] + " " + result['snippet']
                matches = re.findall(phone_pattern, text_content)
                
                for match in matches:
                    normalized = re.sub(r'[^\d]', '', match)
                    if len(normalized) >= 8:
                        phone_numbers.append(match)
            
            # 중복 제거
            unique_phones = list(set(phone_numbers))
            
            if unique_phones:
                self.logger.info(f"📞 전화번호 발견: {org_name} - {unique_phones}")
            
            return unique_phones
            
        except Exception as e:
            self.logger.error(f"❌ 전화번호 검색 실패: {org_name} - {e}")
            return []
    
    def search_for_homepage(self, driver, org_name: str, address: str = "") -> Optional[str]:
        """
        홈페이지 URL 검색
        
        Args:
            driver: WebDriver 인스턴스
            org_name: 기관명
            address: 주소 (선택사항)
            
        Returns:
            Optional[str]: 발견된 홈페이지 URL
        """
        try:
            # 검색 쿼리 생성
            query_parts = [org_name, "홈페이지", "사이트"]
            if address:
                query_parts.append(address.split()[0])
            
            query = " ".join(query_parts)
            
            # 구글 검색 실행
            search_results = self.search_google(driver, query, max_results=5)
            
            # 공식 홈페이지로 보이는 URL 찾기
            for result in search_results:
                url = result['url']
                title = result['title'].lower()
                
                # 공식 사이트로 보이는 키워드 체크
                if any(keyword in title for keyword in ['공식', '홈페이지', org_name.lower()]):
                    # 일반적인 홈페이지 도메인 패턴 체크
                    if any(domain in url for domain in ['.or.kr', '.go.kr', '.com', '.net', '.org']):
                        self.logger.info(f"🌐 홈페이지 발견: {org_name} - {url}")
                        return url
            
            # 첫 번째 결과 반환 (공식 사이트를 찾지 못한 경우)
            if search_results:
                url = search_results[0]['url']
                self.logger.info(f"🌐 홈페이지 (추정): {org_name} - {url}")
                return url
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 홈페이지 검색 실패: {org_name} - {e}")
            return None
    
    def get_search_delay(self) -> float:
        """검색 간 지연 시간 반환"""
        return random.uniform(2.0, 4.0)


# 전역 함수들 (기존 코드와의 호환성을 위해)
def search_google_improved(driver, query: str, fax_patterns: List[str]):
    """개선된 구글 검색 (호환성 함수)"""
    engine = GoogleSearchEngine()
    return engine.search_google(driver, query)

def search_google_for_fax(driver, org_name: str, address: str = ""):
    """팩스번호 구글 검색 (호환성 함수)"""
    engine = GoogleSearchEngine()
    return engine.search_for_fax(driver, org_name, address) 