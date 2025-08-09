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
from .homepage_crawler import HomepageCrawler

class GoogleSearchEngine:
    """구글 검색 엔진 클래스"""
    
    def __init__(self, logger=None):
        """
        구글 검색 엔진 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # HomepageCrawler 초기화
        self.homepage_crawler = HomepageCrawler(logger)
        
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
    
    def search(self, query: str, driver, max_results: int = 5) -> List[Dict]:
        """
        구글 검색 실행 (comp.py 호환성 메서드)
        
        Args:
            query: 검색 쿼리
            driver: WebDriver 인스턴스
            max_results: 최대 결과 수
            
        Returns:
            List[Dict]: 검색 결과 목록
        """
        return self.search_google(driver, query, max_results)
    
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
    
    def search_for_institution_name(self, driver, org_name: str, address: str = "", additional_info: str = "") -> Optional[str]:
        """
        기관명 검색 (5단계 폴백 시스템) - 봇 우회 강화
        
        Args:
            driver: WebDriver 인스턴스
            org_name: 기관명 또는 전화번호
            address: 주소 (선택사항)
            additional_info: 추가 정보
            
        Returns:
            Optional[str]: 검색된 기관명 또는 None
        """
        if not driver:
            self.logger.error("❌ WebDriver가 제공되지 않음")
            return None
        
        # 봇 우회를 위한 랜덤 지연
        import time
        import random
        initial_delay = random.uniform(1.0, 3.0)
        time.sleep(initial_delay)
        
        max_retries = 3
        bot_detection_retry = 0
        
        for retry in range(max_retries):
            try:
                self.logger.info(f"🔍 기관명 검색 시작 ({retry + 1}/{max_retries}): {org_name}")
                
                # 1단계: 기본 구글 검색
                result = self._search_basic_google(driver, org_name, address)
                if result:
                    self.logger.info(f"✅ 1단계 성공: {result}")
                    return result
                
                # 2단계: 다양한 검색 쿼리 시도
                result = self._search_with_variations(driver, org_name, address)
                if result:
                    self.logger.info(f"✅ 2단계 성공: {result}")
                    return result
                
                # 3단계: AI 기반 검색 (사용 가능한 경우)
                result = self._search_with_ai_analysis(driver, org_name, address)
                if result:
                    self.logger.info(f"✅ 3단계 성공: {result}")
                    return result
                
                # 4단계: 홈페이지 직접 크롤링
                result = self._search_homepage_direct(driver, org_name, address)
                if result:
                    self.logger.info(f"✅ 4단계 성공: {result}")
                    return result
                
                # 5단계: 최종 검증 및 필터링
                result = self._final_validation_search(driver, org_name)
                if result:
                    self.logger.info(f"✅ 5단계 성공: {result}")
                    return result
                
                # 재시도 전 봇 감지 회피 대기
                if retry < max_retries - 1:
                    retry_delay = random.uniform(3.0, 8.0) * (retry + 1)
                    self.logger.warning(f"⏱️ 재시도 전 대기: {retry_delay:.1f}초")
                    time.sleep(retry_delay)
            
            except Exception as e:
                error_msg = str(e).lower()
                
                # 봇 감지 체크
                if any(keyword in error_msg for keyword in ['bot', 'captcha', 'blocked', 'detected', 'too many requests']):
                    bot_detection_retry += 1
                    self.logger.warning(f"🤖 봇 감지 발생 ({bot_detection_retry}회): {e}")
                    
                    # 봇 감지시 긴 대기
                    bot_delay = random.uniform(15.0, 30.0) * bot_detection_retry
                    self.logger.warning(f"🛡️ 봇 감지 대기: {bot_delay:.1f}초")
                    time.sleep(bot_delay)
                    
                    # 드라이버 복구 시도
                    if hasattr(driver, 'refresh'):
                        try:
                            driver.refresh()
                            time.sleep(random.uniform(2.0, 4.0))
                        except:
                            pass
                else:
                    self.logger.error(f"❌ 검색 오류 ({retry + 1}/{max_retries}): {e}")
                    
                    if retry < max_retries - 1:
                        error_delay = random.uniform(2.0, 5.0)
                        time.sleep(error_delay)
        
        self.logger.warning(f"❌ 모든 단계 실패: {org_name}")
        return None
    
    def _search_basic_google(self, driver, org_name: str, address: str = "") -> Optional[str]:
        """1단계: 기본 구글 검색 (봇 우회 강화)"""
        try:
            import time
            import random
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.keys import Keys
            from bs4 import BeautifulSoup
            
            # 검색 쿼리 생성
            if address:
                search_query = f"{address.split()[0]} {org_name} 연락처"
            else:
                search_query = f"{org_name} 기관명"
            
            # 구글 검색 페이지로 이동 (봇 우회)
            driver.get('https://www.google.com')
            time.sleep(random.uniform(1.5, 3.0))
            
            # 검색창 찾기
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            
            # 인간처럼 천천히 입력
            search_box.clear()
            for char in search_query:
                search_box.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
            
            time.sleep(random.uniform(0.8, 1.5))
            search_box.send_keys(Keys.RETURN)
            
            # 결과 페이지 로딩 대기
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            
            time.sleep(random.uniform(2.0, 4.0))
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            text_content = soup.get_text()
            
            # 기관명 추출 시도
            institution_name = self._extract_institution_name(text_content, org_name)
            
            if institution_name:
                return institution_name
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 기본 구글 검색 실패: {e}")
            return None
    
    def _search_with_variations(self, driver, org_name: str, address: str = "") -> Optional[str]:
        """2단계: 다양한 검색 쿼리 시도"""
        try:
            import time
            import random
            
            # 다양한 검색 쿼리 패턴
            query_variations = [
                f"{org_name} 실제기관명",
                f"{org_name} 정식명칭",
                f"{org_name} 공식명칭",
                f"{org_name} site:*.go.kr",
                f"{org_name} site:*.or.kr"
            ]
            
            if address:
                location = address.split()[0]
                query_variations.extend([
                    f"{location} {org_name}",
                    f'"{org_name}" {location}'
                ])
            
            for query in query_variations:
                try:
                    result = self._perform_google_search(driver, query)
                    if result:
                        return result
                    
                    # 검색 간 지연
                    time.sleep(random.uniform(2.0, 4.0))
                    
                except Exception as e:
                    self.logger.debug(f"검색 변형 실패: {query} - {e}")
                    continue
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 검색 변형 실패: {e}")
            return None
    
    def _search_with_ai_analysis(self, driver, org_name: str, address: str = "") -> Optional[str]:
        """3단계: AI 기반 검색 분석 (사용 가능한 경우)"""
        try:
            # 이 부분은 나중에 AI 모델이 사용 가능할 때 구현
            return None
            
        except Exception as e:
            self.logger.debug(f"AI 분석 검색 건너뛰기: {e}")
            return None
    
    def _search_homepage_direct(self, driver, org_name: str, address: str = "") -> Optional[str]:
        """4단계: 홈페이지 직접 크롤링"""
        try:
            # HomepageCrawler 사용
            homepage_result = self.homepage_crawler.crawl_homepage_for_institution(
                driver, org_name, address
            )
            
            if homepage_result and homepage_result.get('institution_name'):
                return homepage_result['institution_name']
            
            return None
            
        except Exception as e:
            self.logger.debug(f"홈페이지 직접 크롤링 실패: {e}")
            return None
    
    def _final_validation_search(self, driver, org_name: str) -> Optional[str]:
        """5단계: 최종 검증 및 필터링"""
        try:
            # 최후의 수단으로 단순한 검색 시도
            result = self._perform_simple_search(driver, org_name)
            
            if result:
                # 결과 검증
                if self._validate_institution_name(result, org_name):
                    return result
            
            return None
            
        except Exception as e:
            self.logger.debug(f"최종 검증 검색 실패: {e}")
            return None
    
    def _perform_google_search(self, driver, query: str) -> Optional[str]:
        """구글 검색 수행 (공통 로직)"""
        try:
            import time
            import random
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.keys import Keys
            from bs4 import BeautifulSoup
            
            # 새로운 탭에서 검색 (봇 감지 회피)
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])
            
            # 구글 검색
            driver.get('https://www.google.com')
            time.sleep(random.uniform(1.0, 2.5))
            
            search_box = WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.NAME, 'q'))
            )
            
            search_box.clear()
            search_box.send_keys(query)
            time.sleep(random.uniform(0.5, 1.0))
            search_box.send_keys(Keys.RETURN)
            
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            
            time.sleep(random.uniform(1.5, 3.0))
            
            # 결과 분석
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            text_content = soup.get_text()
            
            # 탭 닫기
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            
            return self._extract_institution_name(text_content, query)
            
        except Exception as e:
            # 탭 정리
            try:
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
            except:
                pass
            
            self.logger.debug(f"구글 검색 실패: {query} - {e}")
            return None
    
    def _perform_simple_search(self, driver, org_name: str) -> Optional[str]:
        """단순 검색 수행"""
        try:
            return self._perform_google_search(driver, f'"{org_name}"')
        except Exception as e:
            self.logger.debug(f"단순 검색 실패: {e}")
            return None
    
    def _extract_institution_name(self, text_content: str, org_name: str) -> Optional[str]:
        """텍스트에서 기관명 추출"""
        try:
            # 기본적인 기관명 추출 로직
            # 더 정교한 로직이 필요하면 나중에 개선
            
            # 간단한 패턴 매칭
            if org_name in text_content:
                # org_name 주변의 텍스트에서 기관명 찾기
                lines = text_content.split('\n')
                for line in lines:
                    if org_name in line:
                        # 라인에서 기관명처럼 보이는 부분 추출
                        words = line.split()
                        for word in words:
                            if len(word) > 2 and ('센터' in word or '기관' in word or '청' in word):
                                return word.strip()
            
            return None
            
        except Exception as e:
            self.logger.debug(f"기관명 추출 실패: {e}")
            return None
    
    def _validate_institution_name(self, name: str, original: str) -> bool:
        """기관명 검증"""
        try:
            if not name or len(name) < 2:
                return False
            
            # 금지된 패턴 체크
            forbidden_patterns = ['옥션원모바일', '광고', '배너', '클릭', '링크']
            for pattern in forbidden_patterns:
                if pattern in name:
                    return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"기관명 검증 실패: {e}")
            return False
    
    def _is_valid_institution_name(self, text: str, original_name: str) -> bool:
        """유효한 기관명인지 검증"""
        try:
            if not text or len(text.strip()) < 2:
                return False
            
            text = text.strip()
            
            # 금지된 키워드 확인
            forbidden_keywords = [
                "옥션원모바일", "광고", "배너", "클릭", "바로가기", 
                "네이버", "구글", "검색", "모바일", "앱", "다운로드"
            ]
            
            for keyword in forbidden_keywords:
                if keyword in text:
                    return False
            
            # 검색결과 패턴 확인
            invalid_patterns = [
                r'검색결과.*센터',
                r'.*모바일.*센터',
                r'\d+.*센터.*\d+',  # 숫자가 포함된 이상한 패턴
            ]
            
            for pattern in invalid_patterns:
                if re.search(pattern, text):
                    return False
            
            # 유효한 기관 패턴 확인
            valid_patterns = [
                r'.*주민센터$',
                r'.*구청$',
                r'.*시청$',
                r'.*동사무소$',
                r'.*면사무소$',
                r'.*읍사무소$',
                r'.*동주민센터$'
            ]
            
            for pattern in valid_patterns:
                if re.search(pattern, text):
                    return True
            
            # 원본 기관명과의 유사성 검사
            if original_name in text or text in original_name:
                return True
            
            return False
            
        except Exception as e:
            self.logger.debug(f"기관명 검증 오류: {e}")
            return False
    
    def _extract_institution_from_text(self, text: str, org_name: str) -> Optional[str]:
        """텍스트에서 기관명 추출"""
        try:
            # 간단한 추출 로직 - 줄바꿈이나 특수문자로 분리된 첫 번째 유효한 부분
            parts = re.split(r'[\n\r\|·•]', text)
            
            for part in parts:
                part = part.strip()
                if self._is_valid_institution_name(part, org_name):
                    return part
            
            return None
            
        except Exception as e:
            self.logger.debug(f"기관명 추출 오류: {e}")
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