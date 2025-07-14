#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 결과 검증 엔진 - 홈페이지 파싱을 통한 전화번호/팩스번호 검증
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import undetected_chromedriver as uc
from config.settings import CRAWLING_PARAMS, WEBDRIVER_CONFIGS

class VerificationEngine:
    """AI 결과 검증 엔진"""
    
    def __init__(self, logger=None):
        """
        검증 엔진 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.driver = None
        self.session = requests.Session()
        
        # 검증 패턴
        self.phone_patterns = CRAWLING_PARAMS['phone_patterns']
        self.fax_patterns = CRAWLING_PARAMS['fax_patterns']
        
        # 사용자 에이전트 설정
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        self.logger.info("🔍 검증 엔진 초기화 완료")
    
    def initialize_webdriver(self) -> bool:
        """
        WebDriver 초기화 (다중 백업 방식)
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            fallback_drivers = WEBDRIVER_CONFIGS['fallback_drivers']
            
            for driver_type in fallback_drivers:
                try:
                    if 'undetected_chromedriver' in driver_type:
                        self.logger.info("🚗 Undetected ChromeDriver 초기화 시도")
                        options = uc.ChromeOptions()
                        for option in WEBDRIVER_CONFIGS['chrome_options']:
                            if option not in ['--headless']:  # 검증 시에는 헤드리스 모드 제외
                                options.add_argument(option)
                        
                        self.driver = uc.Chrome(options=options)
                        self.driver.set_page_load_timeout(30)
                        self.logger.info("✅ Undetected ChromeDriver 초기화 성공")
                        return True
                        
                    elif 'selenium.webdriver.Chrome' in driver_type:
                        self.logger.info("🚗 Selenium ChromeDriver 초기화 시도")
                        from selenium.webdriver.chrome.options import Options
                        options = Options()
                        for option in WEBDRIVER_CONFIGS['chrome_options']:
                            options.add_argument(option)
                        
                        self.driver = webdriver.Chrome(options=options)
                        self.driver.set_page_load_timeout(30)
                        self.logger.info("✅ Selenium ChromeDriver 초기화 성공")
                        return True
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ {driver_type} 초기화 실패: {e}")
                    continue
            
            self.logger.error("❌ 모든 WebDriver 초기화 실패")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 초기화 중 오류: {e}")
            return False
    
    def verify_contact_info(self, institution_name: str, homepage_url: str, 
                           ai_phone: str, ai_fax: str) -> Dict:
        """
        연락처 정보 검증
        
        Args:
            institution_name: 기관명
            homepage_url: 홈페이지 URL
            ai_phone: AI 추출 전화번호
            ai_fax: AI 추출 팩스번호
            
        Returns:
            Dict: 검증 결과
        """
        try:
            self.logger.info(f"🔍 [{institution_name}] 연락처 정보 검증 시작")
            
            if not homepage_url or homepage_url.strip() == '':
                self.logger.warning(f"⚠️ [{institution_name}] 홈페이지 URL이 없음")
                return {
                    'verified_phone': '',
                    'verified_fax': '',
                    'phone_match': False,
                    'fax_match': False,
                    'verification_status': 'no_homepage'
                }
            
            # 홈페이지 파싱 시도
            parsed_data = self.parse_homepage(homepage_url)
            
            if not parsed_data:
                self.logger.warning(f"⚠️ [{institution_name}] 홈페이지 파싱 실패")
                return {
                    'verified_phone': '',
                    'verified_fax': '',
                    'phone_match': False,
                    'fax_match': False,
                    'verification_status': 'parse_failed'
                }
            
            # 연락처 정보 비교
            result = self.compare_contact_info(
                institution_name, ai_phone, ai_fax, parsed_data
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ [{institution_name}] 연락처 정보 검증 실패: {e}")
            return {
                'verified_phone': '',
                'verified_fax': '',
                'phone_match': False,
                'fax_match': False,
                'verification_status': 'error'
            }
    
    def parse_homepage(self, url: str) -> Optional[Dict]:
        """
        홈페이지 파싱 (BS4 + Selenium)
        
        Args:
            url: 홈페이지 URL
            
        Returns:
            Optional[Dict]: 파싱된 연락처 정보
        """
        try:
            # URL 정규화
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url
            
            self.logger.info(f"🌐 홈페이지 파싱 시작: {url}")
            
            # 1단계: requests + BeautifulSoup 시도
            bs4_result = self._parse_with_bs4(url)
            if bs4_result and (bs4_result.get('phones') or bs4_result.get('faxes')):
                self.logger.info("✅ BS4 파싱 성공")
                return bs4_result
            
            # 2단계: Selenium 시도 (JS 렌더링 필요한 경우)
            selenium_result = self._parse_with_selenium(url)
            if selenium_result:
                self.logger.info("✅ Selenium 파싱 성공")
                return selenium_result
            
            self.logger.warning(f"⚠️ 홈페이지 파싱 실패: {url}")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 홈페이지 파싱 오류: {e}")
            return None
    
    def _parse_with_bs4(self, url: str) -> Optional[Dict]:
        """BeautifulSoup을 사용한 파싱"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 텍스트 추출
            text_content = soup.get_text()
            
            # 연락처 정보 추출
            phones = self._extract_phones(text_content)
            faxes = self._extract_faxes(text_content)
            
            return {
                'phones': phones,
                'faxes': faxes,
                'method': 'bs4'
            }
            
        except Exception as e:
            self.logger.debug(f"BS4 파싱 실패: {e}")
            return None
    
    def _parse_with_selenium(self, url: str) -> Optional[Dict]:
        """Selenium을 사용한 파싱 (JS 렌더링)"""
        try:
            if not self.driver:
                if not self.initialize_webdriver():
                    return None
            
            self.driver.get(url)
            
            # 페이지 로딩 대기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # 추가 대기 (동적 콘텐츠 로딩)
            time.sleep(3)
            
            # 텍스트 추출
            text_content = self.driver.find_element(By.TAG_NAME, "body").text
            
            # 연락처 정보 추출
            phones = self._extract_phones(text_content)
            faxes = self._extract_faxes(text_content)
            
            return {
                'phones': phones,
                'faxes': faxes,
                'method': 'selenium'
            }
            
        except Exception as e:
            self.logger.debug(f"Selenium 파싱 실패: {e}")
            return None
    
    def _extract_phones(self, text: str) -> List[str]:
        """텍스트에서 전화번호 추출"""
        try:
            phones = []
            
            for pattern in self.phone_patterns:
                matches = re.findall(pattern, text)
                phones.extend(matches)
            
            # 중복 제거 및 정규화
            unique_phones = []
            for phone in phones:
                normalized = self._normalize_phone(phone)
                if normalized and normalized not in unique_phones:
                    unique_phones.append(normalized)
            
            return unique_phones
            
        except Exception as e:
            self.logger.error(f"❌ 전화번호 추출 실패: {e}")
            return []
    
    def _extract_faxes(self, text: str) -> List[str]:
        """텍스트에서 팩스번호 추출"""
        try:
            faxes = []
            
            for pattern in self.fax_patterns:
                matches = re.findall(pattern, text)
                faxes.extend(matches)
            
            # 중복 제거 및 정규화
            unique_faxes = []
            for fax in faxes:
                normalized = self._normalize_phone(fax)
                if normalized and normalized not in unique_faxes:
                    unique_faxes.append(normalized)
            
            return unique_faxes
            
        except Exception as e:
            self.logger.error(f"❌ 팩스번호 추출 실패: {e}")
            return []
    
    def _normalize_phone(self, phone: str) -> str:
        """전화번호 정규화"""
        try:
            # 숫자만 추출
            digits = re.sub(r'[^\d]', '', phone)
            
            # 길이 검증
            if len(digits) < 9 or len(digits) > 11:
                return ''
            
            # 형식 통일 (02-1234-5678)
            if len(digits) == 9:
                return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
            elif len(digits) == 10:
                if digits.startswith('02'):
                    return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
                else:
                    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11:
                return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
            
            return ''
            
        except Exception as e:
            self.logger.error(f"❌ 전화번호 정규화 실패: {e}")
            return ''
    
    def compare_contact_info(self, institution_name: str, ai_phone: str, 
                           ai_fax: str, parsed_data: Dict) -> Dict:
        """
        AI 추출 정보와 파싱 정보 비교
        
        Args:
            institution_name: 기관명
            ai_phone: AI 추출 전화번호
            ai_fax: AI 추출 팩스번호
            parsed_data: 파싱된 데이터
            
        Returns:
            Dict: 비교 결과
        """
        try:
            parsed_phones = parsed_data.get('phones', [])
            parsed_faxes = parsed_data.get('faxes', [])
            
            # 전화번호 비교
            phone_match = False
            verified_phone = ''
            
            if ai_phone and parsed_phones:
                normalized_ai_phone = self._normalize_phone(ai_phone)
                for parsed_phone in parsed_phones:
                    if normalized_ai_phone == parsed_phone:
                        phone_match = True
                        verified_phone = parsed_phone
                        break
                
                if not phone_match and parsed_phones:
                    verified_phone = parsed_phones[0]  # 첫 번째 파싱된 전화번호 사용
            
            # 팩스번호 비교
            fax_match = False
            verified_fax = ''
            
            if ai_fax and parsed_faxes:
                normalized_ai_fax = self._normalize_phone(ai_fax)
                for parsed_fax in parsed_faxes:
                    if normalized_ai_fax == parsed_fax:
                        fax_match = True
                        verified_fax = parsed_fax
                        break
                
                if not fax_match and parsed_faxes:
                    verified_fax = parsed_faxes[0]  # 첫 번째 파싱된 팩스번호 사용
            
            # 결과 로깅
            self._log_comparison_result(
                institution_name, ai_phone, ai_fax, 
                verified_phone, verified_fax, phone_match, fax_match
            )
            
            return {
                'verified_phone': verified_phone,
                'verified_fax': verified_fax,
                'phone_match': phone_match,
                'fax_match': fax_match,
                'verification_status': 'success',
                'parsed_phones': parsed_phones,
                'parsed_faxes': parsed_faxes
            }
            
        except Exception as e:
            self.logger.error(f"❌ [{institution_name}] 연락처 정보 비교 실패: {e}")
            return {
                'verified_phone': '',
                'verified_fax': '',
                'phone_match': False,
                'fax_match': False,
                'verification_status': 'error'
            }
    
    def _log_comparison_result(self, institution_name: str, ai_phone: str, 
                             ai_fax: str, verified_phone: str, verified_fax: str,
                             phone_match: bool, fax_match: bool):
        """비교 결과 로깅"""
        try:
            self.logger.info(f"📊 [{institution_name}] 검증 결과:")
            
            # 전화번호 결과
            if ai_phone:
                if phone_match:
                    self.logger.info(f"   📞 전화번호: ✅ 일치 ({ai_phone})")
                else:
                    self.logger.info(f"   📞 전화번호: ❌ 불일치 (AI: {ai_phone}, 파싱: {verified_phone})")
            else:
                self.logger.info(f"   📞 전화번호: ➖ AI 추출 없음")
            
            # 팩스번호 결과
            if ai_fax:
                if fax_match:
                    self.logger.info(f"   📠 팩스번호: ✅ 일치 ({ai_fax})")
                else:
                    self.logger.info(f"   📠 팩스번호: ❌ 불일치 (AI: {ai_fax}, 파싱: {verified_fax})")
            else:
                self.logger.info(f"   📠 팩스번호: ➖ AI 추출 없음")
                
        except Exception as e:
            self.logger.error(f"❌ 비교 결과 로깅 실패: {e}")
    
    def batch_verify(self, institutions: List[Dict]) -> List[Dict]:
        """
        배치 검증 처리
        
        Args:
            institutions: 기관 정보 리스트
            
        Returns:
            List[Dict]: 검증 결과 리스트
        """
        try:
            self.logger.info(f"🔍 배치 검증 시작: {len(institutions)}개 기관")
            
            results = []
            
            for i, institution in enumerate(institutions):
                try:
                    result = self.verify_contact_info(
                        institution.get('institution_name', ''),
                        institution.get('homepage', ''),
                        institution.get('phone', ''),
                        institution.get('fax', '')
                    )
                    
                    # 원본 데이터에 검증 결과 추가
                    institution_result = institution.copy()
                    institution_result.update(result)
                    results.append(institution_result)
                    
                    # 진행률 로그
                    if (i + 1) % 10 == 0:
                        self.logger.info(f"📊 검증 진행률: {i + 1}/{len(institutions)} ({(i + 1)/len(institutions)*100:.1f}%)")
                    
                    # 딜레이 (서버 부하 방지)
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.logger.error(f"❌ 기관 검증 실패: {e}")
                    results.append(institution)
                    continue
            
            self.logger.info(f"✅ 배치 검증 완료: {len(results)}개 결과")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 배치 검증 실패: {e}")
            return institutions
    
    def cleanup(self):
        """정리 작업"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
            
            if self.session:
                self.session.close()
            
            self.logger.info("🧹 검증 엔진 정리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 정리 작업 실패: {e}")
    
    def __del__(self):
        """소멸자"""
        self.cleanup() 