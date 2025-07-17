#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전화번호/팩스번호 기반 해당기관 검색 시스템
"""

import os
import sys
import logging
import pandas as pd
import time
import random
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import google.generativeai as genai
from dotenv import load_dotenv

# 로깅 설정하는 함수
def setup_logger():
    """로깅 시스템 설정하는 메소드"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'phone_fax_institution_finder_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger('PhoneFaxInstitutionFinder')

class PhoneFaxInstitutionFinder:
    """전화번호와 팩스번호로 해당기관을 찾는 클래스"""
    
    def __init__(self):
        """전화번호/팩스번호 기관 찾기 시스템 초기화하는 메소드"""
        self.logger = setup_logger()
        self.driver = None
        self.ai_model = None
        
        # 환경 변수 로드
        load_dotenv()
        
        # 검색 결과 저장
        self.results = []
        self.processed_count = 0
        self.phone_success_count = 0
        self.fax_success_count = 0
        
        # 검색 패턴 정의
        self.search_patterns = {
            'phone': [
                '"{phone_number}"',
                '{phone_number} 전화번호',
                '{phone_number} 연락처',
                '{phone_number} 기관',
                '전화 {phone_number}'
            ],
            'fax': [
                '"{fax_number}"',
                '{fax_number} 팩스',
                '{fax_number} 팩스번호',
                '{fax_number} 기관',
                '팩스 {fax_number}'
            ]
        }
        
        # 기관명 추출 패턴
        self.institution_patterns = [
            r'([가-힣]+(?:동|구|시|군|읍|면)\s*(?:주민센터|사무소))',
            r'([가-힣]+(?:구청|시청|군청|도청))',
            r'([가-힣]+(?:대학교|대학|학교|병원|의료원|보건소))',
            r'([가-힣]+(?:복지관|센터|도서관|체육관))',
            r'([가-힣]+(?:협회|단체|재단|법인|조합|공사|공단))',
            r'([가-힣\s]{2,20}(?:주민센터|사무소|청|병원|학교|센터))',
        ]
        
        self.logger.info("🔍 전화번호/팩스번호 기관 찾기 시스템 초기화 완료")
    
    def initialize_ai_model(self) -> bool:
        """AI 모델 초기화하는 메소드"""
        try:
            api_key = os.getenv('GEMINI_API_KEY')
            if not api_key:
                self.logger.warning("⚠️ GEMINI_API_KEY가 설정되지 않음 - AI 기능 비활성화")
                return False
            
            genai.configure(api_key=api_key)
            self.ai_model = genai.GenerativeModel('gemini-2.0-flash-lite-001')
            self.logger.info("🤖 AI 모델 초기화 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ AI 모델 초기화 실패: {e}")
            return False
    
    def create_webdriver(self) -> bool:
        """WebDriver 생성하는 메소드"""
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
            
            # 봇 우회 및 성능 최적화 옵션
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-ipc-flooding-protection')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-features=TranslateUI')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--disable-sync')
            
            # 메모리 최적화
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=5')
            
            # User-Agent 설정
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            chrome_options.add_argument(f'--user-agent={user_agent}')
            
            # 드라이버 생성
            self.driver = uc.Chrome(options=chrome_options, version_main=None)
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            # 웹드라이버 감지 방지
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info("🚗 WebDriver 초기화 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 초기화 실패: {e}")
            return False
    
    def load_excel_data(self, file_path: str) -> pd.DataFrame:
        """엑셀 데이터 로드하는 메소드"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
            
            # 엑셀 파일 로드
            df = pd.read_excel(file_path)
            
            self.logger.info(f"📊 데이터 로드 완료: {len(df)}행")
            self.logger.info(f"📋 컬럼: {list(df.columns)}")
            
            # 전화번호나 팩스번호가 있는 행만 필터링
            phone_column = '전화번호'
            fax_column = '팩스번호'
            
            # 두 컬럼 중 하나라도 값이 있는 행 선택
            condition = (
                (df[phone_column].notna() & (df[phone_column] != '')) |
                (df[fax_column].notna() & (df[fax_column] != ''))
            )
            
            df_filtered = df[condition]
            
            phone_count = df_filtered[df_filtered[phone_column].notna() & (df_filtered[phone_column] != '')].shape[0]
            fax_count = df_filtered[df_filtered[fax_column].notna() & (df_filtered[fax_column] != '')].shape[0]
            
            self.logger.info(f"📞 전화번호가 있는 행: {phone_count}개")
            self.logger.info(f"📠 팩스번호가 있는 행: {fax_count}개")
            self.logger.info(f"🎯 처리 대상: {len(df_filtered)}행")
            
            return df_filtered
                
        except Exception as e:
            self.logger.error(f"❌ 엑셀 데이터 로드 실패: {e}")
            return pd.DataFrame()
    
    def normalize_phone_number(self, phone_number: str) -> str:
        """전화번호 정규화하는 메소드"""
        if pd.isna(phone_number) or phone_number == '':
            return ''
        
        # 숫자만 추출
        numbers = re.findall(r'\d+', str(phone_number))
        if len(numbers) >= 3:
            return f"{numbers[0]}-{numbers[1]}-{numbers[2]}"
        elif len(numbers) == 2:
            return f"{numbers[0]}-{numbers[1]}"
        else:
            return str(phone_number)
    
    def search_google_for_institution(self, number: str, number_type: str) -> Optional[str]:
        """구글에서 전화번호/팩스번호로 기관 검색하는 메소드"""
        try:
            search_patterns = self.search_patterns.get(number_type, [])
            
            # 여러 검색 패턴 시도
            for pattern in search_patterns:
                if number_type == 'phone':
                    search_query = pattern.format(phone_number=number)
                else:  # fax
                    search_query = pattern.format(fax_number=number)
                
                self.logger.info(f"🔍 {number_type} 검색 중: {search_query}")
                
                # 구글 검색
                search_url = f"https://www.google.com/search?q={search_query.replace(' ', '+')}"
                self.driver.get(search_url)
                
                # 랜덤 지연
                time.sleep(random.uniform(2, 4))
                
                # 페이지 소스 가져오기
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # 기관명 추출 시도
                institution_name = self._extract_institution_from_page(soup, number)
                
                if institution_name:
                    self.logger.info(f"✅ {number_type} 기관명 발견: {institution_name}")
                    return institution_name
                
                # 다음 패턴 시도 전 지연
                time.sleep(random.uniform(1, 2))
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 구글 검색 실패: {number} ({number_type}) - {e}")
            return None
    
    def _extract_institution_from_page(self, soup: BeautifulSoup, number: str) -> Optional[str]:
        """검색 결과 페이지에서 기관명 추출하는 메소드"""
        try:
            # 페이지 텍스트 가져오기
            page_text = soup.get_text()
            
            # 정규식 패턴으로 기관명 찾기
            for pattern in self.institution_patterns:
                matches = re.findall(pattern, page_text)
                if matches:
                    # 가장 적절한 매치 선택
                    for match in matches:
                        if self._is_valid_institution_name(match):
                            return match.strip()
            
            # AI 모델 사용 (사용 가능한 경우)
            if self.ai_model:
                return self._extract_with_ai(page_text, number)
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 기관명 추출 실패: {e}")
            return None
    
    def _is_valid_institution_name(self, name: str) -> bool:
        """유효한 기관명인지 확인하는 메소드"""
        if not name or len(name) < 2:
            return False
        
        # 유효한 기관명 키워드
        valid_keywords = [
            '주민센터', '사무소', '청', '구청', '시청', '군청', '도청',
            '병원', '의료원', '보건소', '학교', '대학', '대학교',
            '센터', '복지관', '도서관', '체육관', '공원',
            '협회', '단체', '재단', '법인', '조합', '공사', '공단'
        ]
        
        return any(keyword in name for keyword in valid_keywords)
    
    def _extract_with_ai(self, page_text: str, number: str) -> Optional[str]:
        """AI 모델로 기관명 추출하는 메소드"""
        try:
            # 텍스트 길이 제한 (토큰 제한 고려)
            limited_text = page_text[:3000]
            
            prompt = f"""
다음 텍스트에서 번호 '{number}'와 관련된 기관명을 찾아주세요.
기관명은 주민센터, 사무소, 구청, 시청, 병원, 학교, 센터 등이 포함된 공공기관이나 단체명입니다.

텍스트:
{limited_text}

기관명만 정확히 추출해서 답변해주세요. 없으면 '없음'이라고 답변해주세요.
"""
            
            response = self.ai_model.generate_content(prompt)
            result = response.text.strip()
            
            if result and result != '없음' and self._is_valid_institution_name(result):
                return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ AI 추출 실패: {e}")
            return None
    
    def process_phone_fax_data(self, df: pd.DataFrame) -> List[Dict]:
        """전화번호/팩스번호 데이터 처리하는 메소드"""
        results = []
        total_count = len(df)
        
        self.logger.info(f"🚀 처리 시작: {total_count}개 데이터")
        
        for idx, row in df.iterrows():
            try:
                # 전화번호와 팩스번호 추출
                phone_number = str(row.get('전화번호', ''))
                fax_number = str(row.get('팩스번호', ''))
                
                # 정규화
                normalized_phone = self.normalize_phone_number(phone_number) if phone_number and phone_number != 'nan' else ''
                normalized_fax = self.normalize_phone_number(fax_number) if fax_number and fax_number != 'nan' else ''
                
                self.logger.info(f"📞 처리 중 ({idx+1}/{total_count}): 전화({normalized_phone}), 팩스({normalized_fax})")
                
                # 전화번호 기관 검색
                phone_institution = ''
                if normalized_phone:
                    phone_institution = self.search_google_for_institution(normalized_phone, 'phone')
                    if phone_institution:
                        self.phone_success_count += 1
                
                # 팩스번호 기관 검색
                fax_institution = ''
                if normalized_fax:
                    fax_institution = self.search_google_for_institution(normalized_fax, 'fax')
                    if fax_institution:
                        self.fax_success_count += 1
                
                # 결과 저장
                result = {
                    '팩스번호': normalized_fax,
                    '해당기관': fax_institution if fax_institution else '미발견',
                    '전화번호': normalized_phone,
                    '해당기관.1': phone_institution if phone_institution else '미발견'
                }
                
                results.append(result)
                self.processed_count += 1
                
                # 진행률 출력
                if (idx + 1) % 5 == 0:
                    phone_rate = (self.phone_success_count / max(1, self.processed_count)) * 100
                    fax_rate = (self.fax_success_count / max(1, self.processed_count)) * 100
                    self.logger.info(f"📊 진행률: {idx+1}/{total_count} (전화 성공률: {phone_rate:.1f}%, 팩스 성공률: {fax_rate:.1f}%)")
                
                # 요청 지연 (봇 감지 방지)
                time.sleep(random.uniform(4, 7))
                
            except Exception as e:
                self.logger.error(f"❌ 행 처리 실패 {idx}: {e}")
                continue
        
        return results
    
    def save_results_to_desktop(self, results: List[Dict]) -> str:
        """결과를 데스크톱에 저장하는 메소드"""
        try:
            # 데스크톱 경로 가져오기
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            
            # 파일명 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"전화팩스기관검색결과_{timestamp}.xlsx"
            filepath = os.path.join(desktop_path, filename)
            
            # DataFrame 생성 및 저장
            df_results = pd.DataFrame(results)
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df_results.to_excel(writer, index=False, sheet_name='전화팩스기관검색결과')
            
            self.logger.info(f"💾 결과 저장 완료: {filepath}")
            
            # 통계 정보
            total_processed = len(results)
            phone_successful = len([r for r in results if r['해당기관.1'] != '미발견'])
            fax_successful = len([r for r in results if r['해당기관'] != '미발견'])
            
            self.logger.info(f"📊 처리 통계:")
            self.logger.info(f"   - 총 처리: {total_processed}개")
            self.logger.info(f"   - 전화번호 성공: {phone_successful}개")
            self.logger.info(f"   - 팩스번호 성공: {fax_successful}개")
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            return ""
    
    def cleanup(self):
        """리소스 정리하는 메소드"""
        try:
            if self.driver:
                self.driver.quit()
                self.logger.info("🧹 WebDriver 정리 완료")
        except Exception as e:
            self.logger.error(f"❌ 정리 중 오류: {e}")
    
    def run(self, excel_path: str) -> bool:
        """전체 프로세스 실행하는 메소드"""
        try:
            self.logger.info("🚀 전화번호/팩스번호 기관 찾기 시작!")
            
            # 1. AI 모델 초기화 (선택사항)
            self.initialize_ai_model()
            
            # 2. WebDriver 초기화
            if not self.create_webdriver():
                return False
            
            # 3. 데이터 로드
            df = self.load_excel_data(excel_path)
            if df.empty:
                return False
            
            # 4. 전화번호/팩스번호 데이터 처리
            results = self.process_phone_fax_data(df)
            
            if not results:
                self.logger.error("❌ 처리된 결과가 없습니다")
                return False
            
            # 5. 결과 저장
            output_path = self.save_results_to_desktop(results)
            
            if output_path:
                self.logger.info(f"✅ 완료! 결과 파일: {output_path}")
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 실행 중 오류: {e}")
            return False
        finally:
            self.cleanup()

def main():
    """메인 실행 함수"""
    try:
        # 파일 경로
        excel_path = r"C:\Users\MyoengHo Shin\pjt\cradcrawlpython\rawdatafile\failed_data_250715.xlsx"
        
        # 전화번호/팩스번호 기관 찾기 실행
        finder = PhoneFaxInstitutionFinder()
        success = finder.run(excel_path)
        
        if success:
            print("🎉 전화번호/팩스번호 기관명 검색이 완료되었습니다!")
        else:
            print("❌ 처리 중 오류가 발생했습니다.")
            
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")

if __name__ == "__main__":
    main() 