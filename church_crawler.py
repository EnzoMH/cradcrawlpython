import pandas as pd
import time
import os
import sys
import re
import json
import subprocess
import undetected_chromedriver as uc
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import requests
import google.generativeai as genai
from datetime import datetime
import logging
import dotenv

dotenv.load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('church_crawler.log'),
        logging.StreamHandler()
    ]
)

class ChurchCrawler:
    def __init__(self):
        # Gemini API 설정 (여기에 API 키를 입력하세요)
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')  # API 키를 입력하세요
        if not self.gemini_api_key:
            logging.error("GEMINI_API_KEY가 .env 파일에 설정되지 않았습니다!")
            sys.exit(1)

        genai.configure(api_key=self.gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
        # 크롬 드라이버 설정
        self.setup_chrome_driver()
        
        # 결과 저장용
        self.results = []
        self.batch_size = 2500
        self.current_batch = 0
        
    def setup_chrome_driver(self):
        """Chrome WebDriver 설정"""
        try:
            chrome_options = uc.ChromeOptions()
            # headless=False로 설정 (요청사항)
            # chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # undetected-chromedriver 사용
            self.driver = uc.Chrome(options=chrome_options)
            
        except Exception as e:
            logging.error(f"Chrome driver 초기화 실패: {e}")
            sys.exit(1)
    
    def load_church_data(self, file_path):
        """Excel 파일에서 교회 데이터 로드"""
        try:
            df = pd.read_excel(file_path)
            logging.info(f"총 {len(df)}개의 교회 데이터를 로드했습니다.")
            return df
        except Exception as e:
            logging.error(f"Excel 파일 로드 실패: {e}")
            return None
    
    def extract_phone_info_with_gemini(self, html_content, church_name):
        """Gemini API를 사용하여 전화번호/팩스번호 추출"""
        try:
            prompt = f"""
다음 HTML 내용에서 '{church_name}'의 전화번호와 팩스번호를 추출해주세요.

HTML 내용:
{html_content[:5000]}  # 토큰 제한 고려하여 처음 5000자만

응답 형식:
{{
    "phone": "전화번호 (예: 02-1234-5678)",
    "fax": "팩스번호 (예: 02-1234-5679)"
}}

- 전화번호가 없으면 "phone": null
- 팩스번호가 없으면 "fax": null
- 번호는 하이픈(-) 포함된 형태로 반환
- 대표번호, 메인번호 우선 추출
- JSON 형식만 반환 (다른 텍스트 없이)
"""
            
            logging.info(f"[Gemini API] {church_name} - 정보 추출 요청 중...")
            response = self.model.generate_content(prompt)
            result = response.text.strip()

            logging.info(f"[Gemini API] {church_name} - 원본 응답: {result}")
            
            # JSON 파싱 시도
            try:
                json_start = result.find('{')
                json_end = result.rfind('}') + 1
                if json_start != -1 and json_end > json_start:
                    json_str = result[json_start:json_end]
                    parsed_result = json.loads(json_str)
                    
                    # 추출 결과 로깅
                    phone = parsed_result.get('phone')
                    fax = parsed_result.get('fax')
                    
                    if phone or fax:
                        extracted_info = []
                        if phone:
                            extracted_info.append(f"전화번호: {phone}")
                        if fax:
                            extracted_info.append(f"팩스번호: {fax}")
                        logging.info(f"[추출 성공] {church_name} - {', '.join(extracted_info)}")
                    else:
                        logging.warning(f"[추출 실패] {church_name} - 전화번호/팩스번호 없음")
                    
                    return parsed_result
            except json.JSONDecodeError as e:
                logging.error(f"[Gemini API] {church_name} - JSON 파싱 실패: {e}")
                logging.error(f"[Gemini API] {church_name} - 파싱 시도한 문자열: {result}")
            except Exception as e:
                logging.error(f"[Gemini API] {church_name} - 응답 처리 실패: {e}")
            
            logging.warning(f"[추출 실패] {church_name} - Gemini API 응답을 처리할 수 없음")
            return {"phone": None, "fax": None}
            
        except Exception as e:
            logging.error(f"[Gemini API] {church_name} - API 호출 실패: {e}")
            logging.error(f"Gemini API 호출 실패: {e}")
            return {"phone": None, "fax": None}
    
    def validate_phone_number(self, phone, address):
        """전화번호 검증 - 지역코드와 주소 매칭"""
        if not phone or not address:
            return "invalid"
        
        # 전화번호에서 지역코드 추출
        phone_clean = re.sub(r'[^\d-]', '', phone)
        area_code_match = re.match(r'(\d{2,3})-', phone_clean)
        
        if not area_code_match:
            return "invalid"
        
        area_code = area_code_match.group(1)
        
        # 지역코드와 주소 매칭 테이블
        area_mapping = {
            '02': ['서울'],
            '031': ['경기', '인천'],
            '032': ['인천'],
            '033': ['강원'],
            '041': ['충남', '충청남도'],
            '042': ['대전'],
            '043': ['충북', '충청북도'],
            '044': ['세종'],
            '051': ['부산'],
            '052': ['울산'],
            '053': ['대구'],
            '054': ['경북', '경상북도'],
            '055': ['경남', '경상남도'],
            '061': ['전남', '전라남도'],
            '062': ['광주'],
            '063': ['전북', '전라북도'],
            '064': ['제주']
        }
        
        if area_code in area_mapping:
            for region in area_mapping[area_code]:
                if region in address:
                    return "valid"
            return "suspicious"  # 지역코드는 있지만 주소와 불일치
        
        # 휴대폰 번호인 경우
        if area_code in ['010', '011', '016', '017', '018', '019']:
            return "mobile"
        
        return "invalid"
    
    def search_and_validate(self, church_name, search_type, address):
        """구글 검색을 통한 전화번호/팩스번호 검증"""
        try:
            search_query = f"{church_name} {search_type}"
            self.driver.get(f"https://www.google.com/search?q={search_query}")
            time.sleep(2)
            
            # 검색 결과 HTML 가져오기
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # 검색 결과에서 전화번호 패턴 찾기
            phone_pattern = r'(\d{2,3}-\d{3,4}-\d{4})'
            text_content = soup.get_text()
            
            phone_matches = re.findall(phone_pattern, text_content)
            
            validated_numbers = []
            for phone in phone_matches:
                validation = self.validate_phone_number(phone, address)
                validated_numbers.append({
                    'number': phone,
                    'validation': validation
                })
            
            # 우선순위: valid > mobile > suspicious
            for item in validated_numbers:
                if item['validation'] == 'valid':
                    return item
            
            for item in validated_numbers:
                if item['validation'] == 'mobile':
                    return item
                    
            return None
            
        except Exception as e:
            logging.error(f"검색 검증 실패: {e}")
            return None
    
    def crawl_website(self, url, church_name, address):
        """웹사이트 크롤링 및 정보 추출"""
        try:
            # URL 정규화
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url
            
            self.driver.get(url)
            time.sleep(3)
            
            # 페이지 HTML 가져오기
            html = self.driver.page_source
            
            # BeautifulSoup으로 파싱
            soup = BeautifulSoup(html, 'html.parser')
            
            # 불필요한 태그 제거
            for tag in soup(['script', 'style', 'nav', 'header', 'footer']):
                tag.decompose()
            
            # 텍스트 추출 및 전처리
            text_content = soup.get_text()
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            cleaned_content = '\n'.join(lines)
            
            # Gemini API로 전화번호/팩스번호 추출
            extracted_info = self.extract_phone_info_with_gemini(cleaned_content, church_name)
            
            result = {
                'phone_website': extracted_info.get('phone'),
                'fax_website': extracted_info.get('fax'),
                'phone_search': None,
                'fax_search': None
            }
            
            # 구글 검색으로 검증
            if extracted_info.get('phone'):
                phone_validation = self.validate_phone_number(extracted_info.get('phone'), address)
                result['phone_validation'] = phone_validation
            
            # 전화번호 검색 검증
            phone_search_result = self.search_and_validate(church_name, "전화번호", address)
            if phone_search_result:
                result['phone_search'] = phone_search_result['number']
                result['phone_search_validation'] = phone_search_result['validation']
            
            # 팩스번호 검색 검증
            fax_search_result = self.search_and_validate(church_name, "팩스번호", address)
            if fax_search_result:
                # 전화번호와 같은지 확인
                if (result.get('phone_website') and 
                    fax_search_result['number'] != result.get('phone_website')):
                    result['fax_search'] = fax_search_result['number']
                    result['fax_search_validation'] = fax_search_result['validation']
            
            return result
            
        except Exception as e:
            logging.error(f"웹사이트 크롤링 실패 ({url}): {e}")
            return None
    
    def save_progress(self, data, batch_num):
        """진행상황 임시 저장"""
        filename = f"교회저장임시데이터_{batch_num}.xlsx"
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False)
        logging.info(f"배치 {batch_num} 저장 완료: {filename}")
    
    def shutdown_pc(self):
        """PC 전원 끄기"""
        try:
            logging.info("작업 완료! 1분 후 PC를 종료합니다...")
            time.sleep(60)  # 1분 대기
            subprocess.run(["shutdown", "/s", "/t", "0"], shell=True)
        except Exception as e:
            logging.error(f"PC 종료 실패: {e}")
    
    def process_churches(self, df):
        """교회 데이터 처리 메인 함수"""
        total_count = len(df)
        processed_count = 0
        
        for index, row in df.iterrows():
            try:
                # NaN 값들을 빈 문자열로 변환
                church_name = '' if pd.isna(row.get('기관명')) else str(row.get('기관명')).strip()
                address = '' if pd.isna(row.get('주소')) else str(row.get('주소')).strip()
                phone = '' if pd.isna(row.get('전화번호')) else str(row.get('전화번호')).strip()
                fax = '' if pd.isna(row.get('팩스번호')) else str(row.get('팩스번호')).strip()
                website = '' if pd.isna(row.get('홈페이지')) else str(row.get('홈페이지')).strip()
                
                logging.info(f"처리중 ({processed_count+1}/{total_count}): {church_name}")
                
                result_data = {
                    '기관명': church_name,
                    '주소': address,
                    '기존_전화번호': phone,
                    '기존_팩스번호': fax,
                    '홈페이지': website,
                    '추출_전화번호': None,
                    '추출_팩스번호': None,
                    '검증_결과': None,
                    '처리_시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 홈페이지가 있는 경우 크롤링 (빈 문자열이나 'nan' 제외)
                if website and website.lower() not in ['', 'nan', 'none']:
                    crawl_result = self.crawl_website(website, church_name, address)
                    if crawl_result:
                        extracted_phone = crawl_result.get('phone_website') or crawl_result.get('phone_search')
                        extracted_fax = crawl_result.get('fax_website') or crawl_result.get('fax_search')
                        
                        result_data.update({
                            '추출_전화번호': extracted_phone,
                            '추출_팩스번호': extracted_fax,
                            '검증_결과': json.dumps(crawl_result, ensure_ascii=False)
                        })
                        
                        # 전화번호/팩스번호 추출 결과 로깅
                        extraction_results = []
                        if extracted_phone:
                            extraction_results.append(f"전화번호: {extracted_phone}")
                        if extracted_fax:
                            extraction_results.append(f"팩스번호: {extracted_fax}")
                        
                        if extraction_results:
                            logging.info(f"[추출 성공] {church_name} -> {', '.join(extraction_results)}")
                        else:
                            logging.warning(f"[추출 실패] {church_name} -> 전화번호/팩스번호 추출되지 않음")
                    else:
                        logging.warning(f"[크롤링 실패] {church_name} -> 웹사이트 크롤링 실패")
                else:
                    # 홈페이지가 없어도 구글 검색으로 전화번호/팩스번호 찾기
                    logging.info(f"[웹사이트 없음] {church_name} -> 구글 검색으로 전화번호/팩스번호 찾기")
                    
                    search_result = {
                        'phone_search': None,
                        'fax_search': None
                    }
                    
                    # 전화번호 검색
                    phone_search_result = self.search_and_validate(church_name, "전화번호", address)
                    if phone_search_result:
                        search_result['phone_search'] = phone_search_result['number']
                        search_result['phone_search_validation'] = phone_search_result['validation']
                    
                    # 팩스번호 검색
                    fax_search_result = self.search_and_validate(church_name, "팩스번호", address)
                    if fax_search_result:
                        # 전화번호와 다른지 확인
                        if (search_result.get('phone_search') and 
                            fax_search_result['number'] != search_result.get('phone_search')):
                            search_result['fax_search'] = fax_search_result['number']
                            search_result['fax_search_validation'] = fax_search_result['validation']
                    
                    extracted_phone = search_result.get('phone_search')
                    extracted_fax = search_result.get('fax_search')
                    
                    if extracted_phone or extracted_fax:
                        result_data.update({
                            '추출_전화번호': extracted_phone,
                            '추출_팩스번호': extracted_fax,
                            '검증_결과': json.dumps(search_result, ensure_ascii=False)
                        })
                        
                        # 추출 결과 로깅
                        extraction_results = []
                        if extracted_phone:
                            extraction_results.append(f"전화번호: {extracted_phone}")
                        if extracted_fax:
                            extraction_results.append(f"팩스번호: {extracted_fax}")
                        
                        logging.info(f"[검색 추출 성공] {church_name} -> {', '.join(extraction_results)}")
                    else:
                        logging.warning(f"[검색 추출 실패] {church_name} -> 구글 검색에서도 찾을 수 없음")
                
                self.results.append(result_data)
                processed_count += 1
                
                # 배치 단위로 저장
                if len(self.results) >= self.batch_size:
                    self.current_batch += 1
                    self.save_progress(self.results, self.current_batch)
                    
                    # 배치 완료 시 통계 로깅
                    success_count = sum(1 for r in self.results if r.get('추출_전화번호') or r.get('추출_팩스번호'))
                    logging.info(f"📊 [배치 {self.current_batch} 완료] 성공: {success_count}/{len(self.results)}개")
                    
                    self.results = []
                
                # 요청 간격 (서버 부하 방지)
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"💥 [처리 오류] {church_name} → {e}")
                continue
        
        # 남은 데이터 저장
        if self.results:
            self.current_batch += 1
            success_count = sum(1 for r in self.results if r.get('추출_전화번호') or r.get('추출_팩스번호'))
            logging.info(f"📊 [최종 배치 {self.current_batch}] 성공: {success_count}/{len(self.results)}개")
            self.save_progress(self.results, self.current_batch)
        
        # 최종 결과 저장
        self.save_final_results()
        
        logging.info("모든 교회 데이터 처리 완료!")
        
        # PC 종료
        self.shutdown_pc()
    
    def save_final_results(self):
        """최종 결과 통합 저장"""
        try:
            all_data = []
            for i in range(1, self.current_batch + 1):
                filename = f"교회저장임시데이터_{i}.xlsx"
                if os.path.exists(filename):
                    df = pd.read_excel(filename)
                    all_data.extend(df.to_dict('records'))
            
            if all_data:
                final_df = pd.DataFrame(all_data)
                final_df.to_excel('교회데이터최종.xlsx', index=False)
                logging.info("최종 결과 저장 완료: 교회데이터최종.xlsx")
        except Exception as e:
            logging.error(f"최종 결과 저장 실패: {e}")
    
    def run(self):
        """메인 실행 함수"""
        try:
            # 교회 데이터 로드
            df = self.load_church_data('church.xlsx')
            if df is None:
                return
            
            logging.info("교회 데이터 크롤링을 시작합니다...")
            
            # 처리 시작
            self.process_churches(df)
            
        except Exception as e:
            logging.error(f"실행 중 오류 발생: {e}")
        finally:
            if hasattr(self, 'driver'):
                self.driver.quit()

def main():
    """메인 함수"""
    print("교회 데이터 크롤러를 시작합니다.")
    print("주의: Gemini API 키를 설정해야 합니다!")
    
    crawler = ChurchCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
