import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import json
import re
import os
from typing import List, Dict, Tuple
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 상수 정의
DEFAULT_ITEMS_PER_PAGE = 10
DEFAULT_DELAY = 1
DEFAULT_DETAIL_DELAY = 0.5
DEFAULT_TIMEOUT = 10
TEMP_SAVE_INTERVAL = 100

# 저장 경로 설정
SAVE_PATH = r"C:\Users\kimyh\Desktop"


class TaekwondoCrawler:
    """태권도장 정보 크롤러"""
    
    def __init__(self, use_selenium=True, save_path=SAVE_PATH):
        """
        크롤러 초기화
        
        Args:
            use_selenium (bool): Selenium 사용 여부. True면 동적 데이터 지원, False면 정적 데이터만
            save_path (str): 파일 저장 경로
        """
        self.base_url = "https://tkdcon.net"
        self.list_url = "https://tkdcon.net/portalk/support/tkdStd/selectTkdStdList.do"
        self.detail_url = "https://tkdcon.net/portalk/support/tkdStd/selectTkdStdDetailLyrPop.do"
        self.use_selenium = use_selenium
        self.session = requests.Session()
        self.driver = None
        self.save_path = save_path
        
        # 저장 경로 확인 및 생성
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)
            logger.info(f"저장 경로 생성: {self.save_path}")
        
        if use_selenium:
            self.setup_selenium()
    
    def setup_selenium(self):
        """Selenium WebDriver 설정 (개선된 버전)"""
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')  # GPU 오류 방지
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-logging')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--remote-debugging-port=9222')
        
        # 페이지 로드 전략 설정
        options.add_argument('--page-load-strategy=eager')
        
        # 타임아웃 설정
        options.add_argument('--script-timeout=60000')  # 60초
        
        self.driver = webdriver.Chrome(options=options)
        
        # 타임아웃 설정
        self.driver.set_page_load_timeout(30)
        self.driver.set_script_timeout(60)  # JavaScript 실행 타임아웃 60초로 증가
        
        logger.info("Selenium WebDriver 초기화 완료")
    
    def get_total_items_and_pages(self) -> Tuple[int, int]:
        """전체 항목 수와 페이지 수 동적으로 확인 (개선된 버전)"""
        try:
            if self.use_selenium:
                # 먼저 페이지 로드
                self.driver.get(self.list_url)
                
                # 페이지 완전 로드 대기 (더 긴 시간)
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "studio_cont"))
                )
                
                # 추가 대기 (JavaScript 실행 완료를 위해)
                time.sleep(3)
                
                try:
                    # 페이지 상단의 전체 개수 정보 찾기
                    total_element = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".total_count"))
                    )
                    total_text = total_element.text
                    total_items = int(re.sub(r'[^\d]', '', total_text))
                except:
                    # total_count 찾기 실패 시 대체 방법
                    logger.warning("total_count 요소를 찾을 수 없어 기본값 사용")
                    total_items = 9401
                
                try:
                    # 마지막 페이지 번호 확인
                    last_page_link = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".paging_btn.next_end"))
                    )
                    onclick = last_page_link.get_attribute('onclick')
                    last_page = int(re.search(r'fnSearchList\((\d+)\)', onclick).group(1))
                except:
                    logger.warning("마지막 페이지 번호를 찾을 수 없어 기본값 사용")
                    last_page = 941
                    
                return total_items, last_page
            else:
                # BeautifulSoup 사용 시 (기존 코드 유지)
                response = self.session.get(self.list_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                total_text = soup.find(class_='total_count').text
                total_items = int(re.sub(r'[^\d]', '', total_text))
                
                last_page_link = soup.find('a', class_='paging_btn next_end')
                onclick = last_page_link.get('onclick')
                last_page = int(re.search(r'fnSearchList\((\d+)\)', onclick).group(1))
                
                return total_items, last_page
        except Exception as e:
            logger.warning(f"전체 페이지 수 확인 실패, 기본값 사용: {e}")
            return 9401, 941
    
    def get_total_pages(self) -> int:
        """전체 페이지 수 확인 (하위 호환성을 위한 메서드)"""
        _, total_pages = self.get_total_items_and_pages()
        return total_pages
    
    def crawl_with_bs4(self, page_num: int) -> List[Dict]:
        """
        BeautifulSoup을 사용한 크롤링 (정적 데이터만)
        
        Args:
            page_num (int): 크롤링할 페이지 번호
            
        Returns:
            List[Dict]: 페이지의 태권도장 정보 리스트
        """
        results = []
        
        # 페이지 요청 - POST 방식
        params = {
            'pageIndex': page_num,
            'pageUnit': DEFAULT_ITEMS_PER_PAGE
        }
        
        response = self.session.post(self.list_url, data=params)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 태권도장 목록 파싱
        studios = soup.find_all('div', class_='studio_cont')
        
        for studio in studios:
            try:
                data = {
                    'name': studio.find('span', class_='txt_name').text.strip(),
                    'address': studio.find('span', class_='txt_addr').text.replace('지번주소', '').strip(),
                    'phone': studio.find('span', class_='txt_tel').text.strip()
                }
                
                # 상세보기 링크에서 파라미터 추출
                detail_link = studio.find('a', href=True)
                if detail_link:
                    href = detail_link['href']
                    # JavaScript 함수에서 파라미터 추출
                    params = re.findall(r"'(\w+)'", href)
                    if len(params) >= 3:
                        data['gym_code'] = params[0]
                        data['area_code'] = params[1]
                        data['district_code'] = params[2]
                
                results.append(data)
            except Exception as e:
                logger.error(f"BS4 항목 파싱 오류: {e}")
                continue
        
        return results
    
    def crawl_with_selenium(self, page_num: int) -> List[Dict]:
        """
        Selenium을 사용한 크롤링 (개선된 버전)
        """
        results = []
        
        try:
            # 첫 페이지가 아닌 경우 페이지 이동
            if page_num == 1:
                self.driver.get(self.list_url)
            else:
                # fnSearchList 함수로 페이지 이동
                script = f"fnSearchList({page_num});"
                self.driver.execute_script(script)
            
            # 페이지 로드 대기
            WebDriverWait(self.driver, DEFAULT_TIMEOUT).until(
                EC.presence_of_element_located((By.CLASS_NAME, "studio_cont"))
            )
            
            # 현재 페이지 번호 확인 (페이지 이동 확인용)
            try:
                current_page = self.driver.find_element(By.CSS_SELECTOR, ".paging_numbers .current span").get_attribute("data-num")
                logger.info(f"현재 페이지: {current_page}")
            except Exception as e:
                logger.debug(f"페이지 번호 확인 실패: {e}")
            
            time.sleep(1)  # 추가 대기
            
            # 태권도장 목록 찾기
            studios = self.driver.find_elements(By.CLASS_NAME, "studio_cont")
            
            for studio in studios:
                try:
                    data = {
                        'name': studio.find_element(By.CLASS_NAME, "txt_name").text,
                        'address': studio.find_element(By.CLASS_NAME, "txt_addr").text.replace('지번주소', '').strip(),
                        'phone': studio.find_element(By.CLASS_NAME, "txt_tel").text
                    }
                    
                    # 상세보기 링크에서 파라미터 추출
                    detail_link = studio.find_element(By.CSS_SELECTOR, "a[href*='fnTkdStdDetailLyrPop']")
                    onclick = detail_link.get_attribute('href')
                    
                    params = re.findall(r"'(\w+)'", onclick)
                    if len(params) >= 3:
                        data['gym_code'] = params[0]
                        data['area_code'] = params[1]
                        data['district_code'] = params[2]
                    
                    results.append(data)
                except Exception as e:
                    logger.error(f"Selenium 항목 파싱 오류: {e}")
                    continue
            
            return results
        except Exception as e:
            logger.error(f"Selenium 크롤링 중 오류 발생: {e}")
            return []
    
    def get_detail_info(self, gym_code: str, area_code: str, district_code: str) -> Dict:
        """
        AJAX로 상세 정보 가져오기 (BeautifulSoup 방식)
        
        Args:
            gym_code (str): 체육관 코드
            area_code (str): 지역 코드
            district_code (str): 구역 코드
            
        Returns:
            Dict: 상세 정보
        """
        data = {
            "searchGymCd": gym_code,
            "searchAreaCd": area_code,
            "searchDistrictCd": district_code
        }
        
        try:
            response = self.session.post(self.detail_url, data=data)
            return response.json()
        except Exception as e:
            logger.error(f"상세 정보 요청 실패: {e}")
            return {}
    
    def get_detail_info_selenium(self, gym_code: str, area_code: str, district_code: str) -> Dict:
        """
        Selenium을 사용하여 상세 정보 가져오기 (개선된 버전)
        """
        # 더 간단한 방법으로 AJAX 요청 시도
        try:
            # 직접 requests를 사용해서 상세 정보 가져오기
            return self.get_detail_info(gym_code, area_code, district_code)
        except:
            pass
        
        # 원래 JavaScript 방식 (타임아웃 처리 개선)
        script = f"""
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '{self.detail_url}', false);  // 동기 요청으로 변경
        xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
        
        var params = 'searchGymCd={gym_code}&searchAreaCd={area_code}&searchDistrictCd={district_code}';
        
        try {{
            xhr.send(params);
            if (xhr.status === 200) {{
                return JSON.parse(xhr.responseText);
            }}
        }} catch(e) {{
            console.error('AJAX 요청 실패:', e);
        }}
        
        return {{}};
        """
        
        try:
            result = self.driver.execute_script(script)
            return result if result else {}
        except Exception as e:
            logger.error(f"JavaScript 실행 오류: {e}")
            return {}
    
    def crawl_all_pages(self, start_page=1, end_page=None, delay=DEFAULT_DELAY) -> List[Dict]:
        """
        모든 페이지 크롤링
        
        Args:
            start_page (int): 시작 페이지
            end_page (int): 종료 페이지 (None이면 전체)
            delay (int): 페이지 간 딜레이 (초)
            
        Returns:
            List[Dict]: 전체 태권도장 정보 리스트
        """
        all_data = []
        
        if end_page is None:
            end_page = self.get_total_pages()
        
        for page in range(start_page, end_page + 1):
            logger.info(f"페이지 {page}/{end_page} 크롤링 중...")
            
            try:
                # 페이지 데이터 크롤링
                if self.use_selenium:
                    page_data = self.crawl_with_selenium(page)
                else:
                    page_data = self.crawl_with_bs4(page)
                
                # 각 항목의 상세 정보 가져오기
                for item in page_data:
                    if 'gym_code' in item:
                        if self.use_selenium:
                            detail = self.get_detail_info_selenium(
                                item['gym_code'], 
                                item['area_code'], 
                                item['district_code']
                            )
                        else:
                            detail = self.get_detail_info(
                                item['gym_code'], 
                                item['area_code'], 
                                item['district_code']
                            )
                        
                        # 상세 정보 병합
                        item.update(detail)
                        time.sleep(DEFAULT_DETAIL_DELAY)  # API 부하 방지
                
                all_data.extend(page_data)
                
                # 중간 저장 (100페이지마다)
                if page % TEMP_SAVE_INTERVAL == 0:
                    temp_filename = os.path.join(self.save_path, f"taekwondo_data_temp_{page}.xlsx")
                    self.save_to_excel(all_data, temp_filename)
                
            except Exception as e:
                logger.error(f"페이지 {page} 크롤링 오류: {e}")
                continue
            
            time.sleep(delay)  # 페이지 간 딜레이
        
        return all_data
    
    def save_to_excel(self, data: List[Dict], filename: str, only_essential=True):
        """
        데이터를 엑셀 파일로 저장
        
        Args:
            data (List[Dict]): 저장할 데이터
            filename (str): 파일명 (경로 포함 가능)
            only_essential (bool): True면 필수 컬럼만, False면 모든 유용한 컬럼 포함
        """
        if not data:
            logger.warning("저장할 데이터가 없습니다.")
            return
            
        df = pd.DataFrame(data)
        
        # 불필요한 컬럼 제거
        unnecessary_columns = ['tkdStdVO', 'clubResultList', 'result', 'basePath', 'resultList']
        for col in unnecessary_columns:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        if only_essential:
            # 필수 컬럼만 선택
            essential_columns = {
                'name': '도장명',
                'address': '도장 주소', 
                'phone': '연락처'
            }
            
            # 존재하는 필수 컬럼만 선택
            available_columns = {}
            for eng_col, kor_col in essential_columns.items():
                if eng_col in df.columns:
                    available_columns[eng_col] = kor_col
            
            final_columns = available_columns
        else:
            # 모든 유용한 컬럼 포함 (기존 로직)
            required_columns = {
                'name': '도장명',
                'address': '도장 주소', 
                'phone': '연락처',
                'gym_code': '체육관코드',
                'area_code': '지역코드',
                'district_code': '구역코드'
            }
            
            available_columns = {}
            for eng_col, kor_col in required_columns.items():
                if eng_col in df.columns:
                    available_columns[eng_col] = kor_col
            
            # 추가 컬럼들도 포함
            additional_columns = {}
            for col in df.columns:
                if col not in required_columns and col not in unnecessary_columns:
                    additional_columns[col] = col
            
            final_columns = {**available_columns, **additional_columns}
        
        if final_columns:
            # 컬럼 선택 및 이름 변경
            df_selected = df[list(final_columns.keys())].copy()
            df_selected.rename(columns=final_columns, inplace=True)
            
            # 중복 제거
            df_selected = df_selected.drop_duplicates()
            
            # 파일 경로가 절대 경로가 아닌 경우 save_path 추가
            if not os.path.isabs(filename):
                filename = os.path.join(self.save_path, filename)
            
            # 엑셀 저장
            df_selected.to_excel(filename, index=False, engine='openpyxl')
            
            logger.info(f"데이터 저장 완료: {filename}")
            logger.info(f"총 {len(df_selected)}개 항목, {len(df_selected.columns)}개 컬럼")
            logger.info(f"저장된 컬럼: {list(df_selected.columns)}")
        else:
            logger.warning("저장할 유효한 컬럼이 없습니다.")
    
    def cleanup(self):
        """리소스 정리"""
        if self.driver:
            self.driver.quit()
            logger.info("Selenium WebDriver 종료")


def main():
    """메인 실행 함수"""
    # 저장 경로를 지정하여 크롤러 생성
    crawler = TaekwondoCrawler(use_selenium=True, save_path=SAVE_PATH)
    
    try:
        # 테스트: 첫 3페이지만 크롤링
        # data = crawler.crawl_all_pages(start_page=1, end_page=3)
        
        # 전체 크롤링 (시간이 오래 걸림)
        data = crawler.crawl_all_pages()
        
        # 엑셀 저장 (자동으로 C:\Users\kimyh\Desktop에 저장됨)
        crawler.save_to_excel(data, "태권도장리스트(명호).xlsx")
        
        print(f"총 {len(data)}개의 태권도장 정보를 수집했습니다.")
        print(f"파일 저장 위치: {os.path.join(SAVE_PATH, '태권도장리스트(명호).xlsx')}")
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {e}")
    finally:
        crawler.cleanup()


if __name__ == "__main__":
    main()
    
    # BeautifulSoup 방식 사용 예제 (정적 데이터만)
    # crawler_bs4 = TaekwondoCrawler(use_selenium=False, save_path=SAVE_PATH)
    # try:
    #     data = crawler_bs4.crawl_all_pages(start_page=1, end_page=3)
    #     crawler_bs4.save_to_excel(data, "taekwondo_studios_bs4.xlsx")
    # finally:
    #     crawler_bs4.cleanup()