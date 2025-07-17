#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
parallel_phone_fax_finder.py
병렬 처리 전화번호/팩스번호 기반 해당기관 검색 시스템 - 고급 봇 우회 버전
"""

import os
import sys
import logging
import pandas as pd
import time
import random
import re
import multiprocessing
import traceback
import socket
import tempfile
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import google.generativeai as genai
from dotenv import load_dotenv

# Utils 모듈 import
from utils.worker_manager import WorkerManager
from utils.system_monitor import SystemMonitor

class AdvancedPortManager:
    """고급 포트 관리 시스템 - 동적 로테이션"""
    
    def __init__(self):
        # 포트 범위를 더 넓게 설정 (봇 감지 회피)
        self.base_ports = [9222, 9333, 9444, 9555, 9666, 9777, 9888, 9999]
        self.used_ports = set()
        self.port_rotation_count = 0
        self.max_port_reuse = 3  # 포트 재사용 제한
        
    def get_rotated_port(self, worker_id: int) -> int:
        """워커별 동적 포트 할당"""
        # 포트 풀에서 순환 선택
        base_idx = (worker_id + self.port_rotation_count) % len(self.base_ports)
        base_port = self.base_ports[base_idx]
        
        # 최대 100개 포트 시도 (더 안전한 포트 확보)
        for offset in range(100):
            port = base_port + offset
            
            # 포트 사용 가능성 확인
            if self._is_port_available(port) and port not in self.used_ports:
                self.used_ports.add(port)
                return port
        
        # 모든 포트가 사용 중인 경우 강제 할당
        fallback_port = base_port + worker_id + 1000 + random.randint(0, 500)
        self.used_ports.add(fallback_port)
        return fallback_port
    
    def _is_port_available(self, port: int) -> bool:
        """포트 사용 가능 여부 확인"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex(('localhost', port))
                return result != 0  # 포트가 사용 중이 아님
        except:
            return False
    
    def release_port(self, port: int):
        """포트 해제"""
        self.used_ports.discard(port)
    
    def rotate_ports(self):
        """포트 로테이션 카운터 증가"""
        self.port_rotation_count += 1

class StealthWebDriverManager:
    """스텔스 WebDriver 관리 클래스 - 고급 봇 우회"""
    
    def __init__(self, logger=None):
        """스텔스 WebDriver 관리자 초기화"""
        self.logger = logger or logging.getLogger(__name__)
        self.port_manager = AdvancedPortManager()
        
        # 최신 User-Agent 풀 (2024년 기준)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.76',
        ]
        
        # 화면 해상도 풀 (일반적인 해상도들)
        self.screen_sizes = [
            (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
            (1600, 900), (1280, 720), (1920, 1200), (2560, 1440)
        ]
    
    def create_stealth_driver(self, worker_id: int = 0) -> object:
        """스텔스 드라이버 생성 - 최신 봇 우회 기법 적용"""
        try:
            # 워커별 시작 지연 (봇 감지 회피)
            startup_delay = random.uniform(1.0, 3.0) * (worker_id + 1)
            time.sleep(startup_delay)
            
            self.logger.info(f"🛡️ 워커 {worker_id}: 스텔스 드라이버 생성 중...")
            
            # Chrome 옵션 설정
            chrome_options = uc.ChromeOptions()
            
            # 🔥 최신 봇 감지 우회 옵션 (2024년 기준)
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--mute-audio')
            chrome_options.add_argument('--no-first-run')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-notifications')
            
            # 🚫 고급 봇 감지 회피 옵션 (새로운 추가 항목들)
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-ipc-flooding-protection')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-features=TranslateUI')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--disable-sync')
            chrome_options.add_argument('--disable-plugins')
            chrome_options.add_argument('--disable-component-extensions')
            
            # 🎭 핑거프린트 무작위화
            selected_ua = random.choice(self.user_agents)
            selected_size = random.choice(self.screen_sizes)
            
            chrome_options.add_argument(f'--user-agent={selected_ua}')
            chrome_options.add_argument(f'--window-size={selected_size[0]},{selected_size[1]}')
            
            # 🔧 Chrome 138+ 호환성 개선
            chrome_options.add_argument('--no-crash-dialog')
            chrome_options.add_argument('--disable-crash-reporter')
            chrome_options.add_argument('--disable-hang-monitor')
            chrome_options.add_argument('--disable-prompt-on-repost')
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--disable-logging-redirect')
            chrome_options.add_argument('--log-level=3')
            
            # 💾 메모리 최적화 (AMD Ryzen 5 5500U 환경 고려)
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=512')
            chrome_options.add_argument('--aggressive-cache-discard')
            chrome_options.add_argument('--max-unused-resource-memory-usage-percentage=10')
            chrome_options.add_argument('--disable-background-mode')
            
            # 🌐 동적 포트 할당 (핵심 봇 우회 기법)
            debug_port = self.port_manager.get_rotated_port(worker_id)
            chrome_options.add_argument(f'--remote-debugging-port={debug_port}')
            
            # 📁 워커별 독립 프로필 디렉토리
            profile_dir = tempfile.mkdtemp(prefix=f'stealth_worker_{worker_id}_')
            chrome_options.add_argument(f'--user-data-dir={profile_dir}')
            
            # 🔐 고급 스텔스 옵션
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # 🎯 Canvas/WebGL 핑거프린트 변조
            chrome_options.add_argument('--disable-canvas-aa')
            chrome_options.add_argument('--disable-2d-canvas-clip-aa')
            chrome_options.add_argument('--disable-gl-drawing-for-tests')
            
            # 🛡️ 추가 보안 레이어
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-background-networking')
            chrome_options.add_argument('--disable-component-update')
            chrome_options.add_argument('--disable-domain-reliability')
            chrome_options.add_argument('--disable-client-side-phishing-detection')
            
            # undetected_chromedriver 생성 (최신 버전 지원)
            driver = uc.Chrome(
                options=chrome_options,
                version_main=None,  # 자동 버전 감지
                driver_executable_path=None,  # 자동 다운로드
                browser_executable_path=None,  # 기본 Chrome 사용
                use_subprocess=True,  # 안정성 향상
                log_level=3  # 로그 최소화
            )
            
            # 🔮 JavaScript 레벨 스텔스 적용
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    // WebDriver 속성 숨기기
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined,
                    });
                    
                    // Chrome 객체 속성 숨기기
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    
                    // 플러그인 배열 수정
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5],
                    });
                    
                    // 언어 배열 수정
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['ko-KR', 'ko', 'en-US', 'en'],
                    });
                    
                    // Permission API 우회
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                    );
                """
            })
            
            self.logger.info(f"✅ 워커 {worker_id}: 스텔스 드라이버 생성 완료 (포트: {debug_port})")
            return driver
            
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 스텔스 드라이버 생성 실패 - {e}")
            return None
    
    def cleanup_driver(self, driver, worker_id: int):
        """드라이버 정리"""
        try:
            if driver:
                driver.quit()
                self.logger.info(f"🧹 워커 {worker_id}: 드라이버 정리 완료")
        except Exception as e:
            self.logger.error(f"❌ 워커 {worker_id}: 드라이버 정리 실패 - {e}")

# 로깅 설정하는 함수
def setup_logger(name="ParallelPhoneFaxFinder"):
    """로깅 시스템 설정하는 메소드"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'parallel_phone_fax_finder_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(name)

def process_batch_worker(batch_data: List[Dict], worker_id: int, api_key: str = None) -> List[Dict]:
    """
    배치 데이터 처리하는 워커 함수 - 스텔스 모드
    
    Args:
        batch_data: 처리할 데이터 배치
        worker_id: 워커 ID
        api_key: Gemini API 키 (선택사항)
        
    Returns:
        List[Dict]: 처리된 결과 리스트
    """
    try:
        logger = setup_logger(f"stealth_worker_{worker_id}")
        logger.info(f"🛡️ 스텔스 워커 {worker_id} 시작: {len(batch_data)}개 데이터 처리")
        
        # StealthWebDriverManager를 사용한 스텔스 드라이버 생성
        stealth_manager = StealthWebDriverManager(logger)
        driver = stealth_manager.create_stealth_driver(worker_id)
        
        if not driver:
            logger.error(f"❌ 스텔스 워커 {worker_id}: 드라이버 생성 실패")
            return []
        
        # AI 모델 초기화 (사용 가능한 경우)
        ai_model = None
        if api_key:
            try:
                genai.configure(api_key=api_key)
                ai_model = genai.GenerativeModel('gemini-2.0-flash-lite-001')
                logger.info(f"🤖 워커 {worker_id}: AI 모델 초기화 성공")
            except Exception as e:
                logger.warning(f"⚠️ 워커 {worker_id}: AI 모델 초기화 실패 - {e}")
        
        # 🎯 고급 검색 패턴 정의 (다양한 검색 전략)
        search_patterns = {
            'phone': [
                '"{phone_number}"',
                '{phone_number} 전화번호',
                '{phone_number} 연락처',
                '{phone_number} 기관',
                '전화 {phone_number}',
                '{phone_number} 대표번호',
                '{phone_number} 문의처',
                '{phone_number} 사무실',
                '연락처 {phone_number}',
                '{phone_number} 공식',
                '{phone_number} site:kr',
                '{phone_number} 관공서',
                '{phone_number} 센터'
            ],
            'fax': [
                '"{fax_number}"',
                '{fax_number} 팩스',
                '{fax_number} 팩스번호',
                '{fax_number} 기관',
                '팩스 {fax_number}',
                '{fax_number} FAX',
                '{fax_number} 전송',
                '{fax_number} 사무실',
                'FAX {fax_number}',
                '{fax_number} site:kr',
                '{fax_number} 관공서',
                '{fax_number} 센터',
                '{fax_number} 공식'
            ]
        }
        
        # 🏢 확장된 기관명 추출 패턴 (더 정확한 기관 인식)
        institution_patterns = [
            # 행정기관
            r'([가-힣]+(?:동|구|시|군|읍|면)\s*(?:주민센터|행정복지센터|사무소|동사무소))',
            r'([가-힣]+(?:구청|시청|군청|도청|청사))',
            r'([가-힣]+(?:구|시|군|도)\s*(?:청|청사))',
            
            # 교육기관
            r'([가-힣]+(?:대학교|대학|학교|초등학교|중학교|고등학교|유치원))',
            r'([가-힣]+(?:교육청|교육지원청|교육지원센터))',
            
            # 의료기관
            r'([가-힣]+(?:병원|의료원|보건소|의원|클리닉|한의원))',
            r'([가-힣]+(?:보건|의료)\s*(?:센터|소))',
            
            # 복지/문화시설
            r'([가-힣]+(?:복지관|문화센터|도서관|체육관|체육센터|수영장))',
            r'([가-힣]+(?:복지|문화|체육|여성|청소년)\s*(?:센터|관))',
            
            # 공공기관
            r'([가-힣]+(?:협회|단체|재단|법인|조합|공사|공단|공기업))',
            r'([가-힣]+(?:관리사무소|관리소|관리공단))',
            
            # 일반 패턴 (더 유연한 매칭)
            r'([가-힣\s]{2,25}(?:주민센터|행정복지센터|사무소|청|병원|학교|센터|관|소))',
            r'([가-힣\s]{3,20}(?:대학교|대학|공사|공단|재단|법인))',
            
            # 특수 기관
            r'([가-힣]+(?:경찰서|소방서|우체국|세무서|법원|검찰청))',
            r'([가-힣]+(?:상공회의소|상공회|농협|수협|신협))'
        ]
        
        results = []
        
        for idx, row_data in enumerate(batch_data):
            try:
                phone_number = row_data.get('전화번호', '')
                fax_number = row_data.get('팩스번호', '')
                
                # 정규화
                normalized_phone = normalize_phone_number(phone_number) if phone_number and phone_number != 'nan' else ''
                normalized_fax = normalize_phone_number(fax_number) if fax_number and fax_number != 'nan' else ''
                
                logger.info(f"📞 워커 {worker_id} 처리 중 ({idx+1}/{len(batch_data)}): 전화({normalized_phone}), 팩스({normalized_fax})")
                
                # 전화번호 기관 검색
                phone_institution = ''
                if normalized_phone:
                    phone_institution = search_google_for_institution(
                        driver, normalized_phone, 'phone', search_patterns, 
                        institution_patterns, ai_model, logger
                    )
                
                # 팩스번호 기관 검색
                fax_institution = ''
                if normalized_fax:
                    fax_institution = search_google_for_institution(
                        driver, normalized_fax, 'fax', search_patterns, 
                        institution_patterns, ai_model, logger
                    )
                
                # 결과 저장
                result = {
                    '팩스번호': normalized_fax,
                    '해당기관': fax_institution if fax_institution else '미발견',
                    '전화번호': normalized_phone,
                    '해당기관.1': phone_institution if phone_institution else '미발견',
                    '처리워커': f"워커_{worker_id}",
                    '처리시간': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                results.append(result)
                
                # 🛡️ 스텔스 요청 지연 (인간 행동 패턴 시뮬레이션)
                stealth_delay = random.uniform(4, 7)  # 더 긴 지연으로 봇 감지 회피
                time.sleep(stealth_delay)
                
            except Exception as e:
                logger.error(f"❌ 워커 {worker_id} 행 처리 실패 {idx}: {e}")
                continue
        
        # 정리 - 스텔스 매니저 사용
        stealth_manager.cleanup_driver(driver, worker_id)
        
        logger.info(f"✅ 스텔스 워커 {worker_id} 완료: {len(results)}개 결과")
        return results
        
    except Exception as e:
        logger.error(f"❌ 스텔스 워커 {worker_id} 전체 실패: {e}")
        traceback.print_exc()
        return []

def normalize_phone_number(phone_number: str) -> str:
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

def search_google_for_institution(driver, number: str, number_type: str, search_patterns: Dict, 
                                 institution_patterns: List, ai_model, logger) -> Optional[str]:
    """구글에서 전화번호/팩스번호로 기관 검색하는 메소드 - 스텔스 모드"""
    try:
        patterns = search_patterns.get(number_type, [])
        
        # 여러 검색 패턴 시도
        for pattern in patterns:
            if number_type == 'phone':
                search_query = pattern.format(phone_number=number)
            else:  # fax
                search_query = pattern.format(fax_number=number)
            
            logger.info(f"🕵️ {number_type} 스텔스 검색 중: {search_query}")
            
            # 🎭 인간 행동 시뮬레이션 - 검색 전 지연
            human_delay = random.uniform(1.5, 3.5)
            time.sleep(human_delay)
            
            # 구글 검색 (User-Agent는 이미 스텔스 드라이버에서 설정됨)
            search_url = f"https://www.google.com/search?q={search_query.replace(' ', '+')}"
            
            # 🚀 페이지 로드 방식 개선
            try:
                driver.get(search_url)
                
                # 🔄 페이지 로드 완료 대기 (더 안정적)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # 🎯 추가 인간 행동 시뮬레이션 - 스크롤
                driver.execute_script("window.scrollTo(0, Math.floor(Math.random() * 500));")
                
            except TimeoutException:
                logger.warning(f"⚠️ 페이지 로드 타임아웃: {search_query}")
                continue
            
            # 🤖 봇 감지 회피를 위한 추가 지연
            detection_avoidance_delay = random.uniform(2.5, 4.5)
            time.sleep(detection_avoidance_delay)
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 기관명 추출 시도
            institution_name = extract_institution_from_page(soup, number, institution_patterns, ai_model, logger)
            
            if institution_name:
                logger.info(f"✅ {number_type} 기관명 발견: {institution_name}")
                return institution_name
            
            # 다음 패턴 시도 전 더 긴 지연 (봇 감지 회피)
            pattern_delay = random.uniform(2.0, 4.0)
            time.sleep(pattern_delay)
        
        return None
        
    except Exception as e:
        logger.error(f"❌ 스텔스 구글 검색 실패: {number} ({number_type}) - {e}")
        return None

def extract_institution_from_page(soup: BeautifulSoup, number: str, institution_patterns: List, 
                                 ai_model, logger) -> Optional[str]:
    """검색 결과 페이지에서 기관명 추출하는 메소드"""
    try:
        # 페이지 텍스트 가져오기
        page_text = soup.get_text()
        
        # 정규식 패턴으로 기관명 찾기
        for pattern in institution_patterns:
            matches = re.findall(pattern, page_text)
            if matches:
                # 가장 적절한 매치 선택
                for match in matches:
                    if is_valid_institution_name(match):
                        return match.strip()
        
        # AI 모델 사용 (사용 가능한 경우)
        if ai_model:
            return extract_with_ai(page_text, number, ai_model, logger)
        
        return None
        
    except Exception as e:
        logger.error(f"❌ 기관명 추출 실패: {e}")
        return None

def is_valid_institution_name(name: str) -> bool:
    """유효한 기관명인지 확인하는 메소드 - 확장 버전"""
    if not name or len(name) < 2:
        return False
    
    # 🏛️ 확장된 유효한 기관명 키워드
    valid_keywords = [
        # 행정기관
        '주민센터', '행정복지센터', '사무소', '동사무소', '청', '구청', '시청', '군청', '도청', '청사',
        
        # 교육기관  
        '학교', '초등학교', '중학교', '고등학교', '대학', '대학교', '유치원', '교육청', '교육지원청', '교육지원센터',
        
        # 의료기관
        '병원', '의료원', '보건소', '의원', '클리닉', '한의원', '보건센터', '의료센터',
        
        # 복지/문화시설
        '센터', '복지관', '도서관', '체육관', '체육센터', '수영장', '문화센터', '여성센터', '청소년센터',
        
        # 공공기관
        '협회', '단체', '재단', '법인', '조합', '공사', '공단', '공기업', '관리사무소', '관리소', '관리공단',
        
        # 특수기관
        '경찰서', '소방서', '우체국', '세무서', '법원', '검찰청', '상공회의소', '상공회', '농협', '수협', '신협'
    ]
    
    # ❌ 제외할 키워드 (잘못된 인식 방지)
    invalid_keywords = [
        '번호', '전화', '팩스', 'fax', '연락처', '문의', '검색', '결과', '사이트', 'site',
        '홈페이지', 'www', 'http', 'com', 'co.kr', '광고', '상품', '서비스'
    ]
    
    # 제외 키워드 확인
    name_lower = name.lower()
    if any(invalid in name_lower for invalid in invalid_keywords):
        return False
    
    return any(keyword in name for keyword in valid_keywords)

def extract_with_ai(page_text: str, number: str, ai_model, logger) -> Optional[str]:
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
        
        response = ai_model.generate_content(prompt)
        result = response.text.strip()
        
        if result and result != '없음' and is_valid_institution_name(result):
            return result
        
        return None
        
    except Exception as e:
        logger.error(f"❌ AI 추출 실패: {e}")
        return None

class ParallelPhoneFaxFinder:
    """병렬 처리 전화번호/팩스번호 기관 찾기 클래스"""
    
    def __init__(self):
        """병렬 전화번호/팩스번호 기관 찾기 시스템 초기화하는 메소드"""
        self.logger = setup_logger()
        self.system_monitor = SystemMonitor(self.logger)
        
        # 환경 변수 로드
        load_dotenv()
        
        # 병렬 처리 설정 (AMD Ryzen 5 5500U 환경 최적화)
        self.max_workers = 10  # 6코어 12스레드 활용
        self.batch_size = 350   # 워커당 처리할 데이터 수
        
        # 통계
        self.total_processed = 0
        self.phone_success_count = 0
        self.fax_success_count = 0
        
        self.logger.info("🛡️ 스텔스 병렬 전화번호/팩스번호 기관 찾기 시스템 초기화 완료")
        self.logger.info(f"🚀 AMD Ryzen 5 5500U 최적화: {self.max_workers}개 워커, 배치 크기: {self.batch_size}")
        self.logger.info("🔥 고급 봇 감지 우회 기능 활성화")
    
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
    
    def split_data_into_batches(self, df: pd.DataFrame) -> List[List[Dict]]:
        """데이터를 배치로 분할하는 메소드"""
        try:
            # DataFrame을 딕셔너리 리스트로 변환
            data_list = df.to_dict('records')
            
            # 배치로 분할
            batches = []
            for i in range(0, len(data_list), self.batch_size):
                batch = data_list[i:i + self.batch_size]
                batches.append(batch)
            
            self.logger.info(f"📦 데이터 분할 완료: {len(batches)}개 배치")
            for i, batch in enumerate(batches):
                self.logger.info(f"   배치 {i+1}: {len(batch)}개 데이터")
            
            return batches
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 분할 실패: {e}")
            return []
    
    def process_parallel(self, df: pd.DataFrame) -> List[Dict]:
        """병렬 처리 실행하는 메소드"""
        try:
            self.logger.info("🚀 병렬 처리 시작!")
            
            # 시스템 모니터링 시작
            self.system_monitor.start_monitoring()
            
            # 데이터 배치 분할
            batches = self.split_data_into_batches(df)
            
            if not batches:
                return []
            
            # API 키 가져오기
            api_key = os.getenv('GEMINI_API_KEY')
            
            all_results = []
            
            # ProcessPoolExecutor를 사용한 병렬 처리
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # 각 배치를 워커에 할당
                future_to_worker = {}
                for worker_id, batch in enumerate(batches[:self.max_workers]):
                    future = executor.submit(process_batch_worker, batch, worker_id, api_key)
                    future_to_worker[future] = worker_id
                
                # 남은 배치들 처리
                remaining_batches = batches[self.max_workers:]
                next_worker_id = self.max_workers
                
                # 완료된 작업 처리
                for future in as_completed(future_to_worker):
                    worker_id = future_to_worker[future]
                    
                    try:
                        result = future.result()
                        all_results.extend(result)
                        
                        self.logger.info(f"✅ 워커 {worker_id} 완료: {len(result)}개 결과")
                        
                        # 남은 배치가 있으면 새로운 작업 시작
                        if remaining_batches:
                            next_batch = remaining_batches.pop(0)
                            new_future = executor.submit(process_batch_worker, next_batch, next_worker_id, api_key)
                            future_to_worker[new_future] = next_worker_id
                            next_worker_id += 1
                        
                    except Exception as e:
                        self.logger.error(f"❌ 워커 {worker_id} 오류: {e}")
                        continue
            
            # 시스템 모니터링 중지
            self.system_monitor.stop_monitoring()
            
            self.logger.info(f"🎉 병렬 처리 완료: {len(all_results)}개 총 결과")
            
            return all_results
            
        except Exception as e:
            self.logger.error(f"❌ 병렬 처리 실패: {e}")
            return []
    
    def save_results_to_desktop(self, results: List[Dict]) -> str:
        """결과를 데스크톱에 저장하는 메소드"""
        try:
            # 데스크톱 경로 가져오기
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            
            # 파일명 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"병렬_전화팩스기관검색결과_{timestamp}.xlsx"
            filepath = os.path.join(desktop_path, filename)
            
            # DataFrame 생성 및 저장
            df_results = pd.DataFrame(results)
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df_results.to_excel(writer, index=False, sheet_name='병렬전화팩스기관검색결과')
            
            self.logger.info(f"💾 결과 저장 완료: {filepath}")
            
            # 통계 정보
            total_processed = len(results)
            phone_successful = len([r for r in results if r['해당기관.1'] != '미발견'])
            fax_successful = len([r for r in results if r['해당기관'] != '미발견'])
            
            phone_rate = (phone_successful / total_processed) * 100 if total_processed > 0 else 0
            fax_rate = (fax_successful / total_processed) * 100 if total_processed > 0 else 0
            
            self.logger.info(f"📊 최종 처리 통계:")
            self.logger.info(f"   - 총 처리: {total_processed}개")
            self.logger.info(f"   - 전화번호 성공: {phone_successful}개 ({phone_rate:.1f}%)")
            self.logger.info(f"   - 팩스번호 성공: {fax_successful}개 ({fax_rate:.1f}%)")
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            return ""
    
    def run(self, excel_path: str) -> bool:
        """전체 병렬 프로세스 실행하는 메소드"""
        try:
            self.logger.info("🚀 병렬 전화번호/팩스번호 기관 찾기 시작!")
            
            # 1. 데이터 로드
            df = self.load_excel_data(excel_path)
            if df.empty:
                return False
            
            # 2. 병렬 처리 실행
            results = self.process_parallel(df)
            
            if not results:
                self.logger.error("❌ 처리된 결과가 없습니다")
                return False
            
            # 3. 결과 저장
            output_path = self.save_results_to_desktop(results)
            
            if output_path:
                self.logger.info(f"✅ 완료! 결과 파일: {output_path}")
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 실행 중 오류: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        # 파일 경로
        excel_path = r"C:\Users\MyoengHo Shin\pjt\cradcrawlpython\rawdatafile\failed_data_250715.xlsx"
        
        # 병렬 전화번호/팩스번호 기관 찾기 실행
        finder = ParallelPhoneFaxFinder()
        success = finder.run(excel_path)
        
        if success:
            print("🎉 병렬 전화번호/팩스번호 기관명 검색이 완료되었습니다!")
        else:
            print("❌ 처리 중 오류가 발생했습니다.")
            
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")

if __name__ == "__main__":
    # Windows의 multiprocessing 이슈 해결
    multiprocessing.freeze_support()
    main() 