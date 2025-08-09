#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
모듈화된 크롤링 시스템 - 메인 애플리케이션
리팩토링 완료: 모든 유틸리티 모듈 통합
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from dotenv import load_dotenv

# 새로 통합된 유틸리티 모듈들
from utils.ai_model_manager import AIModelManager
from utils.system.worker_manager import WorkerManager
from utils.valid.phone_validator import PhoneValidator
from utils.crawler.google_search_engine import GoogleSearchEngine
from utils.crawler.homepage_crawler import HomepageCrawler
from utils.system.system_analyzer import SystemAnalyzer
from utils.data.excel_processor import ExcelProcessor
from utils.data.data_mapper import DataMapper
from utils.crawler.crawling_engine import CrawlingEngine
from utils.valid.verification_engine import VerificationEngine

# 새로 통합된 설정 모듈들
from config.settings import get_optimal_config, display_system_config
from config.performance_profiles import PerformanceManager
from config.crawling_settings import CrawlingSettings

def setup_logger():
    """로깅 설정"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'crawler_main_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger('MainCrawler')

class MainCrawler:
    """메인 크롤링 애플리케이션"""
    
    def __init__(self):
        """메인 크롤러 초기화"""
        self.logger = setup_logger()
        
        # 환경 변수 로드
        load_dotenv()
        
        # 성능 관리자 초기화
        self.performance_manager = PerformanceManager(self.logger)
        
        # 크롤링 설정 초기화
        self.crawling_settings = CrawlingSettings()
        
        # AI 모델 관리자 초기화
        self.ai_model_manager = AIModelManager(self.logger)
        
        # 워커 관리자 초기화
        self.worker_manager = WorkerManager(self.logger)
        
        # 전화번호 검증기 초기화
        self.phone_validator = PhoneValidator(self.logger)
        
        # 구글 검색 엔진 초기화
        self.google_search_engine = GoogleSearchEngine(self.logger)
        
        # 홈페이지 크롤러 초기화
        self.homepage_crawler = HomepageCrawler(self.logger)
        
        # 시스템 분석기 초기화
        self.system_analyzer = SystemAnalyzer()
        
        # 엑셀 프로세서 초기화
        self.excel_processor = ExcelProcessor()
        
        # 데이터 매퍼 초기화
        self.data_mapper = DataMapper()
        
        # 크롤링 엔진 초기화 (새로운 모듈들과 통합)
        self.crawling_engine = CrawlingEngine(
            performance_manager=self.performance_manager,
            ai_model_manager=self.ai_model_manager,
            phone_validator=self.phone_validator,
            google_search_engine=self.google_search_engine,
            homepage_crawler=self.homepage_crawler
        )
        
        # 검증 엔진 초기화
        self.verification_engine = VerificationEngine()
        
        self.logger.info("🎯 메인 크롤링 시스템 초기화 완료")
    
    def display_system_info(self):
        """시스템 정보 출력"""
        try:
            print("=" * 80)
            print("🚀 모듈화된 크롤링 시스템 v2.0")
            print("=" * 80)
            
            # 성능 프로필 정보 출력
            self.performance_manager.display_performance_info()
            
            # AI 모델 상태 출력
            print("🤖 AI 모델 상태:")
            print(f"   - {self.ai_model_manager.get_model_status()}")
            print(f"   - 사용 가능한 모델 수: {self.ai_model_manager.get_available_models_count()}개")
            
            # 크롤링 설정 정보
            profile = self.performance_manager.get_current_profile()
            print(f"⚙️  크롤링 설정:")
            print(f"   - 최대 워커 수: {profile.max_workers}개")
            print(f"   - 배치 크기: {profile.batch_size}개")
            print(f"   - 크롤링 지연: {profile.crawling_delay_min:.1f}-{profile.crawling_delay_max:.1f}초")
            print(f"   - 메모리 임계값: {profile.memory_threshold}%")
            
            print("=" * 80)
            
        except Exception as e:
            self.logger.error(f"❌ 시스템 정보 출력 실패: {e}")
    
    def run_crawling(self, excel_path: str, institution_type: str = 'academy'):
        """
        크롤링 실행
        
        Args:
            excel_path: 엑셀 파일 경로
            institution_type: 기관 유형 ('academy', 'community_center', 'church')
        """
        try:
            self.logger.info(f"📊 크롤링 시작: {excel_path} ({institution_type})")
            
            # 1. 시스템 정보 출력
            self.display_system_info()
            
            # 2. 엑셀 파일 로드 및 처리
            self.logger.info("📁 엑셀 파일 로드 중...")
            df = self.excel_processor.load_excel(excel_path)
            
            if df is None or df.empty:
                self.logger.error("❌ 엑셀 파일 로드 실패")
                return False
            
            self.logger.info(f"✅ 엑셀 파일 로드 완료: {len(df)}개 레코드")
            
            # 3. 데이터 매핑 및 전처리
            self.logger.info("🔄 데이터 매핑 중...")
            mapped_df = self.data_mapper.map_columns(df, institution_type)
            
            if mapped_df is None or mapped_df.empty:
                self.logger.error("❌ 데이터 매핑 실패")
                return False
            
            self.logger.info(f"✅ 데이터 매핑 완료: {len(mapped_df)}개 레코드")
            
            # 4. 시스템 리소스 모니터링 시작
            current_resources = self.performance_manager.get_current_resources()
            self.logger.info(f"📊 시스템 리소스: CPU {current_resources.get('cpu_percent', 0):.1f}%, 메모리 {current_resources.get('memory_percent', 0):.1f}%")
            
            # 5. 동적 성능 조정 (필요시)
            adjustment_result = self.performance_manager.adjust_performance_dynamically(current_resources)
            if adjustment_result['adjusted']:
                self.logger.info(f"⚙️  성능 조정 완료: {adjustment_result['reason']}")
                for adjustment in adjustment_result.get('adjustments', []):
                    self.logger.info(f"   - {adjustment}")
            
            # 6. 크롤링 엔진 실행
            self.logger.info("🕷️  크롤링 엔진 시작...")
            crawling_result = self.crawling_engine.process_institutions(
                mapped_df, 
                institution_type=institution_type
            )
            
            if not crawling_result:
                self.logger.error("❌ 크롤링 실행 실패")
                return False
            
            self.logger.info("✅ 크롤링 완료")
            
            # 7. 결과 검증
            self.logger.info("🔍 결과 검증 중...")
            verification_result = self.verification_engine.verify_results(crawling_result)
            
            if verification_result:
                self.logger.info("✅ 결과 검증 완료")
            else:
                self.logger.warning("⚠️  결과 검증에서 문제 발견")
            
            # 8. 결과 저장
            self.logger.info("💾 결과 저장 중...")
            output_path = self._save_results(crawling_result, institution_type)
            
            if output_path:
                self.logger.info(f"✅ 결과 저장 완료: {output_path}")
                print(f"\n🎉 크롤링 완료! 결과 파일: {output_path}")
                return True
            else:
                self.logger.error("❌ 결과 저장 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 크롤링 실행 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _save_results(self, results, institution_type: str):
        """결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"crawling_results_{institution_type}_{timestamp}.xlsx"
            
            # 결과를 DataFrame으로 변환
            if isinstance(results, list):
                import pandas as pd
                df = pd.DataFrame(results)
            else:
                df = results
            
            # 엑셀 파일로 저장
            success = self.excel_processor.save_excel(df, filename)
            
            if success:
                return filename
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 결과 저장 실패: {e}")
            return None
    
    def run_performance_test(self):
        """성능 테스트 실행"""
        try:
            self.logger.info("🧪 성능 테스트 시작")
            
            # 시스템 정보 출력
            self.display_system_info()
            
            # 리소스 모니터링 테스트
            for i in range(5):
                resources = self.performance_manager.get_current_resources()
                self.logger.info(f"테스트 {i+1}: CPU {resources.get('cpu_percent', 0):.1f}%, 메모리 {resources.get('memory_percent', 0):.1f}%")
                
                # 동적 조정 테스트
                adjustment = self.performance_manager.adjust_performance_dynamically(resources)
                if adjustment['adjusted']:
                    self.logger.info(f"성능 조정: {adjustment['reason']}")
                
                import time
                time.sleep(2)
            
            # AI 모델 테스트
            test_text = "테스트 텍스트입니다. 팩스: 02-1234-5678"
            test_prompt = "다음 텍스트에서 팩스번호를 추출하세요: {text_content}"
            
            ai_result = self.ai_model_manager.extract_with_gemini(test_text, test_prompt)
            self.logger.info(f"AI 모델 테스트 결과: {ai_result[:100]}...")
            
            # 전화번호 검증 테스트
            test_numbers = ["02-1234-5678", "031-123-4567", "1234", "010-1234-5678"]
            for number in test_numbers:
                is_valid = self.phone_validator.is_valid_phone_format(number)
                self.logger.info(f"전화번호 검증 '{number}': {'유효' if is_valid else '무효'}")
            
            self.logger.info("✅ 성능 테스트 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 성능 테스트 실패: {e}")
            self.logger.error(traceback.format_exc())

def main():
    """메인 함수"""
    try:
        # 메인 크롤러 인스턴스 생성
        crawler = MainCrawler()
        
        # 명령행 인수 처리
        if len(sys.argv) < 2:
            print("사용법: python main_crawler.py <excel_file> [institution_type]")
            print("institution_type: academy (기본값), community_center, church")
            print("성능 테스트: python main_crawler.py --test")
            sys.exit(1)
        
        # 성능 테스트 모드
        if sys.argv[1] == '--test':
            crawler.run_performance_test()
            return
        
        # 일반 크롤링 모드
        excel_path = sys.argv[1]
        institution_type = sys.argv[2] if len(sys.argv) > 2 else 'academy'
        
        if not os.path.exists(excel_path):
            print(f"❌ 파일을 찾을 수 없습니다: {excel_path}")
            sys.exit(1)
        
        # 크롤링 실행
        success = crawler.run_crawling(excel_path, institution_type)
        
        if success:
            print("✅ 크롤링이 성공적으로 완료되었습니다!")
            sys.exit(0)
        else:
            print("❌ 크롤링 실행 중 오류가 발생했습니다.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 