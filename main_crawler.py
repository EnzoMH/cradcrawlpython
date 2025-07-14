#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
모듈화된 크롤링 시스템 - 메인 애플리케이션
Intel Core i5-4210M 환경 최적화
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from dotenv import load_dotenv

# 사용자 정의 모듈
from utils.system_analyzer import SystemAnalyzer
from utils.excel_processor import ExcelProcessor
from utils.data_mapper import DataMapper
from utils.crawling_engine import CrawlingEngine
from utils.verification_engine import VerificationEngine
from config.settings import get_optimal_config, display_system_config

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

def main():
    """메인 함수"""
    logger = setup_logger()
    
    try:
        # 환경 변수 로드
        load_dotenv()
        
        logger.info("🚀 모듈화된 크롤링 시스템 시작!")
        print("=" * 80)
        print("🚀 모듈화된 크롤링 시스템")
        print("=" * 80)
        
        # 1. 시스템 분석 및 최적화 설정
        logger.info("🖥️  시스템 분석 시작")
        system_analyzer = SystemAnalyzer(logger)
        
        # 2. 엑셀 파일 처리
        logger.info("📊 엑셀 파일 처리 시작")
        excel_processor = ExcelProcessor(logger)
        
        # 파일 경로 입력
        while True:
            file_path = input("\n📂 처리할 엑셀 파일 경로를 입력하세요 (기본값: academy2.xlsx): ").strip()
            if not file_path:
                file_path = 'academy2.xlsx'
            
            if os.path.exists(file_path):
                break
            else:
                print(f"❌ 파일이 존재하지 않습니다: {file_path}")
        
        # 엑셀 파일 로드
        if not excel_processor.load_excel_file(file_path):
            logger.error("❌ 엑셀 파일 로드 실패")
            return
        
        # 헤더 행 감지
        header_row = excel_processor.detect_header_row()
        
        # AI 초기화
        api_key = os.getenv('GEMINI_API_KEY')
        if api_key:
            excel_processor.initialize_ai(api_key)
        
        # 헤더 분석 및 매핑
        logger.info("🤖 헤더 분석 및 매핑 시작")
        
        # AI 헤더 분석 시도
        header_mapping = excel_processor.analyze_headers_with_ai()
        
        # AI 분석 결과 확인
        if not header_mapping:
            logger.warning("⚠️ AI 헤더 분석 실패, 수동 매핑 필요")
            header_mapping = excel_processor.manual_header_mapping()
        else:
            # 사용자 확인
            print("\n🤖 AI 헤더 분석 결과:")
            for standard_col, original_header in header_mapping.items():
                standard_name = excel_processor.standard_columns.get(standard_col, standard_col)
                print(f"   {standard_name} ← {original_header}")
            
            confirm = input("\n이 매핑을 사용하시겠습니까? (y/n): ").strip().lower()
            if confirm != 'y':
                header_mapping = excel_processor.manual_header_mapping()
        
        # 헤더 매핑 적용
        if not excel_processor.apply_header_mapping(header_mapping):
            logger.error("❌ 헤더 매핑 적용 실패")
            return
        
        # 3. 데이터 정제
        logger.info("🗂️  데이터 정제 시작")
        data_mapper = DataMapper(logger)
        
        processed_data = excel_processor.get_processed_data()
        if not data_mapper.load_data(processed_data):
            logger.error("❌ 데이터 매퍼 로드 실패")
            return
        
        # 전체 데이터 정제 프로세스 실행
        if not data_mapper.process_all():
            logger.error("❌ 데이터 정제 실패")
            return
        
        # 정제된 데이터 가져오기
        cleaned_data = data_mapper.get_processed_data()
        
        # 4. 크롤링 엔진 초기화
        logger.info("🚀 크롤링 엔진 초기화")
        crawling_engine = CrawlingEngine(logger)
        
        # 5. 처리 방식 선택
        print("\n📋 처리 방식을 선택하세요:")
        print("1. 전체 데이터 처리 (권장)")
        print("2. 지역별 개별 처리")
        print("3. 테스트 모드 (첫 10개 데이터만)")
        
        while True:
            choice = input("선택 (1-3): ").strip()
            if choice in ['1', '2', '3']:
                break
            print("❌ 잘못된 선택입니다. 1, 2, 3 중 하나를 선택하세요.")
        
        # 6. 크롤링 실행
        all_results = []
        
        if choice == '1':
            # 전체 데이터 처리
            logger.info("🔄 전체 데이터 처리 시작")
            institutions = cleaned_data.to_dict('records')
            results = crawling_engine.process_institution_batch(
                institutions, crawling_engine.process_institution_parallel
            )
            all_results.extend(results)
            
        elif choice == '2':
            # 지역별 개별 처리
            logger.info("🗺️  지역별 개별 처리 시작")
            from utils.constants import REGIONS
            
            for region in REGIONS:
                region_data = cleaned_data[cleaned_data['region'] == region]
                if not region_data.empty:
                    logger.info(f"🔄 {region} 지역 처리 시작")
                    results = crawling_engine.process_region_data(region_data, region)
                    all_results.extend(results)
                    
                    # 중간 결과 저장
                    if results:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"results_{region}_{timestamp}.xlsx"
                        crawling_engine.save_results(results, filename)
                else:
                    logger.info(f"⚠️ {region} 지역 데이터 없음")
            
        elif choice == '3':
            # 테스트 모드
            logger.info("🧪 테스트 모드 시작")
            test_data = cleaned_data.head(10)
            institutions = test_data.to_dict('records')
            results = crawling_engine.process_institution_batch(
                institutions, crawling_engine.process_institution_parallel
            )
            all_results.extend(results)
        
        # 7. 최종 데이터 정제 및 검증
        if all_results:
            logger.info("🔍 최종 데이터 정제 및 검증")
            
            # 최종 데이터 정제
            final_mapper = DataMapper(logger)
            import pandas as pd
            final_df = pd.DataFrame(all_results)
            
            final_mapper.load_data(final_df)
            final_mapper.process_all()
            
            final_data = final_mapper.get_processed_data()
            final_results = final_data.to_dict('records')
            
            # 8. 최종 결과 저장
            logger.info("💾 최종 결과 저장")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_filename = f"final_crawling_results_{timestamp}.xlsx"
            saved_path = crawling_engine.save_results(final_results, final_filename)
            
            # 9. 통계 및 요약
            logger.info("📊 크롤링 완료 통계")
            stats = crawling_engine.get_crawling_stats()
            cleanup_stats = final_mapper.get_cleanup_summary()
            
            print("\n" + "=" * 80)
            print("📊 크롤링 완료 통계")
            print("=" * 80)
            print(f"전체 처리 기관: {stats['total_institutions']:,}개")
            print(f"성공 추출: {stats['successful_extractions']:,}개")
            print(f"실패 추출: {stats['failed_extractions']:,}개")
            print(f"검증 완료: {stats['verified_contacts']:,}개")
            
            if stats['start_time'] and stats['end_time']:
                elapsed = stats['end_time'] - stats['start_time']
                print(f"총 소요시간: {elapsed}")
                
                if stats['processed_institutions'] > 0:
                    success_rate = (stats['successful_extractions'] / stats['processed_institutions']) * 100
                    print(f"성공률: {success_rate:.1f}%")
            
            print(f"\n💾 최종 결과 파일: {saved_path}")
            print("=" * 80)
            
        else:
            logger.warning("⚠️ 처리된 결과가 없습니다")
        
        logger.info("🎉 모든 작업이 완료되었습니다!")
        
    except KeyboardInterrupt:
        logger.info("⏹️  사용자에 의해 중단됨")
        
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류 발생: {e}")
        traceback.print_exc()
        
    finally:
        # 정리 작업
        logger.info("🧹 정리 작업 시작")
        
        try:
            if 'system_analyzer' in locals():
                system_analyzer.cleanup()
            if 'crawling_engine' in locals():
                crawling_engine.cleanup()
        except Exception as e:
            logger.error(f"❌ 정리 작업 실패: {e}")
        
        logger.info("✅ 프로그램 종료")

if __name__ == "__main__":
    main() 