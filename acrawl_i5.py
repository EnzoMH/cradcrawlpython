#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Intel Core i5-4210M 환경 최적화된 교회 크롤러
- Intel Core i5-4210M (2코어 4스레드) 환경 최적화
- 멀티프로세싱 처리 (4개 워커)
- 메모리 사용량 관리
- Chrome 브라우저 최적화
"""

import os
import sys
import logging
import traceback
import multiprocessing
from datetime import datetime
from dotenv import load_dotenv

from utils.system.web_driver_manager import WebDriverManager
from utils.data.data_processor import DataProcessor
from utils.crawler.info_extractor import InfoExtractor
from utils.system.system_monitor import SystemMonitor
from utils.constants import REGIONS

def setup_logger(name):
    """로깅 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'church_crawler_{name}.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(name)

def process_region(region_name: str, excel_path: str, worker_id: int = 0):
    """지역별 데이터 처리"""
    try:
        # 로거 설정
        logger = setup_logger(f"worker_{worker_id}")
        
        # 컴포넌트 초기화
        web_driver = WebDriverManager(logger)
        data_processor = DataProcessor(logger)
        info_extractor = InfoExtractor(web_driver, logger)
        system_monitor = SystemMonitor(logger)
        
        # 데이터 로드
        if not data_processor.load_data(excel_path):
            return []
        
        # AI 초기화
        api_key = os.getenv('GEMINI_API_KEY')
        if api_key:
            info_extractor.initialize_ai(api_key)
        
        # 모니터링 시작
        system_monitor.start_monitoring()
        
        try:
            # 1. 전화번호 추출
            logger.info(f"🔄 {region_name} 지역 전화번호 추출 시작")
            system_monitor.update_stats(current_phase="전화번호추출", current_region=region_name)
            
            region_data = data_processor.get_region_data(region_name)
            phone_results = []
            
            for idx, row in region_data.iterrows():
                result = info_extractor.search_google_for_phone(
                    row['name'], row['location'], row['address']
                )
                if result:
                    row_dict = row.to_dict()
                    row_dict['phone'] = result
                    phone_results.append(row_dict)
                
                system_monitor.update_stats(processed_count=idx + 1)
                if (idx + 1) % 100 == 0:
                    system_monitor.display_realtime_statistics()
            
            data_processor.save_results(phone_results, region_name, 'phone')
            
            # 2. 팩스번호 추출
            logger.info(f"🔄 {region_name} 지역 팩스번호 추출 시작")
            system_monitor.update_stats(current_phase="팩스번호추출")
            
            fax_results = []
            for row_dict in phone_results:
                if row_dict.get('phone'):  # 전화번호가 있는 경우만
                    result = info_extractor.search_google_for_fax(
                        row_dict['name'], row_dict['location'], row_dict['address']
                    )
                    if result:
                        row_dict['fax'] = result
                    fax_results.append(row_dict)
                
                system_monitor.update_stats(processed_count=len(fax_results))
                if len(fax_results) % 100 == 0:
                    system_monitor.display_realtime_statistics()
            
            data_processor.save_results(fax_results, region_name, 'fax')
            
            # 3. 홈페이지 추출
            logger.info(f"🔄 {region_name} 지역 홈페이지 추출 시작")
            system_monitor.update_stats(current_phase="홈페이지추출")
            
            homepage_results = []
            for row_dict in fax_results:
                result = info_extractor.search_google_for_homepage(
                    row_dict['name'], row_dict['location'], row_dict['address']
                )
                if result:
                    row_dict['homepage'] = result
                homepage_results.append(row_dict)
                
                system_monitor.update_stats(processed_count=len(homepage_results))
                if len(homepage_results) % 100 == 0:
                    system_monitor.display_realtime_statistics()
            
            data_processor.save_results(homepage_results, region_name, 'homepage')
            
            return homepage_results
            
        finally:
            system_monitor.stop_monitoring()
            web_driver.cleanup()
        
    except Exception as e:
        logger.error(f"❌ {region_name} 지역 처리 중 오류 발생: {e}")
        traceback.print_exc()
        return []

def main():
    """메인 함수"""
    try:
        # 환경 변수 로드
        load_dotenv()
        
        # 엑셀 파일 경로
        excel_path = 'academy2.xlsx'
        if not os.path.exists(excel_path):
            print(f"❌ 파일이 존재하지 않음: {excel_path}")
            return
        
        print("🚀 학원교습소 데이터 크롤링 시작!")
        print("="*60)
        
        # 멀티프로세싱 설정 (i5-4210M 환경 최적화)
        n_processes = 4  # 2코어 4스레드
        
        # 지역별 처리
        all_results = []
        
        for region in REGIONS:
            results = process_region(region, excel_path)
            all_results.extend(results)
        
        # 최종 결과 저장
        if all_results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            final_filename = os.path.join(desktop_path, 
                f"학원데이터교습소_전체데이터_추출완료_{timestamp}.xlsx")
            
            import pandas as pd
            # 최신 pandas 버전 호환성을 위해 ExcelWriter 사용
        with pd.ExcelWriter(final_filename, engine='openpyxl') as writer:
            pd.DataFrame(all_results).to_excel(writer, index=False)
            print(f"\n✅ 전체 데이터 추출 완료: {final_filename}")
        
        print("\n🎉 모든 작업이 완료되었습니다!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 