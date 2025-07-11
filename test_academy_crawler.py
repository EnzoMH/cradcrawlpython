#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import random
import os
import time
from datetime import datetime
import sys

# acrawl_i5.py에서 크롤러 클래스 import
from acrawl_i5 import I5ChurchCrawler

def create_test_data():
    """academy2.xlsx에서 랜덤으로 30개 데이터 추출"""
    try:
        # 원본 데이터 로드
        df = pd.read_excel('academy2.xlsx')
        print(f"원본 데이터 로드 완료: {len(df)}개 행")
        
        # 랜덤으로 30개 선택
        if len(df) < 30:
            print(f"데이터가 30개 미만입니다. 전체 {len(df)}개 사용")
            test_df = df.copy()
        else:
            test_df = df.sample(n=30, random_state=42)  # random_state로 재현 가능
        
        # 테스트 파일 저장
        test_filename = f"test_academy_30samples_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        test_df.to_excel(test_filename, index=False)
        print(f"테스트 데이터 저장: {test_filename}")
        
        # 지역별 분포 확인
        location_counts = test_df['위치'].value_counts()
        print("\n=== 테스트 데이터 지역별 분포 ===")
        for location, count in location_counts.items():
            print(f"{location}: {count}개")
        
        return test_filename, test_df
        
    except Exception as e:
        print(f"테스트 데이터 생성 중 오류: {e}")
        return None, None

def run_test_crawler(test_filename):
    """테스트 크롤러 실행"""
    try:
        print(f"\n=== 테스트 크롤러 시작 ===")
        print(f"테스트 파일: {test_filename}")
        
        # 크롤러 인스턴스 생성 (테스트용 설정)
        crawler = I5ChurchCrawler(
            excel_path=test_filename,
            worker_id=0  # 테스트용 워커 ID
        )
        
        # 크롤러에서 로드된 데이터 사용 (이미 컬럼명이 변환됨)
        test_df = crawler.df
        print(f"테스트 데이터 로드: {len(test_df)}개")
        
        # 🧪 간단한 전화번호 추출 테스트 (처음 5개만)
        print("\n🧪 전화번호 추출 테스트 (처음 5개)")
        print("-" * 50)
        
        test_results = []
        for idx, row in test_df.head(5).iterrows():
            try:
                print(f"\n📍 {idx+1}. {row['name']} ({row['location']})")
                
                # 전화번호 추출 시도
                result = crawler._process_single_academy_phone(row)
                test_results.append(result)
                
                # 결과 출력
                if result.get('phone'):
                    print(f"   ✅ 전화번호: {result['phone']}")
                else:
                    print(f"   ❌ 전화번호 추출 실패")
                
                # 딜레이 (테스트용)
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ 오류: {e}")
                test_results.append({
                    'name': row['name'],
                    'location': row['location'],
                    'phone': None,
                    'error': str(e)
                })
        
        # 테스트 결과 저장
        if test_results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            result_filename = os.path.join(desktop_path, f"테스트결과_30샘플_{timestamp}.xlsx")
            
            pd.DataFrame(test_results).to_excel(result_filename, index=False)
            print(f"\n✅ 테스트 결과 저장: {result_filename}")
        
        print("\n=== 테스트 크롤러 완료 ===")
        
        # 성공률 계산
        success_count = len([r for r in test_results if r.get('phone')])
        success_rate = (success_count / len(test_results)) * 100 if test_results else 0
        print(f"📊 성공률: {success_count}/{len(test_results)} ({success_rate:.1f}%)")
        
    except Exception as e:
        print(f"테스트 크롤러 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 리소스 정리
        if 'crawler' in locals():
            crawler._cleanup()

def main():
    """메인 함수"""
    print("=== 학원/교습소 크롤러 테스트 시작 ===")
    print("랜덤 30개 데이터로 테스트를 진행합니다.\n")
    
    # 1. 테스트 데이터 생성
    test_filename, test_df = create_test_data()
    
    if test_filename is None:
        print("테스트 데이터 생성 실패")
        return
    
    # 2. 자동 실행 (테스트용)
    print(f"\n테스트 데이터가 준비되었습니다: {test_filename}")
    print("🚀 테스트를 자동으로 시작합니다...")
    
    try:
        # 3. 테스트 크롤러 실행
        run_test_crawler(test_filename)
    
    except KeyboardInterrupt:
        print("\n\n테스트가 중단되었습니다.")
    except Exception as e:
        print(f"테스트 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 