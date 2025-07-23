#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
최종 센터 크롤링 시스템 - auction1.co.kr

데이터 구조:
- URL: https://www.auction1.co.kr/etc_service/dong_office.php?sido=11&gugun=680
- 센터 정보: <div class='content_addr_list'> 안에 포함
- 전화번호: TEL : <span class='brown'>02-3423-7670</span>
- 팩스: FAX : <span class='brown'>02-3423-8954</span>
- 홈페이지: <a href='URL'>홈페이지</a>
- 주소: 소재지 : <span class='black'>주소</span>
"""

import os
import time
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib3
import re
from datetime import datetime
from typing import List, Dict, Optional

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CenterCrawler:
    """센터 정보 크롤링 클래스"""
    
    def __init__(self):
        self.base_url = "https://www.auction1.co.kr"
        self.endpoint = "/etc_service/dong_office.php"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
    def load_region_data(self, file_path: str) -> pd.DataFrame:
        """지역 데이터 로드"""
        print(f"📂 지역 데이터 로드: {file_path}")
        df = pd.read_excel(file_path)
        print(f"✅ {len(df)}개 지역 로드 완료")
        return df
    
    def extract_center_info(self, html_content: str, sido: str, gugun: str) -> List[Dict]:
        """HTML에서 센터 정보 추출"""
        centers = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # content_addr_list 클래스를 가진 div 찾기
            center_divs = soup.find_all('div', class_='content_addr_list')
            
            print(f"  📋 발견된 센터: {len(center_divs)}개")
            
            for div in center_divs:
                center_info = {
                    'sido': sido,
                    'gugun': gugun,
                    'center_name': '',
                    'phone': '',
                    'fax': '',
                    'homepage': '',
                    'address': '',
                    'postal_code': ''
                }
                
                # 센터명 추출
                title_span = div.find('span', class_='content_addr_title')
                if title_span:
                    center_info['center_name'] = title_span.get_text(strip=True)
                
                # 홈페이지 링크 추출 - 모든 링크 중에서 '홈페이지' 텍스트가 포함된 것 찾기
                links = div.find_all('a', href=True)
                for link in links:
                    link_text = link.get_text(strip=True)
                    if '홈페이지' in link_text:
                        center_info['homepage'] = link.get('href')
                        break
                
                # 연락처 정보 추출
                ul_tag = div.find('ul')
                if ul_tag:
                    li_tags = ul_tag.find_all('li')
                    
                    for li in li_tags:
                        li_text = li.get_text(strip=True)
                        
                        # 전화번호 추출 - TEL이 포함된 li에서 brown 클래스 span 찾기
                        if 'TEL' in li_text and 'FAX' not in li_text:
                            phone_span = li.find('span', class_='brown')
                            if phone_span:
                                center_info['phone'] = phone_span.get_text(strip=True)
                        
                        # 팩스 추출 - FAX가 포함된 li에서 brown 클래스 span 찾기
                        elif 'FAX' in li_text:
                            fax_span = li.find('span', class_='brown')
                            if fax_span:
                                center_info['fax'] = fax_span.get_text(strip=True)
                        
                        # 주소 추출 - 소재지가 포함된 li에서 black 클래스 span 찾기
                        elif '소재지' in li_text or '주소' in li_text:
                            addr_span = li.find('span', class_='black')
                            if addr_span:
                                addr_text = addr_span.get_text(strip=True)
                                center_info['address'] = addr_text
                                
                                # 우편번호 추출
                                postal_match = re.search(r'우:\s*(\d{5})', li_text)
                                if postal_match:
                                    center_info['postal_code'] = postal_match.group(1)
                
                # 최소한 센터명이 있는 경우 추가 (전화번호나 팩스번호 중 하나라도 있으면 좋음)
                if center_info['center_name']:
                    centers.append(center_info)
                    fax_info = f", FAX: {center_info['fax']}" if center_info['fax'] else ""
                    homepage_info = f", 홈페이지: ✓" if center_info['homepage'] else ""
                    print(f"    ✅ {center_info['center_name']}: TEL: {center_info['phone']}{fax_info}{homepage_info}")
                
        except Exception as e:
            print(f"  ❌ 센터 정보 추출 실패: {e}")
        
        return centers
    
    def crawl_region(self, sido: str, gugun: str, sido_code: str, gugun_code: str) -> List[Dict]:
        """특정 지역 크롤링"""
        try:
            url = f"{self.base_url}{self.endpoint}?sido={sido_code}&gugun={gugun_code}"
            print(f"🔍 크롤링: {sido} {gugun}")
            print(f"🌐 URL: {url}")
            
            response = self.session.get(url, timeout=15, verify=False)
            
            if response.status_code == 200:
                centers = self.extract_center_info(response.text, sido, gugun)
                print(f"✅ {sido} {gugun}: {len(centers)}개 센터 수집")
                return centers
            else:
                print(f"❌ {sido} {gugun}: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ {sido} {gugun} 크롤링 실패: {e}")
            return []
    
    def crawl_all_regions(self, region_df: pd.DataFrame) -> pd.DataFrame:
        """전체 지역 크롤링"""
        print(f"🚀 전체 크롤링 시작: {len(region_df)}개 지역")
        
        all_centers = []
        
        for idx, row in region_df.iterrows():
            sido = row['시도']
            gugun = row['군구']
            
            # URL에서 sido, gugun 코드 추출
            url_path = row['주소.1']  # /dong_office.php?sido=11&gugun=680
            
            # 정규표현식으로 코드 추출
            sido_match = re.search(r'sido=(\d+)', url_path)
            gugun_match = re.search(r'gugun=(\d+)', url_path)
            
            if sido_match and gugun_match:
                sido_code = sido_match.group(1)
                gugun_code = gugun_match.group(1)
                
                print(f"\n📍 진행상황: {idx+1}/{len(region_df)}")
                centers = self.crawl_region(sido, gugun, sido_code, gugun_code)
                all_centers.extend(centers)
                
                # 요청 간격 (서버 부하 방지)
                time.sleep(random.uniform(1.0, 2.0))
                
                # 중간 저장 (10개 지역마다)
                if (idx + 1) % 10 == 0:
                    self._save_intermediate_results(all_centers, idx + 1)
            else:
                print(f"⚠️ {sido} {gugun}: URL 코드 추출 실패")
        
        # 최종 결과 DataFrame 생성
        if all_centers:
            result_df = pd.DataFrame(all_centers)
            print(f"\n✅ 크롤링 완료: 총 {len(all_centers)}개 센터")
            return result_df
        else:
            print("\n⚠️ 크롤링된 데이터가 없습니다")
            return pd.DataFrame()
    
    def _save_intermediate_results(self, centers: List[Dict], progress: int):
        """중간 결과 저장"""
        try:
            if centers:
                temp_df = pd.DataFrame(centers)
                temp_filename = f"center_progress_{progress}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                
                # 컬럼 순서 정리
                column_order = ['sido', 'gugun', 'center_name', 'phone', 'fax', 'homepage', 'address', 'postal_code']
                existing_columns = [col for col in column_order if col in temp_df.columns]
                temp_df_ordered = temp_df[existing_columns]
                
                # Excel 저장 (여러 엔진 대응)
                try:
                    with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
                        temp_df_ordered.to_excel(writer, index=False, sheet_name='센터정보')
                except ImportError:
                    try:
                        with pd.ExcelWriter(temp_filename, engine='xlsxwriter') as writer:
                            temp_df_ordered.to_excel(writer, index=False, sheet_name='센터정보')
                    except ImportError:
                        temp_df_ordered.to_excel(temp_filename, index=False)
                
                print(f"💾 중간 저장: {temp_filename} ({len(centers)}개)")
        except Exception as e:
            print(f"⚠️ 중간 저장 실패: {e}")
            import traceback
            traceback.print_exc()
    
    def save_results(self, result_df: pd.DataFrame, filename: str = None):
        """최종 결과 저장"""
        if filename is None:
            filename = f"center_crawling_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        try:
            # 빈 DataFrame 체크
            if result_df.empty:
                print("⚠️ 저장할 데이터가 없습니다.")
                return False
            
            # 컬럼 순서 정리 (보기 좋게)
            column_order = ['sido', 'gugun', 'center_name', 'phone', 'fax', 'homepage', 'address', 'postal_code']
            existing_columns = [col for col in column_order if col in result_df.columns]
            result_df_ordered = result_df[existing_columns]
            
            # Excel 저장 (openpyxl 없으면 xlsxwriter 사용)
            try:
                with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                    result_df_ordered.to_excel(writer, index=False, sheet_name='센터정보')
            except ImportError:
                print("📝 openpyxl이 없어서 xlsxwriter를 사용합니다.")
                try:
                    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                        result_df_ordered.to_excel(writer, index=False, sheet_name='센터정보')
                except ImportError:
                    print("📝 xlsxwriter도 없어서 기본 엔진을 사용합니다.")
                    result_df_ordered.to_excel(filename, index=False)
            
            print(f"💾 최종 결과 저장: {filename}")
            
            # 파일 크기 확인
            file_size = os.path.getsize(filename)
            print(f"📁 파일 크기: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            
            # 요약 정보 출력
            print(f"\n📊 크롤링 결과 요약:")
            print(f"  - 총 센터 수: {len(result_df)}개")
            
            if len(result_df) > 0:
                print(f"  - 시도별 분포:")
                sido_counts = result_df['sido'].value_counts().head(10)
                for sido, count in sido_counts.items():
                    print(f"    {sido}: {count}개")
                
                # 데이터 완성도 통계
                total_centers = len(result_df)
                phone_count = len(result_df[result_df['phone'].notna() & (result_df['phone'] != '')])
                fax_count = len(result_df[result_df['fax'].notna() & (result_df['fax'] != '')])
                homepage_count = len(result_df[result_df['homepage'].notna() & (result_df['homepage'] != '')])
                address_count = len(result_df[result_df['address'].notna() & (result_df['address'] != '')])
                
                print(f"\n  📞 데이터 완성도:")
                print(f"    전화번호: {phone_count}개 ({phone_count/total_centers*100:.1f}%)")
                print(f"    팩스번호: {fax_count}개 ({fax_count/total_centers*100:.1f}%)")
                print(f"    홈페이지: {homepage_count}개 ({homepage_count/total_centers*100:.1f}%)")
                print(f"    주소정보: {address_count}개 ({address_count/total_centers*100:.1f}%)")
            
            return True
            
        except Exception as e:
            print(f"❌ 결과 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("🏢 auction1.co.kr 센터 정보 크롤링 시스템")
    print("=" * 60)
    
    try:
        # 크롤러 초기화
        crawler = CenterCrawler()
        
        # 지역 데이터 로드
        region_file = "rawdatafile/센터크롤링정보.xlsx"
        region_df = crawler.load_region_data(region_file)
        
        print(f"\n📊 시도별 지역 수:")
        print(region_df['시도'].value_counts())
        
        # 샘플 테스트 먼저 실행
        print(f"\n🔍 샘플 테스트 (서울 강남구)...")
        sample_row = region_df.iloc[0]
        
        # URL에서 코드 추출
        url_path = sample_row['주소.1']
        sido_match = re.search(r'sido=(\d+)', url_path)
        gugun_match = re.search(r'gugun=(\d+)', url_path)
        
        if sido_match and gugun_match:
            sample_centers = crawler.crawl_region(
                sample_row['시도'], 
                sample_row['군구'],
                sido_match.group(1),
                gugun_match.group(1)
            )
            
            if sample_centers:
                print("✅ 샘플 테스트 성공!")
                print("샘플 데이터:")
                for center in sample_centers[:2]:
                    print(f"  - {center['center_name']}: {center['phone']}")
                
                # 사용자 확인
                choice = input("\n전체 크롤링을 시작하시겠습니까? (y/n): ").lower().strip()
                
                if choice == 'y':
                    # 전체 크롤링 실행
                    result_df = crawler.crawl_all_regions(region_df)
                    
                    if not result_df.empty:
                        # 결과 저장
                        save_success = crawler.save_results(result_df)
                        if save_success:
                            print(f"\n🎉 크롤링 및 저장 완료!")
                        else:
                            print(f"\n⚠️ 크롤링은 완료되었지만 저장에 실패했습니다.")
                    else:
                        print(f"\n⚠️ 크롤링된 데이터가 없습니다.")
                else:
                    print("크롤링을 취소했습니다.")
            else:
                print("❌ 샘플 테스트 실패")
        else:
            print("❌ URL 코드 추출 실패")
        
    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 