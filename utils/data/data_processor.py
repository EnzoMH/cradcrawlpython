#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime
from utils.constants import (
    REGIONS, SEOUL_DISTRICTS, GYEONGGI_CITIES, INCHEON_DISTRICTS,
    REGION_SEOUL, REGION_GYEONGGI, REGION_INCHEON
)

class DataProcessor:
    """데이터 처리 클래스"""
    
    def __init__(self, logger=None):
        """
        데이터 처리자 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.df = None
        self.region_ranges = {}
        
        # 지역 정보
        self.seoul_districts = SEOUL_DISTRICTS
        self.gyeonggi_cities = GYEONGGI_CITIES
        self.incheon_districts = INCHEON_DISTRICTS
    
    def load_data(self, file_path: str) -> bool:
        """
        데이터 파일 로드
        
        Args:
            file_path: 데이터 파일 경로
            
        Returns:
            bool: 로드 성공 여부
        """
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"❌ 파일이 존재하지 않음: {file_path}")
                return False
            
            self.df = pd.read_excel(file_path)
            self.logger.info(f"📊 데이터 로드 완료: {len(self.df)}개 데이터")
            
            # 컬럼 정규화
            if '기관명' in self.df.columns:
                self.df = self.df.rename(columns={
                    '기관명': 'name',
                    '위치': 'location',
                    '주소': 'address', 
                    '전화번호': 'phone',
                    '팩스번호': 'fax',
                    '홈페이지': 'homepage'
                })
            
            # 누락된 컬럼 추가
            for col in ['name', 'location', 'address', 'phone', 'fax', 'homepage']:
                if col not in self.df.columns:
                    self.df[col] = ''
            
            # NaN 값 처리
            self.df = self.df.fillna('')
            
            # 지역별 데이터 분포 분석
            self._analyze_region_distribution()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            return False
    
    def _analyze_region_distribution(self):
        """지역별 데이터 분포 분석"""
        try:
            seoul_count = 0
            gyeonggi_count = 0
            incheon_count = 0
            
            for idx, row in self.df.iterrows():
                location = str(row.get('location', '')).strip()
                
                if location in self.seoul_districts:
                    seoul_count += 1
                elif location in self.gyeonggi_cities:
                    gyeonggi_count += 1
                elif location in self.incheon_districts:
                    incheon_count += 1
            
            # 📊 분포 정보 로깅
            self.logger.info(f"📍 지역별 데이터 분포:")
            self.logger.info(f"   - 서울: {seoul_count:,}개")
            self.logger.info(f"   - 경기도: {gyeonggi_count:,}개")
            self.logger.info(f"   - 인천: {incheon_count:,}개")
            self.logger.info(f"   - 전체: {len(self.df):,}개")
            
            # 지역별 인덱스 범위 저장
            self.region_ranges = {
                'seoul': {'start': 0, 'end': seoul_count, 'count': seoul_count},
                'gyeonggi': {'start': seoul_count, 'end': seoul_count + gyeonggi_count, 'count': gyeonggi_count},
                'incheon': {'start': seoul_count + gyeonggi_count, 'end': seoul_count + gyeonggi_count + incheon_count, 'count': incheon_count}
            }
            
        except Exception as e:
            self.logger.error(f"❌ 지역별 분포 분석 실패: {e}")
            # 기본값 설정
            self.region_ranges = {
                'seoul': {'start': 0, 'end': 8395, 'count': 8395},
                'gyeonggi': {'start': 8395, 'end': 27795, 'count': 19400},
                'incheon': {'start': 27795, 'end': 31414, 'count': 3619}
            }
    
    def get_region_data(self, region_name: str) -> pd.DataFrame:
        """
        특정 지역의 데이터 반환
        
        Args:
            region_name: 지역명 ('seoul', 'gyeonggi', 'incheon')
            
        Returns:
            pd.DataFrame: 지역 데이터
        """
        try:
            if region_name not in REGIONS:
                raise ValueError(f"지원하지 않는 지역: {region_name}")
            
            range_info = self.region_ranges[region_name]
            start_idx = range_info['start']
            end_idx = range_info['end']
            
            return self.df.iloc[start_idx:end_idx].copy()
            
        except Exception as e:
            self.logger.error(f"지역 데이터 추출 실패 ({region_name}): {e}")
            return pd.DataFrame()
    
    def split_region_data_by_chunks(self, region_name: str, chunk_size: int) -> List[pd.DataFrame]:
        """
        지역 데이터를 청크 단위로 분할
        
        Args:
            region_name: 지역명
            chunk_size: 청크 크기
            
        Returns:
            List[pd.DataFrame]: 청크 리스트
        """
        try:
            region_df = self.get_region_data(region_name)
            if region_df.empty:
                return []
            
            chunks = []
            total_rows = len(region_df)
            
            for i in range(0, total_rows, chunk_size):
                end_idx = min(i + chunk_size, total_rows)
                chunk = region_df.iloc[i:end_idx].copy()
                chunks.append(chunk)
            
            self.logger.info(f"📦 {region_name} 지역 데이터 분할 완료: {len(chunks)}개 청크")
            return chunks
            
        except Exception as e:
            self.logger.error(f"지역 데이터 분할 실패 ({region_name}): {e}")
            return []
    
    def save_results(self, results: List[Dict], region_name: str, phase: str) -> Optional[str]:
        """
        결과 저장
        
        Args:
            results: 결과 데이터 리스트
            region_name: 지역명
            phase: 처리 단계 ('phone', 'fax', 'homepage')
            
        Returns:
            Optional[str]: 저장된 파일 경로
        """
        try:
            if not results:
                return None
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            
            filename = os.path.join(desktop_path, 
                f"학원데이터교습소_{phase}추출_{region_name}_{timestamp}.xlsx")
            
            df_result = pd.DataFrame(results)
            # 최신 pandas 버전 호환성을 위해 ExcelWriter 사용
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False)
            
            self.logger.info(f"💾 결과 저장 완료: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"결과 저장 실패: {e}")
            return None 