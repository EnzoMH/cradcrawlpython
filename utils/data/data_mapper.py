#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터 매핑 및 정제 클래스
"""

import pandas as pd
import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime
from utils.constants import REGIONS, SEOUL_DISTRICTS, GYEONGGI_CITIES, INCHEON_DISTRICTS
import os

class DataMapper:
    """데이터 매핑 및 정제"""
    
    def __init__(self, logger=None):
        """
        데이터 매퍼 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.df = None
        self.region_mapping = {
            'seoul': SEOUL_DISTRICTS,
            'gyeonggi': GYEONGGI_CITIES,
            'incheon': INCHEON_DISTRICTS
        }
        
        # 데이터 정제 통계
        self.cleanup_stats = {
            'total_rows': 0,
            'cleaned_phones': 0,
            'cleaned_faxes': 0,
            'cleaned_addresses': 0,
            'normalized_regions': 0,
            'removed_duplicates': 0
        }
        
        self.logger.info("🗂️  데이터 매퍼 초기화 완료")
    
    def load_data(self, df: pd.DataFrame) -> bool:
        """
        데이터프레임 로드
        
        Args:
            df: 입력 데이터프레임
            
        Returns:
            bool: 로드 성공 여부
        """
        try:
            if df is None or df.empty:
                self.logger.error("❌ 빈 데이터프레임")
                return False
            
            self.df = df.copy()
            self.cleanup_stats['total_rows'] = len(self.df)
            
            self.logger.info(f"📊 데이터 로드 완료: {len(self.df)}행 × {len(self.df.columns)}열")
            self.logger.info(f"📋 컬럼: {list(self.df.columns)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 로드 실패: {e}")
            return False
    
    def clean_phone_numbers(self) -> bool:
        """전화번호 정제"""
        try:
            self.logger.info("📞 전화번호 정제 시작")
            
            phone_columns = ['phone', 'ai_phone', 'verified_phone']
            
            for col in phone_columns:
                if col in self.df.columns:
                    before_count = self.df[col].notna().sum()
                    self.df[col] = self.df[col].apply(self._normalize_phone)
                    after_count = self.df[col].notna().sum()
                    
                    cleaned = before_count - after_count
                    if cleaned > 0:
                        self.cleanup_stats['cleaned_phones'] += cleaned
                        self.logger.info(f"   {col}: {cleaned}개 정제됨")
            
            self.logger.info("✅ 전화번호 정제 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 전화번호 정제 실패: {e}")
            return False
    
    def clean_fax_numbers(self) -> bool:
        """팩스번호 정제"""
        try:
            self.logger.info("📠 팩스번호 정제 시작")
            
            fax_columns = ['fax', 'ai_fax', 'verified_fax']
            
            for col in fax_columns:
                if col in self.df.columns:
                    before_count = self.df[col].notna().sum()
                    self.df[col] = self.df[col].apply(self._normalize_phone)
                    after_count = self.df[col].notna().sum()
                    
                    cleaned = before_count - after_count
                    if cleaned > 0:
                        self.cleanup_stats['cleaned_faxes'] += cleaned
                        self.logger.info(f"   {col}: {cleaned}개 정제됨")
            
            self.logger.info("✅ 팩스번호 정제 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 팩스번호 정제 실패: {e}")
            return False
    
    def clean_addresses(self) -> bool:
        """주소 정제"""
        try:
            self.logger.info("🏠 주소 정제 시작")
            
            if 'address' not in self.df.columns:
                self.logger.warning("⚠️ 주소 컬럼이 없음")
                return True
            
            before_count = self.df['address'].notna().sum()
            self.df['address'] = self.df['address'].apply(self._normalize_address)
            after_count = self.df['address'].notna().sum()
            
            cleaned = before_count - after_count
            if cleaned > 0:
                self.cleanup_stats['cleaned_addresses'] = cleaned
                self.logger.info(f"   주소: {cleaned}개 정제됨")
            
            self.logger.info("✅ 주소 정제 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 주소 정제 실패: {e}")
            return False
    
    def normalize_regions(self) -> bool:
        """지역 정보 정규화"""
        try:
            self.logger.info("🗺️  지역 정보 정규화 시작")
            
            if 'region' not in self.df.columns:
                self.logger.warning("⚠️ 지역 컬럼이 없음")
                return True
            
            # 지역별 매핑 적용
            region_mapping = {}
            for region_key, districts in self.region_mapping.items():
                for district in districts:
                    region_mapping[district] = region_key
            
            # 지역 정규화
            normalized_count = 0
            for idx, row in self.df.iterrows():
                original_region = str(row['region']).strip()
                if original_region in region_mapping:
                    new_region = region_mapping[original_region]
                    if new_region != original_region:
                        self.df.at[idx, 'region'] = new_region
                        normalized_count += 1
            
            self.cleanup_stats['normalized_regions'] = normalized_count
            self.logger.info(f"   지역: {normalized_count}개 정규화됨")
            
            # 지역별 분포 로그
            self._log_region_distribution()
            
            self.logger.info("✅ 지역 정보 정규화 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 지역 정보 정규화 실패: {e}")
            return False
    
    def remove_duplicates(self) -> bool:
        """중복 제거"""
        try:
            self.logger.info("🔄 중복 제거 시작")
            
            before_count = len(self.df)
            
            # 기관명 + 주소 기준으로 중복 제거
            if 'institution_name' in self.df.columns and 'address' in self.df.columns:
                self.df = self.df.drop_duplicates(
                    subset=['institution_name', 'address'], 
                    keep='first'
                )
            else:
                # 모든 컬럼 기준으로 중복 제거
                self.df = self.df.drop_duplicates(keep='first')
            
            after_count = len(self.df)
            removed = before_count - after_count
            
            self.cleanup_stats['removed_duplicates'] = removed
            self.logger.info(f"   중복 제거: {removed}개 제거됨")
            
            self.logger.info("✅ 중복 제거 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 중복 제거 실패: {e}")
            return False
    
    def validate_data(self) -> Dict:
        """데이터 유효성 검증"""
        try:
            self.logger.info("✅ 데이터 유효성 검증 시작")
            
            validation_result = {
                'total_rows': len(self.df),
                'valid_phones': 0,
                'valid_faxes': 0,
                'valid_addresses': 0,
                'valid_regions': 0,
                'empty_institution_names': 0,
                'validation_errors': []
            }
            
            # 전화번호 유효성 검증
            if 'phone' in self.df.columns:
                validation_result['valid_phones'] = self.df['phone'].apply(
                    lambda x: self._is_valid_phone(x)
                ).sum()
            
            # 팩스번호 유효성 검증
            if 'fax' in self.df.columns:
                validation_result['valid_faxes'] = self.df['fax'].apply(
                    lambda x: self._is_valid_phone(x)
                ).sum()
            
            # 주소 유효성 검증
            if 'address' in self.df.columns:
                validation_result['valid_addresses'] = self.df['address'].apply(
                    lambda x: len(str(x).strip()) > 5
                ).sum()
            
            # 지역 유효성 검증
            if 'region' in self.df.columns:
                validation_result['valid_regions'] = self.df['region'].apply(
                    lambda x: str(x).strip() in REGIONS
                ).sum()
            
            # 기관명 유효성 검증
            if 'institution_name' in self.df.columns:
                validation_result['empty_institution_names'] = self.df['institution_name'].apply(
                    lambda x: len(str(x).strip()) == 0
                ).sum()
            
            # 검증 결과 로그
            self._log_validation_result(validation_result)
            
            self.logger.info("✅ 데이터 유효성 검증 완료")
            return validation_result
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 유효성 검증 실패: {e}")
            return {}
    
    def add_derived_columns(self) -> bool:
        """파생 컬럼 추가"""
        try:
            self.logger.info("➕ 파생 컬럼 추가 시작")
            
            # 데이터 완성도 점수
            self.df['completeness_score'] = self.df.apply(self._calculate_completeness_score, axis=1)
            
            # 검증 상태 요약
            if 'phone_match' in self.df.columns and 'fax_match' in self.df.columns:
                self.df['verification_summary'] = self.df.apply(self._create_verification_summary, axis=1)
            
            # 데이터 품질 등급
            self.df['data_quality'] = self.df.apply(self._assign_quality_grade, axis=1)
            
            # 처리 타임스탬프
            self.df['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            self.logger.info("✅ 파생 컬럼 추가 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 파생 컬럼 추가 실패: {e}")
            return False
    
    def _normalize_phone(self, phone: Any) -> str:
        """전화번호 정규화"""
        try:
            if pd.isna(phone) or phone == '':
                return ''
            
            phone_str = str(phone).strip()
            
            # 숫자만 추출
            digits = re.sub(r'[^\d]', '', phone_str)
            
            # 길이 검증
            if len(digits) < 9 or len(digits) > 11:
                return ''
            
            # 형식 통일
            if len(digits) == 9:
                return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
            elif len(digits) == 10:
                if digits.startswith('02'):
                    return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
                else:
                    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11:
                return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
            
            return ''
            
        except Exception:
            return ''
    
    def _normalize_address(self, address: Any) -> str:
        """주소 정규화"""
        try:
            if pd.isna(address) or address == '':
                return ''
            
            address_str = str(address).strip()
            
            # 최소 길이 검증
            if len(address_str) < 5:
                return ''
            
            # 특수 문자 정리
            address_str = re.sub(r'\s+', ' ', address_str)  # 연속 공백 제거
            address_str = re.sub(r'[^\w\s\-\(\)\.,:;]', '', address_str)  # 특수문자 제거
            
            return address_str.strip()
            
        except Exception:
            return ''
    
    def _is_valid_phone(self, phone: Any) -> bool:
        """전화번호 유효성 검증"""
        try:
            if pd.isna(phone) or phone == '':
                return False
            
            phone_str = str(phone).strip()
            
            # 기본 패턴 검증
            pattern = r'^\d{2,3}-\d{3,4}-\d{4}$'
            return bool(re.match(pattern, phone_str))
            
        except Exception:
            return False
    
    def _calculate_completeness_score(self, row) -> float:
        """데이터 완성도 점수 계산"""
        try:
            score = 0
            max_score = 0
            
            # 필수 필드 점수
            essential_fields = ['institution_name', 'region', 'address']
            for field in essential_fields:
                max_score += 20
                if field in row and str(row[field]).strip():
                    score += 20
            
            # 연락처 정보 점수
            contact_fields = ['phone', 'fax', 'homepage']
            for field in contact_fields:
                max_score += 10
                if field in row and str(row[field]).strip():
                    score += 10
            
            # AI 검증 점수
            if 'verification_status' in row and row['verification_status'] == 'success':
                max_score += 10
                score += 10
            
            return round(score / max_score * 100, 1) if max_score > 0 else 0
            
        except Exception:
            return 0
    
    def _create_verification_summary(self, row) -> str:
        """검증 상태 요약 생성"""
        try:
            phone_match = row.get('phone_match', False)
            fax_match = row.get('fax_match', False)
            
            if phone_match and fax_match:
                return "전화번호+팩스번호 일치"
            elif phone_match:
                return "전화번호 일치"
            elif fax_match:
                return "팩스번호 일치"
            else:
                return "검증 불일치"
                
        except Exception:
            return "검증 실패"
    
    def _assign_quality_grade(self, row) -> str:
        """데이터 품질 등급 할당"""
        try:
            score = row.get('completeness_score', 0)
            
            if score >= 90:
                return "A"
            elif score >= 80:
                return "B"
            elif score >= 70:
                return "C"
            elif score >= 60:
                return "D"
            else:
                return "F"
                
        except Exception:
            return "F"
    
    def _log_region_distribution(self):
        """지역별 분포 로그"""
        try:
            if 'region' not in self.df.columns:
                return
            
            distribution = self.df['region'].value_counts()
            
            self.logger.info("📊 지역별 분포:")
            for region, count in distribution.items():
                percentage = (count / len(self.df)) * 100
                self.logger.info(f"   {region}: {count:,}개 ({percentage:.1f}%)")
                
        except Exception as e:
            self.logger.error(f"❌ 지역별 분포 로그 실패: {e}")
    
    def _log_validation_result(self, result: Dict):
        """검증 결과 로그"""
        try:
            total = result['total_rows']
            
            self.logger.info("📊 데이터 유효성 검증 결과:")
            self.logger.info(f"   전체 데이터: {total:,}개")
            self.logger.info(f"   유효 전화번호: {result['valid_phones']:,}개 ({result['valid_phones']/total*100:.1f}%)")
            self.logger.info(f"   유효 팩스번호: {result['valid_faxes']:,}개 ({result['valid_faxes']/total*100:.1f}%)")
            self.logger.info(f"   유효 주소: {result['valid_addresses']:,}개 ({result['valid_addresses']/total*100:.1f}%)")
            self.logger.info(f"   유효 지역: {result['valid_regions']:,}개 ({result['valid_regions']/total*100:.1f}%)")
            self.logger.info(f"   빈 기관명: {result['empty_institution_names']:,}개")
            
        except Exception as e:
            self.logger.error(f"❌ 검증 결과 로그 실패: {e}")
    
    def get_cleanup_summary(self) -> Dict:
        """정제 작업 요약 반환"""
        return self.cleanup_stats.copy()
    
    def get_processed_data(self) -> pd.DataFrame:
        """처리된 데이터 반환"""
        return self.df.copy() if self.df is not None else pd.DataFrame()
    
    def save_processed_data(self, output_path: str = None) -> str:
        """처리된 데이터 저장"""
        try:
            if self.df is None:
                raise ValueError("저장할 데이터가 없습니다")
            
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
                output_path = os.path.join(desktop_path, f"cleaned_data_{timestamp}.xlsx")
            
            # 최신 pandas 버전 호환성을 위해 ExcelWriter 사용
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                self.df.to_excel(writer, index=False)
            self.logger.info(f"💾 정제된 데이터 저장 완료: {output_path}")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 저장 실패: {e}")
            return ""
    
    def process_all(self) -> bool:
        """전체 데이터 정제 프로세스 실행"""
        try:
            self.logger.info("🚀 전체 데이터 정제 프로세스 시작")
            
            # 1. 전화번호 정제
            if not self.clean_phone_numbers():
                return False
            
            # 2. 팩스번호 정제
            if not self.clean_fax_numbers():
                return False
            
            # 3. 주소 정제
            if not self.clean_addresses():
                return False
            
            # 4. 지역 정보 정규화
            if not self.normalize_regions():
                return False
            
            # 5. 중복 제거
            if not self.remove_duplicates():
                return False
            
            # 6. 파생 컬럼 추가
            if not self.add_derived_columns():
                return False
            
            # 7. 데이터 유효성 검증
            validation_result = self.validate_data()
            
            # 8. 정제 요약 로그
            self._log_cleanup_summary()
            
            self.logger.info("✅ 전체 데이터 정제 프로세스 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 전체 데이터 정제 프로세스 실패: {e}")
            return False
    
    def _log_cleanup_summary(self):
        """정제 작업 요약 로그"""
        try:
            self.logger.info("📊 데이터 정제 작업 요약:")
            self.logger.info(f"   전체 데이터: {self.cleanup_stats['total_rows']:,}개")
            self.logger.info(f"   정제된 전화번호: {self.cleanup_stats['cleaned_phones']:,}개")
            self.logger.info(f"   정제된 팩스번호: {self.cleanup_stats['cleaned_faxes']:,}개")
            self.logger.info(f"   정제된 주소: {self.cleanup_stats['cleaned_addresses']:,}개")
            self.logger.info(f"   정규화된 지역: {self.cleanup_stats['normalized_regions']:,}개")
            self.logger.info(f"   제거된 중복: {self.cleanup_stats['removed_duplicates']:,}개")
            
        except Exception as e:
            self.logger.error(f"❌ 정제 요약 로그 실패: {e}") 