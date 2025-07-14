#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
엑셀 파일 처리 및 AI 헤더 분석 클래스
"""

import os
import pandas as pd
import logging
import google.generativeai as genai
from typing import Dict, List, Optional, Tuple
from datetime import datetime

class ExcelProcessor:
    """엑셀 파일 처리 및 AI 헤더 분석"""
    
    def __init__(self, logger=None):
        """
        엑셀 처리기 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.df = None
        self.original_headers = []
        self.mapped_headers = {}
        self.ai_model = None
        
        # 표준 컬럼 정의
        self.standard_columns = {
            'region': '지역',
            'institution_name': '기관명',
            'address': '주소',
            'phone': '전화번호',
            'fax': '팩스번호',
            'homepage': '홈페이지',
            'ai_phone': 'AI추출전화번호',
            'ai_fax': 'AI추출팩스번호'
        }
        
        self.logger.info("📊 엑셀 처리기 초기화 완료")
    
    def initialize_ai(self, api_key: str) -> bool:
        """
        Gemini AI 초기화
        
        Args:
            api_key: Gemini API 키
            
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            genai.configure(api_key=api_key)
            self.ai_model = genai.GenerativeModel('gemini-1.5-flash')
            self.logger.info("🤖 Gemini AI 초기화 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Gemini AI 초기화 실패: {e}")
            return False
    
    def load_excel_file(self, file_path: str) -> bool:
        """
        엑셀 파일 로드
        
        Args:
            file_path: 엑셀 파일 경로
            
        Returns:
            bool: 로드 성공 여부
        """
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"❌ 파일이 존재하지 않음: {file_path}")
                return False
            
            # 파일 확장자 확인
            file_ext = os.path.splitext(file_path)[1].lower()
            if file_ext not in ['.xlsx', '.xls', '.csv']:
                self.logger.error(f"❌ 지원하지 않는 파일 형식: {file_ext}")
                return False
            
            self.logger.info(f"📂 파일 로드 시작: {file_path}")
            
            # 파일 로드
            if file_ext == '.csv':
                self.df = pd.read_csv(file_path, encoding='utf-8-sig')
            else:
                self.df = pd.read_excel(file_path)
            
            self.logger.info(f"✅ 파일 로드 완료: {len(self.df)}행 × {len(self.df.columns)}열")
            
            # 헤더 정보 저장
            self.original_headers = list(self.df.columns)
            self.logger.info(f"📋 원본 헤더: {self.original_headers}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 파일 로드 실패: {e}")
            return False
    
    def detect_header_row(self) -> int:
        """
        헤더 행 자동 감지
        
        Returns:
            int: 헤더 행 번호 (0부터 시작)
        """
        try:
            if self.df is None:
                return 0
            
            # 첫 번째 행이 헤더인지 확인
            first_row = self.df.iloc[0]
            
            # 문자열 비율 계산
            string_count = sum(1 for val in first_row if isinstance(val, str))
            string_ratio = string_count / len(first_row)
            
            # 문자열 비율이 70% 이상이면 헤더로 판단
            if string_ratio >= 0.7:
                self.logger.info("✅ 첫 번째 행을 헤더로 감지")
                return 0
            
            # 상세 분석 수행
            self.logger.info("🔍 헤더 행 상세 분석 시작")
            return self._detailed_header_analysis()
            
        except Exception as e:
            self.logger.error(f"❌ 헤더 행 감지 실패: {e}")
            return 0
    
    def _detailed_header_analysis(self) -> int:
        """헤더 행 상세 분석"""
        try:
            max_rows_to_check = min(10, len(self.df))
            best_header_row = 0
            best_score = 0
            
            for row_idx in range(max_rows_to_check):
                row = self.df.iloc[row_idx]
                score = self._calculate_header_score(row)
                
                if score > best_score:
                    best_score = score
                    best_header_row = row_idx
            
            self.logger.info(f"✅ 헤더 행 감지 완료: {best_header_row + 1}번째 행 (점수: {best_score:.2f})")
            return best_header_row
            
        except Exception as e:
            self.logger.error(f"❌ 헤더 행 상세 분석 실패: {e}")
            return 0
    
    def _calculate_header_score(self, row) -> float:
        """헤더 점수 계산"""
        try:
            score = 0
            
            # 문자열 비율
            string_count = sum(1 for val in row if isinstance(val, str))
            score += (string_count / len(row)) * 40
            
            # 빈 값 비율 (적을수록 좋음)
            empty_count = sum(1 for val in row if pd.isna(val) or val == '')
            score += (1 - empty_count / len(row)) * 30
            
            # 헤더 키워드 포함 여부
            header_keywords = ['이름', '명칭', '기관', '주소', '전화', '팩스', '홈페이지', 
                             '지역', '구', '시', '군', 'name', 'address', 'phone', 'fax']
            
            keyword_count = 0
            for val in row:
                if isinstance(val, str):
                    for keyword in header_keywords:
                        if keyword in val.lower():
                            keyword_count += 1
                            break
            
            score += (keyword_count / len(row)) * 30
            
            return score
            
        except Exception as e:
            self.logger.error(f"❌ 헤더 점수 계산 실패: {e}")
            return 0
    
    def analyze_headers_with_ai(self) -> Dict:
        """
        AI를 사용한 헤더 분석 및 매핑 제안
        
        Returns:
            Dict: 헤더 매핑 결과
        """
        try:
            if not self.ai_model:
                self.logger.warning("⚠️ AI 모델이 초기화되지 않음")
                return self._fallback_header_mapping()
            
            self.logger.info("🤖 AI 헤더 분석 시작")
            
            # AI 프롬프트 생성
            prompt = self._generate_header_analysis_prompt()
            
            # AI 분석 실행
            response = self.ai_model.generate_content(prompt)
            
            # 응답 파싱
            mapping = self._parse_ai_response(response.text)
            
            if mapping:
                self.logger.info("✅ AI 헤더 분석 완료")
                self._log_header_mapping(mapping)
                return mapping
            else:
                self.logger.warning("⚠️ AI 분석 실패, 폴백 매핑 사용")
                return self._fallback_header_mapping()
            
        except Exception as e:
            self.logger.error(f"❌ AI 헤더 분석 실패: {e}")
            return self._fallback_header_mapping()
    
    def _generate_header_analysis_prompt(self) -> str:
        """AI 헤더 분석 프롬프트 생성"""
        headers_str = ", ".join(self.original_headers)
        
        prompt = f"""
다음 엑셀 파일의 헤더를 분석하여 적절한 컬럼 매핑을 제안해주세요.

원본 헤더: {headers_str}

매핑할 표준 컬럼:
- region: 지역 정보 (시/구/군)
- institution_name: 기관명 (학원, 교회, 센터 등)
- address: 주소 정보 (도로명주소, 지번주소)
- phone: 전화번호
- fax: 팩스번호
- homepage: 홈페이지/웹사이트

응답 형식 (JSON):
{{
    "region": "원본헤더명",
    "institution_name": "원본헤더명",
    "address": "원본헤더명",
    "phone": "원본헤더명",
    "fax": "원본헤더명",
    "homepage": "원본헤더명"
}}

매핑할 수 없는 항목은 null로 표시해주세요.
"""
        return prompt
    
    def _parse_ai_response(self, response_text: str) -> Dict:
        """AI 응답 파싱"""
        try:
            import json
            import re
            
            # JSON 부분 추출
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if not json_match:
                return {}
            
            json_str = json_match.group(0)
            mapping = json.loads(json_str)
            
            # 유효성 검증
            valid_mapping = {}
            for key, value in mapping.items():
                if key in self.standard_columns and value and value in self.original_headers:
                    valid_mapping[key] = value
            
            return valid_mapping
            
        except Exception as e:
            self.logger.error(f"❌ AI 응답 파싱 실패: {e}")
            return {}
    
    def _fallback_header_mapping(self) -> Dict:
        """폴백 헤더 매핑 (키워드 기반)"""
        try:
            mapping = {}
            
            # 키워드 기반 매핑
            mapping_rules = {
                'region': ['지역', '시', '구', '군', '위치', 'region', 'location'],
                'institution_name': ['기관명', '이름', '명칭', '학원명', '교회명', 'name', 'institution'],
                'address': ['주소', '소재지', 'address', 'addr'],
                'phone': ['전화번호', '전화', 'phone', 'tel'],
                'fax': ['팩스번호', '팩스', 'fax'],
                'homepage': ['홈페이지', '웹사이트', 'homepage', 'website', 'url']
            }
            
            for standard_col, keywords in mapping_rules.items():
                for header in self.original_headers:
                    header_lower = header.lower()
                    for keyword in keywords:
                        if keyword in header_lower:
                            mapping[standard_col] = header
                            break
                    if standard_col in mapping:
                        break
            
            self.logger.info("✅ 폴백 헤더 매핑 완료")
            return mapping
            
        except Exception as e:
            self.logger.error(f"❌ 폴백 헤더 매핑 실패: {e}")
            return {}
    
    def _log_header_mapping(self, mapping: Dict):
        """헤더 매핑 결과 로그 출력"""
        try:
            self.logger.info("📋 헤더 매핑 결과:")
            for standard_col, original_header in mapping.items():
                standard_name = self.standard_columns.get(standard_col, standard_col)
                self.logger.info(f"   {standard_name} ← {original_header}")
            
            # 매핑되지 않은 헤더
            unmapped = [h for h in self.original_headers if h not in mapping.values()]
            if unmapped:
                self.logger.info(f"❓ 매핑되지 않은 헤더: {unmapped}")
                
        except Exception as e:
            self.logger.error(f"❌ 헤더 매핑 로그 출력 실패: {e}")
    
    def manual_header_mapping(self) -> Dict:
        """사용자 수동 헤더 매핑"""
        try:
            self.logger.info("✋ 수동 헤더 매핑 시작")
            print("\n" + "="*60)
            print("📋 수동 헤더 매핑")
            print("="*60)
            
            print(f"원본 헤더: {self.original_headers}")
            print("\n표준 컬럼:")
            for key, name in self.standard_columns.items():
                print(f"  {key}: {name}")
            
            mapping = {}
            
            for key, name in self.standard_columns.items():
                if key in ['ai_phone', 'ai_fax']:  # AI 결과 컬럼은 나중에 추가
                    continue
                    
                print(f"\n{name}에 해당하는 원본 헤더를 선택하세요:")
                for i, header in enumerate(self.original_headers):
                    print(f"  {i+1}. {header}")
                print("  0. 없음")
                
                try:
                    choice = input("선택 (번호): ").strip()
                    if choice == '0':
                        continue
                    
                    index = int(choice) - 1
                    if 0 <= index < len(self.original_headers):
                        mapping[key] = self.original_headers[index]
                        print(f"✅ {name} ← {self.original_headers[index]}")
                    else:
                        print("❌ 잘못된 선택")
                        
                except ValueError:
                    print("❌ 숫자를 입력해주세요")
                    continue
            
            self.logger.info("✅ 수동 헤더 매핑 완료")
            self._log_header_mapping(mapping)
            
            return mapping
            
        except Exception as e:
            self.logger.error(f"❌ 수동 헤더 매핑 실패: {e}")
            return {}
    
    def apply_header_mapping(self, mapping: Dict) -> bool:
        """
        헤더 매핑 적용
        
        Args:
            mapping: 헤더 매핑 딕셔너리
            
        Returns:
            bool: 적용 성공 여부
        """
        try:
            if self.df is None:
                self.logger.error("❌ 데이터프레임이 로드되지 않음")
                return False
            
            # 매핑 적용
            rename_dict = {}
            for standard_col, original_header in mapping.items():
                if original_header in self.df.columns:
                    rename_dict[original_header] = standard_col
            
            self.df = self.df.rename(columns=rename_dict)
            
            # 누락된 표준 컬럼 추가
            for standard_col in self.standard_columns.keys():
                if standard_col not in self.df.columns:
                    self.df[standard_col] = ''
            
            # NaN 값 처리
            self.df = self.df.fillna('')
            
            self.mapped_headers = mapping
            self.logger.info("✅ 헤더 매핑 적용 완료")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 헤더 매핑 적용 실패: {e}")
            return False
    
    def get_processed_data(self) -> pd.DataFrame:
        """
        처리된 데이터 반환
        
        Returns:
            pd.DataFrame: 처리된 데이터프레임
        """
        return self.df.copy() if self.df is not None else pd.DataFrame()
    
    def save_processed_data(self, output_path: str = None) -> str:
        """
        처리된 데이터 저장
        
        Args:
            output_path: 출력 파일 경로 (기본값: None)
            
        Returns:
            str: 저장된 파일 경로
        """
        try:
            if self.df is None:
                raise ValueError("저장할 데이터가 없습니다")
            
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
                output_path = os.path.join(desktop_path, f"processed_data_{timestamp}.xlsx")
            
            # 최신 pandas 버전 호환성을 위해 ExcelWriter 사용
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                self.df.to_excel(writer, index=False)
            self.logger.info(f"💾 처리된 데이터 저장 완료: {output_path}")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 저장 실패: {e}")
            return ""
    
    def get_data_summary(self) -> Dict:
        """데이터 요약 정보 반환"""
        try:
            if self.df is None:
                return {}
            
            summary = {
                'total_rows': len(self.df),
                'total_columns': len(self.df.columns),
                'columns': list(self.df.columns),
                'empty_cells': self.df.isnull().sum().sum(),
                'data_types': self.df.dtypes.to_dict()
            }
            
            # 지역별 분포
            if 'region' in self.df.columns:
                summary['region_distribution'] = self.df['region'].value_counts().to_dict()
            
            return summary
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 요약 실패: {e}")
            return {} 