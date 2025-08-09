#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valid4 문제 해결 스크립트
mappingdata250809.csv의 매칭 실패 데이터를 재처리하는 개선된 로직
"""

import pandas as pd
import logging
import time
from typing import Dict, List, Tuple
import traceback

class Valid4IssueResolver:
    """Valid4 문제 해결 관리자"""
    
    def __init__(self):
        self.logger = self._setup_logger()
        self.failed_data = None
        self.need_web_search_data = None
        
    def _setup_logger(self):
        """로거 설정"""
        logger = logging.getLogger("Valid4IssueResolver")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def analyze_csv_issues(self, csv_path: str = "mappingdata250809.csv") -> Dict:
        """CSV 파일의 문제점 분석"""
        try:
            self.logger.info(f"📊 CSV 파일 분석 시작: {csv_path}")
            
            # CSV 로드 (인코딩 자동 감지)
            encodings = ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding)
                    self.logger.info(f"✅ CSV 로드 성공: {encoding} 인코딩")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise Exception("모든 인코딩 시도 실패")
            total_rows = len(df)
            
            # 문제 상황 분석
            analysis = {
                'total_rows': total_rows,
                'matching_failures': 0,
                'web_search_needed': 0,
                'phase0_complete': 0,
                'zero_confidence': 0,
                'partial_matches': 0
            }
            
            # 매칭 실패 계산
            phone_failures = df['전화_매칭_유형'].str.contains('매칭실패', na=False).sum()
            fax_failures = df['팩스_매칭_유형'].str.contains('매칭실패', na=False).sum()
            analysis['matching_failures'] = phone_failures + fax_failures
            
            # 웹 검색 필요한 데이터
            analysis['web_search_needed'] = df['최종_결과'].str.contains('웹 검색 완료 - 추가 검증 필요', na=False).sum()
            
            # Phase 0 완료 데이터
            analysis['phase0_complete'] = df['최종_결과'].str.contains('Phase 0 자동 라벨링 완료', na=False).sum()
            
            # 신뢰도 0인 데이터
            analysis['zero_confidence'] = df['Phase0_신뢰도'].eq(0).sum()
            
            # 부분 매칭 (전화 또는 팩스 중 하나만 성공)
            phone_success = df['Phase0_전화매칭'].eq(True)
            fax_success = df['Phase0_팩스매칭'].eq(True)
            analysis['partial_matches'] = ((phone_success & ~fax_success) | (~phone_success & fax_success)).sum()
            
            self.logger.info("📈 분석 결과:")
            self.logger.info(f"   - 전체 레코드: {analysis['total_rows']:,}개")
            self.logger.info(f"   - 매칭 실패: {analysis['matching_failures']:,}건")
            self.logger.info(f"   - 웹 검색 필요: {analysis['web_search_needed']:,}건")
            self.logger.info(f"   - Phase 0 완료: {analysis['phase0_complete']:,}건")
            self.logger.info(f"   - 신뢰도 0: {analysis['zero_confidence']:,}건")
            self.logger.info(f"   - 부분 매칭: {analysis['partial_matches']:,}건")
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"❌ CSV 분석 실패: {e}")
            return {}
    
    def extract_failed_records(self, csv_path: str = "mappingdata250809.csv") -> Tuple[pd.DataFrame, pd.DataFrame]:
        """실패한 레코드들을 추출"""
        try:
            # CSV 로드 (인코딩 자동 감지)
            encodings = ['utf-8', 'cp949', 'euc-kr', 'utf-8-sig']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding)
                    self.logger.info(f"✅ CSV 로드 성공: {encoding} 인코딩")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise Exception("모든 인코딩 시도 실패")
            
            # 매칭 실패 레코드 (전화나 팩스 중 하나라도 실패)
            failed_mask = (
                df['전화_매칭_유형'].str.contains('매칭실패', na=False) |
                df['팩스_매칭_유형'].str.contains('매칭실패', na=False) |
                df['Phase0_신뢰도'].eq(0)
            )
            self.failed_data = df[failed_mask].copy()
            
            # 웹 검색이 필요한 레코드
            web_search_mask = df['최종_결과'].str.contains('웹 검색 완료 - 추가 검증 필요', na=False)
            self.need_web_search_data = df[web_search_mask].copy()
            
            self.logger.info(f"🔍 추출 완료:")
            self.logger.info(f"   - 매칭 실패 레코드: {len(self.failed_data)}개")
            self.logger.info(f"   - 웹 검색 필요 레코드: {len(self.need_web_search_data)}개")
            
            return self.failed_data, self.need_web_search_data
            
        except Exception as e:
            self.logger.error(f"❌ 실패 레코드 추출 실패: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def generate_reprocessing_recommendations(self) -> List[str]:
        """재처리 권장사항 생성"""
        recommendations = [
            "🔧 Valid4.py 개선 권장사항:",
            "",
            "1. Phase 0 자동 라벨링 강화:",
            "   - 센터 데이터베이스 업데이트 (최신 전화/팩스 정보)",
            "   - 유사 문자열 매칭 알고리즘 도입 (fuzzy matching)",
            "   - 지역 기반 매칭 로직 강화",
            "",
            "2. 웹 검색 로직 개선:",
            "   - 다중 검색 엔진 활용 (Google + Naver + Daum)",
            "   - 검색 쿼리 다양화 (기관명 + 지역 + 전화번호 조합)",
            "   - AI 기반 검색 결과 분석 강화",
            "",
            "3. 에러 처리 및 재시도 메커니즘:",
            "   - 네트워크 오류 시 자동 재시도",
            "   - 부분 매칭 시 추가 검증 로직",
            "   - 매칭 실패 시 대안 검색 전략",
            "",
            "4. 성능 최적화:",
            "   - 배치 처리 크기 조정",
            "   - 동시 작업자 수 최적화",
            "   - 캐싱 메커니즘 도입",
            "",
            "5. 데이터 품질 개선:",
            "   - 입력 데이터 정규화",
            "   - 중복 제거 로직",
            "   - 데이터 검증 강화"
        ]
        
        return recommendations
    
    def save_analysis_report(self, analysis: Dict, recommendations: List[str]) -> str:
        """분석 리포트 저장"""
        try:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            report_file = f"valid4_analysis_report_{timestamp}.txt"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("Valid4 시스템 분석 리포트\n")
                f.write("=" * 60 + "\n")
                f.write(f"생성 시간: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("📊 데이터 분석 결과:\n")
                f.write("-" * 30 + "\n")
                for key, value in analysis.items():
                    f.write(f"{key}: {value:,}\n")
                
                f.write("\n📈 성공률 계산:\n")
                f.write("-" * 30 + "\n")
                if analysis.get('total_rows', 0) > 0:
                    success_rate = (analysis.get('phase0_complete', 0) / analysis['total_rows']) * 100
                    failure_rate = (analysis.get('matching_failures', 0) / (analysis['total_rows'] * 2)) * 100  # 전화+팩스
                    f.write(f"Phase 0 성공률: {success_rate:.1f}%\n")
                    f.write(f"매칭 실패율: {failure_rate:.1f}%\n")
                
                f.write("\n" + "\n".join(recommendations) + "\n")
                
                f.write("\n" + "=" * 60 + "\n")
                f.write("리포트 끝\n")
                f.write("=" * 60 + "\n")
            
            self.logger.info(f"📋 분석 리포트 저장: {report_file}")
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ 리포트 저장 실패: {e}")
            return ""

def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("🔧 Valid4 문제 해결 도구")
    print("=" * 60)
    
    resolver = Valid4IssueResolver()
    
    try:
        # 1. CSV 분석
        print("\n📊 1단계: CSV 데이터 분석")
        analysis = resolver.analyze_csv_issues()
        
        if not analysis:
            print("❌ 분석 실패")
            return
        
        # 2. 실패 레코드 추출
        print("\n🔍 2단계: 실패 레코드 추출")
        failed_data, web_search_data = resolver.extract_failed_records()
        
        # 3. 권장사항 생성
        print("\n💡 3단계: 개선 권장사항 생성")
        recommendations = resolver.generate_reprocessing_recommendations()
        
        # 4. 리포트 저장
        print("\n📋 4단계: 분석 리포트 저장")
        report_file = resolver.save_analysis_report(analysis, recommendations)
        
        # 5. 요약 출력
        print("\n" + "=" * 60)
        print("🎯 요약 및 다음 단계")
        print("=" * 60)
        print(f"📈 분석 완료: 총 {analysis.get('total_rows', 0):,}개 레코드")
        print(f"❌ 처리 필요: {len(failed_data):,}개 실패 레코드")
        print(f"🔍 웹 검색 대기: {len(web_search_data):,}개 레코드")
        print(f"📋 상세 리포트: {report_file}")
        
        print("\n🔧 권장 조치:")
        print("1. Valid4.py의 CenterDataManager 업데이트")
        print("2. Valid4WebSearchManager의 검색 로직 강화")
        print("3. 실패 레코드 배치 재처리 실행")
        print("4. 웹 검색 대기 레코드 추가 검증")
        
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
