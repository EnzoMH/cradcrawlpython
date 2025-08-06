#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import time
import psutil
import logging
import threading
from typing import Dict
from datetime import datetime

class SystemMonitor:
    """시스템 모니터링 클래스"""
    
    def __init__(self, logger=None):
        """
        시스템 모니터 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.process = psutil.Process()
        
        # 모니터링 설정
        self.monitoring_active = False
        self.monitoring_thread = None
        self.monitoring_interval = 30  # 30초마다 갱신
        
        # 통계
        self.stats = {
            'start_time': datetime.now(),
            'processed_count': 0,
            'success_count': 0,
            'phone_extracted': 0,
            'fax_extracted': 0,
            'homepage_extracted': 0,
            'total_records': 0,
            'current_phase': '초기화',
            'current_region': '',
            'cpu_percent': 0,
            'memory_mb': 0,
            'memory_percent': 0
        }
        
        # 메모리 관리 설정
        self.memory_cleanup_interval = 30  # 30개마다 메모리 정리
    
    def start_monitoring(self):
        """모니터링 시작"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.monitoring_thread = threading.Thread(target=self._monitor_system, daemon=True)
            self.monitoring_thread.start()
            self.logger.info("🔍 시스템 모니터링 시작")
    
    def stop_monitoring(self):
        """모니터링 중지"""
        if self.monitoring_active:
            self.monitoring_active = False
            if self.monitoring_thread and self.monitoring_thread.is_alive():
                self.monitoring_thread.join(timeout=3)
            self.logger.info("🛑 시스템 모니터링 중지")
    
    def update_stats(self, **kwargs):
        """
        통계 업데이트
        
        Args:
            **kwargs: 업데이트할 통계 항목
        """
        self.stats.update(kwargs)
        
        # 메모리 정리 체크
        if self.stats['processed_count'] % self.memory_cleanup_interval == 0:
            self.cleanup_memory()
    
    def get_stats(self) -> Dict:
        """
        현재 통계 반환
        
        Returns:
            Dict: 통계 정보
        """
        return self.stats.copy()
    
    def display_realtime_statistics(self):
        """실시간 통계 표시"""
        try:
            # 경과 시간 계산
            elapsed_time = datetime.now() - self.stats['start_time']
            elapsed_minutes = elapsed_time.total_seconds() / 60
            
            # 처리 속도 계산
            if elapsed_minutes > 0:
                processing_speed = self.stats['processed_count'] / elapsed_minutes
                estimated_total_time = self.stats['total_records'] / processing_speed if processing_speed > 0 else 0
                remaining_time = estimated_total_time - elapsed_minutes
            else:
                processing_speed = 0
                remaining_time = 0
            
            # 📊 실시간 통계 출력
            print("\n" + "="*60)
            print("🔍 실시간 진행 상황")
            print("="*60)
            print(f"📍 현재 작업: {self.stats['current_phase']} ({self.stats['current_region']})")
            print(f"📊 전화번호: {self.stats['phone_extracted']:,} / {self.stats['total_records']:,} ({self.stats['phone_extracted']/self.stats['total_records']*100:.1f}%) {'✅' if self.stats['phone_extracted'] > 0 else '⏳'}")
            print(f"📊 팩스번호: {self.stats['fax_extracted']:,} / {self.stats['total_records']:,} ({self.stats['fax_extracted']/self.stats['total_records']*100:.1f}%) {'✅' if self.stats['fax_extracted'] > 0 else '⏳'}")
            print(f"📊 홈페이지: {self.stats['homepage_extracted']:,} / {self.stats['total_records']:,} ({self.stats['homepage_extracted']/self.stats['total_records']*100:.1f}%) {'✅' if self.stats['homepage_extracted'] > 0 else '⏳'}")
            print(f"📊 전체 처리: {self.stats['processed_count']:,} / {self.stats['total_records']:,} ({self.stats['processed_count']/self.stats['total_records']*100:.1f}%)")
            print(f"⏱️ 경과시간: {elapsed_minutes:.1f}분")
            print(f"🚀 처리속도: {processing_speed:.1f}개/분")
            if remaining_time > 0:
                print(f"⏰ 예상 완료: {remaining_time:.1f}분 후")
            print(f"💻 CPU 사용: {self.stats['cpu_percent']:.1f}%")
            print(f"💾 메모리: {self.stats['memory_mb']:.1f}MB ({self.stats['memory_percent']:.1f}%)")
            print("="*60)
            
        except Exception as e:
            self.logger.error(f"통계 표시 중 오류 발생: {e}")
    
    def _monitor_system(self):
        """시스템 리소스 모니터링"""
        while self.monitoring_active:
            try:
                # CPU 사용량
                self.stats['cpu_percent'] = self.process.cpu_percent()
                
                # 메모리 사용량
                memory_info = self.process.memory_info()
                self.stats['memory_mb'] = memory_info.rss / 1024 / 1024
                self.stats['memory_percent'] = self.process.memory_percent()
                
                self.logger.info(
                    f"시스템 상태 - CPU: {self.stats['cpu_percent']:.1f}%, "
                    f"메모리: {self.stats['memory_mb']:.1f}MB, "
                    f"처리: {self.stats['processed_count']}개, "
                    f"성공: {self.stats['success_count']}개"
                )
                
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"모니터링 중 오류 발생: {e}")
                time.sleep(60)
    
    def cleanup_memory(self):
        """메모리 정리"""
        try:
            # 파이썬 가비지 컬렉션
            gc.collect()
            
            # 시스템 캐시 정리 (Linux)
            if os.name == 'posix':
                os.system('sync')
                
            self.logger.info("🧹 메모리 정리 완료")
            
        except Exception as e:
            self.logger.error(f"메모리 정리 중 오류 발생: {e}")
    
    def __del__(self):
        """소멸자"""
        self.stop_monitoring() 