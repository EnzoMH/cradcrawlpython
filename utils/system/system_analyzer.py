#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
시스템 분석 및 동적 워커 관리 클래스
"""

import psutil
import time
import threading
import logging
from typing import Dict, Optional
from datetime import datetime
from config.settings import get_optimal_config, display_system_config

class SystemAnalyzer:
    """시스템 분석 및 동적 워커 관리"""
    
    def __init__(self, logger=None):
        """
        시스템 분석기 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.config = get_optimal_config()
        self.current_workers = 0
        self.max_workers = self.get_optimal_workers()
        self.optimal_workers = self.max_workers  # comp.py 호환성을 위한 별칭
        self.monitoring = False
        self.monitor_thread = None
        self.last_adjustment = time.time()
        self.adjustment_cooldown = 30  # 30초 쿨다운
        
        # 시스템 정보 출력
        display_system_config(self.config)
        
        # 초기 로그
        self.logger.info(f"🖥️  시스템 분석기 초기화 완료")
        self.logger.info(f"⚙️  최적 워커 수: {self.max_workers}개")
    
    def get_optimal_workers(self) -> int:
        """최적 워커 수 계산"""
        try:
            cpu_count = psutil.cpu_count(logical=True)
            multiplier = self.config['max_workers_multiplier']
            optimal = int(cpu_count * multiplier)
            
            # 최소 1개, 최대 CPU 스레드 수 * 2
            optimal = max(1, min(optimal, cpu_count * 2))
            
            return optimal
            
        except Exception as e:
            self.logger.error(f"❌ 최적 워커 수 계산 실패: {e}")
            return 4  # 기본값
    
    def get_current_resources(self) -> Dict:
        """현재 시스템 리소스 상태 반환"""
        try:
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 메모리 사용률
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # 디스크 사용률
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # 네트워크 상태
            net_io = psutil.net_io_counters()
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_available_gb': round(memory.available / (1024**3), 1),
                'disk_percent': disk_percent,
                'network_sent_mb': round(net_io.bytes_sent / (1024**2), 1),
                'network_recv_mb': round(net_io.bytes_recv / (1024**2), 1),
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"❌ 리소스 정보 수집 실패: {e}")
            return {}
    
    def should_adjust_workers(self, resources: Dict) -> Optional[str]:
        """워커 수 조정 필요성 판단"""
        try:
            # 쿨다운 체크
            if time.time() - self.last_adjustment < self.adjustment_cooldown:
                return None
            
            memory_threshold = self.config['memory_threshold']
            cpu_threshold = 90  # CPU 임계값
            
            # 메모리 부족 시 워커 감소
            if resources['memory_percent'] > memory_threshold:
                return 'decrease'
            
            # CPU 과부하 시 워커 감소
            if resources['cpu_percent'] > cpu_threshold:
                return 'decrease'
            
            # 리소스 여유 시 워커 증가
            if (resources['memory_percent'] < memory_threshold - 10 and 
                resources['cpu_percent'] < 70 and 
                self.current_workers < self.max_workers):
                return 'increase'
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 워커 조정 판단 실패: {e}")
            return None
    
    def adjust_workers(self, action: str) -> int:
        """워커 수 조정"""
        try:
            old_workers = self.current_workers
            
            if action == 'increase':
                self.current_workers = min(self.current_workers + 1, self.max_workers)
            elif action == 'decrease':
                self.current_workers = max(self.current_workers - 1, 1)
            
            # 조정이 실제로 발생한 경우
            if self.current_workers != old_workers:
                self.last_adjustment = time.time()
                self.logger.info(f"⚙️  워커 수 조정: {old_workers} → {self.current_workers}")
                
                # 조정 사유 로그
                if action == 'increase':
                    self.logger.info("📈 리소스 여유로 워커 증가")
                else:
                    self.logger.info("📉 리소스 부족으로 워커 감소")
            
            return self.current_workers
            
        except Exception as e:
            self.logger.error(f"❌ 워커 조정 실패: {e}")
            return self.current_workers
    
    def start_monitoring(self):
        """실시간 모니터링 시작"""
        try:
            if self.monitoring:
                return
            
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            
            self.logger.info("🔍 실시간 시스템 모니터링 시작")
            
        except Exception as e:
            self.logger.error(f"❌ 모니터링 시작 실패: {e}")
    
    def stop_monitoring(self):
        """실시간 모니터링 중지"""
        try:
            self.monitoring = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=5)
            
            self.logger.info("🛑 실시간 시스템 모니터링 중지")
            
        except Exception as e:
            self.logger.error(f"❌ 모니터링 중지 실패: {e}")
    
    def _monitor_loop(self):
        """모니터링 루프"""
        while self.monitoring:
            try:
                # 리소스 정보 수집
                resources = self.get_current_resources()
                if not resources:
                    time.sleep(10)
                    continue
                
                # 워커 수 조정 판단
                action = self.should_adjust_workers(resources)
                if action:
                    self.adjust_workers(action)
                
                # 주기적 상태 로그 (5분마다)
                if int(time.time()) % 300 == 0:
                    self.log_system_status(resources)
                
                time.sleep(10)  # 10초마다 체크
                
            except Exception as e:
                self.logger.error(f"❌ 모니터링 루프 오류: {e}")
                time.sleep(30)
    
    def log_system_status(self, resources: Dict):
        """시스템 상태 로그 출력"""
        try:
            self.logger.info("📊 시스템 상태 리포트")
            self.logger.info(f"   CPU: {resources['cpu_percent']:.1f}%")
            self.logger.info(f"   메모리: {resources['memory_percent']:.1f}% "
                           f"(사용가능: {resources['memory_available_gb']}GB)")
            self.logger.info(f"   디스크: {resources['disk_percent']:.1f}%")
            self.logger.info(f"   현재 워커: {self.current_workers}/{self.max_workers}")
            
        except Exception as e:
            self.logger.error(f"❌ 상태 로그 출력 실패: {e}")
    
    def get_memory_usage_mb(self) -> float:
        """현재 프로세스 메모리 사용량 (MB)"""
        try:
            process = psutil.Process()
            return round(process.memory_info().rss / (1024**2), 1)
            
        except Exception as e:
            self.logger.error(f"❌ 메모리 사용량 확인 실패: {e}")
            return 0.0
    
    def is_system_healthy(self) -> bool:
        """시스템 상태 건강성 체크"""
        try:
            resources = self.get_current_resources()
            if not resources:
                return False
            
            # 메모리 사용률이 임계값 이하
            memory_ok = resources['memory_percent'] < self.config['memory_threshold']
            
            # CPU 사용률이 90% 이하
            cpu_ok = resources['cpu_percent'] < 90
            
            # 디스크 사용률이 95% 이하
            disk_ok = resources['disk_percent'] < 95
            
            return memory_ok and cpu_ok and disk_ok
            
        except Exception as e:
            self.logger.error(f"❌ 시스템 건강성 체크 실패: {e}")
            return False
    
    def get_recommended_batch_size(self) -> int:
        """현재 시스템 상태에 따른 권장 배치 크기"""
        try:
            resources = self.get_current_resources()
            if not resources:
                return self.config['batch_size']
            
            base_size = self.config['batch_size']
            
            # 메모리 사용률에 따른 조정
            if resources['memory_percent'] > 85:
                return int(base_size * 0.5)
            elif resources['memory_percent'] > 75:
                return int(base_size * 0.7)
            elif resources['memory_percent'] < 60:
                return int(base_size * 1.2)
            
            return base_size
            
        except Exception as e:
            self.logger.error(f"❌ 배치 크기 계산 실패: {e}")
            return self.config['batch_size']
    
    def cleanup(self):
        """정리 작업"""
        try:
            self.stop_monitoring()
            self.logger.info("🧹 시스템 분석기 정리 완료")
            
        except Exception as e:
            self.logger.error(f"❌ 정리 작업 실패: {e}")
    
    def __del__(self):
        """소멸자"""
        self.cleanup() 