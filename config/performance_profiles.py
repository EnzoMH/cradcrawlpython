#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
동적 성능 조정 로직 및 하드웨어별 최적화 프로필
"""

import psutil
import time
import logging
import multiprocessing
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

class PerformanceLevel(Enum):
    """성능 레벨 열거형"""
    LOW_SPEC = "low_spec"           # 저사양 (Intel i5-4210M 등)
    MEDIUM_SPEC = "medium_spec"     # 중사양 (Intel i5-8400 등)
    HIGH_SPEC = "high_spec"         # 고사양 (AMD Ryzen 5 3600 등)
    ULTRA_SPEC = "ultra_spec"       # 최고사양 (AMD Ryzen 9 등)

@dataclass
class PerformanceProfile:
    """성능 프로필 데이터 클래스"""
    name: str
    max_workers: int
    memory_threshold: float
    cpu_threshold: float
    crawling_delay_min: float
    crawling_delay_max: float
    batch_size: int
    selenium_timeout: int
    retry_count: int
    memory_cleanup_interval: int
    chrome_memory_limit: int
    window_size: str
    enable_javascript: bool
    cache_size: int

class PerformanceManager:
    """동적 성능 조정 관리자"""
    
    def __init__(self, logger=None):
        """
        성능 관리자 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.current_profile = None
        self.monitoring_active = False
        self.last_adjustment = time.time()
        self.adjustment_cooldown = 30  # 30초 쿨다운
        
        # 시스템 정보 수집
        self.system_info = self._analyze_system()
        self.profiles = self._initialize_profiles()
        
        # 자동 프로필 선택
        self.current_profile = self._select_optimal_profile()
        
        self.logger.info(f"🎯 성능 관리자 초기화 완료")
        self.logger.info(f"📊 시스템 정보: {self.system_info}")
        self.logger.info(f"⚙️  선택된 프로필: {self.current_profile.name}")
    
    def _analyze_system(self) -> Dict:
        """시스템 하드웨어 분석"""
        try:
            # CPU 정보
            cpu_count_physical = psutil.cpu_count(logical=False)
            cpu_count_logical = psutil.cpu_count(logical=True)
            cpu_freq = psutil.cpu_freq()
            
            # 메모리 정보
            memory = psutil.virtual_memory()
            total_memory_gb = round(memory.total / (1024**3), 1)
            
            # CPU 이름 추출 시도
            cpu_name = "Unknown"
            try:
                import cpuinfo
                cpu_info = cpuinfo.get_cpu_info()
                cpu_name = cpu_info.get('brand_raw', 'Unknown CPU')
            except:
                # cpuinfo가 없으면 기본 정보 사용
                pass
            
            return {
                'cpu_name': cpu_name,
                'cpu_cores': cpu_count_physical,
                'cpu_threads': cpu_count_logical,
                'cpu_freq_max': cpu_freq.max if cpu_freq else 0,
                'total_memory_gb': total_memory_gb,
                'platform': psutil.os.name
            }
            
        except Exception as e:
            self.logger.error(f"❌ 시스템 분석 실패: {e}")
            return {
                'cpu_name': 'Unknown',
                'cpu_cores': 2,
                'cpu_threads': 4,
                'cpu_freq_max': 2000,
                'total_memory_gb': 8.0,
                'platform': 'unknown'
            }
    
    def _initialize_profiles(self) -> Dict[PerformanceLevel, PerformanceProfile]:
        """성능 프로필 초기화"""
        profiles = {}
        
        # 저사양 프로필 (Intel i5-4210M, 8GB RAM)
        profiles[PerformanceLevel.LOW_SPEC] = PerformanceProfile(
            name="저사양 최적화 (Intel i5-4210M)",
            max_workers=1,                    # 단일 워커
            memory_threshold=85.0,            # 메모리 85% 임계값
            cpu_threshold=70.0,               # CPU 70% 임계값
            crawling_delay_min=3.0,           # 최소 3초 지연
            crawling_delay_max=5.0,           # 최대 5초 지연
            batch_size=2,                     # 작은 배치 크기
            selenium_timeout=15,              # 짧은 타임아웃
            retry_count=2,                    # 적은 재시도
            memory_cleanup_interval=10,       # 자주 메모리 정리
            chrome_memory_limit=128,          # 128MB 메모리 제한
            window_size="800,600",            # 작은 윈도우
            enable_javascript=False,          # JS 비활성화
            cache_size=1                      # 최소 캐시
        )
        
        # 중사양 프로필 (Intel i5-8400, 16GB RAM)
        profiles[PerformanceLevel.MEDIUM_SPEC] = PerformanceProfile(
            name="중사양 최적화 (Intel i5-8400)",
            max_workers=4,                    # 4개 워커
            memory_threshold=80.0,            # 메모리 80% 임계값
            cpu_threshold=75.0,               # CPU 75% 임계값
            crawling_delay_min=2.0,           # 최소 2초 지연
            crawling_delay_max=3.0,           # 최대 3초 지연
            batch_size=5,                     # 중간 배치 크기
            selenium_timeout=20,              # 중간 타임아웃
            retry_count=3,                    # 보통 재시도
            memory_cleanup_interval=20,       # 보통 메모리 정리
            chrome_memory_limit=256,          # 256MB 메모리 제한
            window_size="1366,768",           # 중간 윈도우
            enable_javascript=True,           # JS 활성화
            cache_size=32                     # 32MB 캐시
        )
        
        # 고사양 프로필 (AMD Ryzen 5 3600, 16GB RAM)
        profiles[PerformanceLevel.HIGH_SPEC] = PerformanceProfile(
            name="고사양 최적화 (AMD Ryzen 5 3600)",
            max_workers=12,                   # 12개 워커
            memory_threshold=85.0,            # 메모리 85% 임계값
            cpu_threshold=80.0,               # CPU 80% 임계값
            crawling_delay_min=1.0,           # 최소 1초 지연
            crawling_delay_max=2.0,           # 최대 2초 지연
            batch_size=8,                     # 큰 배치 크기
            selenium_timeout=30,              # 긴 타임아웃
            retry_count=3,                    # 보통 재시도
            memory_cleanup_interval=50,       # 덜 자주 메모리 정리
            chrome_memory_limit=512,          # 512MB 메모리 제한
            window_size="1920,1080",          # 큰 윈도우
            enable_javascript=True,           # JS 활성화
            cache_size=64                     # 64MB 캐시
        )
        
        # 최고사양 프로필 (AMD Ryzen 9, 32GB RAM)
        profiles[PerformanceLevel.ULTRA_SPEC] = PerformanceProfile(
            name="최고사양 최적화 (AMD Ryzen 9)",
            max_workers=24,                   # 24개 워커
            memory_threshold=90.0,            # 메모리 90% 임계값
            cpu_threshold=85.0,               # CPU 85% 임계값
            crawling_delay_min=0.5,           # 최소 0.5초 지연
            crawling_delay_max=1.0,           # 최대 1초 지연
            batch_size=15,                    # 매우 큰 배치 크기
            selenium_timeout=30,              # 긴 타임아웃
            retry_count=4,                    # 많은 재시도
            memory_cleanup_interval=100,      # 가끔 메모리 정리
            chrome_memory_limit=1024,         # 1GB 메모리 제한
            window_size="1920,1080",          # 큰 윈도우
            enable_javascript=True,           # JS 활성화
            cache_size=128                    # 128MB 캐시
        )
        
        return profiles
    
    def _select_optimal_profile(self) -> PerformanceProfile:
        """시스템 사양에 따른 최적 프로필 자동 선택"""
        try:
            cpu_cores = self.system_info['cpu_cores']
            cpu_threads = self.system_info['cpu_threads']
            memory_gb = self.system_info['total_memory_gb']
            cpu_name = self.system_info['cpu_name'].lower()
            
            # CPU 이름 기반 특정 모델 감지
            if 'i5-4210m' in cpu_name or 'celeron' in cpu_name:
                return self.profiles[PerformanceLevel.LOW_SPEC]
            elif 'ryzen 9' in cpu_name or 'i9' in cpu_name:
                return self.profiles[PerformanceLevel.ULTRA_SPEC]
            elif 'ryzen 7' in cpu_name or 'ryzen 5 3600' in cpu_name:
                return self.profiles[PerformanceLevel.HIGH_SPEC]
            
            # 일반적인 하드웨어 사양 기반 선택
            if cpu_cores <= 2 or memory_gb <= 8:
                return self.profiles[PerformanceLevel.LOW_SPEC]
            elif cpu_cores <= 4 or memory_gb <= 16:
                if cpu_threads >= 8:
                    return self.profiles[PerformanceLevel.HIGH_SPEC]
                else:
                    return self.profiles[PerformanceLevel.MEDIUM_SPEC]
            elif cpu_cores >= 6 and memory_gb >= 16:
                if cpu_threads >= 16 and memory_gb >= 32:
                    return self.profiles[PerformanceLevel.ULTRA_SPEC]
                else:
                    return self.profiles[PerformanceLevel.HIGH_SPEC]
            else:
                return self.profiles[PerformanceLevel.MEDIUM_SPEC]
                
        except Exception as e:
            self.logger.error(f"❌ 프로필 선택 실패: {e}")
            return self.profiles[PerformanceLevel.MEDIUM_SPEC]
    
    def get_current_profile(self) -> PerformanceProfile:
        """현재 성능 프로필 반환"""
        return self.current_profile
    
    def set_profile(self, level: PerformanceLevel) -> bool:
        """성능 프로필 수동 설정"""
        try:
            if level in self.profiles:
                self.current_profile = self.profiles[level]
                self.logger.info(f"⚙️  성능 프로필 변경: {self.current_profile.name}")
                return True
            else:
                self.logger.error(f"❌ 존재하지 않는 프로필: {level}")
                return False
        except Exception as e:
            self.logger.error(f"❌ 프로필 설정 실패: {e}")
            return False
    
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
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_available_gb': round(memory.available / (1024**3), 1),
                'disk_percent': disk_percent,
                'timestamp': time.time()
            }
            
        except Exception as e:
            self.logger.error(f"❌ 리소스 정보 수집 실패: {e}")
            return {}
    
    def should_adjust_performance(self, current_resources: Dict) -> Tuple[bool, str]:
        """성능 조정이 필요한지 판단"""
        try:
            # 쿨다운 체크
            if time.time() - self.last_adjustment < self.adjustment_cooldown:
                return False, "쿨다운 중"
            
            cpu_percent = current_resources.get('cpu_percent', 0)
            memory_percent = current_resources.get('memory_percent', 0)
            
            # 과부하 상태 체크
            if cpu_percent > self.current_profile.cpu_threshold:
                return True, f"CPU 과부하: {cpu_percent:.1f}% > {self.current_profile.cpu_threshold}%"
            
            if memory_percent > self.current_profile.memory_threshold:
                return True, f"메모리 과부하: {memory_percent:.1f}% > {self.current_profile.memory_threshold}%"
            
            return False, "정상 범위"
            
        except Exception as e:
            self.logger.error(f"❌ 성능 조정 판단 실패: {e}")
            return False, "오류 발생"
    
    def adjust_performance_dynamically(self, current_resources: Dict) -> Dict:
        """동적 성능 조정"""
        try:
            should_adjust, reason = self.should_adjust_performance(current_resources)
            
            if not should_adjust:
                return {
                    'adjusted': False,
                    'reason': reason,
                    'profile': self.current_profile.name
                }
            
            cpu_percent = current_resources.get('cpu_percent', 0)
            memory_percent = current_resources.get('memory_percent', 0)
            
            # 현재 프로필의 복사본 생성
            adjusted_profile = PerformanceProfile(**self.current_profile.__dict__)
            adjustments = []
            
            # CPU 과부하 조정
            if cpu_percent > self.current_profile.cpu_threshold:
                # 워커 수 감소
                if adjusted_profile.max_workers > 1:
                    adjusted_profile.max_workers = max(1, adjusted_profile.max_workers // 2)
                    adjustments.append(f"워커 수 감소: {adjusted_profile.max_workers}")
                
                # 지연 시간 증가
                adjusted_profile.crawling_delay_min *= 1.5
                adjusted_profile.crawling_delay_max *= 1.5
                adjustments.append(f"지연 시간 증가: {adjusted_profile.crawling_delay_min:.1f}-{adjusted_profile.crawling_delay_max:.1f}초")
            
            # 메모리 과부하 조정
            if memory_percent > self.current_profile.memory_threshold:
                # 배치 크기 감소
                adjusted_profile.batch_size = max(1, adjusted_profile.batch_size // 2)
                adjustments.append(f"배치 크기 감소: {adjusted_profile.batch_size}")
                
                # 메모리 정리 간격 단축
                adjusted_profile.memory_cleanup_interval = max(5, adjusted_profile.memory_cleanup_interval // 2)
                adjustments.append(f"메모리 정리 간격 단축: {adjusted_profile.memory_cleanup_interval}")
                
                # Chrome 메모리 제한 감소
                adjusted_profile.chrome_memory_limit = max(64, adjusted_profile.chrome_memory_limit // 2)
                adjustments.append(f"Chrome 메모리 제한 감소: {adjusted_profile.chrome_memory_limit}MB")
            
            # 조정된 프로필 적용
            self.current_profile = adjusted_profile
            self.last_adjustment = time.time()
            
            self.logger.warning(f"⚙️  동적 성능 조정 실행: {reason}")
            for adjustment in adjustments:
                self.logger.info(f"   - {adjustment}")
            
            return {
                'adjusted': True,
                'reason': reason,
                'adjustments': adjustments,
                'profile': self.current_profile.name
            }
            
        except Exception as e:
            self.logger.error(f"❌ 동적 성능 조정 실패: {e}")
            return {
                'adjusted': False,
                'reason': f"조정 실패: {str(e)}",
                'profile': self.current_profile.name
            }
    
    def get_chrome_options_for_profile(self) -> list:
        """현재 프로필에 맞는 Chrome 옵션 생성"""
        try:
            options = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions',
                '--mute-audio',
                '--no-first-run',
                '--disable-infobars',
                '--disable-notifications',
                f'--window-size={self.current_profile.window_size}',
                f'--max_old_space_size={self.current_profile.chrome_memory_limit}',
                '--memory-pressure-off',
                '--aggressive-cache-discard'
            ]
            
            # JavaScript 활성화/비활성화
            if not self.current_profile.enable_javascript:
                options.append('--disable-javascript')
            
            # 캐시 크기 설정
            cache_size_bytes = self.current_profile.cache_size * 1024 * 1024
            options.extend([
                f'--disk-cache-size={cache_size_bytes}',
                f'--media-cache-size={cache_size_bytes}'
            ])
            
            # 저사양 환경 추가 최적화
            if self.current_profile.chrome_memory_limit <= 128:
                options.extend([
                    '--disable-images',
                    '--disable-plugins',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-application-cache'
                ])
            
            return options
            
        except Exception as e:
            self.logger.error(f"❌ Chrome 옵션 생성 실패: {e}")
            return []
    
    def get_profile_summary(self) -> Dict:
        """현재 프로필 요약 정보 반환"""
        try:
            return {
                'name': self.current_profile.name,
                'max_workers': self.current_profile.max_workers,
                'memory_threshold': f"{self.current_profile.memory_threshold}%",
                'cpu_threshold': f"{self.current_profile.cpu_threshold}%",
                'crawling_delay': f"{self.current_profile.crawling_delay_min}-{self.current_profile.crawling_delay_max}초",
                'batch_size': self.current_profile.batch_size,
                'chrome_memory': f"{self.current_profile.chrome_memory_limit}MB",
                'window_size': self.current_profile.window_size,
                'javascript_enabled': self.current_profile.enable_javascript
            }
        except Exception as e:
            self.logger.error(f"❌ 프로필 요약 생성 실패: {e}")
            return {}
    
    def display_performance_info(self):
        """성능 정보 출력"""
        try:
            print("=" * 60)
            print("🖥️  시스템 정보 및 성능 프로필")
            print("=" * 60)
            
            # 시스템 정보
            print(f"💻 CPU: {self.system_info['cpu_name']}")
            print(f"🔧 코어/스레드: {self.system_info['cpu_cores']}코어 {self.system_info['cpu_threads']}스레드")
            print(f"🧠 메모리: {self.system_info['total_memory_gb']}GB")
            print(f"🖥️  플랫폼: {self.system_info['platform']}")
            
            print("-" * 60)
            
            # 프로필 정보
            summary = self.get_profile_summary()
            print(f"⚙️  성능 프로필: {summary['name']}")
            print(f"   - 최대 워커 수: {summary['max_workers']}개")
            print(f"   - 메모리 임계값: {summary['memory_threshold']}")
            print(f"   - CPU 임계값: {summary['cpu_threshold']}")
            print(f"   - 크롤링 지연: {summary['crawling_delay']}")
            print(f"   - 배치 크기: {summary['batch_size']}개")
            print(f"   - Chrome 메모리: {summary['chrome_memory']}")
            print(f"   - 윈도우 크기: {summary['window_size']}")
            print(f"   - JavaScript: {'활성화' if summary['javascript_enabled'] else '비활성화'}")
            
            print("=" * 60)
            
        except Exception as e:
            self.logger.error(f"❌ 성능 정보 출력 실패: {e}")


# 전역 함수들 (기존 코드와의 호환성을 위해)
def get_optimal_performance_config():
    """최적 성능 설정 반환 (호환성 함수)"""
    manager = PerformanceManager()
    return manager.get_current_profile()

def create_performance_manager():
    """성능 관리자 생성 (호환성 함수)"""
    return PerformanceManager() 