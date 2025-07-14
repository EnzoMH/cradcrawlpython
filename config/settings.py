#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
시스템별 최적화 설정
"""

import platform
import psutil
import cpuinfo

# 시스템별 기본 설정
SYSTEM_CONFIGS = {
    # Intel Core i5 시리즈
    'i5-4210M': {
        'max_workers_multiplier': 1.5,
        'memory_threshold': 80,
        'crawling_delay': 1.0,
        'batch_size': 100,
        'selenium_timeout': 10,
        'retry_count': 3
    },
    'i5-8250U': {
        'max_workers_multiplier': 1.8,
        'memory_threshold': 82,
        'crawling_delay': 0.8,
        'batch_size': 150,
        'selenium_timeout': 8,
        'retry_count': 3
    },
    
    # Intel Core i7 시리즈
    'i7-8700K': {
        'max_workers_multiplier': 2.0,
        'memory_threshold': 85,
        'crawling_delay': 0.5,
        'batch_size': 200,
        'selenium_timeout': 6,
        'retry_count': 2
    },
    'i7-10700K': {
        'max_workers_multiplier': 2.2,
        'memory_threshold': 88,
        'crawling_delay': 0.3,
        'batch_size': 250,
        'selenium_timeout': 5,
        'retry_count': 2
    },
    
    # AMD Ryzen 시리즈
    'Ryzen 5 3600': {
        'max_workers_multiplier': 2.0,
        'memory_threshold': 85,
        'crawling_delay': 0.4,
        'batch_size': 200,
        'selenium_timeout': 6,
        'retry_count': 2
    },
    'Ryzen 7 3700X': {
        'max_workers_multiplier': 2.5,
        'memory_threshold': 90,
        'crawling_delay': 0.2,
        'batch_size': 300,
        'selenium_timeout': 4,
        'retry_count': 2
    },
    
    # 기본 설정 (알 수 없는 CPU)
    'default': {
        'max_workers_multiplier': 1.0,
        'memory_threshold': 75,
        'crawling_delay': 1.5,
        'batch_size': 50,
        'selenium_timeout': 15,
        'retry_count': 3
    }
}

# 메모리 기반 동적 설정
MEMORY_BASED_CONFIGS = {
    # 4GB 이하
    'low_memory': {
        'max_workers_multiplier': 0.8,
        'memory_threshold': 70,
        'batch_size': 50,
        'selenium_timeout': 20
    },
    # 8GB
    'medium_memory': {
        'max_workers_multiplier': 1.2,
        'memory_threshold': 80,
        'batch_size': 100,
        'selenium_timeout': 10
    },
    # 16GB 이상
    'high_memory': {
        'max_workers_multiplier': 2.0,
        'memory_threshold': 85,
        'batch_size': 200,
        'selenium_timeout': 5
    }
}

# WebDriver 설정
WEBDRIVER_CONFIGS = {
    'chrome_options': [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-web-security',
        '--disable-features=VizDisplayCompositor',
        '--disable-extensions',
        '--disable-plugins',
        '--disable-images',
        '--disable-javascript',  # 필요시 제거
        '--headless'  # 필요시 제거
    ],
    'fallback_drivers': [
        'selenium.webdriver.Chrome',
        'undetected_chromedriver.Chrome',
        'selenium.webdriver.Firefox',
        'selenium.webdriver.Edge'
    ]
}

# 크롤링 파라미터
CRAWLING_PARAMS = {
    'search_engines': ['google', 'naver', 'daum'],
    'max_search_results': 5,
    'phone_patterns': [
        r'(\d{2,3}-\d{3,4}-\d{4})',
        r'(\d{2,3}\.\d{3,4}\.\d{4})',
        r'(\d{10,11})'
    ],
    'fax_patterns': [
        r'팩스[\s:]*(\d{2,3}-\d{3,4}-\d{4})',
        r'FAX[\s:]*(\d{2,3}-\d{3,4}-\d{4})',
        r'fax[\s:]*(\d{2,3}-\d{3,4}-\d{4})'
    ],
    'homepage_patterns': [
        r'https?://[^\s<>"{}|\\^`\[\]]+',
        r'www\.[^\s<>"{}|\\^`\[\]]+',
        r'[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}'
    ]
}

def get_system_info():
    """시스템 정보 자동 탐색"""
    try:
        # CPU 정보
        cpu_info = cpuinfo.get_cpu_info()
        cpu_name = cpu_info.get('brand_raw', 'Unknown CPU')
        cpu_cores = psutil.cpu_count(logical=False)
        cpu_threads = psutil.cpu_count(logical=True)
        
        # 메모리 정보
        memory = psutil.virtual_memory()
        total_memory_gb = round(memory.total / (1024**3), 1)
        
        # 시스템 정보
        system_info = {
            'cpu_name': cpu_name,
            'cpu_cores': cpu_cores,
            'cpu_threads': cpu_threads,
            'total_memory_gb': total_memory_gb,
            'os': platform.system(),
            'os_version': platform.version(),
            'architecture': platform.architecture()[0]
        }
        
        return system_info
        
    except Exception as e:
        print(f"❌ 시스템 정보 탐색 실패: {e}")
        return None

def get_optimal_config():
    """시스템에 최적화된 설정 반환"""
    system_info = get_system_info()
    if not system_info:
        return SYSTEM_CONFIGS['default']
    
    cpu_name = system_info['cpu_name']
    total_memory_gb = system_info['total_memory_gb']
    
    # CPU 모델 기반 설정 찾기
    config = None
    for model_name, model_config in SYSTEM_CONFIGS.items():
        if model_name.lower() in cpu_name.lower():
            config = model_config.copy()
            break
    
    # CPU 모델을 찾지 못한 경우 기본 설정 사용
    if not config:
        config = SYSTEM_CONFIGS['default'].copy()
    
    # 메모리 기반 동적 조정
    if total_memory_gb <= 4:
        memory_config = MEMORY_BASED_CONFIGS['low_memory']
    elif total_memory_gb <= 8:
        memory_config = MEMORY_BASED_CONFIGS['medium_memory']
    else:
        memory_config = MEMORY_BASED_CONFIGS['high_memory']
    
    # 메모리 기반 설정 적용
    config.update(memory_config)
    
    # 시스템 정보 추가
    config['system_info'] = system_info
    
    return config

def display_system_config(config):
    """시스템 설정 정보 출력"""
    print("\n" + "="*60)
    print("🖥️  시스템 정보 및 최적화 설정")
    print("="*60)
    
    if 'system_info' in config:
        info = config['system_info']
        print(f"💻 CPU: {info['cpu_name']}")
        print(f"🔧 코어/스레드: {info['cpu_cores']}코어 {info['cpu_threads']}스레드")
        print(f"🧠 메모리: {info['total_memory_gb']}GB")
        print(f"🖥️  OS: {info['os']} {info['architecture']}")
        print("-" * 60)
    
    print(f"⚙️  최적화 설정:")
    print(f"   - 최대 워커 수: {int(psutil.cpu_count() * config['max_workers_multiplier'])}개")
    print(f"   - 메모리 임계값: {config['memory_threshold']}%")
    print(f"   - 크롤링 지연: {config['crawling_delay']}초")
    print(f"   - 배치 크기: {config['batch_size']}개")
    print(f"   - Selenium 타임아웃: {config['selenium_timeout']}초")
    print(f"   - 재시도 횟수: {config['retry_count']}회")
    print("="*60) 