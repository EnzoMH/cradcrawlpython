#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ChromeDriver 캐시 정리 스크립트
undetected_chromedriver의 문제 해결용
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

def cleanup_undetected_chrome_cache():
    """undetected_chromedriver 캐시 정리"""
    print("🧹 undetected_chromedriver 캐시 정리 시작...")
    
    # 정리할 경로들
    cache_paths = [
        os.path.expanduser("~/.undetected_chromedriver"),
        os.path.join(os.path.expanduser("~"), "AppData", "Roaming", "undetected_chromedriver"),
        os.path.join(os.path.expanduser("~"), "appdata", "roaming", "undetected_chromedriver"),
        os.path.join(tempfile.gettempdir(), "undetected_chromedriver")
    ]
    
    cleaned_count = 0
    
    for cache_path in cache_paths:
        if os.path.exists(cache_path):
            try:
                print(f"🗑️ 정리 중: {cache_path}")
                shutil.rmtree(cache_path, ignore_errors=True)
                cleaned_count += 1
                print(f"✅ 정리 완료: {cache_path}")
            except Exception as e:
                print(f"⚠️ 정리 실패: {cache_path} - {e}")
    
    print(f"🎉 캐시 정리 완료: {cleaned_count}개 경로")

def force_kill_chrome_processes():
    """모든 Chrome 프로세스 강제 종료"""
    print("🚨 Chrome 프로세스 강제 종료...")
    
    try:
        # Windows
        subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'], 
                      capture_output=True, text=True)
        subprocess.run(['taskkill', '/f', '/im', 'chromedriver.exe'], 
                      capture_output=True, text=True)
        print("✅ Windows Chrome 프로세스 종료 완료")
    except:
        try:
            # Linux/Mac
            subprocess.run(['pkill', '-f', 'chrome'], 
                          capture_output=True, text=True)
            subprocess.run(['pkill', '-f', 'chromedriver'], 
                          capture_output=True, text=True)
            print("✅ Linux/Mac Chrome 프로세스 종료 완료")
        except:
            print("⚠️ Chrome 프로세스 종료 실패 (이미 종료되었을 수 있음)")

def cleanup_temp_chrome_profiles():
    """임시 Chrome 프로필 정리"""
    print("🗂️ 임시 Chrome 프로필 정리...")
    
    temp_dir = tempfile.gettempdir()
    cleaned_count = 0
    
    try:
        for item in os.listdir(temp_dir):
            if item.startswith(('chrome_', 'scoped_dir')):
                item_path = os.path.join(temp_dir, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
                        cleaned_count += 1
                        print(f"🗑️ 정리: {item}")
                except:
                    pass
        
        print(f"✅ 임시 프로필 정리 완료: {cleaned_count}개")
    except Exception as e:
        print(f"⚠️ 임시 프로필 정리 실패: {e}")

def main():
    """메인 정리 함수"""
    print("=" * 50)
    print("🔧 ChromeDriver 완전 정리 시스템")
    print("=" * 50)
    
    # 1. Chrome 프로세스 강제 종료
    force_kill_chrome_processes()
    
    # 2. undetected_chromedriver 캐시 정리
    cleanup_undetected_chrome_cache()
    
    # 3. 임시 Chrome 프로필 정리
    cleanup_temp_chrome_profiles()
    
    print("=" * 50)
    print("🎉 ChromeDriver 정리 완료!")
    print("이제 Valid4.py를 다시 실행해보세요.")
    print("=" * 50)

if __name__ == "__main__":
    main()
