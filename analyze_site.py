#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import json
import re

def analyze_auction_site():
    """auction1.co.kr 주민센터 정보 사이트 분석"""
    
    # 동대문구 주민센터 정보 사이트 테스트
    url = 'https://www.ddm.go.kr/eng/contents.do?key=1020'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    }
    
    try:
        print(f"🔍 사이트 분석 시작: {url}")
        # SSL 검증 비활성화로 인증서 문제 해결
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        print(f"✅ Status Code: {response.status_code}")
        print(f"📄 Content Length: {len(response.text)}")
        
        # BeautifulSoup으로 HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n" + "="*80)
        print("📋 HTML 구조 분석")
        print("="*80)
        
        # 페이지 제목
        title = soup.find('title')
        if title:
            print(f"📌 페이지 제목: {title.get_text().strip()}")
        
        # 테이블 찾기
        tables = soup.find_all('table')
        print(f"📊 테이블 개수: {len(tables)}")
        
        for i, table in enumerate(tables):
            print(f"\n--- 테이블 {i+1} ---")
            rows = table.find_all('tr')
            print(f"행 개수: {len(rows)}")
            
            # 첫 번째 몇 행만 보기
            for j, row in enumerate(rows[:3]):
                cells = row.find_all(['td', 'th'])
                cell_texts = [cell.get_text().strip() for cell in cells]
                print(f"  행 {j+1}: {cell_texts}")
        
        # JavaScript 데이터 찾기
        scripts = soup.find_all('script')
        print(f"\n🔧 스크립트 태그 개수: {len(scripts)}")
        
        for i, script in enumerate(scripts):
            if script.string and len(script.string.strip()) > 50:
                print(f"\n--- 스크립트 {i+1} (일부) ---")
                script_text = script.string.strip()
                print(script_text[:200] + "..." if len(script_text) > 200 else script_text)
        
        # 전화번호 패턴 찾기
        phone_pattern = r'(\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4})'
        phones = re.findall(phone_pattern, response.text)
        print(f"\n📞 발견된 전화번호 패턴: {len(phones)}개")
        if phones:
            for phone in phones[:5]:  # 처음 5개만 표시
                print(f"  - {phone}")
        
        # 기관명 패턴 찾기
        institution_patterns = [
            r'([가-힣]+(?:동|읍|면|리))\s*(?:주민센터|행정복지센터|동사무소)',
            r'([가-힣]+(?:구|시|군))\s*청',
            r'([가-힣]+(?:구|시|군))\s*(?:구청|시청|군청)',
        ]
        
        print(f"\n🏢 발견된 기관명 패턴:")
        all_institutions = []
        for pattern in institution_patterns:
            institutions = re.findall(pattern, response.text)
            all_institutions.extend(institutions)
            if institutions:
                print(f"  패턴 '{pattern}': {len(institutions)}개")
                for inst in institutions[:3]:
                    print(f"    - {inst}")
        
        print(f"\n📝 총 기관명: {len(set(all_institutions))}개 (중복 제거)")
        
        # 원본 HTML 일부 저장
        print("\n" + "="*80)
        print("📄 HTML 소스 (처음 2000자)")
        print("="*80)
        print(response.text[:2000])
        
        print("\n" + "="*80)
        print("📄 HTML 소스 (마지막 1000자)")
        print("="*80)
        print(response.text[-1000:])
        
        return {
            'status_code': response.status_code,
            'content_length': len(response.text),
            'tables_count': len(tables),
            'scripts_count': len(scripts),
            'phones_found': len(phones),
            'institutions_found': len(set(all_institutions)),
            'html_content': response.text
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    result = analyze_auction_site()
    if result:
        print(f"\n🎯 분석 완료!")
        print(f"  - 상태 코드: {result['status_code']}")
        print(f"  - 콘텐츠 길이: {result['content_length']:,} 바이트")
        print(f"  - 테이블 수: {result['tables_count']}")
        print(f"  - 스크립트 수: {result['scripts_count']}")
        print(f"  - 전화번호 패턴: {result['phones_found']}개")
        print(f"  - 기관명 패턴: {result['institutions_found']}개") 