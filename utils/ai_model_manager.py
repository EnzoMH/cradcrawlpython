#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 모델 관리 클래스 - 다중 Gemini API 키 지원
"""

import os
import logging
import google.generativeai as genai
from typing import Dict, List, Optional

# AI 모델 설정
AI_MODEL_CONFIG = {
    "temperature": 0.1,
    "top_p": 0.8,
    "top_k": 40,
    "max_output_tokens": 2048,
}

class AIModelManager:
    """AI 모델 관리 클래스 - 4개의 Gemini API 키 지원"""
    
    def __init__(self, logger=None):
        """
        AI 모델 관리자 초기화
        
        Args:
            logger: 로깅 객체 (기본값: None)
        """
        self.logger = logger or logging.getLogger(__name__)
        self.gemini_models = []
        self.gemini_config = AI_MODEL_CONFIG
        self.current_model_index = 0
        self.setup_models()
    
    def setup_models(self):
        """4개의 AI 모델 초기화"""
        try:
            # API 키들 가져오기
            api_keys = {
                'GEMINI_1': os.getenv('GEMINI_API_KEY_1'),
                'GEMINI_2': os.getenv('GEMINI_API_KEY_2'),
                'GEMINI_3': os.getenv('GEMINI_API_KEY_3'),
                'GEMINI_4': os.getenv('GEMINI_API_KEY_4')
            }
            
            # 최소 하나의 API 키는 있어야 함
            valid_keys = {k: v for k, v in api_keys.items() if v}
            if not valid_keys:
                raise ValueError("GEMINI_API_KEY_1, GEMINI_API_KEY_2, GEMINI_API_KEY_3, 또는 GEMINI_API_KEY_4 환경 변수 중 최소 하나는 설정되어야 합니다.")
            
            # 각 API 키에 대해 모델 초기화
            for model_name, api_key in valid_keys.items():
                try:
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel(
                        "gemini-2.0-flash-lite-001",
                        generation_config=self.gemini_config
                    )
                    
                    self.gemini_models.append({
                        'model': model,
                        'api_key': api_key[:10] + "...",
                        'name': model_name,
                        'failures': 0
                    })
                    
                    self.logger.info(f"🤖 {model_name} 모델 초기화 성공")
                    
                except Exception as e:
                    self.logger.error(f"❌ {model_name} 모델 초기화 실패: {e}")
                    continue
            
            if not self.gemini_models:
                raise ValueError("사용 가능한 Gemini 모델이 없습니다.")
            
            self.logger.info(f"🎉 총 {len(self.gemini_models)}개의 Gemini 모델 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"❌ AI 모델 초기화 실패: {e}")
            raise
    
    def get_next_model(self) -> Optional[Dict]:
        """다음 사용 가능한 모델 선택 (라운드 로빈 방식)"""
        if not self.gemini_models:
            return None
        
        # 실패 횟수가 적은 모델 우선 선택
        available_models = [m for m in self.gemini_models if m['failures'] < 3]
        if not available_models:
            # 모든 모델이 실패한 경우 실패 횟수 리셋
            for model in self.gemini_models:
                model['failures'] = 0
            available_models = self.gemini_models
        
        # 라운드 로빈 방식으로 선택
        model = available_models[self.current_model_index % len(available_models)]
        self.current_model_index = (self.current_model_index + 1) % len(available_models)
        
        return model
    
    def extract_with_gemini(self, text_content: str, prompt_template: str) -> str:
        """
        Gemini API를 통한 정보 추출 (다중 모델 지원)
        
        Args:
            text_content: 분석할 텍스트 내용
            prompt_template: 프롬프트 템플릿 ({text_content} 플레이스홀더 포함)
            
        Returns:
            str: AI 응답 결과
        """
        if not self.gemini_models:
            return "오류: 사용 가능한 모델이 없습니다."
        
        # 모든 모델을 시도해볼 수 있도록 최대 시도 횟수 설정
        max_attempts = len(self.gemini_models)
        
        for attempt in range(max_attempts):
            current_model = self.get_next_model()
            if not current_model:
                continue
            
            try:
                # 텍스트 길이 제한 (Gemini API 제한)
                max_length = 32000
                if len(text_content) > max_length:
                    front_portion = int(max_length * 0.67)
                    back_portion = max_length - front_portion
                    text_content = text_content[:front_portion] + "\n... (중략) ...\n" + text_content[-back_portion:]
                
                # 프롬프트 생성
                prompt = prompt_template.format(text_content=text_content)
                
                # 현재 모델로 API 호출
                response = current_model['model'].generate_content(prompt)
                result_text = response.text
                
                # 성공 시 로그 출력
                self.logger.info(f"✅ {current_model['name']} API 성공 - 응답 (일부): {result_text[:200]}...")
                
                return result_text
                
            except Exception as e:
                # 실패 시 다음 모델로 시도
                current_model['failures'] += 1
                self.logger.warning(f"⚠️ {current_model['name']} API 실패 (시도 {attempt + 1}/{max_attempts}): {str(e)}")
                
                if attempt < max_attempts - 1:
                    self.logger.info(f"🔄 다음 모델로 재시도 중...")
                    continue
                else:
                    self.logger.error(f"❌ 모든 Gemini 모델 실패")
                    return f"오류: 모든 API 호출 실패 - 마지막 오류: {str(e)}"
        
        return "오류: 모든 모델 시도 실패"
    
    def get_model_status(self) -> str:
        """모델 상태 정보 반환"""
        if not self.gemini_models:
            return "❌ 사용 가능한 모델 없음"
        
        status_info = []
        for model in self.gemini_models:
            status = "✅ 정상" if model['failures'] < 3 else "❌ 실패"
            status_info.append(f"{model['name']}: {status} (실패: {model['failures']}회)")
        
        return " | ".join(status_info)
    
    def reset_failures(self):
        """모든 모델의 실패 횟수 초기화"""
        for model in self.gemini_models:
            model['failures'] = 0
        self.logger.info("🔄 모든 모델의 실패 횟수 초기화 완료")
    
    def get_available_models_count(self) -> int:
        """사용 가능한 모델 수 반환"""
        return len([m for m in self.gemini_models if m['failures'] < 3])
    
    def is_available(self) -> bool:
        """사용 가능한 모델이 있는지 확인"""
        return self.get_available_models_count() > 0 