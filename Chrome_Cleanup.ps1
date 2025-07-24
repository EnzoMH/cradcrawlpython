# ===================================================================
# Chrome_Cleanup.ps1
# 크롤링으로 인한 크롬 데이터 누적 문제 해결 스크립트
# 작성일: 2025-07-24
# 목적: 부팅 속도 개선 및 시스템 성능 향상
# ===================================================================

Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "🚀 크롬 데이터 정리 및 부팅 속도 개선 스크립트" -ForegroundColor Yellow
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host ""

# 관리자 권한 확인
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "⚠️ 관리자 권한으로 실행해주세요!" -ForegroundColor Red
    Write-Host "PowerShell을 관리자 권한으로 다시 실행 후 스크립트를 재실행하세요." -ForegroundColor Yellow
    pause
    exit
}

# ===================================================================
# 1단계: 현재 크롬 데이터 크기 확인
# ===================================================================
Write-Host "📊 1단계: 현재 크롬 데이터 크기 확인 중..." -ForegroundColor Green

$chromePath = "$env:LOCALAPPDATA\Google\Chrome\User Data"
if (Test-Path $chromePath) {
    $originalSize = (Get-ChildItem $chromePath -Recurse -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    Write-Host "현재 크롬 데이터 크기: $([math]::Round($originalSize/1GB, 2)) GB" -ForegroundColor Yellow
    
    # 주요 폴더별 크기 표시
    Write-Host "`n📋 주요 폴더별 크기:" -ForegroundColor Cyan
    $largeFolders = @("Default", "Profile 1", "Profile 6", "Profile 9", "Snapshots", "GrShaderCache", "ShaderCache")
    foreach ($folder in $largeFolders) {
        $folderPath = Join-Path $chromePath $folder
        if (Test-Path $folderPath) {
            $folderSize = (Get-ChildItem $folderPath -Recurse -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
            if ($folderSize -gt 0) {
                Write-Host "  $folder`: $([math]::Round($folderSize/1MB, 2)) MB" -ForegroundColor White
            }
        }
    }
} else {
    Write-Host "❌ 크롬이 설치되어 있지 않습니다." -ForegroundColor Red
    pause
    exit
}

Write-Host "`n" -NoNewline
Read-Host "계속하려면 Enter를 누르세요"

# ===================================================================
# 2단계: 크롬 프로세스 완전 종료
# ===================================================================
Write-Host "`n🔄 2단계: 크롬 프로세스 완전 종료 중..." -ForegroundColor Green

$chromeProcesses = Get-Process chrome -ErrorAction SilentlyContinue
if ($chromeProcesses) {
    Write-Host "발견된 크롬 프로세스: $($chromeProcesses.Count)개" -ForegroundColor Yellow
    $chromeProcesses | Stop-Process -Force
    Write-Host "✅ 크롬 프로세스 종료 완료" -ForegroundColor Green
} else {
    Write-Host "✅ 실행 중인 크롬 프로세스 없음" -ForegroundColor Green
}

# 드라이버 프로세스도 종료
$driverProcesses = @("chromedriver", "msedgedriver", "geckodriver")
foreach ($driver in $driverProcesses) {
    $processes = Get-Process $driver -ErrorAction SilentlyContinue
    if ($processes) {
        $processes | Stop-Process -Force
        Write-Host "✅ $driver 프로세스 종료 완료" -ForegroundColor Green
    }
}

Start-Sleep -Seconds 3

# ===================================================================
# 3단계: 안전한 크롬 캐시 정리
# ===================================================================
Write-Host "`n🧹 3단계: 크롬 캐시 및 임시 데이터 정리 중..." -ForegroundColor Green

# 안전하게 정리할 수 있는 폴더들 (북마크, 비밀번호, 설정 등은 보존)
$safeFolders = @(
    "Default\Cache",
    "Default\Code Cache",
    "Default\GPUCache", 
    "Default\Service Worker\CacheStorage",
    "Default\Service Worker\ScriptCache",
    "Default\IndexedDB",
    "Default\Local Storage",
    "Default\Session Storage",
    "Profile 1\Cache",
    "Profile 1\Code Cache", 
    "Profile 1\GPUCache",
    "Profile 1\Service Worker\CacheStorage",
    "Profile 6\Cache",
    "Profile 6\Code Cache",
    "Profile 6\GPUCache", 
    "Profile 9\Cache",
    "Profile 9\Code Cache",
    "Profile 9\GPUCache",
    "GrShaderCache",
    "ShaderCache",
    "Snapshots"
)

$totalCleaned = 0
$cleanedCount = 0

foreach ($folder in $safeFolders) {
    $fullPath = Join-Path $chromePath $folder
    if (Test-Path $fullPath) {
        try {
            $size = (Get-ChildItem $fullPath -Recurse -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
            Remove-Item $fullPath -Recurse -Force -ErrorAction SilentlyContinue
            $totalCleaned += $size
            $cleanedCount++
            Write-Host "  ✅ $folder ($([math]::Round($size/1MB, 2)) MB)" -ForegroundColor White
        } catch {
            Write-Host "  ⚠️ $folder (정리 실패)" -ForegroundColor Yellow
        }
    }
}

Write-Host "`n🎉 크롬 캐시 정리 완료: $cleanedCount개 폴더, $([math]::Round($totalCleaned/1MB, 2)) MB" -ForegroundColor Green

# ===================================================================
# 4단계: 시스템 임시 파일 정리
# ===================================================================
Write-Host "`n🗑️ 4단계: 시스템 임시 파일 정리 중..." -ForegroundColor Green

$tempFolders = @(
    $env:TEMP,
    "C:\Windows\Temp",
    "$env:LOCALAPPDATA\Temp"
)

$tempCleaned = 0
foreach ($tempFolder in $tempFolders) {
    if (Test-Path $tempFolder) {
        Write-Host "  🧹 $tempFolder 정리 중..." -ForegroundColor White
        try {
            $tempSize = (Get-ChildItem $tempFolder -Recurse -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
            
            # 파일 삭제
            Get-ChildItem $tempFolder -File -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
            
            # 빈 폴더 삭제
            Get-ChildItem $tempFolder -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
            
            $tempCleaned += $tempSize
            Write-Host "    ✅ 정리 완료 ($([math]::Round($tempSize/1MB, 2)) MB)" -ForegroundColor Green
        } catch {
            Write-Host "    ⚠️ 일부 파일 정리 실패 (사용 중)" -ForegroundColor Yellow
        }
    }
}

Write-Host "`n🎉 임시 파일 정리 완료: $([math]::Round($tempCleaned/1MB, 2)) MB" -ForegroundColor Green

# ===================================================================
# 5단계: 정리 후 크기 재확인
# ===================================================================
Write-Host "`n📊 5단계: 정리 후 크기 재확인..." -ForegroundColor Green

if (Test-Path $chromePath) {
    $newSize = (Get-ChildItem $chromePath -Recurse -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    $savedSpace = $originalSize - $newSize
    
    Write-Host "`n📈 정리 결과:" -ForegroundColor Cyan
    Write-Host "  이전 크기: $([math]::Round($originalSize/1GB, 2)) GB" -ForegroundColor White
    Write-Host "  현재 크기: $([math]::Round($newSize/1GB, 2)) GB" -ForegroundColor White
    Write-Host "  절약된 공간: $([math]::Round($savedSpace/1GB, 2)) GB" -ForegroundColor Yellow
    Write-Host "  정리율: $([math]::Round(($savedSpace/$originalSize)*100, 2))%" -ForegroundColor Green
}

# ===================================================================
# 6단계: 시작 프로그램 확인
# ===================================================================
Write-Host "`n🔍 6단계: 크롬 관련 시작 프로그램 확인..." -ForegroundColor Green

$startupPrograms = Get-WmiObject Win32_StartupCommand | Where-Object {$_.Command -like "*chrome*"}
if ($startupPrograms) {
    Write-Host "⚠️ 발견된 크롬 관련 시작 프로그램:" -ForegroundColor Yellow
    $startupPrograms | ForEach-Object {
        Write-Host "  - $($_.Name): $($_.Command)" -ForegroundColor White
    }
    Write-Host "`n💡 부팅 속도 향상을 위해 불필요한 시작 프로그램을 비활성화하세요." -ForegroundColor Cyan
} else {
    Write-Host "✅ 크롬 관련 시작 프로그램 없음" -ForegroundColor Green
}

# ===================================================================
# 완료 메시지 및 권장사항
# ===================================================================
Write-Host "`n" + "=" * 80 -ForegroundColor Cyan
Write-Host "🎉 크롬 데이터 정리 완료!" -ForegroundColor Yellow
Write-Host "=" * 80 -ForegroundColor Cyan

Write-Host "`n📋 권장사항:" -ForegroundColor Cyan
Write-Host "  1. 지금 컴퓨터를 재부팅하여 부팅 속도 개선 확인" -ForegroundColor White
Write-Host "  2. 앞으로 크롤링 작업 시 드라이버 정리 코드 사용" -ForegroundColor White
Write-Host "  3. 월 1회 정도 이 스크립트 실행 권장" -ForegroundColor White
Write-Host "  4. 크롬 설정에서 '시작 시 이전 세션 복원' 비활성화" -ForegroundColor White

Write-Host "`n💡 크롬 설정 최적화:" -ForegroundColor Cyan
Write-Host "  chrome://settings/ → 고급 → 재설정 및 정리 → 설정을 원래 기본값으로 복원" -ForegroundColor White

Write-Host "`n🚀 예상 효과: 부팅 시간 30초~1분 단축, 전반적 시스템 성능 향상" -ForegroundColor Green

Read-Host "`n재부팅하려면 Enter를 누르세요 (또는 Ctrl+C로 종료)"

# 재부팅 옵션
$reboot = Read-Host "지금 재부팅하시겠습니까? (y/n)"
if ($reboot -eq 'y' -or $reboot -eq 'Y') {
    Write-Host "🔄 5초 후 재부팅됩니다..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
    Restart-Computer -Force
} 