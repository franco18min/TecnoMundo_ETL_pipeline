# Script PowerShell para empaquetar Lambdas en Windows
# Solución simple sin compilar pandas

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "🚀 Empaquetador de Lambdas para Windows" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

$baseDir = "c:\Users\tc\Desktop\TecnoMundo_ETL_pipeline\lambda_functions"
Set-Location $baseDir

# Lambda Bronze (sin dependencias, usaremos Layer)
Write-Host "🔧 Empaquetando Lambda Bronze..." -ForegroundColor Yellow
Set-Location "$baseDir\bronze_ingestion"
if (Test-Path "package") { Remove-Item -Recurse -Force package }
if (Test-Path "function.zip") { Remove-Item -Force function.zip }
New-Item -ItemType Directory -Force -Path package | Out-Null
Copy-Item lambda_function.py package\
Compress-Archive -Path package\* -DestinationPath function.zip -Force
$size = (Get-Item function.zip).Length / 1KB
Write-Host "✅ Bronze empaquetada: $([math]::Round($size, 1)) KB" -ForegroundColor Green
Write-Host ""

# Lambda Silver
Write-Host "🔧 Empaquetando Lambda Silver..." -ForegroundColor Yellow
Set-Location "$baseDir\silver_transformation"
if (Test-Path "package") { Remove-Item -Recurse -Force package }
if (Test-Path "function.zip") { Remove-Item -Force function.zip }
New-Item -ItemType Directory -Force -Path package | Out-Null
Write-Host "📦 Instalando dependencias..." -ForegroundColor Gray
pip install boto3==1.34.0 python-dateutil==2.8.2 -t package\ --quiet
Copy-Item lambda_function.py package\
Compress-Archive -Path package\* -DestinationPath function.zip -Force
$size = (Get-Item function.zip).Length / 1MB
Write-Host "✅ Silver empaquetada: $([math]::Round($size, 1)) MB" -ForegroundColor Green
Write-Host ""

# Lambda Gold
Write-Host "🔧 Empaquetando Lambda Gold..." -ForegroundColor Yellow
Set-Location "$baseDir\gold_enrichment"
if (Test-Path "package") { Remove-Item -Recurse -Force package }
if (Test-Path "function.zip") { Remove-Item -Force function.zip }
New-Item -ItemType Directory -Force -Path package | Out-Null
Write-Host "📦 Instalando dependencias..." -ForegroundColor Gray
pip install boto3==1.34.0 -t package\ --quiet
Copy-Item lambda_function.py package\
Compress-Archive -Path package\* -DestinationPath function.zip -Force
$size = (Get-Item function.zip).Length / 1MB
Write-Host "✅ Gold empaquetada: $([math]::Round($size, 1)) MB" -ForegroundColor Green
Write-Host ""

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "✅ Empaquetado completado" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Archivos creados:" -ForegroundColor White
Write-Host "  - bronze_ingestion\function.zip" -ForegroundColor Gray
Write-Host "  - silver_transformation\function.zip" -ForegroundColor Gray
Write-Host "  - gold_enrichment\function.zip" -ForegroundColor Gray
Write-Host ""
Write-Host "⚠️  IMPORTANTE: Lambda Bronze necesita Layer 'AWSSDKPandas-Python311'" -ForegroundColor Yellow
Write-Host "    (Se agrega en la consola de AWS después de crear la función)" -ForegroundColor Yellow

Set-Location $baseDir
