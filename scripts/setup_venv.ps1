$ErrorActionPreference = 'Stop'

Set-Location $PSScriptRoot\..

if (!(Test-Path .\.venv)) {
  python -m venv --system-site-packages .venv
}

$python = ".\\.venv\\Scripts\\python.exe"


& $python -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
  Write-Warning "pip upgrade failed (often SSL/cert or proxy). Continuing with existing pip."
}

& $python -m pip install wheel
if ($LASTEXITCODE -ne 0) {
  throw "Failed to install 'wheel'. If you see SSL/cert errors, either configure your corporate proxy/trust store (or set PIP_INDEX_URL to your internal PyPI mirror), or delete .venv and re-run this script to recreate it with --system-site-packages."
}

& $python -m pip install -e . --no-build-isolation
if ($LASTEXITCODE -ne 0) {
  throw "Editable install failed. If you see SSL/cert errors, configure your corporate proxy/trust store or set PIP_INDEX_URL to your internal PyPI mirror."
}

Write-Host ""
Write-Host "Venv ready. Activate with: .\.venv\Scripts\Activate.ps1"
Write-Host "Run backend with:  python -m flask_app"
Write-Host "Run frontend with: streamlit run streamlit_app.py"
