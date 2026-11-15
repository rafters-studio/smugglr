# Temporary: drives install.ps1 through the issue's cases. Removed before review.
$ErrorActionPreference = 'Stop'
$script = Get-Content -Raw (Join-Path $PSScriptRoot '..\install.ps1')
$bin = Join-Path $HOME '.local\bin'
$exe = Join-Path $bin 'smugglr.exe'
function Run-Case([string] $name, [scriptblock] $setup) {
  Write-Host "== $name"
  $saved = @{ v = $env:SMUGGLR_VERSION; a = $env:PROCESSOR_ARCHITECTURE; p = $env:Path }
  & $setup
  $out = ''; $err = $null
  try { $out = Invoke-Expression $script *>&1 | Out-String } catch { $err = $_.Exception.Message }
  $env:SMUGGLR_VERSION = $saved.v; $env:PROCESSOR_ARCHITECTURE = $saved.a; $env:Path = $saved.p
  Write-Host $out
  if ($err) { Write-Host "THREW: $err" }
  $files = if (Test-Path $bin) { (Get-ChildItem -Force $bin | ForEach-Object Name) -join ',' } else { '<no dir>' }
  Write-Host "files in .local\bin: $files"
  return @{ out = "$out"; err = $err; files = $files }
}
function Check($cond, $what) { if (-not $cond) { throw "FAIL: $what" } else { Write-Host "ok: $what" } }

Write-Host "PowerShell $($PSVersionTable.PSVersion)"
if (Test-Path "$HOME\.local") { Remove-Item -Recurse -Force "$HOME\.local" }

$r = Run-Case 'latest' { }
Check (-not $r.err) 'latest installs'
Check ($r.files -eq 'smugglr.exe') 'only smugglr.exe written'
Check ($r.out -match 'smugglr-x86_64-pc-windows-msvc.zip') 'names the archive'
Check ($r.out -like "*$bin is not on your PATH*") 'PATH advice'
Check ($r.out -like "*SetEnvironmentVariable('Path'*") 'PATH command'
& $exe --version; Check ($LASTEXITCODE -eq 0) 'smugglr --version'

$r = Run-Case 'latest-again-on-path' { $env:Path = "$bin;$env:Path" }
Check (-not $r.err) 'reinstall over an existing install'
Check ($r.files -eq 'smugglr.exe') 'only smugglr.exe after replace'
Check ($r.out -notlike '*not on your PATH*') 'no PATH advice when on PATH'

$r = Run-Case 'pinned-before-version-flag' { $env:SMUGGLR_VERSION = 'v0.6.1' }
Check ($r.err -like "*did not run 'smugglr --version'; nothing was installed*") 'v0.6.1 binary has no --version: refused'
Check ($r.files -eq 'smugglr.exe') 'no staged file left behind'
$v = & $exe --version; Write-Host $v
Check ($v -eq 'smugglr 0.6.4') 'earlier v0.6.4 install untouched by the failed verify'

$r = Run-Case 'pinned-no-v' { $env:SMUGGLR_VERSION = '0.6.3' }
Check (-not $r.err) 'pinned installs'
Check ($r.out -like '*smugglr v0.6.3*') 'pinned without v reports v0.6.3'
$v = & $exe --version; Write-Host $v
Check ($v -eq 'smugglr 0.6.3') 'installed binary is 0.6.3'

Set-Content -LiteralPath $exe -Value 'earlier install'
$r = Run-Case 'no-archive' { $env:SMUGGLR_VERSION = 'v0.5.0' }
Check ($r.err -like '*x86_64-pc-windows-msvc*' -and $r.err -like '*https://github.com/*') 'refusal names target and URL'
Check ((Get-Content -Raw $exe).Trim() -eq 'earlier install') 'earlier install untouched'
Check ($r.files -eq 'smugglr.exe') 'nothing else written'

Remove-Item -Recurse -Force "$HOME\.local"
$r = Run-Case 'arm64' { $env:PROCESSOR_ARCHITECTURE = 'ARM64' }
Check ($r.err -like '*unsupported platform Windows ARM64*') 'refuses ARM64 naming the platform'
Check ($r.files -eq '<no dir>') 'arm64 installs nothing'

Write-Host 'install.ps1: all cases passed'
