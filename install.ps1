# install.ps1: installs the smugglr binary from a GitHub release.
#
#   irm https://raw.githubusercontent.com/rafters-studio/smugglr/main/install.ps1 | iex
#   $env:SMUGGLR_VERSION = 'v0.6.3'; irm https://raw.githubusercontent.com/rafters-studio/smugglr/main/install.ps1 | iex
#
# Windows x86_64, in Windows PowerShell 5.1 and PowerShell 7. It writes one file,
# $HOME\.local\bin\smugglr.exe, and only after the downloaded binary has run from
# that directory.
#
# iex runs this in the caller's session, so everything lives in one function:
# its preferences and variables stay inside it, and a failure throws instead of
# calling exit, which would close the caller's window.

function Install-Smugglr {
  $ErrorActionPreference = 'Stop'
  $ProgressPreference = 'SilentlyContinue'

  $Repo = 'rafters-studio/smugglr'
  $InstallDir = Join-Path $HOME '.local\bin'

  function Write-Info([string] $Message) { Write-Host "smugglr install: $Message" }
  function Stop-Install([string] $Message) { throw "smugglr install: error: $Message" }

  # Windows PowerShell 5.1 may not offer TLS 1.2 by default; github.com requires it.
  [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

  # The Rust target triple the release names its archives by.
  $os = if ($env:OS -eq 'Windows_NT') { 'Windows' } else { [string] [Environment]::OSVersion.Platform }
  # A 32-bit PowerShell on 64-bit Windows reports x86 here and the real one in W6432.
  $arch = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
  if ($os -ne 'Windows' -or $arch -ne 'AMD64') {
    Stop-Install "unsupported platform $os $arch`: releases carry x86_64 for Windows"
  }
  $Target = 'x86_64-pc-windows-msvc'

  # The tag to install: SMUGGLR_VERSION, or the one the latest release redirects to.
  if ($env:SMUGGLR_VERSION) {
    $Version = if ($env:SMUGGLR_VERSION.StartsWith('v')) { $env:SMUGGLR_VERSION } else { "v$($env:SMUGGLR_VERSION)" }
  } else {
    $latestUrl = "https://github.com/$Repo/releases/latest"
    $latest = $null
    try {
      $response = Invoke-WebRequest -Uri $latestUrl -UseBasicParsing
      $base = $response.BaseResponse
      # Windows PowerShell 5.1 answers an HttpWebResponse, PowerShell 7 an HttpResponseMessage.
      if ($base.PSObject.Properties['ResponseUri']) {
        $latest = $base.ResponseUri.AbsoluteUri
      } else {
        $latest = $base.RequestMessage.RequestUri.AbsoluteUri
      }
    } catch {
      $latest = $null
    }
    if (-not $latest) { Stop-Install "could not reach github.com to find the latest release for $Target" }
    $Version = $latest.TrimEnd('/').Split('/')[-1]
    if ($Version -notmatch '^v[0-9]') {
      Stop-Install "could not read the latest release tag from $latest for $Target"
    }
  }

  $archive = "smugglr-$Target.zip"
  $url = "https://github.com/$Repo/releases/download/$Version/$archive"
  Write-Info "downloading $archive from release $Version"

  $workDir = Join-Path ([IO.Path]::GetTempPath()) ("smugglr-install-" + [Guid]::NewGuid().ToString('N'))
  $staged = $null
  try {
    New-Item -ItemType Directory -Path $workDir | Out-Null
    $archivePath = Join-Path $workDir $archive

    $status = 0
    try {
      Invoke-WebRequest -Uri $url -OutFile $archivePath -UseBasicParsing
      $status = 200
    } catch {
      $errorResponse = $_.Exception.Response
      if ($errorResponse) { $status = [int] $errorResponse.StatusCode }
    }
    switch ($status) {
      200 { }
      0 { Stop-Install "could not download $url for $Target" }
      404 { Stop-Install "release $Version has no archive for ${Target}: $url" }
      default { Stop-Install "download of $url for $Target answered HTTP $status" }
    }

    $unpacked = Join-Path $workDir 'unpacked'
    $unpackFailed = $false
    try {
      Expand-Archive -Path $archivePath -DestinationPath $unpacked
    } catch {
      $unpackFailed = $true
    }
    if ($unpackFailed) { Stop-Install "could not unpack $archive for $Target" }
    $exe = Join-Path $unpacked 'smugglr.exe'
    if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) {
      Stop-Install "$archive for $Target does not contain smugglr.exe"
    }

    # Staged beside its destination, so the swap below is a rename: the verify runs
    # where the binary will live, and a failure leaves any earlier install alone.
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    $staged = Join-Path $InstallDir ".smugglr.install.$PID.exe"
    Copy-Item -LiteralPath $exe -Destination $staged -Force
    $ran = $false
    try {
      & $staged --version *> $null
      $ran = ($LASTEXITCODE -eq 0)
    } catch {
      $ran = $false
    }
    if (-not $ran) {
      Stop-Install "the $Target binary from release $Version did not run 'smugglr --version'; nothing was installed"
    }
    $dest = Join-Path $InstallDir 'smugglr.exe'
    if (Test-Path -LiteralPath $dest) {
      [IO.File]::Replace($staged, $dest, [NullString]::Value)
    } else {
      [IO.File]::Move($staged, $dest)
    }
    $staged = $null

    Write-Info "installed smugglr $Version to $dest"

    $onPath = $false
    foreach ($entry in ($env:Path -split ';')) {
      if ($entry -and ($entry.TrimEnd('\') -ieq $InstallDir.TrimEnd('\'))) { $onPath = $true }
    }
    if (-not $onPath) {
      Write-Info "$InstallDir is not on your PATH. To add it:"
      Write-Info "  [Environment]::SetEnvironmentVariable('Path', '$InstallDir;' + [Environment]::GetEnvironmentVariable('Path', 'User'), 'User')"
      Write-Info "then open a new shell."
    }
  } finally {
    if ($staged -and (Test-Path -LiteralPath $staged)) { Remove-Item -LiteralPath $staged -Force }
    if (Test-Path -LiteralPath $workDir) { Remove-Item -LiteralPath $workDir -Recurse -Force }
  }
}

# Last, so a truncated download runs nothing.
Install-Smugglr
