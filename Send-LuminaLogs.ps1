<#
.SYNOPSIS
  Send randomized log entries to a Lumina ingestion endpoint.

.DESCRIPTION
  Script-style cmdlet that sends a specified number of randomized log entries
  to `/v1/logs` (single) or `/v1/logs/batch` (batch). Attributes, traces and
  duration fields are randomly populated to provide varied test inputs.

.EXAMPLE
  # Send 300 entries in batches of 100 to localhost
  .\Send-LuminaLogs.ps1 -BaseUrl 'http://localhost:5000' -Count 300 -Batch -BatchSize 100 -IncludeTrace

  # Send 50 single-entry requests
  .\Send-LuminaLogs.ps1 -BaseUrl 'http://localhost:5000' -Count 50 -Batch:$false
#>

param(
  [Parameter(Mandatory=$false)] [string]$BaseUrl = 'http://localhost:5000',
  [Parameter(Mandatory=$false)] [int]$Count = 300,
  [Parameter(Mandatory=$false)] [string]$Stream = 'test-stream',
  [Parameter(Mandatory=$false)] [int]$MaxAttributes = 6,
  [Parameter(Mandatory=$false)] [int]$MaxMessageWords = 12,
  [Parameter(Mandatory=$false)] [switch]$Batch,
  [Parameter(Mandatory=$false)] [int]$BatchSize = 100,
  [Parameter(Mandatory=$false)] [switch]$IncludeTrace,
  [Parameter(Mandatory=$false)] [int]$MaxDurationMs = 2000
)

$levels = @('debug','info','warn','error')
$words = @('alpha','bravo','charlie','delta','echo','foxtrot','golf','hotel','india','juliet','kilo','lima','mike','november','oscar','papa','quebec','romeo','sierra','tango','uniform','victor','whiskey','xray','yankee','zulu')
$firstNames = @('alex','sam','chris','taylor','jordan','casey','pat','jamie','morgan','drew')
$lastNames = @('smith','johnson','williams','brown','jones','miller','davis','garcia','rodriguez','wilson')

$entries = for ($i = 1; $i -le $Count; $i++) {
  $level = $levels[(Get-Random -Maximum $levels.Length)]
  $wordCount = Get-Random -Minimum 3 -Maximum ($MaxMessageWords + 1)
  $message = (1..$wordCount | ForEach-Object { $words[(Get-Random -Maximum $words.Length)] }) -join ' '
  $message = "msg #$i - $message - $([guid]::NewGuid())"

  $attrCount = Get-Random -Minimum 0 -Maximum ($MaxAttributes + 1)
  $attributes = @{}
  for ($a = 1; $a -le $attrCount; $a++) {
    $key = "attr$a"
    switch (Get-Random -Maximum 4) {
      0 { $val = Get-Random -Minimum 0 -Maximum 10000 }
      1 { $val = [math]::Round((Get-Random -Minimum 0 -Maximum 10000) / 100.0, 2) }
      2 { $val = ([int](Get-Random -Minimum 0 -Maximum 2)) -eq 0 }
      default { $val = [guid]::NewGuid().ToString().Substring(0,8) }
    }
    $attributes[$key] = $val
  }

  # Add fixed additional attributes
  $attributes['userId'] = Get-Random -Minimum 1 -Maximum 201
  $attributes['ip'] = "{0}.{1}.{2}.{3}" -f (Get-Random -Minimum 1 -Maximum 254), (Get-Random -Minimum 0 -Maximum 254), (Get-Random -Minimum 0 -Maximum 254), (Get-Random -Minimum 1 -Maximum 254)
  $attributes['userName'] = ( $firstNames[(Get-Random -Maximum $firstNames.Length)] + '.' + $lastNames[(Get-Random -Maximum $lastNames.Length)] + (Get-Random -Minimum 1 -Maximum 999) )
  $attributes['workstation'] = ( $env:COMPUTERNAME + '-' + (Get-Random -Minimum 1 -Maximum 99) )
  $attributes['sessionStarted'] = (Get-Date).AddMinutes(- (Get-Random -Minimum 0 -Maximum 10080)).ToString('o')
  $attributes['version'] = "{0}.{1}.{2}" -f (Get-Random -Minimum 0 -Maximum 4), (Get-Random -Minimum 0 -Maximum 10), (Get-Random -Minimum 0 -Maximum 100)

  $entry = @{
    stream = $Stream
    level = $level
    message = $message
    attributes = $attributes
  }

  if ($IncludeTrace) {
    if ((Get-Random -Maximum 4) -eq 0) {
      $entry.traceId = [guid]::NewGuid().ToString()
      $entry.spanId = [guid]::NewGuid().ToString().Substring(0,8)
      $entry.durationMs = Get-Random -Minimum 1 -Maximum $MaxDurationMs
    } elseif ((Get-Random -Maximum 3) -eq 0) {
      $entry.traceId = [guid]::NewGuid().ToString()
    }
  }

  $entry
}

if ($Batch) {
  $arr = [System.Collections.ArrayList]@($entries)
  $total = $arr.Count
  for ($start = 0; $start -lt $total; $start += $BatchSize) {
    $end = [math]::Min($start + $BatchSize - 1, $total - 1)
    $chunk = $arr[$start..$end]
    $payload = @{ stream = $Stream; entries = $chunk }
    $json = $payload | ConvertTo-Json -Depth 10
    Invoke-RestMethod -Uri "$BaseUrl/v1/logs/batch" -Method Post -ContentType 'application/json' -Body $json
    Start-Sleep -Milliseconds (Get-Random -Minimum 50 -Maximum 250)
  }
} else {
  foreach ($e in $entries) {
    $json = $e | ConvertTo-Json -Depth 10
    Invoke-RestMethod -Uri "$BaseUrl/v1/logs" -Method Post -ContentType 'application/json' -Body $json
    Start-Sleep -Milliseconds (Get-Random -Minimum 20 -Maximum 150)
  }
}
