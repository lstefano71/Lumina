<#
.SYNOPSIS
    Interactive SQL REPL for Lumina query/sql endpoints.

.DESCRIPTION
    Lightweight PowerShell REPL to execute SQL against Lumina and quickly inspect
    metadata such as rewritten SQL (when debug is enabled), columns, execution time,
    and a preview of returned rows.

    Supported modes:
      - sql-post          -> POST /v1/query/sql (text/plain)
      - sql-get           -> GET /v1/query/sql?q=...
      - sql-parameterized -> POST /v1/query/sql/parameterized (application/json)

.PARAMETER BaseUrl
    Lumina base URL. Default: http://localhost:5000

.PARAMETER DebugEnabled
    Initial debug flag state. Always sent as debug=true/false.

.PARAMETER Mode
    Initial query mode. One of: sql-post, sql-get, sql-parameterized.

.PARAMETER Rows
    Number of rows to preview on successful queries. Default: 10.

.PARAMETER Raw
    Show full raw JSON payload in addition to summarized output.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$BaseUrl = "http://localhost:5000",

    [Parameter(Mandatory = $false)]
    [switch]$DebugEnabled,

    [Parameter(Mandatory = $false)]
    [ValidateSet("sql-post", "sql-get", "sql-parameterized")]
    [string]$Mode = "sql-post",

    [Parameter(Mandatory = $false)]
    [ValidateRange(1, 1000)]
    [int]$Rows = 10,

    [Parameter(Mandatory = $false)]
    [switch]$Raw
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Normalize-BaseUrl {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )

    $normalized = $Url.TrimEnd('/')
    $null = [Uri]::new($normalized)
    return $normalized
}

function ConvertTo-DisplayJson {
    param(
        [Parameter(Mandatory = $false)]
        [object]$Value,

        [Parameter(Mandatory = $false)]
        [int]$Depth = 20
    )

    if ($null -eq $Value) {
        return "null"
    }

    return ($Value | ConvertTo-Json -Depth $Depth)
}

function Get-ResponseRows {
    param([Parameter(Mandatory = $true)][object]$Response)

    if ($Response.PSObject.Properties.Name -contains 'rows') {
        return @($Response.rows)
    }

    if ($Response.PSObject.Properties.Name -contains 'Rows') {
        return @($Response.Rows)
    }

    return @()
}

function Get-ResponseValue {
    param(
        [Parameter(Mandatory = $true)][object]$Response,
        [Parameter(Mandatory = $true)][string]$CamelName,
        [Parameter(Mandatory = $true)][string]$PascalName,
        [Parameter(Mandatory = $false)]$Default = $null
    )

    if ($Response.PSObject.Properties.Name -contains $CamelName) {
        return $Response.$CamelName
    }

    if ($Response.PSObject.Properties.Name -contains $PascalName) {
        return $Response.$PascalName
    }

    return $Default
}

function Write-ReplHelp {
    Write-Host "Lumina SQL REPL commands:" -ForegroundColor Cyan
    Write-Host "  .help                         Show this help"
    Write-Host "  .status                       Show current REPL settings"
    Write-Host "  .base <url>                   Set base URL"
    Write-Host "  .mode <sql-post|sql-get|sql-parameterized>"
    Write-Host "                               Set endpoint mode"
    Write-Host "  .debug <on|off>               Toggle debug query flag"
    Write-Host "  .rows <n>                     Set preview row count"
    Write-Host "  .meta <on|off>                Toggle metadata output"
    Write-Host "  .raw <on|off>                 Toggle raw JSON output"
    Write-Host "  .sql <query>                  Execute a single-line SQL query"
    Write-Host "  .begin                        Enter multiline SQL mode"
    Write-Host "  .end                          Execute multiline SQL and exit multiline mode"
    Write-Host "  .abort                        Exit multiline SQL mode without executing"
    Write-Host "  .clear                        Clear buffered SQL text"
    Write-Host "  .param set <name> <jsonValue> Set parameter (parameterized mode)"
    Write-Host "  .param remove <name>          Remove parameter"
    Write-Host "  .param clear                  Clear all parameters"
    Write-Host "  .param list                   Show current parameters"
    Write-Host "  .quit                         Exit REPL"
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor DarkCyan
    Write-Host "  - Type SQL directly to execute immediately"
    Write-Host "  - Use .begin to start multiline input, then .end to execute"
    Write-Host "  - You can still execute immediately with .sql SELECT * FROM 'test-stream' LIMIT 10"
}

function Write-ReplStatus {
    param(
        [Parameter(Mandatory = $true)][string]$CurrentBaseUrl,
        [Parameter(Mandatory = $true)][string]$CurrentMode,
        [Parameter(Mandatory = $true)][bool]$CurrentDebug,
        [Parameter(Mandatory = $true)][int]$CurrentRows,
        [Parameter(Mandatory = $true)][bool]$CurrentMeta,
        [Parameter(Mandatory = $true)][bool]$CurrentRaw,
        [Parameter(Mandatory = $true)][bool]$IsMultilineMode,
        [Parameter(Mandatory = $true)][hashtable]$CurrentParams,
        [Parameter(Mandatory = $false)][string[]]$CurrentBuffer = @()
    )

    Write-Host "REPL status:" -ForegroundColor Cyan
    Write-Host "  Base URL:  $CurrentBaseUrl" -ForegroundColor Gray
    Write-Host "  Mode:      $CurrentMode" -ForegroundColor Gray
    Write-Host "  Debug:     $CurrentDebug" -ForegroundColor Gray
    Write-Host "  Rows:      $CurrentRows" -ForegroundColor Gray
    Write-Host "  Meta:      $CurrentMeta" -ForegroundColor Gray
    Write-Host "  Raw:       $CurrentRaw" -ForegroundColor Gray
    Write-Host "  Multiline: $IsMultilineMode" -ForegroundColor Gray
    Write-Host "  Params:    $($CurrentParams.Count)" -ForegroundColor Gray
    Write-Host "  Buffer:    $($CurrentBuffer.Count) line(s)" -ForegroundColor Gray
}

function Write-QuerySuccess {
    param(
        [Parameter(Mandatory = $true)][object]$Response,
        [Parameter(Mandatory = $true)][int]$PreviewRows,
        [Parameter(Mandatory = $true)][bool]$ShowMeta,
        [Parameter(Mandatory = $true)][bool]$ShowRaw
    )

    $rows = Get-ResponseRows -Response $Response
    $rowCount = [int](Get-ResponseValue -Response $Response -CamelName "rowCount" -PascalName "RowCount" -Default 0)
    $executionTimeMs = [double](Get-ResponseValue -Response $Response -CamelName "executionTimeMs" -PascalName "ExecutionTimeMs" -Default 0)
    $columns = @(Get-ResponseValue -Response $Response -CamelName "columns" -PascalName "Columns" -Default @())
    $registeredStreams = @(Get-ResponseValue -Response $Response -CamelName "registeredStreams" -PascalName "RegisteredStreams" -Default @())
    $originalSql = Get-ResponseValue -Response $Response -CamelName "originalSql" -PascalName "OriginalSql" -Default $null
    $rewrittenSql = Get-ResponseValue -Response $Response -CamelName "rewrittenSql" -PascalName "RewrittenSql" -Default $null

    Write-Host "Query succeeded." -ForegroundColor Green

    if ($ShowMeta) {
        Write-Host "  Rows returned:  $rowCount" -ForegroundColor Gray
        Write-Host "  Execution time: $([Math]::Round($executionTimeMs, 2)) ms" -ForegroundColor Gray

        if ($columns.Count -gt 0) {
            $columnText = @($columns | ForEach-Object {
                if ($_.PSObject.Properties.Name -contains 'name') { $_.name }
                elseif ($_.PSObject.Properties.Name -contains 'Name') { $_.Name }
                else { $_.ToString() }
            }) -join ', '
            Write-Host "  Columns: $columnText" -ForegroundColor Gray
        }

        if ($registeredStreams.Count -gt 0) {
            Write-Host "  Registered streams: $($registeredStreams -join ', ')" -ForegroundColor DarkGray
        }

        if (-not [string]::IsNullOrWhiteSpace([string]$originalSql)) {
            Write-Host "" 
            Write-Host "Original SQL:" -ForegroundColor DarkCyan
            Write-Host $originalSql
        }

        if (-not [string]::IsNullOrWhiteSpace([string]$rewrittenSql)) {
            Write-Host "" 
            Write-Host "Rewritten SQL:" -ForegroundColor DarkCyan
            Write-Host $rewrittenSql
        }
    }

    if ($rows.Count -gt 0) {
        $take = [Math]::Min($PreviewRows, $rows.Count)
        Write-Host ""
        Write-Host "Rows preview ($take/$($rows.Count)):" -ForegroundColor Yellow
        @($rows | Select-Object -First $take) | Format-Table -AutoSize | Out-Host
    } else {
        Write-Host "No rows returned." -ForegroundColor Yellow
    }

    if ($ShowRaw) {
        Write-Host ""
        Write-Host "Raw response:" -ForegroundColor DarkCyan
        Write-Host (ConvertTo-DisplayJson -Value $Response -Depth 30)
    }
}

function Write-QueryError {
    param(
        [Parameter(Mandatory = $true)]$ErrorRecord
    )

    $statusCode = $null
    $responseBody = $null
    $errorMessage = $null

    if ($ErrorRecord.Exception -and $ErrorRecord.Exception.Response) {
        if ($ErrorRecord.Exception.Response.StatusCode) {
            try { $statusCode = [int]$ErrorRecord.Exception.Response.StatusCode } catch { $statusCode = $null }
        }

        try {
            if ($ErrorRecord.Exception.Response.Content) {
                $responseBody = $ErrorRecord.Exception.Response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            }
        } catch {
            $responseBody = $null
        }
    }

    if ($ErrorRecord.Exception -and $ErrorRecord.Exception.Message) {
        $errorMessage = $ErrorRecord.Exception.Message
    } elseif ($ErrorRecord.ErrorDetails -and $ErrorRecord.ErrorDetails.Message) {
        $errorMessage = $ErrorRecord.ErrorDetails.Message
    } else {
        $errorMessage = $ErrorRecord.ToString()
    }

    Write-Host "Query failed." -ForegroundColor Red
    if ($statusCode) {
        Write-Host "  Status Code: $statusCode" -ForegroundColor Red
    }
    Write-Host "  Message: $errorMessage" -ForegroundColor Red

    if (-not [string]::IsNullOrWhiteSpace($responseBody)) {
        $parsed = $null
        try {
            $parsed = $responseBody | ConvertFrom-Json -Depth 20
        } catch {
            $parsed = $null
        }

        if ($parsed) {
            $parsedError = Get-ResponseValue -Response $parsed -CamelName "error" -PascalName "Error" -Default $null
            $parsedOriginalSql = Get-ResponseValue -Response $parsed -CamelName "originalSql" -PascalName "OriginalSql" -Default $null
            $parsedRewrittenSql = Get-ResponseValue -Response $parsed -CamelName "rewrittenSql" -PascalName "RewrittenSql" -Default $null

            if (-not [string]::IsNullOrWhiteSpace([string]$parsedError)) {
                Write-Host "  Error: $parsedError" -ForegroundColor DarkRed
            }
            if (-not [string]::IsNullOrWhiteSpace([string]$parsedOriginalSql)) {
                Write-Host ""
                Write-Host "Original SQL:" -ForegroundColor DarkCyan
                Write-Host $parsedOriginalSql
            }
            if (-not [string]::IsNullOrWhiteSpace([string]$parsedRewrittenSql)) {
                Write-Host ""
                Write-Host "Rewritten SQL:" -ForegroundColor DarkCyan
                Write-Host $parsedRewrittenSql
            }
        }

        Write-Host ""
        Write-Host "Raw error response:" -ForegroundColor DarkYellow
        Write-Host $responseBody -ForegroundColor DarkYellow
    }
}

function Invoke-LuminaSqlQuery {
    param(
        [Parameter(Mandatory = $true)][string]$CurrentBaseUrl,
        [Parameter(Mandatory = $true)][string]$CurrentMode,
        [Parameter(Mandatory = $true)][bool]$CurrentDebug,
        [Parameter(Mandatory = $true)][string]$Sql,
        [Parameter(Mandatory = $true)][hashtable]$CurrentParams
    )

    $debugValue = if ($CurrentDebug) { "true" } else { "false" }

    if ($CurrentMode -eq "sql-post") {
        $uri = "$CurrentBaseUrl/v1/query/sql?debug=$debugValue"
        return Invoke-RestMethod -Uri $uri -Method Post -ContentType "text/plain; charset=utf-8" -Body $Sql -ErrorAction Stop
    }

    if ($CurrentMode -eq "sql-get") {
        $escapedSql = [Uri]::EscapeDataString($Sql)
        $uri = "$CurrentBaseUrl/v1/query/sql?q=$escapedSql&debug=$debugValue"
        return Invoke-RestMethod -Uri $uri -Method Get -ErrorAction Stop
    }

    if ($CurrentMode -eq "sql-parameterized") {
        $uri = "$CurrentBaseUrl/v1/query/sql/parameterized?debug=$debugValue"
        $body = @{
            sql = $Sql
            parameters = $CurrentParams
        } | ConvertTo-Json -Depth 20

        return Invoke-RestMethod -Uri $uri -Method Post -ContentType "application/json; charset=utf-8" -Body $body -ErrorAction Stop
    }

    throw "Unsupported mode: $CurrentMode"
}

try {
    $baseUrl = Normalize-BaseUrl -Url $BaseUrl
} catch {
    Write-Host "Invalid BaseUrl: '$BaseUrl'. Use a full URL, e.g. http://localhost:5000" -ForegroundColor Red
    exit 1
}

$mode = $Mode
$debugEnabled = $DebugEnabled.IsPresent
$rowsToShow = $Rows
$showMeta = $true
$showRaw = $Raw.IsPresent
$isMultilineMode = $false
$paramsMap = @{}
$sqlBuffer = New-Object System.Collections.Generic.List[string]

Write-Host "Lumina SQL REPL" -ForegroundColor Green
Write-Host "Type .help for commands. SQL executes immediately; use .begin for multiline." -ForegroundColor Gray
Write-ReplStatus -CurrentBaseUrl $baseUrl -CurrentMode $mode -CurrentDebug $debugEnabled -CurrentRows $rowsToShow -CurrentMeta $showMeta -CurrentRaw $showRaw -IsMultilineMode $isMultilineMode -CurrentParams $paramsMap -CurrentBuffer $sqlBuffer
Write-Host ""

while ($true) {
    $promptSuffix = if ($isMultilineMode) { "|ml" } else { "" }
    $prompt = "lumina[$mode|debug=$debugEnabled$promptSuffix]> "
    Write-Host -NoNewline $prompt
    $inputLine = [Console]::ReadLine()

    if ($null -eq $inputLine) {
        continue
    }

    $line = $inputLine.Trim()
    if ([string]::IsNullOrWhiteSpace($line)) {
        continue
    }

    if ($line.StartsWith('.')) {
        $parts = @($line -split '\s+')
        $command = $parts[0].ToLowerInvariant()

        if ($command -eq '.quit' -or $command -eq '.exit') {
            break
        }

        if ($command -eq '.help') {
            Write-ReplHelp
            continue
        }

        if ($command -eq '.status') {
            Write-ReplStatus -CurrentBaseUrl $baseUrl -CurrentMode $mode -CurrentDebug $debugEnabled -CurrentRows $rowsToShow -CurrentMeta $showMeta -CurrentRaw $showRaw -IsMultilineMode $isMultilineMode -CurrentParams $paramsMap -CurrentBuffer $sqlBuffer
            continue
        }

        if ($command -eq '.begin') {
            if ($isMultilineMode) {
                Write-Host "Already in multiline mode. Enter SQL lines, then use .end or .abort." -ForegroundColor Yellow
                continue
            }

            $sqlBuffer.Clear()
            $isMultilineMode = $true
            Write-Host "Multiline mode enabled. Enter SQL lines, then use .end to execute or .abort to cancel." -ForegroundColor Green
            continue
        }

        if ($command -eq '.end') {
            if (-not $isMultilineMode) {
                Write-Host "Not in multiline mode. Use .begin first." -ForegroundColor Yellow
                continue
            }

            if ($sqlBuffer.Count -eq 0) {
                Write-Host "Multiline buffer is empty." -ForegroundColor Yellow
                continue
            }

            $sqlText = ($sqlBuffer -join [Environment]::NewLine).Trim()
            if ([string]::IsNullOrWhiteSpace($sqlText)) {
                Write-Host "Multiline buffer is empty." -ForegroundColor Yellow
                continue
            }

            try {
                $response = Invoke-LuminaSqlQuery -CurrentBaseUrl $baseUrl -CurrentMode $mode -CurrentDebug $debugEnabled -Sql $sqlText -CurrentParams $paramsMap
                Write-QuerySuccess -Response $response -PreviewRows $rowsToShow -ShowMeta $showMeta -ShowRaw $showRaw
            } catch {
                Write-QueryError -ErrorRecord $_
            } finally {
                $sqlBuffer.Clear()
                $isMultilineMode = $false
            }

            continue
        }

        if ($command -eq '.abort') {
            if (-not $isMultilineMode) {
                Write-Host "Not in multiline mode." -ForegroundColor Yellow
                continue
            }

            $sqlBuffer.Clear()
            $isMultilineMode = $false
            Write-Host "Multiline mode cancelled." -ForegroundColor Green
            continue
        }

        if ($command -eq '.base') {
            if ($parts.Count -lt 2) {
                Write-Host "Usage: .base <url>" -ForegroundColor Yellow
                continue
            }

            $newUrl = $line.Substring(5).Trim()
            try {
                $baseUrl = Normalize-BaseUrl -Url $newUrl
                Write-Host "Base URL set to $baseUrl" -ForegroundColor Green
            } catch {
                Write-Host "Invalid Base URL: $newUrl" -ForegroundColor Red
            }

            continue
        }

        if ($command -eq '.mode') {
            if ($parts.Count -lt 2) {
                Write-Host "Usage: .mode <sql-post|sql-get|sql-parameterized>" -ForegroundColor Yellow
                continue
            }

            $newMode = $parts[1].ToLowerInvariant()
            if ($newMode -in @('sql-post', 'sql-get', 'sql-parameterized')) {
                $mode = $newMode
                Write-Host "Mode set to $mode" -ForegroundColor Green
            } else {
                Write-Host "Invalid mode: $newMode" -ForegroundColor Red
            }

            continue
        }

        if ($command -eq '.debug') {
            if ($parts.Count -lt 2) {
                Write-Host "Usage: .debug <on|off>" -ForegroundColor Yellow
                continue
            }

            $value = $parts[1].ToLowerInvariant()
            if ($value -eq 'on') {
                $debugEnabled = $true
                Write-Host "Debug enabled." -ForegroundColor Green
            } elseif ($value -eq 'off') {
                $debugEnabled = $false
                Write-Host "Debug disabled." -ForegroundColor Green
            } else {
                Write-Host "Invalid value: $value. Use on/off." -ForegroundColor Red
            }

            continue
        }

        if ($command -eq '.rows') {
            if ($parts.Count -lt 2) {
                Write-Host "Usage: .rows <n>" -ForegroundColor Yellow
                continue
            }

            $n = 0
            if ([int]::TryParse($parts[1], [ref]$n) -and $n -gt 0 -and $n -le 1000) {
                $rowsToShow = $n
                Write-Host "Preview rows set to $rowsToShow" -ForegroundColor Green
            } else {
                Write-Host "Invalid row count. Use an integer from 1 to 1000." -ForegroundColor Red
            }

            continue
        }

        if ($command -eq '.meta') {
            if ($parts.Count -lt 2) {
                Write-Host "Usage: .meta <on|off>" -ForegroundColor Yellow
                continue
            }

            $value = $parts[1].ToLowerInvariant()
            if ($value -eq 'on') {
                $showMeta = $true
                Write-Host "Metadata output enabled." -ForegroundColor Green
            } elseif ($value -eq 'off') {
                $showMeta = $false
                Write-Host "Metadata output disabled." -ForegroundColor Green
            } else {
                Write-Host "Invalid value: $value. Use on/off." -ForegroundColor Red
            }

            continue
        }

        if ($command -eq '.raw') {
            if ($parts.Count -lt 2) {
                Write-Host "Usage: .raw <on|off>" -ForegroundColor Yellow
                continue
            }

            $value = $parts[1].ToLowerInvariant()
            if ($value -eq 'on') {
                $showRaw = $true
                Write-Host "Raw output enabled." -ForegroundColor Green
            } elseif ($value -eq 'off') {
                $showRaw = $false
                Write-Host "Raw output disabled." -ForegroundColor Green
            } else {
                Write-Host "Invalid value: $value. Use on/off." -ForegroundColor Red
            }

            continue
        }

        if ($command -eq '.clear') {
            $sqlBuffer.Clear()
            Write-Host "SQL buffer cleared." -ForegroundColor Green
            continue
        }

        if ($command -eq '.param') {
            if ($parts.Count -lt 2) {
                Write-Host "Usage: .param <set|remove|clear|list> ..." -ForegroundColor Yellow
                continue
            }

            $sub = $parts[1].ToLowerInvariant()

            if ($sub -eq 'clear') {
                $paramsMap.Clear()
                Write-Host "All parameters cleared." -ForegroundColor Green
                continue
            }

            if ($sub -eq 'list') {
                if ($paramsMap.Count -eq 0) {
                    Write-Host "No parameters set." -ForegroundColor Yellow
                } else {
                    Write-Host "Parameters:" -ForegroundColor Cyan
                    foreach ($key in ($paramsMap.Keys | Sort-Object)) {
                        Write-Host "  $key = $(ConvertTo-DisplayJson -Value $paramsMap[$key] -Depth 10)" -ForegroundColor Gray
                    }
                }
                continue
            }

            if ($sub -eq 'remove') {
                if ($parts.Count -lt 3) {
                    Write-Host "Usage: .param remove <name>" -ForegroundColor Yellow
                    continue
                }

                $name = $parts[2]
                if ($paramsMap.ContainsKey($name)) {
                    $paramsMap.Remove($name)
                    Write-Host "Parameter '$name' removed." -ForegroundColor Green
                } else {
                    Write-Host "Parameter '$name' does not exist." -ForegroundColor Yellow
                }

                continue
            }

            if ($sub -eq 'set') {
                if ($parts.Count -lt 4) {
                    Write-Host "Usage: .param set <name> <jsonValue>" -ForegroundColor Yellow
                    continue
                }

                $name = $parts[2]
                $prefix = ".param set $name"
                $rawJsonValue = $line.Substring($prefix.Length).Trim()

                if ([string]::IsNullOrWhiteSpace($name)) {
                    Write-Host "Parameter name cannot be empty." -ForegroundColor Red
                    continue
                }

                try {
                    $parsedJson = $rawJsonValue | ConvertFrom-Json -Depth 20
                    $paramsMap[$name] = $parsedJson
                    Write-Host "Parameter '$name' set." -ForegroundColor Green
                } catch {
                    Write-Host "Invalid JSON value for parameter '$name': $rawJsonValue" -ForegroundColor Red
                    Write-Host "Examples: 123, \"text\", true, null, {\"k\":1}, [1,2,3]" -ForegroundColor DarkYellow
                }

                continue
            }

            Write-Host "Unknown .param subcommand '$sub'." -ForegroundColor Red
            continue
        }

        if ($command -eq '.sql') {
            if ($line.Length -le 4) {
                Write-Host "Usage: .sql <query>" -ForegroundColor Yellow
                continue
            }

            $sqlText = $line.Substring(4).Trim()
            if ([string]::IsNullOrWhiteSpace($sqlText)) {
                Write-Host "SQL query cannot be empty." -ForegroundColor Yellow
                continue
            }

            try {
                $response = Invoke-LuminaSqlQuery -CurrentBaseUrl $baseUrl -CurrentMode $mode -CurrentDebug $debugEnabled -Sql $sqlText -CurrentParams $paramsMap
                Write-QuerySuccess -Response $response -PreviewRows $rowsToShow -ShowMeta $showMeta -ShowRaw $showRaw
            } catch {
                Write-QueryError -ErrorRecord $_
            }

            continue
        }

        Write-Host "Unknown command: $command. Type .help." -ForegroundColor Yellow
        continue
    }

    if ($isMultilineMode) {
        $sqlBuffer.Add($inputLine)
        Write-Host "Buffered multiline SQL line ($($sqlBuffer.Count))." -ForegroundColor DarkGray
        continue
    }

    try {
        $response = Invoke-LuminaSqlQuery -CurrentBaseUrl $baseUrl -CurrentMode $mode -CurrentDebug $debugEnabled -Sql $inputLine -CurrentParams $paramsMap
        Write-QuerySuccess -Response $response -PreviewRows $rowsToShow -ShowMeta $showMeta -ShowRaw $showRaw
    } catch {
        Write-QueryError -ErrorRecord $_
    }
}

Write-Host "Exiting Lumina SQL REPL." -ForegroundColor Gray
