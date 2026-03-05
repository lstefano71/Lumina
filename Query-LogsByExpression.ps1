<#
.SYNOPSIS
    Queries a Lumina stream using a SQL-like WHERE expression (server-side pushdown).

.DESCRIPTION
    Uses the revamped /v1/query/sql endpoint and executes SQL against stream views
    registered by Lumina (e.g., stream "test-stream" is queryable as table "test-stream").

    The supplied -Expression is treated as a WHERE predicate. Common shorthand is supported:
      - attr1 != null   -> attr1 IS NOT NULL
      - attr1 = null    -> attr1 IS NULL
      - attr1 <> null   -> attr1 IS NOT NULL

    Examples:
      .\Query-LogsByExpression.ps1 -Expression "attr1 != null"
      .\.Query-LogsByExpression.ps1 -Expression "_l = 'info' and attr2 != null"
      .\Query-LogsByExpression.ps1 -Expression "TRY_CAST(split_part(version, '.', 1) AS INTEGER) > 1"

.PARAMETER Expression
    SQL WHERE predicate expression.

.PARAMETER Stream
    Stream/table name to query. Default: test-stream.

.PARAMETER BaseUrl
    Lumina base URL. Default: http://localhost:5000.

.PARAMETER Limit
    SQL LIMIT value. Default: 1000.

.PARAMETER StartDate
    Optional timestamp lower bound (_t >= StartDate).

.PARAMETER EndDate
    Optional timestamp upper bound (_t <= EndDate).

.PARAMETER Raw
    Output only JSON response from Lumina.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Expression,

    [Parameter(Mandatory = $false)]
    [string]$Stream = "test-stream",

    [Parameter(Mandatory = $false)]
    [string]$BaseUrl = "http://localhost:5000",

    [Parameter(Mandatory = $false)]
    [int]$Limit = 1000,

    [Parameter(Mandatory = $false)]
    [Nullable[DateTime]]$StartDate = $null,

    [Parameter(Mandatory = $false)]
    [Nullable[DateTime]]$EndDate = $null,

    [Parameter(Mandatory = $false)]
    [switch]$Raw
)

function Escape-SqlIdentifier {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ([string]::IsNullOrWhiteSpace($Name)) {
        throw "Stream name cannot be empty."
    }

    if ($Name -match '^[A-Za-z_][A-Za-z0-9_]*$') {
        return $Name
    }

    return '"' + $Name.Replace('"', '""') + '"'
}

function Escape-SqlString {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    return $Value.Replace("'", "''")
}

function Normalize-WhereExpression {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InputExpression
    )

    $expr = $InputExpression.Trim()

    if ([string]::IsNullOrWhiteSpace($expr)) {
        throw "Expression cannot be empty."
    }

    $expr = [regex]::Replace($expr, '(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*(?:!=|<>)\s*null\b', '$1 IS NOT NULL')
    $expr = [regex]::Replace($expr, '(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*null\b', '$1 IS NULL')

    return $expr
}

try {
    $normalizedBaseUrl = $BaseUrl.TrimEnd('/')
    $baseUri = [Uri]::new($normalizedBaseUrl)
} catch {
    Write-Host "Invalid BaseUrl: '$BaseUrl'. Use a full URL, e.g. http://localhost:5000" -ForegroundColor Red
    exit 1
}

if ($Limit -le 0) {
    Write-Host "Limit must be greater than 0." -ForegroundColor Red
    exit 1
}

try {
    $streamTable = Escape-SqlIdentifier -Name $Stream
    $whereExpr = Normalize-WhereExpression -InputExpression $Expression
} catch {
    Write-Host "Invalid input: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

$whereParts = [System.Collections.Generic.List[string]]::new()
$whereParts.Add("($whereExpr)")

if ($StartDate) {
    $startLiteral = Escape-SqlString -Value $StartDate.Value.ToString("yyyy-MM-dd HH:mm:ss.fffffff")
    $whereParts.Add("_t >= TIMESTAMP '$startLiteral'")
}

if ($EndDate) {
    $endLiteral = Escape-SqlString -Value $EndDate.Value.ToString("yyyy-MM-dd HH:mm:ss.fffffff")
    $whereParts.Add("_t <= TIMESTAMP '$endLiteral'")
}

$whereClause = "WHERE " + ($whereParts -join " AND ")

$sql = @"
SELECT *
FROM $streamTable
$whereClause
ORDER BY _t DESC
LIMIT $Limit
"@

$sqlUrl = "$normalizedBaseUrl/v1/query/sql"

if (-not $Raw) {
    Write-Host "Querying Lumina with server-side SQL predicate..." -ForegroundColor Cyan
    Write-Host "Stream: $Stream" -ForegroundColor Gray
    Write-Host "Endpoint: $sqlUrl" -ForegroundColor Gray
    Write-Host "Expression: $Expression" -ForegroundColor Gray
    Write-Host ""
}

try {
    $response = Invoke-RestMethod -Uri $sqlUrl -Method Post -ContentType "text/plain; charset=utf-8" -Body $sql -ErrorAction Stop

    if ($Raw) {
        $response | ConvertTo-Json -Depth 20
        return
    }

    $rows = @()
    if ($response.PSObject.Properties.Name -contains 'Rows') {
        $rows = @($response.Rows)
    } elseif ($response.PSObject.Properties.Name -contains 'rows') {
        $rows = @($response.rows)
    }

    $rowCount = 0
    if ($response.PSObject.Properties.Name -contains 'RowCount') {
        $rowCount = [int]$response.RowCount
    } elseif ($response.PSObject.Properties.Name -contains 'rowCount') {
        $rowCount = [int]$response.rowCount
    }

    $executionMs = 0
    if ($response.PSObject.Properties.Name -contains 'ExecutionTimeMs') {
        $executionMs = [double]$response.ExecutionTimeMs
    } elseif ($response.PSObject.Properties.Name -contains 'executionTimeMs') {
        $executionMs = [double]$response.executionTimeMs
    }

    $registeredStreams = @()
    if ($response.PSObject.Properties.Name -contains 'RegisteredStreams') {
        $registeredStreams = @($response.RegisteredStreams)
    } elseif ($response.PSObject.Properties.Name -contains 'registeredStreams') {
        $registeredStreams = @($response.registeredStreams)
    }

    Write-Host "Query Results:" -ForegroundColor Green
    Write-Host "  Rows returned:   $rowCount" -ForegroundColor Gray
    Write-Host "  Execution time:  $([Math]::Round($executionMs, 2)) ms" -ForegroundColor Gray
    if ($registeredStreams.Count -gt 0) {
        Write-Host "  Registered streams: $($registeredStreams -join ', ')" -ForegroundColor DarkGray
    }
    Write-Host ""

    if ($rows.Count -gt 0) {
        Write-Host "Matching Log Entries:" -ForegroundColor Yellow
        Write-Host ("=" * 80) -ForegroundColor DarkGray

        foreach ($row in $rows) {
            Write-Host ""
            Write-Host "  Timestamp: $($row._t)" -ForegroundColor White
            Write-Host "  Level:     $($row._l)" -ForegroundColor Magenta
            Write-Host "  Message:   $($row._m)" -ForegroundColor White
            Write-Host ("-" * 80) -ForegroundColor DarkGray
        }
    } else {
        Write-Host "No entries matched the expression." -ForegroundColor Yellow
    }

    return $response
}
catch {
    $statusCode = $null
    $responseBody = $null

    if ($_.Exception -and $_.Exception.Response) {
        if ($_.Exception.Response.StatusCode) {
            try { $statusCode = [int]$_.Exception.Response.StatusCode } catch { $statusCode = $null }
        }

        try {
            if ($_.Exception.Response.Content) {
                $responseBody = $_.Exception.Response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            }
        } catch {
            $responseBody = $null
        }
    }

    if ($_.Exception -and $_.Exception.Message) {
        $errorMessage = $_.Exception.Message
    } elseif ($_.ErrorDetails -and $_.ErrorDetails.Message) {
        $errorMessage = $_.ErrorDetails.Message
    } else {
        $errorMessage = $_.ToString()
    }

    if ($Raw) {
        $errorPayload = @{
            statusCode = $statusCode
            message = $errorMessage
            response = $responseBody
            sql = $sql
        }
        $errorPayload | ConvertTo-Json -Depth 10 | Write-Error
    } else {
        Write-Host "Error executing SQL query:" -ForegroundColor Red
        if ($statusCode) {
            Write-Host "  Status Code: $statusCode" -ForegroundColor Red
        }
        Write-Host "  Message: $errorMessage" -ForegroundColor Red
        if ($responseBody) {
            Write-Host "  Response: $responseBody" -ForegroundColor DarkRed
        }

        Write-Host "" 
        Write-Host "SQL sent to Lumina:" -ForegroundColor DarkGray
        Write-Host $sql -ForegroundColor DarkGray
    }

    exit 1
}
