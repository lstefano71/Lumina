<#
.SYNOPSIS
    Queries a Lumina stream and filters results with a SQL-like expression.

.DESCRIPTION
    Fetches logs from /v1/query/logs/{stream} and applies a client-side filter expression.

    Supported expression operators:
      - =, !=, >, >=, <, <=
      - and, or, not
      - null, true, false

    Examples:
      .\Query-LogsByExpression.ps1 -Expression "attr1 != null"
      .\Query-LogsByExpression.ps1 -Expression "level = 'info' and attr2 != null"

.PARAMETER Expression
    Filter expression to evaluate for each returned row.

.PARAMETER Stream
    The stream to query. Default: test-stream.

.PARAMETER BaseUrl
    The Lumina base URL. Default: http://localhost:5000.

.PARAMETER Limit
    Max rows fetched from server before local filtering. Default: 1000.

.PARAMETER StartDate
    Optional start datetime (ISO sent to API).

.PARAMETER EndDate
    Optional end datetime (ISO sent to API).

.PARAMETER Raw
    Output only JSON result.
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

function Convert-ExpressionToPowerShell {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InputExpression
    )

    $expr = $InputExpression

    # Protect string literals ('...') while rewriting identifiers/operators.
    $stringLiterals = @{}
    $matches = [regex]::Matches($expr, "'([^']|'')*'")
    for ($i = $matches.Count - 1; $i -ge 0; $i--) {
        $m = $matches[$i]
        $key = "__STR$i`__"
        $stringLiterals[$key] = $m.Value
        $expr = $expr.Remove($m.Index, $m.Length).Insert($m.Index, $key)
    }

    # SQL-like operators/keywords to PowerShell equivalents.
    $expr = [regex]::Replace($expr, "\bAND\b", "-and", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    $expr = [regex]::Replace($expr, "\bOR\b", "-or", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    $expr = [regex]::Replace($expr, "\bNOT\b", "-not", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    $expr = [regex]::Replace($expr, "\bNULL\b", "`$null", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    $expr = [regex]::Replace($expr, "\bTRUE\b", "`$true", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    $expr = [regex]::Replace($expr, "\bFALSE\b", "`$false", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)

    # Comparison operators
    $expr = $expr -replace "<>", " -ne "
    $expr = $expr -replace "!=", " -ne "
    $expr = $expr -replace "<=", " -le "
    $expr = $expr -replace ">=", " -ge "
    $expr = $expr -replace "(?<![<>!])=(?!=)", " -eq "
    $expr = $expr -replace "(?<!-)>(?!=)", " -gt "
    $expr = $expr -replace "(?<!-)<(?!=)", " -lt "

    # Prefix bare identifiers as row fields: attr1 -> $_.attr1
    $reserved = @(
        'and','or','not',
        'eq','ne','lt','le','gt','ge',
        'true','false','null'
    )

    $expr = [regex]::Replace($expr, "\b[A-Za-z_][A-Za-z0-9_]*\b", {
        param($m)
        $token = $m.Value
        if ($token -like "__STR*__") { return $token }
        if ($reserved -contains $token.ToLowerInvariant()) { return $token }
        if ($token.StartsWith("$")) { return $token }
        return "`$_.${token}"
    })

    foreach ($key in $stringLiterals.Keys) {
        $expr = $expr.Replace($key, $stringLiterals[$key])
    }

    return $expr
}

try {
    $normalizedBaseUrl = $BaseUrl.TrimEnd('/')
    $baseUri = [Uri]::new($normalizedBaseUrl)
} catch {
    Write-Host "Invalid BaseUrl: '$BaseUrl'. Use a full URL, e.g. http://localhost:5000" -ForegroundColor Red
    exit 1
}

$queryPath = "/v1/query/logs/$([Uri]::EscapeDataString($Stream))"
$queryUriBuilder = [UriBuilder]::new([Uri]::new($baseUri, $queryPath))

$queryParams = @{}
$queryParams["limit"] = $Limit

if ($StartDate) {
    $queryParams["start"] = $StartDate.ToString("o")
}

if ($EndDate) {
    $queryParams["end"] = $EndDate.ToString("o")
}

$queryString = ($queryParams.GetEnumerator() | ForEach-Object { "$($_.Key)=$([Uri]::EscapeDataString([string]$_.Value))" }) -join "&"
$queryUriBuilder.Query = $queryString
$fullUrl = $queryUriBuilder.Uri.AbsoluteUri

$psExpression = Convert-ExpressionToPowerShell -InputExpression $Expression

if (-not $Raw) {
    Write-Host "Querying Lumina and applying expression filter..." -ForegroundColor Cyan
    Write-Host "Stream: $Stream" -ForegroundColor Gray
    Write-Host "URL: $fullUrl" -ForegroundColor Gray
    Write-Host "Expression: $Expression" -ForegroundColor Gray
    Write-Host "PowerShell filter: $psExpression" -ForegroundColor DarkGray
    Write-Host ""
}

try {
    $response = Invoke-RestMethod -Uri $fullUrl -Method Get -ErrorAction Stop

    $rows = @()
    if ($response.PSObject.Properties.Name -contains 'Rows') {
        $rows = @($response.Rows)
    } elseif ($response.PSObject.Properties.Name -contains 'rows') {
        $rows = @($response.rows)
    }

    $filterScript = [ScriptBlock]::Create($psExpression)
    $filteredRows = @($rows | Where-Object $filterScript)

    $columns = @()
    if ($response.PSObject.Properties.Name -contains 'Columns') {
        $columns = @($response.Columns)
    } elseif ($response.PSObject.Properties.Name -contains 'columns') {
        $columns = @($response.columns)
    }

    $result = [pscustomobject]@{
        Rows = $filteredRows
        RowCount = $filteredRows.Count
        Columns = $columns
        Expression = $Expression
        Stream = $Stream
        SourceRowCount = $rows.Count
    }

    if ($Raw) {
        $result | ConvertTo-Json -Depth 20
        return
    }

    Write-Host "Query Results:" -ForegroundColor Green
    Write-Host "  Source rows:   $($rows.Count)" -ForegroundColor Gray
    Write-Host "  Filtered rows: $($filteredRows.Count)" -ForegroundColor Gray
    Write-Host ""

    if ($filteredRows.Count -gt 0) {
        Write-Host "Matching Log Entries:" -ForegroundColor Yellow
        Write-Host ("=" * 80) -ForegroundColor DarkGray

        foreach ($row in $filteredRows) {
            Write-Host ""
            Write-Host "  Timestamp: $($row.timestamp)" -ForegroundColor White
            Write-Host "  Level:     $($row.level)" -ForegroundColor Magenta
            Write-Host "  Message:   $($row.message)" -ForegroundColor White
            Write-Host ("-" * 80) -ForegroundColor DarkGray
        }
    } else {
        Write-Host "No entries matched the expression." -ForegroundColor Yellow
    }

    return $result
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
        }
        $errorPayload | ConvertTo-Json -Depth 10 | Write-Error
    } else {
        Write-Host "Error querying Lumina/expression:" -ForegroundColor Red
        if ($statusCode) {
            Write-Host "  Status Code: $statusCode" -ForegroundColor Red
        }
        Write-Host "  Message: $errorMessage" -ForegroundColor Red
        if ($responseBody) {
            Write-Host "  Response: $responseBody" -ForegroundColor DarkRed
        }
    }

    exit 1
}
