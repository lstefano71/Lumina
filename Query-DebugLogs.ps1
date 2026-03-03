<#
.SYNOPSIS
    Queries a Lumina stream for log entries of type "debug".

.DESCRIPTION
    This script queries the Lumina query API to retrieve log entries
    with the "debug" level from a specified stream (default: test-stream).

.PARAMETER Stream
    The name of the stream to query. Default is "test-stream".

.PARAMETER BaseUrl
    The base URL of the Lumina server. Default is "http://localhost:5000".

.PARAMETER Limit
    Maximum number of results to return. Default is 1000.

.PARAMETER StartDate
    Optional start date for the query range.

.PARAMETER EndDate
    Optional end date for the query range.

.PARAMETER Raw
    When specified, outputs only the raw JSON response.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Stream = "test-stream",

    [Parameter(Mandatory = $false)]
    [string]$BaseUrl = "http://localhost:5000",

    [Parameter(Mandatory = $false)]
    [int]$Limit = 1000,

    [Parameter(Mandatory = $false)]
    [Nullable[DateTime]]$StartDate = $null,

    [Parameter(Mandatory = $false)]
    [Nullable[DateTime]]$EndDate = $null

    ,

    [Parameter(Mandatory = $false)]
    [switch]$Raw
)

# Normalize and validate base URL
try {
    $normalizedBaseUrl = $BaseUrl.TrimEnd('/')
    $baseUri = [Uri]::new($normalizedBaseUrl)
} catch {
    Write-Host "Invalid BaseUrl: '$BaseUrl'. Use a full URL, e.g. http://localhost:5000" -ForegroundColor Red
    exit 1
}

# Build the query URL path
$queryPath = "/v1/query/logs/$([Uri]::EscapeDataString($Stream))"
$queryUriBuilder = [UriBuilder]::new([Uri]::new($baseUri, $queryPath))

# Build query parameters
$queryParams = @{}
$queryParams["level"] = "debug"
$queryParams["limit"] = $Limit

if ($StartDate) {
    $queryParams["start"] = $StartDate.ToString("o")  # ISO 8601 format
}

if ($EndDate) {
    $queryParams["end"] = $EndDate.ToString("o")  # ISO 8601 format
}

# Build the full URL with query string
$queryString = ($queryParams.GetEnumerator() | ForEach-Object { "$($_.Key)=$([Uri]::EscapeDataString([string]$_.Value))" }) -join "&"
$queryUriBuilder.Query = $queryString
$fullUrl = $queryUriBuilder.Uri.AbsoluteUri

if (-not $Raw) {
    Write-Host "Querying Lumina for debug logs..." -ForegroundColor Cyan
    Write-Host "Stream: $Stream" -ForegroundColor Gray
    Write-Host "URL: $fullUrl" -ForegroundColor Gray
    Write-Host ""
}

try {
    # Execute the query
    $response = Invoke-RestMethod -Uri $fullUrl -Method Get -ErrorAction Stop

    if ($Raw) {
        $response | ConvertTo-Json -Depth 20
        return
    }

    # Display results
    Write-Host "Query Results:" -ForegroundColor Green

    if ($null -ne $response) {
        if ($response.PSObject.Properties.Name -contains 'RowCount') { $rows = $response.RowCount }
        elseif ($response.PSObject.Properties.Name -contains 'rowCount') { $rows = $response.rowCount }
        else { $rows = 0 }

        if ($response.PSObject.Properties.Name -contains 'ExecutionTimeMs') { $time = $response.ExecutionTimeMs }
        elseif ($response.PSObject.Properties.Name -contains 'executionTimeMs') { $time = $response.executionTimeMs }
        else { $time = 0 }

        if ($response.PSObject.Properties.Name -contains 'Columns' -and $response.Columns) { $cols = $response.Columns -join ', ' }
        elseif ($response.PSObject.Properties.Name -contains 'columns' -and $response.columns) { $cols = $response.columns -join ', ' }
        else { $cols = '' }

        Write-Host "  Rows returned: $rows" -ForegroundColor Gray
        Write-Host "  Execution time: $time ms" -ForegroundColor Gray
        Write-Host "  Columns: $cols" -ForegroundColor Gray
        Write-Host ""

        $rowData = $null
        if ($response.PSObject.Properties.Name -contains 'Rows') { $rowData = $response.Rows }
        elseif ($response.PSObject.Properties.Name -contains 'rows') { $rowData = $response.rows }

        if ($rows -gt 0 -and $rowData) {
            Write-Host "Debug Log Entries:" -ForegroundColor Yellow
            Write-Host ("=" * 80) -ForegroundColor DarkGray

            foreach ($row in $rowData) {
                Write-Host ""
                Write-Host "  Timestamp: $($row.timestamp)" -ForegroundColor White
                Write-Host "  Level:     $($row.level)" -ForegroundColor Magenta
                Write-Host "  Message:   $($row.message)" -ForegroundColor White

                if ($row.attributes -and $row.attributes.PSObject.Properties.Count -gt 0) {
                    Write-Host "  Attributes:" -ForegroundColor DarkCyan
                    foreach ($prop in $row.attributes.PSObject.Properties) {
                        Write-Host "    $($prop.Name): $($prop.Value)" -ForegroundColor DarkGray
                    }
                }
                Write-Host ("-" * 80) -ForegroundColor DarkGray
            }

            # Output the raw data for potential further processing
            Write-Host ""
            Write-Host 'Raw JSON response available in $response variable' -ForegroundColor DarkGray
        } else {
            Write-Host "No debug log entries found." -ForegroundColor Yellow
        }
    } else {
        Write-Host "No response received from server." -ForegroundColor Yellow
    }

    # Return the response for pipeline use
    return $response

} catch {
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
        Write-Host "Error querying Lumina:" -ForegroundColor Red
        if ($statusCode) {
            Write-Host "  Status Code: $statusCode" -ForegroundColor Red
        }
        Write-Host "  Message: $errorMessage" -ForegroundColor Red
        if ($responseBody) {
            Write-Host "  Response: $responseBody" -ForegroundColor DarkRed
        }
    }

    if ($statusCode -eq 404) {
        Write-Host ""; Write-Host "The stream '$Stream' may not exist or has no data." -ForegroundColor Yellow
        Write-Host "Make sure the Lumina server is running and the stream has been created." -ForegroundColor Yellow
    } elseif (-not $statusCode) {
        Write-Host ""; Write-Host "Could not connect to Lumina server at $BaseUrl" -ForegroundColor Yellow
        Write-Host "Make sure the server is running." -ForegroundColor Yellow
    }

    exit 1
}
