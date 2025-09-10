param (
    [string]$BaseUrl             = "https://rabbitmq.svc.com",  # e.g., https://rabbitmq.svc.com
    [string]$VHost               = "/",                          # vhost; "/" will be encoded as %2F
    [string]$Exchange            = "refta.notify",
    [string]$RoutingKey          = "queue_name",
    [string]$Message             = "this is a test",
    [string]$Username            = "abc",
    [string]$Password            = "abc",

    # Instrumentation
    [string]$InstrumentationUrl  = "https://instrumentation.svc.com/api/events",
    [string]$AppName             = "SqlServer",
    [Guid]  $ActivityIdent       = [Guid]::Parse("1AKK257D3-D5D3-4268-Z984-07A202BD9AEB") # <-- replace with your real GUID
)

# -----------------------
# Config & Helpers
# -----------------------
$MESSAGE_LOG_LIMIT = 128
function Truncate-ForLogging([string]$s) {
    if ([string]::IsNullOrEmpty($s)) { return "" }
    if ($s.Length -le $MESSAGE_LOG_LIMIT) { return $s }
    return $s.Substring(0, $MESSAGE_LOG_LIMIT) + "..."
}

function Encode-VHost([string]$v) {
    # Encode "/" -> "%2F", spaces -> %20, etc.
    return [System.Uri]::EscapeDataString($v)
}

function Send-ToInstrumentation {
    param(
        [string]$Url,
        [string]$Message,
        [string]$Level,          # e.g. "LEVEL_INFO" | "LEVEL_WARNING" | "LEVEL_ERROR"
        [Guid]  $CorrelationId,
        [Guid]  $ActivityIdent,
        [string]$AppName
    )
    if ([string]::IsNullOrWhiteSpace($Url)) { return }

    $payload = @(
        @{
            CorrelationIdent = $CorrelationId.ToString()
            ActivityIdent    = $ActivityIdent
            ActivityName     = "SendToRabbitMQ script"
            ContextName      = $null
            AppName          = $AppName
            InstanceName     = $env:COMPUTERNAME
            Source           = $null
            Command          = $null
            MessageFormat    = "MESSAGE_FORMAT_TEXT"
            MessageValue     = $Message
            Level            = $Level
            EventDateTimeUtc = (Get-Date).ToUniversalTime().ToString("o")
        }
    )

    try {
        $json = $payload | ConvertTo-Json -Depth 6
    }
    catch {
        Write-Warning "Instrumentation JSON serialization failed (corr=$CorrelationId): $($_.Exception.Message)"
        return
    }

    try {
        Invoke-RestMethod -Uri $Url -Method Post -Body $json -ContentType "application/json" | Out-Null
    }
    catch {
        Write-Warning "Failed to POST instrumentation (corr=$CorrelationId): $($_.Exception.Message)"
    }
}

# -----------------------
# Publish flow
# -----------------------
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
$CorrelationId = [guid]::NewGuid()

# Auth header
$authBytes  = [System.Text.Encoding]::UTF8.GetBytes("$Username`:$Password")
$authHeader = "Basic " + [System.Convert]::ToBase64String($authBytes)
$headers    = @{ Authorization = $authHeader }

# Body for publish
$bodyObj = @{
    properties       = @{}
    routing_key      = $RoutingKey
    payload          = $Message
    payload_encoding = "string"
}

# Convert body to JSON
try {
    $bodyJson = $bodyObj | ConvertTo-Json -Depth 6
}
catch {
    Write-Error "Failed to convert publish body to JSON: $($_.Exception.Message)"
    Send-ToInstrumentation -Url $InstrumentationUrl `
        -Message ("Failed to serialize RabbitMQ JSON: {0}" -f (Truncate-ForLogging $_.Exception.Message)) `
        -Level "LEVEL_ERROR" -CorrelationId $CorrelationId -ActivityIdent $ActivityIdent -AppName $AppName
    exit 1
}

# URL for publish
$encodedVHost = Encode-VHost $VHost
$uri = "$BaseUrl/api/exchanges/$encodedVHost/$Exchange/publish"

# Publish and log
try {
    $resp = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers -Body $bodyJson -ContentType "application/json"

    # routed detection
    $routed = $false
    if ($null -ne $resp) {
        if ($resp -is [string]) {
            $routed = [regex]::Match($resp, '"routed"\s*:\s*true').Success
        } else {
            try { $routed = [bool]$resp.routed } catch { $routed = $false }
        }
    }

    if ($routed) {
        Write-Host "✅ Message routed to exchange '$Exchange' with routing_key '$RoutingKey'."
        Send-ToInstrumentation -Url $InstrumentationUrl `
            -Message ("Message routed. Exchange={0}, RoutingKey={1}, Payload={2}" -f $Exchange, $RoutingKey, (Truncate-ForLogging $Message)) `
            -Level "LEVEL_INFO" -CorrelationId $CorrelationId -ActivityIdent $ActivityIdent -AppName $AppName
    } else {
        $respText = if ($resp -is [string]) { $resp } else { ($resp | ConvertTo-Json -Depth 6) }
        Write-Warning "Publish returned non-routed response."
        Send-ToInstrumentation -Url $InstrumentationUrl `
            -Message ("Non-routed response from {0}: {1}" -f $uri, (Truncate-ForLogging $respText)) `
            -Level "LEVEL_WARNING" -CorrelationId $CorrelationId -ActivityIdent $ActivityIdent -AppName $AppName
        exit 2
    }
}
catch {
    Write-Error "Failed to publish to RabbitMQ at $uri : $($_.Exception.Message)"
    Send-ToInstrumentation -Url $InstrumentationUrl `
        -Message ("Publish failure at {0}: {1}" -f $uri, (Truncate-ForLogging $_.Exception.Message)) `
        -Level "LEVEL_ERROR" -CorrelationId $CorrelationId -ActivityIdent $ActivityIdent -AppName $AppName
    exit 1
}


Example run
.\Publish-RabbitMq.ps1 `
  -BaseUrl "https://rabbitmq.svc.com" `
  -VHost "/" `
  -Exchange "refta.notify" `
  -RoutingKey "queue_name" `
  -Message "this is a test" `
  -Username "abc" -Password "abc" `
  -InstrumentationUrl "https://instrumentation.svc.com/api/events" `
  -AppName "SqlServer" `
  -ActivityIdent "11111111-2222-3333-4444-555555555555"
