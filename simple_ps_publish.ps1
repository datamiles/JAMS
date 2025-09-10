param (
    [string]$BaseUrl    = "https://rabbitmq.svc.com",       # e.g., https://rabbitmq.svc.com
    [string]$VHost      = "/",                               # vhost; "/" will be encoded as %2F
    [string]$Exchange   = "refta.notify",                    # exchange name
    [string]$RoutingKey = "queue_name",                      # routing key to use
    [string]$Message    = "this is a test",                  # payload
    [string]$Username   = "abc",
    [string]$Password   = "abc"
)

# Force TLS1.2 like your C# did
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

# Build Basic auth header
$authBytes = [System.Text.Encoding]::UTF8.GetBytes("$Username`:$Password")
$authHeader = "Basic " + [System.Convert]::ToBase64String($authBytes)
$headers = @{ Authorization = $authHeader }

# Build body exactly like your method: properties, routing_key, payload, payload_encoding
$bodyObj = @{
    properties       = @{}
    routing_key      = $RoutingKey
    payload          = $Message
    payload_encoding = "string"
}

# Serialize to JSON with protection
try {
    $bodyJson = $bodyObj | ConvertTo-Json -Depth 6
}
catch {
    Write-Error "Failed to convert body to JSON: $($_.Exception.Message)"
    exit 1
}

# Encode vhost for URL ("/" -> "%2F", etc.)
function Encode-VHost([string]$v) {
    return [System.Uri]::EscapeDataString($v)
}

$encodedVHost = Encode-VHost $VHost
$uri = "$BaseUrl/api/exchanges/$encodedVHost/$Exchange/publish"

# Publish
try {
    $resp = Invoke-RestMethod -Uri $uri -Method Post -Headers $headers -Body $bodyJson -ContentType "application/json"

    # RabbitMQ returns { "routed": true|false }
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
    } else {
        Write-Warning "⚠️ Publish call returned non-routed response: $($resp | ConvertTo-Json -Depth 6)"
        exit 2
    }
}
catch {
    Write-Error "❌ Failed to publish to RabbitMQ at $uri : $($_.Exception.Message)"
    exit 1
}
