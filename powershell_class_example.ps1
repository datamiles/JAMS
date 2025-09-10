#Requires -Version 5.1
using namespace System
using namespace System.Net
using namespace System.Text
using namespace System.Text.RegularExpressions

class RabbitMQSender {

    # --- Constants / Levels ---
    static [int]   $MESSAGE_LOG_LIMIT = 128
    static [Guid]  $ACTIVITY_ID       = [Guid]::Parse('1AKK257D3-D5D3-4268-Z984-07A202BD9AEB')  # <- keep your actual GUID here
    static [string]$LEVEL_INFO        = 'LEVEL_INFO'
    static [string]$LEVEL_WARNING     = 'LEVEL_WARNING'
    static [string]$LEVEL_ERROR       = 'LEVEL_ERROR'

    # ------------------------------
    #  Public API (equivalent to C# Send)
    # ------------------------------
    static [void] Send(
        [int]   $processId,
        [string]$url,
        [string]$credentials,           # "user:pass"
        [string]$message,
        [string]$instrumentationUrl
    ) {
        # Match C# behavior
        [ServicePointManager]::SecurityProtocol = [SecurityProtocolType]::Tls12

        $correlationId = [Guid]::NewGuid()

        # --- Build Basic Auth header
        $base64 = [Convert]::ToBase64String([Encoding]::ASCII.GetBytes($credentials))
        $headers = @{
            Authorization = "Basic $base64"
        }

        # --- Request body (ConvertTo-Json handles string escaping)
        $bodyObj = @{
            properties       = @{ delivery_mode = 2 }
            routing_key      = ""                         # keep if you were sending directly via exchange->routing_key
            payload          = $message
            payload_encoding = "string"
        }
        $bodyJson = $bodyObj | ConvertTo-Json -Depth 5

        try {
            # Invoke-RestMethod will auto-deserialize {"routed":true} to an object with .routed
            $resp = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $bodyJson -ContentType 'application/json'

            # Support both object and raw string responses
            $routed = $false
            if ($null -ne $resp) {
                if ($resp -is [string]) {
                    $routed = [Regex]::Match($resp, '"routed"\s*:\s*true').Success
                } else {
                    try { $routed = [bool]$resp.routed } catch { $routed = $false }
                }
            }

            if ($routed) {
                [RabbitMQSender]::SendToInstrumentation(
                    $instrumentationUrl,
                    ("Process Id {0} message routed to rabbitmq: {1}" -f $processId, [RabbitMQSender]::TruncateForLogging($message)),
                    [RabbitMQSender]::LEVEL_INFO,
                    $correlationId
                )
            } else {
                # Unknown / unexpected response
                $respText = if ($resp -is [string]) { $resp } else { ($resp | ConvertTo-Json -Depth 5) }
                [RabbitMQSender]::SendToInstrumentation(
                    $instrumentationUrl,
                    ("Unknown response for process id {0}: {1} from {2}" -f $processId, [RabbitMQSender]::TruncateForLogging($respText), $url),
                    [RabbitMQSender]::LEVEL_WARNING,
                    $correlationId
                )
            }
        }
        catch [System.Net.WebException] {
            [RabbitMQSender]::SendToInstrumentation(
                $instrumentationUrl,
                ("Process Id {0} failed to send to rabbitmq: {1}" -f $processId, $_.Exception.Message),
                [RabbitMQSender]::LEVEL_ERROR,
                $correlationId
            )
            throw   # preserve failure semantics
        }
    }

    # ------------------------------
    #  Instrumentation (equivalent to C# SendToInstrumentation)
    # ------------------------------
    static hidden [void] SendToInstrumentation(
        [string]$url,
        [string]$message,
        [string]$level,
        [Guid]  $correlationId
    ) {
        if ([string]::IsNullOrWhiteSpace($url)) { return }

        $payload = @(
            @{
                CorrelationIdent     = $correlationId.ToString()              # name mirrors your C# shape
                ActivityIdent        = [RabbitMQSender]::ACTIVITY_ID
                ActivityName         = 'SendToRabbitMQ sproc'
                ContextName          = $null
                AppName              = 'SqlServer'
                InstanceName         = [Environment]::MachineName
                Source               = $null
                Command              = $null
                MessageFormat        = 'MESSAGE_FORMAT_TEXT'
                MessageValue         = $message
                Level                = $level
                EventDateTimeUtc     = [DateTime]::UtcNow.ToString('O')
            }
        )

        $json = $payload | ConvertTo-Json -Depth 5
        Invoke-RestMethod -Uri $url -Method Post -Body $json -ContentType 'application/json' | Out-Null
    }

    # ------------------------------
    #  Helpers (equivalent to TruncateForLogging + escapeJSON intent)
    # ------------------------------
    static hidden [string] TruncateForLogging([string]$message) {
        if ([string]::IsNullOrEmpty($message)) { return "" }
        if ($message.Length -le [RabbitMQSender]::MESSAGE_LOG_LIMIT) { return $message }
        return $message.Substring(0, [RabbitMQSender]::MESSAGE_LOG_LIMIT) + '...'
    }
}
