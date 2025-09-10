Step 1: Create a folder structure

On your system, modules live under one of these paths (run $env:PSModulePath -split ';' to see yours).

RabbitMQPublisher\
│
├── RabbitMQPublisher.psd1   # Module manifest (metadata file)
└── RabbitMQPublisher.psm1   # Module implementation (class & functions)

Step 2: Define the class in the .psm1

RabbitMQPublisher.psm1
class RabbitMQPublisher {
    [string]$Uri
    [string]$Username
    [string]$Password

    RabbitMQPublisher([string]$uri, [string]$username, [string]$password) {
        $this.Uri      = $uri
        $this.Username = $username
        $this.Password = $password
    }

    [void] PublishMessage([string]$exchange, [string]$routingKey, [string]$message) {
        try {
            # Build headers with basic auth
            $auth = "$($this.Username):$($this.Password)"
            $headers = @{
                Authorization = "Basic " + [System.Convert]::ToBase64String(
                    [System.Text.Encoding]::UTF8.GetBytes($auth)
                )
            }

            # Build request body
            $body = @{
                properties       = @{}
                routing_key      = $routingKey
                payload          = $message
                payload_encoding = "string"
            } | ConvertTo-Json

            # Call RabbitMQ REST API
            Invoke-RestMethod -Uri "$($this.Uri)/api/exchanges/%2F/$exchange/publish" `
                -Method Post `
                -Headers $headers `
                -Body $body `
                -ContentType "application/json"

            Write-Host "✅ Message published successfully to exchange '$exchange'."
        }
        catch {
            Write-Error "❌ Failed to publish message to RabbitMQ. Error: $($_.Exception.Message)"
            throw
        }
    }
}

Export-ModuleMember -Class RabbitMQPublisher

Step 3: Create a module manifest (.psd1)

RabbitMQPublisher.psd1
@{
    RootModule        = 'RabbitMQPublisher.psm1'
    ModuleVersion     = '1.0.0'
    GUID              = 'b2e9a9c4-89c5-4e3f-8e07-abc123456789'
    Author            = 'Your Name'
    CompanyName       = 'Your Company'
    PowerShellVersion = '5.1'
    Description       = 'A PowerShell module to publish messages to RabbitMQ via REST API.'
    FunctionsToExport = @()
    CmdletsToExport   = @()
    VariablesToExport = @()
    AliasesToExport   = @()
    ClassesToExport   = @('RabbitMQPublisher')
}

Step 4: Install and Import the module

Copy the folder RabbitMQPublisher (with both .psm1 and .psd1) into one of your module paths:
$env:PSModulePath -split ';'
Example for user scope:

C:\Users\<you>\Documents\WindowsPowerShell\Modules\RabbitMQPublisher\


Then use it:

Import-Module RabbitMQPublisher

$publisher = [RabbitMQPublisher]::new("https://rabbitmq.svc.com", "abc", "abc")
$publisher.PublishMessage("refta.notify", "queue_name", "hello from module!")
