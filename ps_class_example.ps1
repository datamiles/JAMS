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

            Write-Host "Message published successfully to exchange '$exchange'."
        }
        catch {
            Write-Error "Failed to publish message to RabbitMQ. Error: $($_.Exception.Message)"
            throw
        }
    }
}

# Example usage:
$publisher = [RabbitMQPublisher]::new("https://rabbitmq.svc.com", "abc", "abc")
$publisher.PublishMessage("refta.notify", "queue_name", "this is a test")

. .\RabbitMQPublisher.ps1   # dot-source the file to load the class

$publisher = [RabbitMQPublisher]::new("https://rabbitmq.svc.com", "abc", "abc")
$publisher.PublishMessage("refta.notify", "queue_name", "hello world")
