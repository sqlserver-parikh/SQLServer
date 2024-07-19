 
$hostName = "google.com"  # Change to the desired host name or IP address
$pingInterval = 4         # Number of ping attempts before displaying time

# Get the local computer's hostname and IP address
$localHostName = [System.Net.Dns]::GetHostName()
$localIP = [System.Net.Dns]::GetHostByName($localHostName).AddressList[0].IPAddressToString
$destinationIP = (Resolve-DnsName $hostName).IPAddress

while ($true) {
    $pingResult = Test-Connection -ComputerName $hostName -Count $pingInterval -Quiet
    $successCount = ($pingResult | Where-Object { $_ -eq $true }).Count
    $failureCount = $pingInterval - $successCount

    Write-Host "Ping attempts: $pingInterval"
    Write-Host "Success: $successCount, Failure: $failureCount"
    Write-Host "Local Hostname: $localHostName" 
    Write-Host "Destination Hostname: $HostName"
    Write-Host "Local IP: $localIP"
    Write-Host "Destination IP: $destinationIP "
    Write-Host "Timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Write-Host "-----------------------------------------"

    # Wait for 1 second before next iteration
    Start-Sleep -Seconds 1
}
