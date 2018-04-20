$computers = Get-Content -Path D:\temp\PName.txt
Get-WmiObject -Class win32_powerplan -Namespace root\cimv2\power `
  -CN $computers -Filter "isActive='true'" -EA silentlyContinue| 
Format-Table -Property elementName, __Server -AutoSize
