# invoke-dbt.ps1 is a PowerShell script that sets environment variables 
# for the dbt command-line interface (CLI) and runs dbt with the arguments 
# passed to the script.

# Capture the full command-line arguments passed to the script
$full_command = "dbt $args"

# Generate a timestamp
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

# Get Python version
$python_version = (python --version 2>&1).ToString().Split()[1]

# Get dbt-sqlserver adapter version
$dbt_sqlserver_version = (pip show dbt-sqlserver 2>$null | Select-String -Pattern "Version:").ToString().Split()[-1]
if (-not $dbt_sqlserver_version) {
    $dbt_sqlserver_version = "Not installed"
}

# Set environment variables
$env:DBT_COMMAND_LINE = $full_command
$env:DBT_PYTHON_VERSION = $python_version
$env:DBT_SQLSERVER_VERSION = $dbt_sqlserver_version
$env:DBT_EXECUTION_TIMESTAMP = $timestamp

# Output environment variables for debugging
Write-Host "DBT_COMMAND_LINE: $env:DBT_COMMAND_LINE"
Write-Host "DBT_PYTHON_VERSION: $env:DBT_PYTHON_VERSION"
Write-Host "DBT_SQLSERVER_VERSION: $env:DBT_SQLSERVER_VERSION"
Write-Host "DBT_EXECUTION_TIMESTAMP: $env:DBT_EXECUTION_TIMESTAMP"

try {
    # Run dbt with the arguments passed from the command line
    & dbt @args
}
catch {
    Write-Error "Error running dbt: $_"
}
finally {
    # Clear the environment variables after execution
    Remove-Item Env:\DBT_COMMAND_LINE
    Remove-Item Env:\DBT_PYTHON_VERSION
    Remove-Item Env:\DBT_SQLSERVER_VERSION    
    Remove-Item Env:\DBT_EXECUTION_TIMESTAMP
}