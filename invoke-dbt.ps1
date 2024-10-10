# Capture the full command-line arguments passed to the script
$full_command = "dbt $args"

# Generate a timestamp
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

# Set environment variables
$env:DBT_COMMAND_LINE = $full_command
$env:DBT_EXECUTION_TIMESTAMP = $timestamp

# Output environment variables for debugging
Write-Host "DBT_COMMAND_LINE: $env:DBT_COMMAND_LINE"
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
    Remove-Item Env:\DBT_EXECUTION_TIMESTAMP
}