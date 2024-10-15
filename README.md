# DBT Project: dbt_SimDataMartLoad

## Running DBT

To ensure proper logging and execution, always use the provided wrapper script to run dbt commands:

```
.\invoke-dbt.ps1 [your dbt command and arguments]
```

For example:
```
.\invoke-dbt.ps1 run --models my_model --vars '{"my_var": "my_value"}' --full-refresh
```

### Why Use the Wrapper Script?

The wrapper script ensures that:
1. All dbt runs are properly logged for auditing and debugging purposes.
2. Environment variables are correctly set and cleared after each run.
3. Errors are handled gracefully and logged when they occur.

### Troubleshooting

If you encounter any issues or error messages instructing you to use the wrapper script, please ensure you're running dbt through `invoke-dbt.ps1` as described above.

For further assistance, contact Dana or Khalique.
