# databricks setup on mac

1. install CLI

   ```sh
   brew tap databricks/tap
   brew install databricks
   ```

2. log into workspace

   ```sh
   databricks auth login --host https://dbc-537d718c-4921.cloud.databricks.com
   ```

3. set profile name to `DEFAULT`

4. approve in browser

5. verify

   ```sh
   databricks current-user me
   ```

## how it works

```txt
your laptop (python)                      Databricks serverless
  df.groupBy(...).count()   -- plan -->   Spark executes on the 20M rows
  print(result)            <-- rows --    sends back only the small result
```

PySpark DataFrames are lazy. Calling groupBy or filter only builds a query plan, and no data moves. Connect sends that plan over gRPC (the Spark Connect protocol) to serverless, which runs it next to the data. Only small results like .show() or .collect() come back. thats why counting 20M rows takes seconds and no data ever lands locally.

git folder - shared copy, changes only via push and pull. iterate with the extension, merge with git

