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
