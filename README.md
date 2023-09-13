## Azure Eventhub to Snowflake Function

This codebase was forked from [Azure Functions Samples Java](https://github.com/Azure-Samples/azure-functions-samples-java)

## Build Prerequisites

- Latest [Function Core Tools](https://aka.ms/azfunc-install)
- Azure CLI. This plugin use Azure CLI for authentication, please make sure you have Azure CLI installed and logged in.

  ### Optionally open the folder in the .devcontainer
  - Install the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension
  - Press F1 and select `Dev Containers: Open Folder In Container`

## Create Table in Snowflake
```sql
CREATE OR REPLACE TABLE TESTINGDB.PUBLIC.STREAMINGDATA (
    JSONVALUE variant
);
```

## Local Setup

- ```cmd
    az login
    az account set -s <your subscription id>
    ```
- Rename local.settings.json.template to local.settings.json
- Update local.settings.json, if running locally, and the Application settings in Azure portal, if deploying, with the required parameters as below
  - AzureWebJobsStorage: Connection string to your storage account
  - AzureWebJobsEventHubSender: Connection string to your eventhub
  - AzureWebJobsEventHubName: The name of the eventhub
  - SNOWPIPE_CLIENT_ACCOUNT: Snowflake account ex. "wp48969.west-us-2.azure"
  - SNOWPIPE_CLIENT_USER: Snowflake user ex. "streamingUser"
  - Snowflake Authentication: Only one of the following authetication secrets is requried
    - SNOWPIPE_CLIENT_PASSWORD: Password of the Snowflake user ex. "streamingUser"
    - SNOWPIPE_CLIENT_PRIVATE_KEY: Private Key for the user ex."MIIEvAIBADANBgk-------NOT-REAL-----lTjivwLBQd- mkVqJBR6T2Xh2eg=="
      - See section below for guidance on creating public and private keys
  - SNOWPIPE_CLIENT_WAREHOUSE: Warehouse Size ex. "XSMALL"
  - SNOWPIPE_CLIENT_ROLE: Role to use ex. "datawriter"
  - SNOWPIPE_DB_SCHEMA_TABLE: Fully qualified table name to land streaming data ex."TESTINGDB.PUBLIC.STREAMINGDATA"
  - SNOWPIPE_TABLE_VARIANT_COLUMN: Column to save the eventhub payload ex. "JSONVALUE"
  - SNOWPIPE_CLIENT_STREAMING_CLIENT: "streamingClient"
  - SNOWPIPE_CLIENT_STREAMING_CHANNEL: "snowplowEvents"


## Running the sample locally

```cmd
./mvnw clean package azure-functions:run
```
  - You can test the HTTP trigger using
    - `curl -X POST -H "Content-Type: application/json" -d '{"name":"yourName 8"}' http://localhost:7071/api/snowpipehttptrigger`
    - `curl -X POST -H "Content-Type: application/json" -d '[{"name":"array 1"},{"name":"array 2"},{"name":"array 3"}]' http://localhost:7071/api/snowpipehttptrigger`
  - You can test the Eventhub Trigger using the Generate Data feature from the Eventhub Blade in the Azure Portal


## Deploy the sample on Azure

  - Modify the pom.xml file and change the function properties like functionAppName.
  - `./mvnw clean package azure-functions:deploy`

> NOTE: please replace '/' with '\\' when you are running on windows.


### Generate Public and Private Keys for Snowflake Authentication
   * From your desktop's Command Line / Terminal window, navigate to your working directory and run these two commands:
      ```bash
      openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
      openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
      ```

   * Run the following command to get the public key value that needs to be updated to the Snowflake user:
      ```bash
      cat rsa_key.pub | tr -d '\n' | tr -d ' ' | sed -e 's/-----BEGINPUBLICKEY-----//' -e 's/-----ENDPUBLICKEY-----//'
      ```
* In Snowflake update your user with the public key copied above:
   ```sql
   alter user <my_user> set rsa_public_key='MIIBIjANBgkqh...';
   ````

## Telemetry
This project collects usage data and sends it to Microsoft to help improve our products and services.
Read Microsoft's [privacy statement](https://privacy.microsoft.com/en-us/privacystatement) to learn more.
If you would like to opt out of sending telemetry data to Microsoft, you can set `allowTelemetry` to false in the plugin configuration.
Please read our [document](https://github.com/microsoft/azure-gradle-plugins/wiki/Configuration) to find more details about *allowTelemetry*.
