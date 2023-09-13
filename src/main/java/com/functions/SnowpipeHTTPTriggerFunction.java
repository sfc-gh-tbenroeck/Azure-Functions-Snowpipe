/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FixedDelayRetry;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ArrayNode;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Azure Functions with HTTP Trigger.
 */
public class SnowpipeHTTPTriggerFunction {
    // Define client and channel as static
    private static SnowflakeStreamingIngestClient client;
    private static SnowflakeStreamingIngestChannel channel;

    @FunctionName("snowpipehttptrigger")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {
                    HttpMethod.POST }, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        java.util.logging.Logger contextLogger = context.getLogger();
        contextLogger.info("Java HTTP trigger processed a request.");

        // Parse JSON body
        String body = request.getBody().orElse(null);
        if (body == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a JSON in the request body")
                    .build();
        } else {
            try {
                initializeClientAndChannel(contextLogger);
                String variantColumn = System.getenv("SNOWPIPE_TABLE_VARIANT_COLUMN");
                if (variantColumn != null) {
                    variantColumn = "jsonValue";
                }

                List<Map<String, Object>> rowsBatch = new ArrayList<>();
                // Use Jackson to parse the body
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(body);

                 // Check if the root node is an array
                if (rootNode.isArray()) {
                    ArrayNode arrayNode = (ArrayNode) rootNode;
                    for (int i = 0; i < arrayNode.size(); i++) {
                        JsonNode itemNode = arrayNode.get(i);
                        String item = itemNode.toString();
                        rowsBatch.add(createRow(item, variantColumn));
                    }
                } else {
                    rowsBatch.add(createRow(body, variantColumn));
                }
            sendToSnowpipeBatch(contextLogger, rowsBatch);

            } catch (Exception e) {
                contextLogger.severe("error:" + e.getMessage());
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Failed to send data to Snowpipe: " + e.getMessage()).build();
            }

            return request.createResponseBuilder(HttpStatus.OK).body("Successfully sent data to Snowpipe").build();
        }

    }

    private Map<String, Object> createRow(String json, String variantColumn) {
        Map<String, Object> row = new HashMap<>();
        row.put(variantColumn, json);
        return row;
    }

    private void sendToSnowpipeBatch(java.util.logging.Logger logger, List<Map<String, Object>> rowsBatch) {
        String batchUUID = UUID.randomUUID().toString();
        logger.info("Sending batch with UUID: " + batchUUID);
        InsertValidationResponse response = channel.insertRows(rowsBatch, batchUUID);

        if (response.hasErrors()) {
            logger.severe("Failed to send data to Snowpipe: " + response.getInsertErrors().get(0).getException());
        }
    }

    private void initializeClientAndChannel(java.util.logging.Logger logger) throws Exception {
        // Check if client and channel are null
        if (client == null || channel == null) {
            Properties connectionDetails = new Properties();

            // Fetch values from environment variables
            String account = System.getenv("SNOWPIPE_CLIENT_ACCOUNT");
            String user = System.getenv("SNOWPIPE_CLIENT_USER");
            String password = System.getenv("SNOWPIPE_CLIENT_PASSWORD");
            String private_key = System.getenv("SNOWPIPE_CLIENT_PRIVATE_KEY");

            String warehouse = System.getenv("SNOWPIPE_CLIENT_WAREHOUSE");
            String role = System.getenv("SNOWPIPE_CLIENT_ROLE");

            String streamingClient = System.getenv("SNOWPIPE_CLIENT_STREAMING_CLIENT");
            String streamingChannel = System.getenv("SNOWPIPE_CLIENT_STREAMING_CHANNEL");

            String snowpipeTable = System.getenv("SNOWPIPE_DB_SCHEMA_TABLE");

            String host = account + ".snowflakecomputing.com";
            String baseURL = "https://" + host + ":443";
            String connect_string = "jdbc:snowflake://" + baseURL;

            // Split the SNOWPIPE_TABLE environment variable into database, schema, and
            // table
            if (snowpipeTable != null && snowpipeTable.split("\\.").length == 3) {
                String[] tableDetails = snowpipeTable.split("\\.");
                String database = tableDetails[0];
                String schema = tableDetails[1];
                String table = tableDetails[2];

                // Set properties
                connectionDetails.setProperty("account", account);

                connectionDetails.setProperty("user", user);
                if (private_key != null) {
                    connectionDetails.setProperty("private_key", private_key);
                }

                if (password != null) {
                    connectionDetails.setProperty("password", password);
                }

                connectionDetails.setProperty("url", baseURL);
                connectionDetails.setProperty("host", host);
                connectionDetails.setProperty("database", database);
                connectionDetails.setProperty("schema", schema);
                connectionDetails.setProperty("table", table);

                if (streamingClient == null) {
                    // set a default
                    connectionDetails.setProperty("streamingClient", "streamingClient");
                }
                if (streamingChannel == null) {
                    // set a default
                    connectionDetails.setProperty("streamingChannel", "streamingChannel");
                }
                if (warehouse != null) {
                    connectionDetails.setProperty("warehouse", warehouse);
                }

                if (role != null) {
                    connectionDetails.setProperty("role", role);
                }

                connectionDetails.setProperty("connect_string", connect_string);
                connectionDetails.setProperty("ssl", "on");
                connectionDetails.setProperty("port", "443");
                connectionDetails.setProperty("scheme", "https");

                logger.info("Properties Set");

                client = SnowflakeStreamingIngestClientFactory
                        .builder(streamingClient)
                        .setProperties(connectionDetails)
                        .build();

                logger.info("Client Created");

                // Create an OpenChannelRequest to send data
                OpenChannelRequest requestChannel = OpenChannelRequest
                        .builder(streamingClient)
                        .setDBName(database)
                        .setSchemaName(schema)
                        .setTableName(table)
                        .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                        .build();

                channel = client.openChannel(requestChannel);

                logger.info("requestChannel created");
            } else {
                logger.severe("Invalid SNOWPIPE_TABLE format. Expected format: database.schema.table");
            }
        }
    }
}
