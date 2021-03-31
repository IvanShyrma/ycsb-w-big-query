/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2016 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.yahoo.ycsb.db;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.yahoo.ycsb.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * DynamoDB v1.10.48 client for YCSB.
 */

public class DynamoDBClient extends DB {


  static Set<String> FIELDS_SCAN = Collections.singleton("firstname");

  /**
   * Defines the primary key type used in this particular DB instance.
   * <p>
   * By default, the primary key type is "HASH". Optionally, the user can
   * choose to use hash_and_range key type. See documentation in the
   * DynamoDB.Properties file for more details.
   */
  private enum PrimaryKeyType {
    HASH,
    HASH_AND_RANGE
  }

  private AmazonDynamoDBClient dynamoDB;
  private String primaryKeyName;
  private PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;
  private DynamoDB dynamoDBClient;

  // If the user choose to use HASH_AND_RANGE as primary key type, then
  // the following two variables become relevant. See documentation in the
  // DynamoDB.Properties file for more details.
  private String hashKeyValue;
  private String hashKeyName;

  private boolean consistentRead = false;
  private String endpoint = "http://dynamodb.us-east-1.amazonaws.com";
  private int maxConnects = 50;
  private static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("dynamodb.debug", null);

    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String configuredEndpoint = getProperties().getProperty("dynamodb.endpoint", null);
    String credentialsFile = getProperties().getProperty("dynamodb.awsCredentialsFile", null);
    String primaryKey = getProperties().getProperty("dynamodb.primaryKey", null);
    String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", null);
    String consistentReads = getProperties().getProperty("dynamodb.consistentReads", null);
    String connectMax = getProperties().getProperty("dynamodb.connectMax", null);

    if (null != connectMax) {
      this.maxConnects = Integer.parseInt(connectMax);
    }

    if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
      this.consistentRead = true;
    }

    if (null != configuredEndpoint) {
      this.endpoint = configuredEndpoint;
    }

    if (null == primaryKey || primaryKey.length() < 1) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }

    if (null != primaryKeyTypeString) {
      try {
        this.primaryKeyType = PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid primary key mode specified: " + primaryKeyTypeString +
            ". Expecting HASH or HASH_AND_RANGE.");
      }
    }

    if (this.primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // Wxhen the primary key type is HASH_AND_RANGE, keys used by YCSB
      // are range keys so we can benchmark performance of individual hash
      // partitions. In this case, the user must specify the hash key's name
      // and optionally can designate a value for the hash key.

      String configuredHashKeyName = getProperties().getProperty("dynamodb.hashKeyName", null);
      if (null == configuredHashKeyName || configuredHashKeyName.isEmpty()) {
        throw new DBException("Must specify a non-empty hash key name when the primary key type is HASH_AND_RANGE.");
      }
      this.hashKeyName = configuredHashKeyName;
      this.hashKeyValue = getProperties().getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
    }

    try {
      AWSCredentials credentials = new PropertiesCredentials(new File(credentialsFile));
      ClientConfiguration cconfig = new ClientConfiguration();
      cconfig.setMaxConnections(maxConnects);
      dynamoDB = new AmazonDynamoDBClient(credentials, cconfig);
      dynamoDB.setEndpoint(this.endpoint);

      BasicAWSCredentials awsCreds = new BasicAWSCredentials(credentials.getAWSAccessKeyId(),
          credentials.getAWSSecretKey());
      AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
          .withEndpointConfiguration(new AwsClientBuilder
              .EndpointConfiguration(this.endpoint, "us-east-1"))
          .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
          .build();
      dynamoDBClient = new DynamoDB(client);

      primaryKeyName = primaryKey;
      LOGGER.info("dynamodb connection created with " + this.endpoint);
    } catch (Exception e1) {
      LOGGER.error("DynamoDBClient.init(): Could not initialize DynamoDB client.", e1);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("readkey: " + key + " from table: " + table);
    }

    GetItemRequest req = new GetItemRequest(table, createPrimaryKey(key));
    req.setAttributesToGet(fields);
    req.setConsistentRead(consistentRead);
    GetItemResult res;
    System.out.println("READ---------");
    System.out.println(req.toString());
    try {
      res = dynamoDB.getItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (null != res.getItem()) {
      result.putAll(extractResult(res.getItem()));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Result: " + res.toString());
      }
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
    }

    /*
     * on DynamoDB's scan, startkey is *exclusive* so we need to
     * getItem(startKey) and then use scan for the res
     * REMOVE?
    */
    /*GetItemRequest greq = new GetItemRequest(table, createPrimaryKey(startkey));
    greq.setAttributesToGet(FIELDS_SCAN);

    System.out.println("SCAN1---------");
    System.out.println(greq.toString());
    GetItemResult gres;

    try {
      gres = dynamoDB.getItem(greq);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (null != gres.getItem()) {
      result.add(extractResult(gres.getItem()));
    }*/

    int count = 1; // startKey is done, rest to go.

    /*
        Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#keyid", "_id");
    req.setExpressionAttributeNames(expressionAttributeNames);
    req.setAttributesToGet("#keyid");
    */

    Map<String, AttributeValue> startKey = createPrimaryKey(startkey);
    ScanRequest req = new ScanRequest(table);
    req.setAttributesToGet(FIELDS_SCAN);
    System.out.println("SCAN2---------");
    System.out.println(req.toString());
    while (count < recordcount) {
      req.setExclusiveStartKey(startKey);
      req.setLimit(recordcount - count);
      ScanResult res;
      try {
        res = dynamoDB.scan(req);
      } catch (AmazonServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (AmazonClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }

      count += res.getCount();
      for (Map<String, AttributeValue> items : res.getItems()) {
        result.add(extractResult(items));
      }
      startKey = res.getLastEvaluatedKey();

    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: " + key + " from table: " + table);
    }

    Map<String, AttributeValueUpdate> attributes = new HashMap<>(values.size());
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      AttributeValue v = new AttributeValue(val.getValue().toString());
      attributes.put(val.getKey(), new AttributeValueUpdate().withValue(v).withAction("PUT"));
    }

    UpdateItemRequest req = new UpdateItemRequest(table, createPrimaryKey(key), attributes);
    System.out.println("UPDATE---------");
    System.out.println(req.toString());
    try {
      dynamoDB.updateItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
    }

    Map<String, AttributeValue> attributes = createAttributes(values);
    // adding primary key
    attributes.put(primaryKeyName, new AttributeValue(key));
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // If the primary key type is HASH_AND_RANGE, then what has been put
      // into the attributes map above is the range key part of the primary
      // key, we still need to put in the hash key part here.
      attributes.put(hashKeyName, new AttributeValue(hashKeyValue));
    }

    PutItemRequest putItemRequest = new PutItemRequest(table, attributes);
    try {
      dynamoDB.putItem(putItemRequest);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }

    DeleteItemRequest req = new DeleteItemRequest(table, createPrimaryKey(key));

    try {
      dynamoDB.deleteItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  private static Map<String, AttributeValue> createAttributes(Map<String, ByteIterator> values) {
    Map<String, AttributeValue> attributes = new HashMap<>(values.size() + 1);
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      attributes.put(val.getKey(), new AttributeValue(val.getValue().toString()));
    }
    return attributes;
  }

  @Override
  public Status query1(String table, String filterfield, String filtervalue, int offset, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    //Table dtable = dynamoDBClient.getTable(table);

    //fields = new HashSet<>();
    //fields.add("_id");
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":zipkey", new AttributeValue().withS(filtervalue));

    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#f1", "address");
    expressionAttributeNames.put("#f2", "zip");
    expressionAttributeNames.put("#keyid", "_id");

    int count = 1;
    Map<String, AttributeValue> startKey = null;
    ScanRequest req = new ScanRequest(table);
    //req.setAttributesToGet(fields);
    req.setProjectionExpression("#keyid");
    //System.out.println(fields);
    req.setFilterExpression("#f1.#f2 = :countrykey");
    //req.setLimit(recordcount);

    req.setExpressionAttributeNames(expressionAttributeNames);
    req.setExpressionAttributeValues(expressionAttributeValues);

    while (count < recordcount) {
      req.setExclusiveStartKey(startKey);
      req.setLimit(recordcount - count);
      ScanResult res;
      try {
        System.out.println(req);
        //System.out.println(expressionAttributeValues);
        res = dynamoDB.scan(req);
      } catch (AmazonServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (AmazonClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }
      count += res.getCount();
      for (Map<String, AttributeValue> items : res.getItems()) {
        result.add(extractResult(items));
      }
      startKey = res.getLastEvaluatedKey();
    }

    // parallelScan("customer", 10000, 512, result, filtervalue);

    return Status.OK;
  }


  @Override
  public Status query2(String table, String filterfield1, String filtervalue1, String filterfield2, String filtervalue2,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    //Table dtable = dynamoDBClient.getTable(table);

    //fields = new HashSet<>();
    //fields.add("_id");
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":countrykey", new AttributeValue().withS(filtervalue1));

    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#f1", "address");
    expressionAttributeNames.put("#f2", "country");
    expressionAttributeNames.put("#keyid", "_id");

    int count = 1;
    Map<String, AttributeValue> startKey = null;
    ScanRequest req = new ScanRequest(table);
    //req.setAttributesToGet(fields);
    req.setProjectionExpression("#keyid");
    //System.out.println(fields);
    req.setFilterExpression("#f1.#f2 = :countrykey");
    //req.setLimit(recordcount);

    req.setExpressionAttributeNames(expressionAttributeNames);
    req.setExpressionAttributeValues(expressionAttributeValues);

    while (count < recordcount) {
      req.setExclusiveStartKey(startKey);
      req.setLimit(recordcount - count);
      ScanResult res;
      try {
        System.out.println(req);
        //System.out.println(expressionAttributeValues);
        res = dynamoDB.scan(req);
      } catch (AmazonServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (AmazonClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }
      count += res.getCount();
      for (Map<String, AttributeValue> items : res.getItems()) {
        result.add(extractResult(items));
      }
      startKey = res.getLastEvaluatedKey();
    }

    // parallelScan("customer", 10000, 512, result, filtervalue);

    return Status.OK;
  }

  private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.size());

    for (Entry<String, AttributeValue> attr : item.entrySet()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue()));
      }
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().getS()));
    }
    return rItems;
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    Map<String, AttributeValue> k = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      k.put(hashKeyName, new AttributeValue().withS(hashKeyValue));
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else {
      throw new RuntimeException("Assertion Error: impossible primary key type");
    }
    return k;
  }


  // Runnable task for scanning a single segment of a DynamoDB table
  private class ScanSegmentTask implements Callable<List<Map<String, AttributeValue>>> {

    // DynamoDB table to scan
    private String tableName;
    // number of items each scan request should return
    private int itemLimit;
    // Total number of segments
    // Equals to total number of threads scanning the table in parallel
    private int totalSegments;
    // Segment that will be scanned with by this task
    private int segment;

    private String filtervalue;

    List<Map<String, AttributeValue>> list_2 = new ArrayList<>();

    public ScanSegmentTask(String tableName, int itemLimit, int totalSegments, int segment, String filtervalue) {
      this.tableName = tableName;
      this.itemLimit = itemLimit;
      this.totalSegments = totalSegments;
      this.segment = segment;
      this.filtervalue = filtervalue;
    }

    @Override
    public List<Map<String, AttributeValue>> call() {
      // Table table = dynamoDB.getTable(tableName);
      Map<String, AttributeValue> exclusiveStartKey = null;
      try {
        //ScanSpec spec = new ScanSpec().withMaxResultSize(itemLimit).withTotalSegments(totalSegments)
        //    .withSegment(segment);
        //ScanResult res = dynamoDB.scan(req);
        /*Table table = dynamoDBClient.getTable(tableName);
        ItemCollection<ScanOutcome> items = table.scan(spec);
        Iterator<Item> iterator = items.iterator();
        Item currentItem = null;
        while (iterator.hasNext()) {
          totalScannedItemCount++;
          currentItem = iterator.next();
          System.out.println(currentItem.toString());
        }*/
        do {
          Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
          expressionAttributeValues.put(":countrykey", new AttributeValue().withS(filtervalue));

          Map<String, String> expressionAttributeNames = new HashMap<>();
          expressionAttributeNames.put("#f1", "address");
          expressionAttributeNames.put("#f2", "country");
          expressionAttributeNames.put("#keyid", "_id");
          ScanRequest scanRequest = new ScanRequest()
              .withTableName(tableName)
              .withLimit(itemLimit)
              .withExclusiveStartKey(exclusiveStartKey)
              .withTotalSegments(totalSegments)
              .withSegment(segment)
              .withProjectionExpression("#keyid")
              .withFilterExpression("#f1.#f2 = :countrykey")
              .withExpressionAttributeNames(expressionAttributeNames)
              .withExpressionAttributeValues(expressionAttributeValues);

          ScanResult result = dynamoDB.scan(scanRequest);
          list_2.addAll(result.getItems());

          exclusiveStartKey = result.getLastEvaluatedKey();
        } while (exclusiveStartKey != null);
      } catch (Exception e) {
        System.err.println(e.getMessage());
      } finally {
        return list_2;
      }
    }
  }

  private void parallelScan(String tableName, int itemLimit, int numberOfThreads, Vector<HashMap<String, ByteIterator>> result, String filtervalue) {
    ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
    List<Future<List<Map<String, AttributeValue>>>> holdFuture = new ArrayList<>();

    // Divide DynamoDB table into logical segments
    // Create one task for scanning each segment
    // Each thread will be scanning one segment
    int totalSegments = numberOfThreads;
    for (int segment = 0; segment < totalSegments; segment++) {
      // Runnable task that will only scan one segment
      ScanSegmentTask task = new ScanSegmentTask(tableName, itemLimit, totalSegments, segment, filtervalue);

      // Execute the task
      Future<List<Map<String, AttributeValue>>> future = executor.submit(task);
      System.out.println("add to holdFuture");
      holdFuture.add(future);
    }

    for (Future<List<Map<String, AttributeValue>>> listFuture : holdFuture) {
      boolean flag = false;
      while (!flag) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (listFuture.isDone()) {
          System.out.println("listFuture is done!");
          try {
            List<Map<String, AttributeValue>> itemsa = listFuture.get();
            System.out.println("size "+ itemsa.size());
            for (Map<String, AttributeValue> items : itemsa) {
              System.out.println(items);
              result.add(extractResult(items));
            }
          } catch (InterruptedException | ExecutionException e) {
            System.out.println("error item ");
            e.printStackTrace();
          } finally {
            flag = true;
          }
        }
      }
    }
    System.out.println("shutDownExecutorService!");
    shutDownExecutorService(executor);
  }

  private static void shutDownExecutorService(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
    catch (InterruptedException e) {
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    } finally {
      executor.shutdownNow();
      //Thread.currentThread().interrupt();
    }
  }
}
