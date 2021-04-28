package site.ycsb.db;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AzureCosmosClientExtension extends AzureCosmosClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureCosmosClientExtension.class);

  private static String ORDER_LIST = "order_list";


  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    CosmosContainer container = AzureCosmosClient.containerCache.get(table);
    if (container == null) {
      container = AzureCosmosClient.database.getContainer(table);
      AzureCosmosClient.containerCache.put(table, container);
    }
    PartitionKey pk = new PartitionKey(key);
    CosmosStoredProcedureRequestOptions options = new CosmosStoredProcedureRequestOptions();
    options.setPartitionKey(pk);

    for (int attempt = 0; attempt < NUM_UPDATE_ATTEMPTS; attempt++) {
      try {

        Map<String, String> params = new HashMap<>();
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        for (Map.Entry<String, ByteIterator> pair : values.entrySet()) {
          node.put(pair.getKey(), pair.getValue().toString());
        }
        List<Object> sproc_args = new ArrayList<>();
        sproc_args.add(key);
        sproc_args.add(node);

        CosmosStoredProcedureResponse executeResponse = container.getScripts().getStoredProcedure("updateSproc")
            .execute(sproc_args, options);

        if (executeResponse.getStatusCode() == 200) {
          return Status.OK;
        }

      } catch (CosmosException e) {
        LOGGER.error("Failed to update key {} to collection {} in database {} on attempt {}", key, table,
            AzureCosmosClient.databaseName, attempt, e);
      }
    }

    return Status.ERROR;
  }

  @Override
  public Status query1(String table, String filterfield, String filtervalue, int offset, int recordcount,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    table = "customerq1";
    CosmosContainer container = AzureCosmosClient.containerCache.get(table);
    if (container == null) {
      container = AzureCosmosClient.database.getContainer(table);
      AzureCosmosClient.containerCache.put(table, container);
    }

    try {
      CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
      queryOptions.setQueryMetricsEnabled(false);

      ArrayList<SqlParameter> paramList = new ArrayList<SqlParameter>();
      paramList.add(new SqlParameter("@filtervalue", filtervalue));
      paramList.add(new SqlParameter("@offsetvalue", offset));
      paramList.add(new SqlParameter("@recordcount", recordcount));
      SqlQuerySpec querySpec = new SqlQuerySpec(
          "SELECT c.id FROM root c WHERE c.address.country = @filtervalue OFFSET @offsetvalue LIMIT @recordcount",
          paramList);

      CosmosPagedIterable<ObjectNode> filteredValues = container.queryItems(querySpec, new CosmosQueryRequestOptions(), ObjectNode.class);

      for (FeedResponse<ObjectNode> objectNodeFeedResponse : filteredValues
          .iterableByPage(AzureCosmosClient.preferredPageSize)) {
        List<ObjectNode> pageDocs = objectNodeFeedResponse.getResults();
        for (ObjectNode doc : pageDocs) {
          Map<String, String> stringResults = new HashMap<String, String>(1) {{
            put("id", doc.get("id").toString());
          }};
          HashMap<String, ByteIterator> byteResults = new HashMap<>(doc.size());
          StringByteIterator.putAllAsByteIterators(byteResults, stringResults);
          result.add(byteResults);
        }
      }
      return Status.OK;
    } catch (CosmosException e) {
      LOGGER.error("ERROR {}", e.getMessage());
      return Status.NOT_FOUND;
    }
    //return super.query1(table, filterfield, filtervalue, offset, recordcount, fields, result);
  }

  @Override
  public Status query2(String table, String filterfield1, String filtervalue1, String filterfield2, String filtervalue2, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String addressZip = filtervalue1;
    String month = filtervalue2;
    CosmosContainer customerContainer = getContainerByTableName("customerq2");
    CosmosContainer orderContainer = getContainerByTableName("orderq2");
    CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
    queryOptions.setQueryMetricsEnabled(false);

    try {
      ArrayList<SqlParameter> customerParamList = new ArrayList<>();
      customerParamList.add(new SqlParameter("@addressZip", addressZip));
      SqlQuerySpec customerQuerySpec = new SqlQuerySpec(
          "SELECT c.order_list FROM root c " +
              "WHERE c.address.zip = @addressZip", customerParamList);

      CosmosPagedIterable<ObjectNode> customerValues = customerContainer.queryItems(customerQuerySpec, queryOptions, ObjectNode.class);
      customerValues.forEach(jsonNodes -> {
        ObjectNode doc = jsonNodes;
        //for (ObjectNode doc : pageDocs) {
          Double salePrice = 0D;
          List<String> orderList = new ArrayList<>();
          JsonNode order_listNode = doc.get("order_list");
          for (final JsonNode objNode : order_listNode) {
            orderList.add(objNode.toString());
          }
          if (!orderList.isEmpty()) {
            ArrayList<SqlParameter> orderParamList = new ArrayList<SqlParameter>();
            orderParamList.add(new SqlParameter("@monthFilter", month));
            //orderParamList.add(new SqlParameter("@orderListFilter", String.join(",", orderList)));
            SqlQuerySpec orderQuerySpec = new SqlQuerySpec(
                "SELECT SUM(o.sale_price) as salePrice FROM root o " +
                    " WHERE o.month = @monthFilter" +
                    " AND o.id IN (" + String.join(",", orderList) + ")"// +
                , orderParamList);

            CosmosPagedIterable<ObjectNode> orderValues = orderContainer.queryItems(orderQuerySpec, queryOptions, ObjectNode.class);
/*            orderValues.iterableByPage()
            orderValues.forEach(orderNode -> {
              salePrice.
              salePrice += orderNode.asDouble();
            });*/
            for (FeedResponse<ObjectNode> objectNodeOrderResponse : orderValues.iterableByPage(AzureCosmosClient.preferredPageSize)) {
              List<ObjectNode> pageOrders = objectNodeOrderResponse.getResults();
              for (ObjectNode order : pageOrders) {
                salePrice += order.asDouble();
              }
            }
          }
          Map<String, String> stringResults = new HashMap<String, String>(3) {{
            put(filterfield1, filtervalue1);
            put(filterfield2, filtervalue2);
          }};
          stringResults.put("salePrice", String.valueOf(salePrice));
          HashMap<String, ByteIterator> byteResults = new HashMap<>();
          StringByteIterator.putAllAsByteIterators(byteResults, stringResults);
          result.add(byteResults);

      });
      /*for (FeedResponse<ObjectNode> objectNodeFeedResponse : customerValues.iterableByPage(AzureCosmosClient.preferredPageSize)) {
        List<ObjectNode> pageDocs = objectNodeFeedResponse.getResults();
        for (ObjectNode doc : pageDocs) {
          double salePrice = 0;
          List<String> orderList = new ArrayList<>();
          JsonNode order_listNode = doc.get("order_list");
          for (final JsonNode objNode : order_listNode) {
            orderList.add(objNode.toString());
          }
          if (!orderList.isEmpty()) {
            ArrayList<SqlParameter> orderParamList = new ArrayList<SqlParameter>();
            orderParamList.add(new SqlParameter("@monthFilter", month));
            //orderParamList.add(new SqlParameter("@orderListFilter", String.join(",", orderList)));
            SqlQuerySpec orderQuerySpec = new SqlQuerySpec(
                "SELECT SUM(o.sale_price) as salePrice FROM root o " +
                    //" WHERE o.id IN ("+  String.join(",", orderList) +")"
                    " WHERE o.month = @monthFilter" +
                    //" AND ARRAY_CONTAINS(@orderListFilter, o.id)" +
                    " AND o.id IN (" + String.join(",", orderList) + ")"// +
                //" WHERE o.month = @monthFilter"  +
                //" GROUP BY o.month"  +
                //" ORDER BY SUM(o.sale_price)"
                , orderParamList);

            CosmosPagedIterable<ObjectNode> orderValues = orderContainer.queryItems(orderQuerySpec, queryOptions, ObjectNode.class);
            for (FeedResponse<ObjectNode> objectNodeOrderResponse : orderValues.iterableByPage(AzureCosmosClient.preferredPageSize)) {
              List<ObjectNode> pageOrders = objectNodeOrderResponse.getResults();
              for (ObjectNode order : pageOrders) {
                salePrice += order.asDouble();
              }
            }
          }
          Map<String, String> stringResults = new HashMap<String, String>(3) {{
            put(filterfield1, filtervalue1);
            put(filterfield2, filtervalue2);
          }};
          stringResults.put("salePrice", String.valueOf(salePrice));
          HashMap<String, ByteIterator> byteResults = new HashMap<>();
          StringByteIterator.putAllAsByteIterators(byteResults, stringResults);
          result.add(byteResults);
        }
      }*/
      return Status.OK;
    } catch (CosmosException e) {
      LOGGER.error("ERROR {}", e.getMessage());
      return Status.ERROR;
    }
  }

  private CosmosContainer getContainerByTableName(String table) {
    CosmosContainer container = AzureCosmosClient.containerCache.get(table);
    if (container == null) {
      container = AzureCosmosClient.database.getContainer(table);
      AzureCosmosClient.containerCache.put(table, container);
    }
    return container;
  }
}
