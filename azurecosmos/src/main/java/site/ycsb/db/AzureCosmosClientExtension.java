package site.ycsb.db;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AzureCosmosClientExtension extends AzureCosmosClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureCosmosClientExtension.class);


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
        /*try {
          sproc_args.add(OBJECT_MAPPER.writeValueAsString(params));
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }*/

        CosmosStoredProcedureResponse executeResponse = container.getScripts().getStoredProcedure("updateSproc")
            .execute(sproc_args, options);

        /*LOGGER.info("sproc_args " + sproc_args);
        LOGGER.info(String.format("Stored procedure returned %s (HTTP %d), at cost %.3f RU.\n",
            executeResponse.getResponseAsString(),
            executeResponse.getStatusCode(),
            executeResponse.getRequestCharge()));*/
        if(executeResponse.getStatusCode() == 200){
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
    CosmosContainer container = AzureCosmosClient.containerCache.get(table);
    if (container == null) {
      container = AzureCosmosClient.database.getContainer(table);
      AzureCosmosClient.containerCache.put(table, container);
    }

    CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
    queryOptions.setQueryMetricsEnabled(false);

    ArrayList<SqlParameter> paramList = new ArrayList<SqlParameter>();
    paramList.add(new SqlParameter("@filtervalue", filtervalue));
    paramList.add(new SqlParameter("@offsetvalue", offset));
    paramList.add(new SqlParameter("@recordcount", recordcount));
    SqlQuerySpec querySpec = new SqlQuerySpec(
        "SELECT * FROM Family WHERE address.country = @filtervalue OFFSET @offsetvalue LIMIT @recordcount",
        paramList);

    CosmosPagedIterable<ObjectNode> filteredValues = container.queryItems(querySpec, new CosmosQueryRequestOptions(), ObjectNode.class);


    return super.query1(table, filterfield, filtervalue, offset, recordcount, fields, result);
  }
}
