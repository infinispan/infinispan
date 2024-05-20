package org.infinispan.query.remote.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.api.Experimental;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.query.core.impl.QueryResultImpl;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.tasks.query.RemoteQueryAccess;

@Scope(Scopes.NAMED_CACHE)
@Experimental
public class RemoteQueryAccessImpl implements RemoteQueryAccess {

   @Inject
   RemoteQueryManager remoteQueryManager;

   @Inject
   SerializationContextRegistry serializationContextRegistry;

   @Inject
   AdvancedCache<?, ?> cache;

   @Override
   public QueryResult<?> executeQuery(String queryString, Map<String, Object> namedParametersMap, Integer offset,
                                      Integer maxResults, Integer hitCountAccuracy, boolean isLocal) throws IOException {
      BaseRemoteQueryManager.QueryResultWithProjection result = remoteQueryManager.localQuery(queryString, namedParametersMap, offset, maxResults,
            hitCountAccuracy, cache, isLocal);

      QueryResult<Object> queryResult = result.queryResult();
      String[] projection = result.projection();
      List<Object> results = extractResults(serializationContextRegistry.getUserCtx(), projection != null, queryResult.list());
      return new QueryResultImpl<>(queryResult.count(), results);
   }

   private static List<Object> extractResults(ImmutableSerializationContext serializationContext, boolean projection,
                                              List<Object> results) throws IOException {
      if (serializationContext == null || results.isEmpty()) {
         return results;
      }

      List<Object> unwrappedResults = new ArrayList<>(results.size());
      if (projection) {
         for (Object result : results) {
            Object[] objects = (Object[]) result;
            for (int i = 0; i < objects.length; i++) {
               objects[i] = unmarshall(serializationContext, objects[i]);
            }
            unwrappedResults.add(objects);
         }
      } else {
         for (Object result : results) {
            unwrappedResults.add(unmarshall(serializationContext, result));
         }
      }
      return unwrappedResults;
   }

   private static Object unmarshall(ImmutableSerializationContext context, Object value) throws IOException {
      return value instanceof byte[] bytes ?
            ProtobufUtil.fromWrappedByteArray(context, bytes) :
            value;
   }
}
