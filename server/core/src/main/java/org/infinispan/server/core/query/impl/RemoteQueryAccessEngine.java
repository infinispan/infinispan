package org.infinispan.server.core.query.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.api.Experimental;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.QueryConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.query.core.impl.QueryResultImpl;
import org.infinispan.server.core.query.impl.access.RemoteQueryAccessQuery;
import org.infinispan.server.core.query.impl.logging.Log;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.tasks.query.RemoteQueryAccess;

@Scope(Scopes.NAMED_CACHE)
@Experimental
public class RemoteQueryAccessEngine implements RemoteQueryAccess {

   private static final Log log = LogFactory.getLog(RemoteQueryAccessEngine.class, Log.class);

   @Inject
   RemoteQueryManager remoteQueryManager;

   @Inject
   SerializationContextRegistry serializationContextRegistry;

   @Inject
   AdvancedCache<?, ?> cache;

   private int defaultMaxResults;

   private int defaultHitCountAccuracy;

   @Start
   public void start() {
      Configuration configuration = SecurityActions.getCacheConfiguration(cache);
      QueryConfiguration query = configuration.query();
      defaultMaxResults = query.defaultMaxResults();
      defaultHitCountAccuracy = query.hitCountAccuracy();
   }

   @Override
   public <T> Query<T> query(String query) {
      return new RemoteQueryAccessQuery<>(this, query);
   }

   public int defaultMaxResult() {
      return defaultMaxResults;
   }

   public int defaultHitCountAccuracy() {
      return defaultHitCountAccuracy;
   }

   public QueryResult<?> executeQuery(String queryString, Map<String, Object> namedParametersMap, Integer offset,
                                      Integer maxResults, Integer hitCountAccuracy, boolean isLocal) {
      BaseRemoteQueryManager.QueryResultWithProjection result = remoteQueryManager.localQuery(
            queryString, namedParametersMap, offset, maxResults, hitCountAccuracy, cache, isLocal);
      QueryResult<Object> queryResult = result.queryResult();
      String[] projection = result.projection();
      List<Object> results = extractResults(serializationContextRegistry.getUserCtx(), projection != null, queryResult.list());
      return new QueryResultImpl<>(queryResult.count(), results);
   }

   private static List<Object> extractResults(ImmutableSerializationContext serializationContext, boolean projection,
                                              List<Object> results) {
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

   private static Object unmarshall(ImmutableSerializationContext context, Object value) {
      try {
         return value instanceof byte[] bytes ?
               ProtobufUtil.fromWrappedByteArray(context, bytes) :
               value;
      } catch (IOException e) {
         log.errorUnmarshallingProtobufEntity(e);
         return value;
      }
   }
}
