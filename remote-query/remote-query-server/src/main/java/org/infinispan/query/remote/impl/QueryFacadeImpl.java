package org.infinispan.query.remote.impl;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.core.QueryFacade;
import org.kohsuke.MetaInfServices;

/**
 * A query facade implementation for both Lucene based queries and non-indexed in-memory queries. All work is delegated
 * to {@link RemoteQueryEngine}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices
public final class QueryFacadeImpl implements QueryFacade {

   private static final Log log = LogFactory.getLog(QueryFacadeImpl.class, Log.class);

   /**
    * A special 'hidden' Lucene document field that holds the actual protobuf type name.
    */
   public static final String TYPE_FIELD_NAME = "$type$";

   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
      RemoteQueryManager remoteQueryManager = SecurityActions.getRemoteQueryManager(cache);
      BaseRemoteQueryEngine queryEngine = remoteQueryManager.getQueryEngine();
      if (queryEngine == null) {
         throw log.queryingNotEnabled(cache.getName());
      }

      try {
         QueryRequest request = remoteQueryManager.decodeQueryRequest(query);

         long startOffset = request.getStartOffset() == null ? -1 : request.getStartOffset();
         int maxResults = request.getMaxResults() == null ? -1 : request.getMaxResults();

         // create the query
         Query q = queryEngine.makeQuery(request.getQueryString(), request.getNamedParametersMap(), startOffset, maxResults);

         // execute query and make the response object
         QueryResponse response = makeResponse(q);
         return remoteQueryManager.encodeQueryResponse(response);
      } catch (Exception e) {
         if (log.isDebugEnabled()) {
            log.debugf(e, "Error executing remote query : %s", e.getMessage());
         }
         throw e;
      }
   }

   private QueryResponse makeResponse(Query query) {
      List<?> list = query.list();
      int numResults = list.size();
      String[] projection = query.getProjection();
      int projSize = projection != null ? projection.length : 0;
      List<WrappedMessage> results = new ArrayList<>(projSize == 0 ? numResults : numResults * projSize);

      for (Object o : list) {
         if (projSize == 0) {
            results.add(new WrappedMessage(o));
         } else {
            Object[] row = (Object[]) o;
            for (int i = 0; i < projSize; i++) {
               results.add(new WrappedMessage(row[i]));
            }
         }
      }

      QueryResponse response = new QueryResponse();
      response.setTotalResults(query.getResultSize());
      response.setNumResults(numResults);
      response.setProjectionSize(projSize);
      response.setResults(results);
      return response;
   }
}
