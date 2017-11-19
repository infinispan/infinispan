package org.infinispan.query.remote.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;
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
      BaseRemoteQueryEngine queryEngine = SecurityActions.getCacheComponentRegistry(cache).getComponent(BaseRemoteQueryEngine.class);
      if (queryEngine == null) {
         throw log.queryingNotEnabled(cache.getName());
      }

      // see if we have a non-protobuf compatibility marshaller and use it, otherwise use protobuf
      Marshaller compatMarshaller = null;
      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache);
      if (cacheConfiguration.compatibility().enabled()) {
         Marshaller marshaller = cacheConfiguration.compatibility().marshaller();
         if (!(marshaller instanceof BaseProtoStreamMarshaller)) {
            compatMarshaller = marshaller;
         }
      }

      try {
         // decode the query request object
         QueryRequest request;

         if (compatMarshaller != null) {
            try {
               request = (QueryRequest) compatMarshaller.objectFromByteBuffer(query);
            } catch (ClassNotFoundException e) {
               throw log.errorExecutingQuery(e);
            }
         } else {
            request = ProtobufUtil.fromByteArray(queryEngine.getSerializationContext(), query, 0, query.length, QueryRequest.class);
         }

         long startOffset = request.getStartOffset() == null ? -1 : request.getStartOffset();
         int maxResults = request.getMaxResults() == null ? -1 : request.getMaxResults();

         // create the query
         BaseQuery q = queryEngine.makeQuery(request.getQueryString(), request.getNamedParametersMap(), startOffset, maxResults);

         // execute query and make the response object
         QueryResponse response = makeResponse(q);

         byte[] responseBytes;

         if (compatMarshaller != null) {
            try {
               responseBytes = compatMarshaller.objectToByteBuffer(response);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw log.errorExecutingQuery(e);
            }
         } else {
            responseBytes = ProtobufUtil.toByteArray(queryEngine.getSerializationContext(), response);
         }

         return responseBytes;
      } catch (IOException e) {
         if (log.isDebugEnabled()) {
            log.debug(e.getMessage(), e);
         }
         throw log.errorExecutingQuery(e);
      } catch (Exception e) {
         if (log.isDebugEnabled()) {
            log.debugf(e, "Error executing remote query : %s", e.getMessage());
         }
         throw e;
      }
   }

   private QueryResponse makeResponse(BaseQuery query) {
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
