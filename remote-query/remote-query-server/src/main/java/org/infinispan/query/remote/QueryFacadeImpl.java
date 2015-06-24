package org.infinispan.query.remote;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.logging.Log;
import org.infinispan.server.core.QueryFacade;
import org.kohsuke.MetaInfServices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A query facade implementation for both Lucene based queries and non-indexed queries.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices
public final class QueryFacadeImpl implements QueryFacade {

   private static final Log log = LogFactory.getLog(QueryFacadeImpl.class, Log.class);

   /**
    * A special hidden Lucene document field that holds the actual protobuf type name.
    */
   public static final String TYPE_FIELD_NAME = "$type$";

   /**
    * A special placeholder value that is indexed if the actual field value is null. This placeholder is needed because
    * Lucene does not index null values.
    */
   public static final String NULL_TOKEN = "_null_";

   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {
      try {
         SerializationContext serCtx = ProtobufMetadataManager.getSerializationContextInternal(cache.getCacheManager());
         QueryRequest request = ProtobufUtil.fromByteArray(serCtx, query, 0, query.length, QueryRequest.class);

         Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache);
         boolean isIndexed = cacheConfiguration.indexing().index().isEnabled();
         boolean isCompatMode = cacheConfiguration.compatibility().enabled();
         SearchManager searchManager = isIndexed ? Search.getSearchManager(cache) : null;  // this also checks access permissions

         Query q = new RemoteQueryEngine(cache, searchManager, isCompatMode, serCtx)
               .buildQuery(null, request.getJpqlString(), request.getStartOffset(), request.getMaxResults());

         QueryResponse response = makeResponse(q);
         return ProtobufUtil.toByteArray(serCtx, response);
      } catch (IOException e) {
         throw log.errorExecutingQuery(e);
      }
   }

   private QueryResponse makeResponse(Query q) {
      List<?> list = q.list();
      int numResults = list.size();
      String[] projection = ((BaseQuery) q).getProjection();
      int projSize = projection != null ? projection.length : 0;
      List<WrappedMessage> results = new ArrayList<WrappedMessage>(projSize == 0 ? numResults : numResults * projSize);

      for (Object o : list) {
         if (projSize == 0) {
            results.add(new WrappedMessage(o));
         } else {
            Object[] row = (Object[]) o;
            for (int j = 0; j < projSize; j++) {
               results.add(new WrappedMessage(row[j]));
            }
         }
      }

      QueryResponse response = new QueryResponse();
      response.setTotalResults(q.getResultSize());
      response.setNumResults(numResults);
      response.setProjectionSize(projSize);
      response.setResults(results);
      return response;
   }
}
