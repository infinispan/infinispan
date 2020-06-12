package org.infinispan.query.remote.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.remote.client.impl.QueryRequest;
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

   @Override
   public byte[] query(AdvancedCache<?, ?> cache, byte[] query) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
      RemoteQueryManager remoteQueryManager = SecurityActions.getRemoteQueryManager(cache);
      if (remoteQueryManager.getQueryEngine(cache) == null) {  //todo [anistor] remoteQueryManager should be null if not queryable
         throw log.queryingNotEnabled(cache.getName());
      }

      try {
         MediaType requestMediaType = cache.getValueDataConversion().getRequestMediaType();
         QueryRequest request = remoteQueryManager.decodeQueryRequest(query, requestMediaType);

         int startOffset = request.getStartOffset().intValue();
         int maxResults = request.getMaxResults();

         IndexedQueryMode queryMode = request.getIndexedQueryMode() != null ?
               IndexedQueryMode.valueOf(request.getIndexedQueryMode()) : null;

         return remoteQueryManager.executeQuery(request.getQueryString(),
               request.getNamedParametersMap(), startOffset, maxResults, queryMode, cache, requestMediaType);
      } catch (Exception e) {
         if (log.isDebugEnabled()) {
            log.debugf(e, "Error executing remote query : %s", e.getMessage());
         }
         throw e;
      }
   }
}
