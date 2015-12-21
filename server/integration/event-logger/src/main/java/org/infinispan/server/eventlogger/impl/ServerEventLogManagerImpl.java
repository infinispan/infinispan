package org.infinispan.server.eventlogger.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.eventlogger.ServerEventLogManager;
import org.infinispan.util.logging.events.EventLog;

/**
 *
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@Scope(Scopes.GLOBAL)
public class ServerEventLogManagerImpl implements ServerEventLogManager {
   public static final String EVENT_LOG_CACHE = "___event_log_cache";
   private EmbeddedCacheManager cacheManager;
   private QueryFactory<Query> queryFactory;
   private DefaultExecutorService distExec;

   @Inject
   public void initialize(final EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      internalCacheRegistry.registerInternalCache(EVENT_LOG_CACHE, getTaskHistoryCacheConfiguration(cacheManager).build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));
   }

   private ConfigurationBuilder getTaskHistoryCacheConfiguration(EmbeddedCacheManager cacheManager) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.eviction().size(100l).persistence().passivation(true).expiration().lifespan(7, TimeUnit.DAYS);
      return cfg;
   }

   @Override
   public List<EventLog> getEvents(int start, int count) {
      Query query = getQueryFactory().from(ServerEventImpl.class).orderBy("when", SortOrder.DESC).maxResults(count).startOffset(start).build();
      return query.list();
   }

   private QueryFactory<Query> getQueryFactory() {
      if (queryFactory == null) {
         Cache<Object, Object> cache = cacheManager.getCache(EVENT_LOG_CACHE);
         queryFactory = Search.getQueryFactory(cache);
         distExec = new DefaultExecutorService(cache);
      }
      return queryFactory;
   }


}
