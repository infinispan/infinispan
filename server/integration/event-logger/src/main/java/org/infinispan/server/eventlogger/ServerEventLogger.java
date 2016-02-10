package org.infinispan.server.eventlogger;

import java.security.cert.PKIXRevocationChecker;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

/**
 * ServerEventLogger. This event lgoger takes care of maintaining the server event log cache and
 * provides methods for querying its contents across all nodes. For resilience, the event log is
 * stored in a local, bounded, persistent cache and distributed executors are used to gather logs
 * from all the nodes in the cluster.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class ServerEventLogger implements EventLogger {
   public static final String EVENT_LOG_CACHE = "___event_log_cache";
   private final EmbeddedCacheManager cacheManager;
   private final TimeService timeService;
   private Cache<UUID, ServerEventImpl> eventCache;
   private QueryFactory<Query> queryFactory;

   ServerEventLogger(EmbeddedCacheManager cacheManager, TimeService timeService) {
      this.cacheManager = cacheManager;
      this.timeService = timeService;
   }

   private Cache<UUID, ServerEventImpl> getEventCache() {
      if (eventCache == null) {
         eventCache = cacheManager.getCache(EVENT_LOG_CACHE);
      }
      return eventCache;
   }

   @Override
   public void log(EventLogLevel level, EventLogCategory category, String message) {
      textLog(level, category, message);
      eventLog(new ServerEventImpl(level, category, timeService.instant(), message));
   }

   void textLog(EventLogLevel level, EventLogCategory category, String message) {
      LogFactory.getLogger(category.toString()).log(level.toLoggerLevel(), message);
   }

   void eventLog(ServerEventImpl event) {
      getEventCache().putAsync(Util.threadLocalRandomUUID(), event);
   }

   @Override
   public EventLogger scope(String scope) {
      return new DecoratedServerEventLogger(this).scope(scope);
   }

   @Override
   public EventLogger context(String cacheName) {
      return new DecoratedServerEventLogger(this).context(cacheName);
   }

   @Override
   public EventLogger detail(String detail) {
      return new DecoratedServerEventLogger(this).detail(detail);
   }

   @Override
   public EventLogger who(String s) {
      return new DecoratedServerEventLogger(this).who(s);
   }

   TimeService getTimeService() {
      return timeService;
   }

   @Override
   public List<EventLog> getEvents(int start, int count, Optional<EventLogCategory> category, Optional<EventLogLevel> level) {
      QueryBuilder query = getQueryFactory().from(ServerEventImpl.class).orderBy("when", SortOrder.DESC).maxResults(count).startOffset(start);
      if (category.isPresent()) {
         if (level.isPresent()) {
            query.having("category").eq(category.get()).and().having("level").eq(level.get());
         } else {
            query.having("category").eq(category.get());
         }
      } else if (level.isPresent()) {
         query.having("level").eq(level.get());
      }
      return query.build().list();
   }

   private QueryFactory<Query> getQueryFactory() {
      if (queryFactory == null) {
         Cache<Object, Object> cache = cacheManager.getCache(EVENT_LOG_CACHE);
         queryFactory = Search.getQueryFactory(cache);
      }
      return queryFactory;
   }

}
