package org.infinispan.server.logging.events;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.core.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;


/**
 * ServerEventLogger. This event logger takes care of maintaining the server event log cache and
 * provides methods for querying its contents across all nodes. For resilience, the event log is
 * stored in a local, bounded, persistent cache and distributed executors are used to gather logs
 * from all the nodes in the cluster.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class ServerEventLogger implements EventLogger {
   public static final String EVENT_LOG_CACHE = "___event_log_cache";
   public static final Log log = LogFactory.getLog(ServerEventLogger.class);
   private final EmbeddedCacheManager cacheManager;
   private final TimeService timeService;
   private Cache<UUID, ServerEventImpl> eventCache;

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
      eventLog(new ServerEventImpl(timeService.instant(), level, category, message));
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
   public List<EventLog> getEvents(Instant start, int count, Optional<EventLogCategory> category, Optional<EventLogLevel> level) {
      List<EventLog> events = Collections.synchronizedList(new ArrayList<>());
      AtomicReference<Throwable> throwable = new AtomicReference<>();
      try {
         SecurityActions.getClusterExecutor(cacheManager).submitConsumer(m -> {
            Cache<Object, Object> cache = m.getCache(EVENT_LOG_CACHE);
            String queryStr = "FROM " + ServerEventImpl.class.getName()+ " WHERE when <= :when";   //todo [anistor]  is start parameter supposed to indicate before or after?
            if (category.isPresent()) {
               queryStr += " AND category = :category";
            }
            if (level.isPresent()) {
               queryStr += " AND level = :level";
            }
            queryStr += " ORDER BY when DESC";
            QueryFactory queryFactory = Search.getQueryFactory(cache);
            Query<EventLog> query = queryFactory.create(queryStr);
            query.maxResults(count);
            query.setParameter("when", start);
            category.ifPresent(c -> query.setParameter("category", c));
            level.ifPresent(l -> query.setParameter("level", l));
            return query.list();
         }, (address, nodeEvents, t) -> {
            if (t == null) {
               events.addAll(nodeEvents);
            } else {
               throwable.set(t);
            }
         }).get(1, TimeUnit.MINUTES);
         Throwable th = throwable.get();
         if (th != null) {
            throw new CacheException(th);
         }
      } catch (CacheException e) {
         log.debug("Could not retrieve events", e);
         throw e;
      } catch (Exception e) {
         log.debug("Could not retrieve events", e);
         throw new CacheException(e);
      }
      // must sort and limit again the distributed results
      Collections.sort(events);
      return events.subList(0, Math.min(events.size(), count));
   }
}
