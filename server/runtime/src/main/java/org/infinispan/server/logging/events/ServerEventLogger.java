package org.infinispan.server.logging.events;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
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
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.util.logging.events.EventLoggerNotifier;

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
   private final EventLoggerNotifier notifier;
   private final BlockingManager blockingManager;
   private Cache<UUID, ServerEventImpl> eventCache;

   public ServerEventLogger(EmbeddedCacheManager cacheManager, TimeService timeService, EventLoggerNotifier notifier,
                            BlockingManager blockingManager) {
      this.cacheManager = cacheManager;
      this.timeService = timeService;
      this.notifier = notifier;
      this.blockingManager = blockingManager;
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
      if (eventCache == null) {
         if (!cacheManager.isRunning(EVENT_LOG_CACHE)) {
            // Cache is not running yet, we can't block this thread so offload blocking thread to log the message
            blockingManager.runBlocking(() -> {
               eventCache = cacheManager.getCache(EVENT_LOG_CACHE);
               actualSend(eventCache, event);
            }, "ServerEventLog");
            return;
         }
         eventCache = cacheManager.getCache(EVENT_LOG_CACHE);
      }

      actualSend(eventCache, event);
   }

   void actualSend(Cache<UUID, ServerEventImpl> cache, ServerEventImpl event) {
      eventCache.putAsync(Util.threadLocalRandomUUID(), event)
            .thenAccept(ignore -> CompletionStages.join(notifier.notifyEventLogged(event)));
   }

   @Override
   public EventLogger scope(Address scope) {
      return new DecoratedServerEventLogger(this).scope(scope);
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
            return query.execute().list();
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
      return count > 0
            ? events.subList(0, Math.min(events.size(), count))
            : events;
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      return notifier.addListenerAsync(listener);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      return notifier.removeListenerAsync(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return notifier.getListeners();
   }
}
