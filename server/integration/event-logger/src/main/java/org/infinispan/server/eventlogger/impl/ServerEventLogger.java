package org.infinispan.server.eventlogger.impl;

import java.security.Principal;
import java.util.UUID;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

/**
 * ServerEventLogger.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class ServerEventLogger implements EventLogger {
   private final EmbeddedCacheManager cacheManager;
   private final TimeService timeService;
   private Cache<UUID, ServerEventImpl> eventCache;

   ServerEventLogger(EmbeddedCacheManager cacheManager, TimeService timeService) {
      this.cacheManager = cacheManager;
      this.timeService = timeService;
   }

   private Cache<UUID, ServerEventImpl> getEventCache() {
      if (eventCache == null) {
         eventCache = cacheManager.getCache(ServerEventLogManagerImpl.EVENT_LOG_CACHE);
      }
      return eventCache;
   }

   @Override
   public void log(EventLogLevel level, String message) {
      ServerEventImpl e = new ServerEventImpl(level, timeService.instant(), message);
      getEventCache().putAsync(Util.threadLocalRandomUUID(), e);
   }

   void log(ServerEventImpl event) {
      getEventCache().putAsync(Util.threadLocalRandomUUID(), event);
   }

   @Override
   public EventLogger scope(String scope) {
      return new DecoratedServerEventLogger(this).scope(scope);
   }

   @Override
   public EventLogger context(Cache<?, ?> cache) {
      return new DecoratedServerEventLogger(this).context(cache);
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
   public EventLogger detail(Throwable t) {
      return new DecoratedServerEventLogger(this).detail(t);
   }

   @Override
   public EventLogger who(Subject s) {
      return new DecoratedServerEventLogger(this).who(s);
   }

   @Override
   public EventLogger who(Principal p) {
      return new DecoratedServerEventLogger(this).who(p);
   }

   @Override
   public EventLogger who(String s) {
      return new DecoratedServerEventLogger(this).who(s);
   }

   TimeService getTimeService() {
      return timeService;
   }
}
