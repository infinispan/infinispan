package org.infinispan.spring.common.session;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository.InfinispanSession;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.session.DelegatingIndexResolver;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.FlushMode;
import org.springframework.session.IndexResolver;
import org.springframework.session.MapSession;
import org.springframework.session.PrincipalNameIndexResolver;
import org.springframework.session.SaveMode;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.util.Assert;

/**
 * Infinispan implementation for Spring Session with basic functionality.
 *
 * @author Sebastian Łaskawiec
 * @see <a href="http://projects.spring.io/spring-session">Spring Session Web Page</a>
 * @see SessionRepository
 * @see ApplicationEventPublisherAware
 * @see FindByIndexNameSessionRepository
 * @since 9.0
 */
public abstract class AbstractInfinispanSessionRepository implements FindByIndexNameSessionRepository<InfinispanSession>, ApplicationEventPublisherAware, InitializingBean, DisposableBean {

   private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

   protected final AbstractApplicationPublisherBridge applicationEventPublisher;
   protected final SpringCache cache;
   protected final BasicCache<String, MapSession> nativeCache;

   protected Duration defaultMaxInactiveInterval = Duration.ofSeconds(MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS);
   protected FlushMode flushMode = FlushMode.ON_SAVE;
   protected SaveMode saveMode = SaveMode.ON_SET_ATTRIBUTE;
   protected IndexResolver<Session> indexResolver = new DelegatingIndexResolver<>(new PrincipalNameIndexResolver<>());

   protected AbstractInfinispanSessionRepository(SpringCache cache, AbstractApplicationPublisherBridge eventsBridge) {
      Objects.requireNonNull(cache, "SpringCache can not be null");
      Objects.requireNonNull(eventsBridge, "EventBridge can not be null");
      applicationEventPublisher = eventsBridge;
      this.cache = cache;
      nativeCache = (BasicCache<String, MapSession>) cache.getNativeCache();
   }

   @Override
   public void afterPropertiesSet() {
      applicationEventPublisher.registerListener();
   }

   @Override
   public void destroy() {
      applicationEventPublisher.unregisterListener();
   }

   @Override
   public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
      this.applicationEventPublisher.setApplicationEventPublisher(applicationEventPublisher);
   }

   /**
    * Set the maximum inactive interval in seconds between requests before newly created sessions
    * will be invalidated. A negative time indicates that the session will never time out. The
    * default is 30 minutes.
    *
    * @param defaultMaxInactiveInterval the default maxInactiveInterval
    */
   public void setDefaultMaxInactiveInterval(final Duration defaultMaxInactiveInterval) {
      Assert.notNull(defaultMaxInactiveInterval, "defaultMaxInactiveInterval must not be null");
      this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
   }

   /**
    * Set the {@link IndexResolver} to use.
    *
    * @param indexResolver the index resolver
    */
   public void setIndexResolver(final IndexResolver<Session> indexResolver) {
      Assert.notNull(indexResolver, "indexResolver cannot be null");
      this.indexResolver = indexResolver;
   }

   /**
    * Sets the flush mode. Default flush mode is {@link FlushMode#ON_SAVE}.
    *
    * @param flushMode the new Hazelcast flush mode
    */
   public void setFlushMode(final FlushMode flushMode) {
      Assert.notNull(flushMode, "flushMode cannot be null");
      this.flushMode = flushMode;
   }

   /**
    * Set the save mode. Default save mode is {@link SaveMode#ON_SET_ATTRIBUTE}.
    *
    * @param saveMode the save mode
    */
   public void setSaveMode(final SaveMode saveMode) {
      Assert.notNull(saveMode, "saveMode must not be null");
      this.saveMode = saveMode;
   }

   @Override
   public InfinispanSession createSession() {
      final MapSession cached = new MapSession();
      cached.setMaxInactiveInterval(this.defaultMaxInactiveInterval);
      cached.setCreationTime(Instant.now());
      final InfinispanSession session = new InfinispanSession(cached, true);
      session.flushImmediateIfNecessary();
      return session;
   }

   @Override
   public void save(InfinispanSession session) {
      if (session.isNew) {
         nativeCache.put(
               session.getId(),
               session.getDelegate(),
               session.getMaxInactiveInterval().getSeconds(),
               TimeUnit.SECONDS);
      } else if (session.sessionIdChanged) {
         removeFromCacheWithoutNotifications(session.originalId);
         session.originalId = session.getId();
         nativeCache.put(
               session.getId(),
               session.getDelegate(),
               session.getMaxInactiveInterval().getSeconds(),
               TimeUnit.SECONDS);
      } else if (session.hasChanges()) {
         final SessionUpdateRemappingFunction remappingFunction = new SessionUpdateRemappingFunction();
         if (session.lastAccessedTimeChanged) {
            remappingFunction.setLastAccessedTime(session.getLastAccessedTime());
         }
         if (session.maxInactiveIntervalChanged) {
            remappingFunction.setMaxInactiveInterval(session.getMaxInactiveInterval());
         }
         if (!session.delta.isEmpty()) {
            remappingFunction.setDelta(new HashMap<>(session.delta));
         }
         nativeCache.compute(
               session.getId(),
               remappingFunction,
               session.getMaxInactiveInterval().getSeconds(),
               TimeUnit.SECONDS);
      }
      session.clearChangeFlags();
   }

   protected abstract void removeFromCacheWithoutNotifications(String originalId);

   @Override
   public InfinispanSession findById(String id) {
      final MapSession saved = nativeCache.get(id);
      if (saved == null) {
         return null;
      }
      if (saved.isExpired()) {
         deleteById(saved.getId());
         return null;
      }
      return new InfinispanSession(saved, false);
   }

   @Override
   public void deleteById(String id) {
      final MapSession saved = nativeCache.get(id);
      if (saved != null) {
         applicationEventPublisher.emitSessionDeletedEvent(saved);
         nativeCache.remove(id);
      }
   }

   /**
    * A custom implementation of {@link Session} that uses a {@link MapSession} as the basis for its
    * mapping. It keeps track if changes have been made since last save.
    */
   public final class InfinispanSession implements Session {

      private final MapSession delegate;

      private boolean isNew;

      private boolean sessionIdChanged;

      private boolean lastAccessedTimeChanged;

      private boolean maxInactiveIntervalChanged;

      private String originalId;

      private final Map<String, Object> delta = new HashMap<>();

      public InfinispanSession(final MapSession cached, final boolean isNew) {
         this.delegate = isNew ? cached : new MapSession(cached);
         this.isNew = isNew;
         this.originalId = cached.getId();
         if (this.isNew || (AbstractInfinispanSessionRepository.this.saveMode == SaveMode.ALWAYS)) {
            getAttributeNames()
                  .forEach(
                        (attributeName) ->
                              this.delta.put(attributeName, cached.getAttribute(attributeName)));
         }
      }

      @Override
      public void setLastAccessedTime(final Instant lastAccessedTime) {
         this.delegate.setLastAccessedTime(lastAccessedTime);
         this.lastAccessedTimeChanged = true;
         flushImmediateIfNecessary();
      }

      @Override
      public boolean isExpired() {
         return this.delegate.isExpired();
      }

      @Override
      public Instant getCreationTime() {
         return this.delegate.getCreationTime();
      }

      @Override
      public String getId() {
         return this.delegate.getId();
      }

      @Override
      public String changeSessionId() {
         final String newSessionId = this.delegate.changeSessionId();
         this.sessionIdChanged = true;
         return newSessionId;
      }

      @Override
      public Instant getLastAccessedTime() {
         return this.delegate.getLastAccessedTime();
      }

      @Override
      public void setMaxInactiveInterval(final Duration interval) {
         this.delegate.setMaxInactiveInterval(interval);
         this.maxInactiveIntervalChanged = true;
         flushImmediateIfNecessary();
      }

      @Override
      public Duration getMaxInactiveInterval() {
         return this.delegate.getMaxInactiveInterval();
      }

      @Override
      public <T> T getAttribute(final String attributeName) {
         final T attributeValue = this.delegate.getAttribute(attributeName);
         if (attributeValue != null
               && AbstractInfinispanSessionRepository.this.saveMode.equals(SaveMode.ON_GET_ATTRIBUTE)) {
            this.delta.put(attributeName, attributeValue);
         }
         return attributeValue;
      }

      @Override
      public Set<String> getAttributeNames() {
         return this.delegate.getAttributeNames();
      }

      @Override
      public void setAttribute(final String attributeName, final Object attributeValue) {
         this.delegate.setAttribute(attributeName, attributeValue);
         this.delta.put(attributeName, attributeValue);
         if (SPRING_SECURITY_CONTEXT.equals(attributeName)) {
            final Map<String, String> indexes =
                  AbstractInfinispanSessionRepository.this.indexResolver.resolveIndexesFor(this);
            final String principal =
                  (attributeValue != null) ? indexes.get(PRINCIPAL_NAME_INDEX_NAME) : null;
            this.delegate.setAttribute(PRINCIPAL_NAME_INDEX_NAME, principal);
            this.delta.put(PRINCIPAL_NAME_INDEX_NAME, principal);
         }
         flushImmediateIfNecessary();
      }

      @Override
      public void removeAttribute(final String attributeName) {
         setAttribute(attributeName, null);
      }

      MapSession getDelegate() {
         return this.delegate;
      }

      boolean hasChanges() {
         return (this.lastAccessedTimeChanged
               || this.maxInactiveIntervalChanged
               || !this.delta.isEmpty());
      }

      void clearChangeFlags() {
         this.isNew = false;
         this.lastAccessedTimeChanged = false;
         this.sessionIdChanged = false;
         this.maxInactiveIntervalChanged = false;
         this.delta.clear();
      }

      private void flushImmediateIfNecessary() {
         if (AbstractInfinispanSessionRepository.this.flushMode == FlushMode.IMMEDIATE) {
            AbstractInfinispanSessionRepository.this.save(this);
         }
      }
   }
}
