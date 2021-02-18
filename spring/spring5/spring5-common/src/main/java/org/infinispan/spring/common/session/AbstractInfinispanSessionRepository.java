package org.infinispan.spring.common.session;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.spring.common.provider.SpringCache;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;

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
public abstract class AbstractInfinispanSessionRepository implements FindByIndexNameSessionRepository<MapSession>, ApplicationEventPublisherAware, InitializingBean, DisposableBean {

   protected final AbstractApplicationPublisherBridge applicationEventPublisher;
   protected final SpringCache cache;

   protected AbstractInfinispanSessionRepository(SpringCache cache, AbstractApplicationPublisherBridge eventsBridge) {
      Objects.requireNonNull(cache, "SpringCache can not be null");
      Objects.requireNonNull(eventsBridge, "EventBridge can not be null");
      applicationEventPublisher = eventsBridge;
      this.cache = cache;
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

   @Override
   public MapSession createSession() {
      MapSession result = new MapSession();
      result.setCreationTime(Instant.now());
      return result;
   }

   @Override
   public void save(MapSession session) {
      if (!session.getId().equals(session.getOriginalId())) {
         removeFromCacheWithoutNotifications(session.getOriginalId());
      }
      cache.put(session.getId(), session, session.getMaxInactiveInterval().getSeconds(), TimeUnit.SECONDS);
   }

   protected abstract void removeFromCacheWithoutNotifications(String originalId);

   @Override
   public MapSession findById(String sessionId) {
      return getSession(sessionId, true);
   }

   @Override
   public void deleteById(String sessionId) {
      ValueWrapper valueWrapper = cache.get(sessionId);
      if (valueWrapper == null) {
         return;
      }
      MapSession mapSession = (MapSession) valueWrapper.get();
      if (mapSession != null) {
         applicationEventPublisher.emitSessionDeletedEvent(mapSession);
         cache.evict(sessionId);
      }
   }

   /**
    * Returns session with optional parameter whether or not update time accessed.
    *
    * @param id        Session ID.
    * @param updateTTL <code>true</code> if time accessed needs to be updated.
    * @return Session or <code>null</code> if it doesn't exist.
    */
   public MapSession getSession(String id, boolean updateTTL) {
      ValueWrapper wrapper = cache.get(id);
      if (wrapper == null) {
         return null;
      }
      MapSession session = (MapSession) wrapper.get();
      assert session != null;
      // Copy the MapSession instance to prevent concurrent access to the same instance
      // Even if the cache is remote, it might store the instance in a near-cache
      return updateTTL(new MapSession(session), updateTTL);
   }

   protected MapSession updateTTL(MapSession session, boolean updateTTL) {
      if (updateTTL) {
         session.setLastAccessedTime(Instant.now());
         cache.put(session.getId(), session, session.getMaxInactiveInterval().getSeconds(), TimeUnit.SECONDS);
      }
      return session;
   }
}
