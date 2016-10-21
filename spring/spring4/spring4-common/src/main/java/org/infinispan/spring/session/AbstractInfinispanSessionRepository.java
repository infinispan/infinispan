package org.infinispan.spring.session;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.infinispan.spring.provider.SpringCache;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;

/**
 * Infinispan implementation for Spring Session with basic functionality.
 *
 * @author Sebastian Łaskawiec
 * @see <a href="http://projects.spring.io/spring-session">Spring Session Web Page</a>
 * @see SessionRepository
 * @see ApplicationEventPublisherAware
 * @since 9.0
 */
public abstract class AbstractInfinispanSessionRepository implements SessionRepository<MapSession>, ApplicationEventPublisherAware, InitializingBean, DisposableBean {

   protected final AbstractApplicationPublisherBridge applicationEventPublisher;
   protected final SpringCache cache;

   protected AbstractInfinispanSessionRepository(SpringCache cache, AbstractApplicationPublisherBridge eventsBridge) {
      Objects.requireNonNull(cache, "SpringCache can not be null");
      Objects.requireNonNull(eventsBridge, "EventBridge can not be null");
      applicationEventPublisher = eventsBridge;
      this.cache = cache;
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      applicationEventPublisher.registerListener();
   }

   @Override
   public void destroy() throws Exception {
      applicationEventPublisher.unregisterListener();
   }

   @Override
   public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
      this.applicationEventPublisher.setApplicationEventPublisher(applicationEventPublisher);
   }

   @Override
   public MapSession createSession() {
      MapSession result = new MapSession();
      result.setCreationTime(System.currentTimeMillis());
      return result;
   }

   @Override
   public void save(MapSession session) {
      cache.put(session.getId(), session, session.getMaxInactiveIntervalInSeconds(), TimeUnit.SECONDS);
   }

   @Override
   public MapSession getSession(String id) {
      return getSession(id, true);
   }

   /**
    * Returns session with optional parameter whether or not update time accessed.
    *
    * @param id        Session ID.
    * @param updateTTL <code>true</code> if time accessed needs to be updated.
    * @return Session or <code>null</code> if it doesn't exist.
    */
   public MapSession getSession(String id, boolean updateTTL) {
      return Optional.ofNullable(cache.get(id))
            .map(v -> (MapSession) v.get())
            .map(v -> updateTTL(v, updateTTL))
            .orElse(null);
   }

   protected MapSession updateTTL(MapSession session, boolean updateTTL) {
      if (updateTTL) {
         session.setLastAccessedTime(System.currentTimeMillis());
         cache.put(session.getId(), session, session.getMaxInactiveIntervalInSeconds(), TimeUnit.SECONDS);
      }
      return session;
   }

   @Override
   public void delete(String id) {
      applicationEventPublisher.emitSessionDeletedEvent(id);
      cache.evict(id);
   }
}
