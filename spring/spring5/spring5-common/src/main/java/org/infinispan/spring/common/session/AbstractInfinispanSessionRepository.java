package org.infinispan.spring.common.session;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.spring.common.provider.SpringCache;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
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
   protected final PrincipalNameResolver principalNameResolver = new PrincipalNameResolver();

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
      cache.put(session.getId(), session, session.getMaxInactiveInterval().getSeconds(), TimeUnit.SECONDS);
   }

   @Override
   public MapSession findById(String sessionId) {
      return getSession(sessionId, true);
   }

   @Override
   public void deleteById(String sessionId) {
      MapSession mapSession = (MapSession) cache.get(sessionId).get();
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
      return Optional.ofNullable(cache.get(id))
            .map(v -> (MapSession) v.get())
            .map(v -> updateTTL(v, updateTTL))
            .orElse(null);
   }

   protected MapSession updateTTL(MapSession session, boolean updateTTL) {
      if (updateTTL) {
         session.setLastAccessedTime(Instant.now());
         cache.put(session.getId(), session, session.getMaxInactiveInterval().getSeconds(), TimeUnit.SECONDS);
      }
      return session;
   }

   @Override
   public Map<String, MapSession> findByIndexNameAndIndexValue(String indexName, String indexValue) {
      if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
         return Collections.emptyMap();
      }

      return cache.getNativeCache().values().stream()
            .map(cacheValue -> (MapSession) cacheValue)
            .filter(session -> indexValue.equals(principalNameResolver.resolvePrincipal(session)))
            .collect(Collectors.toMap(MapSession::getId, Function.identity()));
   }
}
