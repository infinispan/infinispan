package org.infinispan.spring.session;

import java.util.Objects;
import java.util.Optional;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.spring.provider.SpringCache;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.session.events.SessionCreatedEvent;
import org.springframework.session.events.SessionDeletedEvent;
import org.springframework.session.events.SessionDestroyedEvent;
import org.springframework.session.events.SessionExpiredEvent;

/**
 * A bridge for passing events between Infinispan (both embedded and remote) and Spring.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public abstract class AbstractApplicationPublisherBridge implements ApplicationEventPublisherAware {

   protected static final Log logger = LogFactory.getLog(AbstractApplicationPublisherBridge.class);

   protected final SpringCache eventSource;
   protected Optional<ApplicationEventPublisher> springEventsPublisher = Optional.empty();

   protected AbstractApplicationPublisherBridge(SpringCache eventSource) {
      Objects.requireNonNull(eventSource);
      this.eventSource = eventSource;
   }

   protected abstract void registerListener();
   public abstract void unregisterListener();

   protected void emitSessionCreatedEvent(String sessionId) {
      logger.debugf("Emitting session created %s", sessionId);
      springEventsPublisher.ifPresent(p -> p.publishEvent(new SessionCreatedEvent(eventSource, sessionId)));
   }

   protected void emitSessionExpiredEvent(String sessionId) {
      logger.debugf("Emitting session expired %s", sessionId);
      springEventsPublisher.ifPresent(p -> p.publishEvent(new SessionExpiredEvent(eventSource, sessionId)));
   }

   protected void emitSessionDestroyedEvent(String sessionId) {
      logger.debugf("Emitting session destroyed %s", sessionId);
      springEventsPublisher.ifPresent(p -> p.publishEvent(new SessionDestroyedEvent(eventSource, sessionId)));
   }

   protected void emitSessionDeletedEvent(String sessionId) {
      logger.debugf("Emitting session deleted %s", sessionId);
      springEventsPublisher.ifPresent(p -> p.publishEvent(new SessionDeletedEvent(eventSource, sessionId)));
   }

   @Override
   public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
      springEventsPublisher = Optional.ofNullable(applicationEventPublisher);
   }
}
