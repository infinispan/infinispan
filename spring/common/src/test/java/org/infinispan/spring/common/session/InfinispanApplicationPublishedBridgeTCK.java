package org.infinispan.spring.common.session;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository.InfinispanSession;
import org.infinispan.spring.common.session.util.EventsWaiter;
import org.infinispan.test.AbstractInfinispanTest;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.session.events.SessionCreatedEvent;
import org.springframework.session.events.SessionDeletedEvent;
import org.springframework.session.events.SessionDestroyedEvent;
import org.springframework.session.events.SessionExpiredEvent;
import org.testng.annotations.Test;

public abstract class InfinispanApplicationPublishedBridgeTCK extends AbstractInfinispanTest {

   protected SpringCache springCache;

   protected AbstractInfinispanSessionRepository sessionRepository;

   protected abstract SpringCache createSpringCache();

   protected abstract void callEviction();

   protected abstract AbstractInfinispanSessionRepository createRepository(SpringCache springCache) throws Exception;

   @Test
   public void testEventBridge() throws Exception {
      //given
      EventsCollector eventsCollector = new EventsCollector();

      sessionRepository.setApplicationEventPublisher(eventsCollector);

      //when
      InfinispanSession sessionToBeDeleted = sessionRepository.createSession();
      sessionToBeDeleted.setAttribute("foo", "bar");
      sessionRepository.save(sessionToBeDeleted);

      InfinispanSession sessionToBeExpired = sessionRepository.createSession();
      sessionToBeExpired.setMaxInactiveInterval(Duration.ofSeconds(1));

      sessionRepository.save(sessionToBeExpired);
      sessionRepository.save(sessionToBeDeleted);
      sessionRepository.deleteById(sessionToBeDeleted.getId());

      // Attempt to delete a non present session
      sessionRepository.deleteById("not-present");

      sleepOneSecond();
      callEviction();

      //then
      assertNull(springCache.get(sessionToBeExpired.getId()));
      assertNull(springCache.get(sessionToBeDeleted.getId()));
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionCreatedEvent.class, 2, 2, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionDeletedEvent.class, 1, 2, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionDestroyedEvent.class, 1, 10, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionExpiredEvent.class, 1, 2, TimeUnit.SECONDS);
      EventsWaiter.assertSessionContent(() -> eventsCollector.getEvents(), SessionDeletedEvent.class, sessionToBeDeleted.getId(), "foo", "bar", 2, TimeUnit.SECONDS);
   }

   @Test
   public void testEventBridgeWithSessionIdChange() throws Exception {
      EventsCollector eventsCollector = new EventsCollector();
      sessionRepository.setApplicationEventPublisher(eventsCollector);

      InfinispanSession session = sessionRepository.createSession();

      sessionRepository.save(session);
      session.changeSessionId();
      sessionRepository.save(session);

      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionCreatedEvent.class, 2, 2, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionDeletedEvent.class, 0, 2, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionDestroyedEvent.class, 0, 2, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionExpiredEvent.class, 0, 2, TimeUnit.SECONDS);
   }

   protected void init() throws Exception {
      springCache = createSpringCache();
      sessionRepository = createRepository(springCache);
   }

   private void sleepOneSecond() {
      long oneSecondSleep = System.currentTimeMillis() + 1000;
      eventually(() -> System.currentTimeMillis() > oneSecondSleep);
   }

   @Test
   public void testUnregistration() throws Exception {
      //given
      EventsCollector eventsCollector = new EventsCollector();

      sessionRepository.setApplicationEventPublisher(eventsCollector);

      //when
      sessionRepository.destroy(); //simulate closing app context
      InfinispanSession sessionToBeExpired = sessionRepository.createSession();
      sessionRepository.save(sessionToBeExpired);

      //then
      assertEquals(eventsCollector.getEvents().count(), 0);
   }

   static class EventsCollector implements ApplicationEventPublisher {
      private final List<ApplicationEvent> events = new CopyOnWriteArrayList<>();

      @Override
      public void publishEvent(ApplicationEvent event) {
         events.add(event);
      }

      @Override
      public void publishEvent(Object event) {
         publishEvent(new PayloadApplicationEvent<>(this, event));
      }

      public Stream<ApplicationEvent> getEvents() {
         return events.stream();
      }
   }
}
