package org.infinispan.spring.common.session;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.spring.common.session.util.EventsWaiter;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.AbstractInfinispanTest;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.session.MapSession;
import org.springframework.session.events.SessionCreatedEvent;
import org.springframework.session.events.SessionDeletedEvent;
import org.springframework.session.events.SessionExpiredEvent;
import org.testng.annotations.Test;

public abstract class InfinispanApplicationPublishedBridgeTCK extends AbstractInfinispanTest {

   protected SpringCache springCache;

   protected AbstractInfinispanSessionRepository sessionRepository;

   protected abstract SpringCache createSpringCache();

   protected abstract void callEviction();

   protected abstract AbstractInfinispanSessionRepository createRepository(SpringCache springCache) throws Exception;

   protected void init() throws Exception {
      springCache = createSpringCache();
      sessionRepository = createRepository(springCache);
   }

   @Test
   public void testEventBridge() throws Exception {
      //given
      EventsCollector eventsCollector = new EventsCollector();

      sessionRepository.setApplicationEventPublisher(eventsCollector);

      //when
      MapSession sessionToBeDeleted = sessionRepository.createSession();
      MapSession sessionToBeExpired = sessionRepository.createSession();
      sessionToBeExpired.setMaxInactiveIntervalInSeconds(1);

      sessionRepository.save(sessionToBeExpired);
      sessionRepository.save(sessionToBeDeleted);
      sessionRepository.delete(sessionToBeDeleted.getId());

      sleepOneSecond();
      callEviction();

      //then
      assertNull(springCache.get(sessionToBeExpired.getId()));
      assertNull(springCache.get(sessionToBeDeleted.getId()));
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionCreatedEvent.class, 2, 2, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionDeletedEvent.class, 1, 2, TimeUnit.SECONDS);
      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionExpiredEvent.class, 1, 2, TimeUnit.SECONDS);
      //FIXME: This doesn't work for remote... why? https://issues.jboss.org/browse/ISPN-7040
//      EventsWaiter.assertNumberOfEvents(() -> eventsCollector.getEvents(), SessionDestroyedEvent.class, 2, 10, TimeUnit.SECONDS);
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
      MapSession sessionToBeExpired = sessionRepository.createSession();
      sessionRepository.save(sessionToBeExpired);

      //then
      assertEquals(eventsCollector.getEvents().count(), 0);
   }

   static class EventsCollector implements ApplicationEventPublisher {
      private List<ApplicationEvent> events = new CopyOnWriteArrayList<>();

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
