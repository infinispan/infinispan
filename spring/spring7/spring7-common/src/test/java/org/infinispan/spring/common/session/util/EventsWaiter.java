package org.infinispan.spring.common.session.util;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.context.ApplicationEvent;
import org.springframework.session.Session;
import org.springframework.session.events.AbstractSessionEvent;

public class EventsWaiter {

   public static void assertNumberOfEvents(Supplier<Stream<ApplicationEvent>> eventCollector,
                                           Class<? extends AbstractSessionEvent> eventClass,
                                           int expectedNumberOfEvents,
                                           int timeout,
                                           TimeUnit timeoutUnit) {
      long stopTime = System.currentTimeMillis() + timeoutUnit.toMillis(timeout);

      long eventsCollected = -1;
      while (System.currentTimeMillis() < stopTime) {
         eventsCollected = eventCollector.get()
               .filter(e -> e.getClass() == eventClass)
               .count();
         if (expectedNumberOfEvents == eventsCollected) {
            return;
         }
      }
      throw new AssertionError("Expected " + expectedNumberOfEvents + " events of a class " + eventClass.getSimpleName() + " but found " + eventsCollected);

   }

   public static void assertSessionContent(Supplier<Stream<ApplicationEvent>> eventCollector,
                                           Class<? extends AbstractSessionEvent> eventClass,
                                           String sessionId,
                                           String attName,
                                           String attValue,
                                           int timeout,
                                           TimeUnit timeoutUnit) {

      long stopTime = System.currentTimeMillis() + timeoutUnit.toMillis(timeout);

      List<Session> sessions = null;

      while (System.currentTimeMillis() < stopTime) {
         sessions = eventCollector.get()
               .filter(e -> e.getClass() == eventClass)
               .map(e -> eventClass.cast(e))
               .filter(e -> e.getSessionId().equals(sessionId))
               .map(e -> (Session) e.getSession())
               .collect(Collectors.toList());
         if (!sessions.isEmpty()) {
            break;
         }
      }
      assertEquals(1, sessions.size());
      assertEquals(attValue, sessions.get(0).getAttribute(attName));
   }

}
