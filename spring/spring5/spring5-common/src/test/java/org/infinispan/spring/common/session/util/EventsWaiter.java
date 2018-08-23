package org.infinispan.spring.common.session.util;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.springframework.context.ApplicationEvent;
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

}
