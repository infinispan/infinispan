package org.infinispan.util.logging.events;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestException;
import org.infinispan.util.logging.events.impl.BasicEventLogger;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "events.BasicEventLoggerTest", groups = "unit")
public class BasicEventLoggerTest extends AbstractInfinispanTest {

   @DataProvider(name = "levels-categories")
   public Object[][] levelCategoriesProvider() {
      return Stream.of(EventLogLevel.values())
            .flatMap(level -> Stream.of(EventLogCategory.values())
                  .map(category -> new Object[]{ level, category }))
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "levels-categories")
   public void shouldSendNotification(EventLogLevel level, EventLogCategory category) throws InterruptedException {
      final EventLoggerNotifier notifier = mock(EventLoggerNotifier.class);
      CountDownLatch latch = new CountDownLatch(1);
      ArgumentCaptor<EventLog> logged = ArgumentCaptor.forClass(EventLog.class);

      EventLogger logger = new BasicEventLogger(notifier, DefaultTimeService.INSTANCE);

      when(notifier.notifyEventLogged(logged.capture())).thenAnswer(invocation -> {
         latch.countDown();
         return CompletableFutures.completedNull();
      });

      logger.log(level, category, "Lorem");

      if (!latch.await(5, TimeUnit.SECONDS)) {
         throw new TestException("Failed notifying about logged data");
      }

      EventLog actual = logged.getValue();
      assertNotNull(actual);
      assertEquals(actual.getLevel(), level);
      assertEquals(actual.getCategory(), category);
      assertEquals(actual.getMessage(), "Lorem");
   }
}
