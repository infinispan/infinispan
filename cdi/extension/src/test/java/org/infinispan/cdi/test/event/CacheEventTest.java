package org.infinispan.cdi.test.event;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the simple form of configuration works. This test is disabled due to a bug with parameterized events in
 * Weld.
 *
 * @author Pete Muir
 * @see Config
 */
@Test(groups = "unstable", testName = "cdi.test.event.CacheEventTest", description = "original group: functional")
public class CacheEventTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(CacheEventTest.class.getPackage());
   }

   @Inject
   @Cache1
   private AdvancedCache<String, String> cache1;

   @Inject
   @Cache2
   private AdvancedCache<String, String> cache2;

   @Inject
   private Cache1Observers observers1;

   @Inject
   private Cache2Observers observers2;

   public void testSmallCache() {
      // Put something into the cache, ensure it is started
      cache1.put("pete", "Edinburgh");
      assertEquals(cache1.get("pete"), "Edinburgh");
      assertEquals(observers1.getCacheStartedEventCount(), 1);
      assertEquals(observers1.getCacheStartedEvent().getCacheName(), "cache1");
      assertEquals(observers1.getCacheEntryCreatedEventCount(), 1);
      assertEquals(observers1.getCacheEntryCreatedEvent().getKey(), "pete");

      // Check cache isolation for events
      cache2.put("mircea", "London");
      assertEquals(cache2.get("mircea"), "London");
      assertEquals(observers2.getCacheStartedEventCount(), 1);
      assertEquals(observers2.getCacheStartedEvent().getCacheName(), "cache2");

      // Remove something
      cache1.remove("pete");
      assertEquals(observers1.getCacheEntryRemovedEventCount(), 1);
      assertEquals(observers1.getCacheEntryRemovedEvent().getKey(), "pete");
      assertEquals(observers1.getCacheEntryRemovedEvent().getValue(), "Edinburgh");

      // Manually stop cache1 to check that we are notified
      assertEquals(observers1.getCacheStoppedEventCount(), 0);
      cache1.stop();
      assertEquals(observers1.getCacheStoppedEventCount(), 1);
      assertEquals(observers1.getCacheStoppedEvent().getCacheName(), "cache1");
   }
}
