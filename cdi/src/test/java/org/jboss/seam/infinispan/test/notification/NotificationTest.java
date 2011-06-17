package org.jboss.seam.infinispan.test.notification;

import static org.jboss.seam.infinispan.test.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

/**
 * Tests that the simple form of configuration works
 * 
 * @author Pete Muir
 * @see Config
 * 
 */
public class NotificationTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment().addPackage(NotificationTest.class.getPackage());
   }

   /**
    * Inject a cache configured by the application
    */
   @Inject
   @Cache1
   private AdvancedCache<String, String> cache1;

   /**
    * Inject a cache configured by application
    */
   @Inject
   @Cache2
   private AdvancedCache<String, String> cache2;

   @Inject
   private Cache1Observers observers1;

   @Inject
   private Cache2Observers observers2;

   @Test(groups = "functional")
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
      assertEquals(observers1.getCacheEntryRemovedEvent().getValue(),
            "Edinburgh");

      // Manually stop cache1 to check that we are notified :-)
      assertEquals(observers1.getCacheStoppedEventCount(), 0);
      cache1.stop();
      assertEquals(observers1.getCacheStoppedEventCount(), 1);
      assertEquals(observers1.getCacheStoppedEvent().getCacheName(), "cache1");
   }

}
