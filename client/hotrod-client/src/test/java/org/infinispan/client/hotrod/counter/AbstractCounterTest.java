package org.infinispan.client.hotrod.counter;

import static org.infinispan.client.hotrod.RemoteCounterManagerFactory.asCounterManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.blockUntilCacheStatusAchieved;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.lifecycle.ComponentStatus;
import org.testng.annotations.BeforeMethod;

/**
 * A base class for counter tests.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public abstract class AbstractCounterTest extends MultiHotRodServersTest {

   private static final int NUMBER_SERVERS = 3;

   @BeforeMethod(alwaysRun = true)
   public void restoreServer() throws Throwable {
      if (servers.size() < NUMBER_SERVERS) {
         // Start Hot Rod servers
         while (servers.size() < NUMBER_SERVERS) {
            addHotRodServer(hotRodCacheConfiguration());
         }
         // Block until views have been received
         blockUntilViewReceived(manager(0).getCache(), NUMBER_SERVERS);
         // Verify that caches running
         for (int i = 0; i < NUMBER_SERVERS; i++) {
            blockUntilCacheStatusAchieved(manager(i).getCache(), ComponentStatus.RUNNING, 10000);
         }
      }
   }

   List<CounterManager> counterManagers() {
      return clients.stream().map(RemoteCounterManagerFactory::asCounterManager).collect(Collectors.toList());
   }

   CounterManager counterManager() {
      return asCounterManager(client(0));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUMBER_SERVERS, hotRodCacheConfiguration());
   }
}
