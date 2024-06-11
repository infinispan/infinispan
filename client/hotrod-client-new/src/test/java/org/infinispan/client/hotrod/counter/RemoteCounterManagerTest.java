package org.infinispan.client.hotrod.counter;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.counter.CounterManagerTestStrategy;
import org.infinispan.server.hotrod.counter.impl.CounterManagerImplTestStrategy;
import org.infinispan.util.logging.Log;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * A {@link CounterManager} implementation test.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "client.hotrod.counter.RemoteCounterManagerTest")
public class RemoteCounterManagerTest extends AbstractCounterTest implements CounterManagerTestStrategy {

   private static final String PERSISTENT_LOCATION = tmpDirectory("RemoteCounterManagerTest");
   private static final String TMP_LOCATION = Paths.get(PERSISTENT_LOCATION, "tmp").toString();
   private static final String SHARED_LOCATION = Paths.get(PERSISTENT_LOCATION,"shared").toString();
   private final CounterManagerTestStrategy strategy;


   public RemoteCounterManagerTest() {
      strategy = new CounterManagerImplTestStrategy(this::allTestCounterManagers, this::log, this::cacheManager);
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      if (!new File(PERSISTENT_LOCATION).mkdirs()) {
         log.warnf("Unable to create persistent location file: '%s'", PERSISTENT_LOCATION);
      }
      super.createBeforeClass();
   }

   @Override
   public void testWeakCounter(Method method) {
      strategy.testWeakCounter(method);
   }

   @Override
   public void testUnboundedStrongCounter(Method method) {
      strategy.testUnboundedStrongCounter(method);
   }

   @Override
   public void testUpperBoundedStrongCounter(Method method) {
      strategy.testUpperBoundedStrongCounter(method);
   }

   @Override
   public void testLowerBoundedStrongCounter(Method method) {
      strategy.testLowerBoundedStrongCounter(method);
   }

   @Override
   public void testBoundedStrongCounter(Method method) {
      strategy.testBoundedStrongCounter(method);
   }

   @Override
   public void testUndefinedCounter() {
      strategy.testUndefinedCounter();
   }

   @Override
   public void testRemove(Method method) {
      strategy.testRemove(method);
   }

   @Override
   public void testGetCounterNames(Method method) {
      strategy.testGetCounterNames(method);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
   }

   @Override
   protected void modifyGlobalConfiguration(GlobalConfigurationBuilder builder) {
      char id = 'A';
      id += cacheManagers.size();
      builder.globalState().enable()
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Character.toString(id)).toString())
            .temporaryLocation(TMP_LOCATION)
            .sharedPersistentLocation(SHARED_LOCATION);
   }

   private Log log() {
      return log;
   }


   private List<CounterManager> allTestCounterManagers() {
      return clients.stream().map(RemoteCounterManagerFactory::asCounterManager).collect(Collectors.toList());
   }

   private EmbeddedCacheManager cacheManager() {
      return manager(0);
   }
}
