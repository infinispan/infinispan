package org.infinispan.server.hotrod.counter;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.HotRodVersion;
import org.infinispan.server.hotrod.counter.impl.CounterManagerImplTestStrategy;
import org.infinispan.server.hotrod.counter.impl.TestCounterManager;
import org.infinispan.util.logging.Log;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * A counter's creation and remove test.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "server.hotrod.counter.CounterManagerOperationTest")
public class CounterManagerOperationTest extends HotRodMultiNodeTest implements CounterManagerTestStrategy {

   private static final String PERSISTENT_LOCATION = tmpDirectory("CounterManagerOperationTest");
   private static final String TMP_LOCATION = Paths.get(PERSISTENT_LOCATION, "tmp").toString();
   private static final String SHARED_LOCATION = Paths.get(PERSISTENT_LOCATION, "shared").toString();
   private final CounterManagerTestStrategy strategy;

   public CounterManagerOperationTest() {
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
   protected void createCacheManagers() {
      for (int i = 0; i < nodeCount(); i++) {
         char id = 'A';
         id += i;
         GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().clusteredDefault();
         builder.globalState().enable()
               .persistentLocation(Paths.get(PERSISTENT_LOCATION, Character.toString(id)).toString())
               .temporaryLocation(TMP_LOCATION)
               .sharedPersistentLocation(SHARED_LOCATION);
         EmbeddedCacheManager cm = createClusteredCacheManager(builder, hotRodCacheConfiguration());
         cacheManagers.add(cm);
      }
      waitForClusterToForm(CounterModuleLifecycle.COUNTER_CACHE_NAME);
   }

   @Override
   protected String cacheName() {
      return "unused-cache";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return new ConfigurationBuilder();
   }

   @Override
   protected byte protocolVersion() {
      return HotRodVersion.HOTROD_27.getVersion();
   }

   private Log log() {
      return log;
   }

   private List<CounterManager> allTestCounterManagers() {
      return clients().stream().map(TestCounterManager::new).collect(Collectors.toList());
   }

   private EmbeddedCacheManager cacheManager() {
      return manager(0);
   }

}
