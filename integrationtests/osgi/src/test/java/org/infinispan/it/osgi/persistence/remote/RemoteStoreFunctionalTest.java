package org.infinispan.it.osgi.persistence.remote;

import static org.infinispan.it.osgi.util.IspnKarafOptions.featureIspnCore;
import static org.infinispan.it.osgi.util.IspnKarafOptions.featureIspnCoreDependencies;
import static org.infinispan.it.osgi.util.IspnKarafOptions.featureRemoteStore;
import static org.infinispan.it.osgi.util.IspnKarafOptions.karafContainer;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

/**
 * Test similar to {@link org.infinispan.persistence.remote.RemoteStoreFunctionalTest}.
 *
 * As opposed to the original RemoteStoreFunctionalTest which starts an embedded HotRod server,
 * the current test requires a remote Infinispan server to be running on localhost with
 * cache "notindexed" and HotRod listening on 11222 port. Running an embedded HotRod server inside Karaf
 * does not work.
 *
 * TODO: Automate starting and stopping remote Infinispan server or move the test to the server test suite.
 *
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@Ignore
public class RemoteStoreFunctionalTest extends BaseStoreFunctionalTest {

   private final String CACHE_NAME = "notindexed";

   @Configuration
   public Option[] config() throws Exception {
      return options(
            karafContainer(),
            featureIspnCoreDependencies(),
            featureIspnCore(),
            featureRemoteStore(),
            junitBundles(),
            keepRuntimeFolder()
      );
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(CACHE_NAME)
            .preload(preload)
            .addServer()
            .host("localhost")
            .port(11222);
      return persistence;
   }

   @Test
   public void testRestoreAtomicMap() throws Exception {
      super.testRestoreAtomicMap(this.getClass().getMethod("testRestoreAtomicMap"));
   }

   @Test
   public void testRestoreTransactionalAtomicMap() throws Exception {
      super.testRestoreTransactionalAtomicMap(this.getClass().getMethod("testRestoreTransactionalAtomicMap"));
   }

   @Test
   public void testStoreByteArrays() throws Exception {
      super.testStoreByteArrays(this.getClass().getMethod("testStoreByteArrays"));
   }
}
