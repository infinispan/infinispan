package org.infinispan.test.integration.store;

import static java.io.File.separator;
import static org.junit.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;

/**
 * Test the Infinispan RocksDB CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public abstract class AbstractInfinispanStoreRocksDBIT {

   @ArquillianResource
   private Deployer deployer;

   @Test
   @InSequence(1)
   public void deployDep1() {
      deployer.deploy("dep1");
   }

   @Test
   @OperateOnDeployment("dep1")
   @InSequence(2)
   public void testRunningInDep1() {
      DefaultCacheManager cm = getOrCreateCacheManager(1);
      Cache<String, String> cache = cm.getCache();
      cache.put("dep1", "dep1");
   }

   @Test
   @InSequence(3)
   public void deployDep2() {
      deployer.deploy("dep2");
   }

   @Test
   @OperateOnDeployment("dep2")
   @InSequence(4)
   public void testRunningInDep2() {
      DefaultCacheManager cm = getOrCreateCacheManager(2);
      Cache<String, String> cache = cm.getCache();
      assertEquals("dep1", cache.get("dep1"));
   }

   @Test
   @InSequence(5)
   public void undeploy() {
      deployer.undeploy("dep1");
      deployer.undeploy("dep2");
   }

   protected DefaultCacheManager getOrCreateCacheManager(int index) {
      String baseDir = CommonsTestingUtil.tmpDirectory(this.getClass().getSimpleName(), "server-" + index);
      String dataDir = baseDir + separator + "data";
      String expiredDir = baseDir + separator + "expired";

      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.transport()
            .defaultTransport()
            .clusterName(AbstractInfinispanStoreRocksDBIT.class.getSimpleName());
      global.globalState().persistentLocation(baseDir);
      global.defaultCacheName("default");
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      builder.persistence()
            .addStore(RocksDBStoreConfigurationBuilder.class)
            .location(dataDir)
            .expiredLocation(expiredDir);
      DefaultCacheManager cacheManager = new DefaultCacheManager(global.build(), builder.build());
      return cacheManager;
   }
}
