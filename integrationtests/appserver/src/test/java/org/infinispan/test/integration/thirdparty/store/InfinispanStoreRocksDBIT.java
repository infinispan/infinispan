package org.infinispan.test.integration.thirdparty.store;

import static java.io.File.separator;
import static org.infinispan.test.integration.thirdparty.DeploymentHelper.addLibrary;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Paths;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.infinispan.testing.Testing;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit5.container.annotation.ArquillianTest;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Test the Infinispan RocksDB CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@ArquillianTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class InfinispanStoreRocksDBIT {

   @ArquillianResource
   private Deployer deployer;

   @Deployment(name = "dep1", managed = false)
   @TargetsContainer("server-1")
   public static Archive<?> deployment1() {
      return archive();
   }

   @Deployment(name = "dep2", managed = false)
   @TargetsContainer("server-2")
   public static Archive<?> deployment2() {
      return archive();
   }

   private static Archive<?> archive() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(InfinispanStoreRocksDBIT.class);
      addLibrary(war, "org.jgroups:jgroups");
      addLibrary(war, "org.infinispan:infinispan-core");
      addLibrary(war, "org.infinispan:infinispan-cachestore-rocksdb");
      addLibrary(war, "org.infinispan:infinispan-testing");
      return war;
   }

   @Test
   @Order(1)
   public void deployDep1() {
      deployer.deploy("dep1");
   }

   @Test
   @OperateOnDeployment("dep1")
   @Order(2)
   public void testRunningInDep1() {
      DefaultCacheManager cm = createCacheManager(1);
      Cache<String, String> cache = cm.getCache();
      cache.put("dep1", "dep1");
   }

   @Test
   @Order(3)
   public void deployDep2() {
      deployer.deploy("dep2");
   }

   @Test
   @OperateOnDeployment("dep2")
   @Order(4)
   public void testRunningInDep2() {
      DefaultCacheManager cm = createCacheManager(2);
      Cache<String, String> cache = cm.getCache();
      assertEquals("dep1", cache.get("dep1"));
   }

   @Test
   @Order(5)
   public void undeploy() {
      deployer.undeploy("dep1");
      deployer.undeploy("dep2");
   }

   private DefaultCacheManager createCacheManager(int index) {
      String baseDir = Testing.tmpDirectory(this.getClass().getSimpleName(), "server-" + index);
      String dataDir = baseDir + separator + "data";
      String expiredDir = baseDir + separator + "expired";
      Util.recursiveFileRemove(Paths.get(baseDir));

      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().transport()
            .defaultTransport()
            .clusterName(InfinispanStoreRocksDBIT.class.getSimpleName())
            .globalState().persistentLocation(baseDir)
            .defaultCacheName("default");
      holder.newConfigurationBuilder("default")
            .clustering().cacheMode(CacheMode.REPL_SYNC)
            .persistence()
            .addStore(RocksDBStoreConfigurationBuilder.class)
            .location(dataDir)
            .expiredLocation(expiredDir);
      return new DefaultCacheManager(holder);
   }
}
