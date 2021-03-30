package org.infinispan.test.integration.thirdparty.store;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;

import org.infinispan.test.integration.store.AbstractInfinispanStoreRocksDBIT;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan RocksDB CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanStoreRocksDBIT extends AbstractInfinispanStoreRocksDBIT {

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
      war.addClass(AbstractInfinispanStoreRocksDBIT.class);
      war.addClass(InfinispanStoreRocksDBIT.class);
      addLibrary(war, "org.jgroups:jgroups");
      addLibrary(war, "org.infinispan:infinispan-core");
      addLibrary(war, "org.infinispan:infinispan-cachestore-rocksdb");
      addLibrary(war, "org.infinispan:infinispan-commons-test");
      return war;
   }
}
