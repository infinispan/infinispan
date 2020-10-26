package org.infinispan.test.integration.thirdparty.store;

import static org.infinispan.test.integration.thirdparty.DeploymentHelper.addLibrary;

import org.infinispan.test.integration.store.AbstractInfinispanStoreRocksDBIT;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
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
   @Deployment
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractInfinispanStoreRocksDBIT.class);
      addLibrary(war, "org.infinispan:infinispan-core");
      addLibrary(war, "org.infinispan:infinispan-cachestore-rocksdb");
      return war;
   }
}
