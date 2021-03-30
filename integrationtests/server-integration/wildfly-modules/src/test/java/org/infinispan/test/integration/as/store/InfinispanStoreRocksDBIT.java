package org.infinispan.test.integration.as.store;

import org.infinispan.commons.util.Version;
import org.infinispan.test.integration.GenericDeploymentHelper;
import org.infinispan.test.integration.store.AbstractInfinispanStoreRocksDBIT;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
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
      WebArchive war = ShrinkWrap.create(WebArchive.class, "rocksdb.war")
            .addClass(AbstractInfinispanStoreRocksDBIT.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
      GenericDeploymentHelper.addLibrary(war, "org.infinispan:infinispan-commons-test");
      return war;
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }
}
