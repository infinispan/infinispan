package org.infinispan.test.integration.as.store;

import org.infinispan.commons.util.Version;
import org.infinispan.test.integration.protostream.ServerIntegrationSCI;
import org.infinispan.test.integration.store.AbstractInfinispanStoreJpaIT;
import org.jboss.arquillian.container.test.api.Deployment;
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
 * Test the Infinispan JPA CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanStoreJpaIT extends AbstractInfinispanStoreJpaIT {

   @Deployment
   public static Archive<?> deployment() {
      WebArchive war = ShrinkWrap
            .create(WebArchive.class, "jpa.war")
            .addClass(AbstractInfinispanStoreJpaIT.class)
            .addClasses(ServerIntegrationSCI.CLASSES)
            .addAsResource("META-INF/ispn-persistence.xml", "META-INF/persistence.xml")
            .addAsResource("jpa-config.xml")
            .add(manifest(), "META-INF/MANIFEST.MF");
      return war;
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }
}
