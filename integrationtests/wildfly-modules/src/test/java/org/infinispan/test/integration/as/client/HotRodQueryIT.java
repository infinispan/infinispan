package org.infinispan.test.integration.as.client;

import org.infinispan.Version;
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
 * Test remote query.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
@RunWith(Arquillian.class)
public class HotRodQueryIT extends BaseHotRodQueryIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "remote-query.war")
            .addClasses(HotRodQueryIT.class, BaseHotRodQueryIT.class, Person.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan.client.hotrod:" + Version.getModuleSlot() + " services, " +
                  "org.infinispan.protostream:" + Version.getModuleSlot() + " services, " +
                  "org.infinispan.query.dsl:" + Version.getModuleSlot() + " services, " +
                  "org.infinispan.commons:" + Version.getModuleSlot() + " services")
            .exportAsString();
      return new StringAsset(manifest);
   }

}
