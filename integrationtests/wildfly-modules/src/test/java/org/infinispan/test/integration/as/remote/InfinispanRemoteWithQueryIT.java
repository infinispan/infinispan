package org.infinispan.test.integration.as.remote;

import org.infinispan.Version;
import org.infinispan.test.integration.as.client.BaseHotRodQueryIT;
import org.infinispan.test.integration.as.client.HotRodQueryIT;
import org.infinispan.test.integration.as.client.Person;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.runner.RunWith;

/**
 * Tests for querying using 'org.infinispan.remote' aggregator module.
 *
 * @since 9.0
 */
@RunWith(Arquillian.class)
public class InfinispanRemoteWithQueryIT extends BaseHotRodQueryIT {

   @Deployment
   public static Archive<?> deployment() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan.remote:" + Version.getModuleSlot())
            .exportAsString();

      return ShrinkWrap
            .create(WebArchive.class, "query-remote.war")
            .addClasses(HotRodQueryIT.class, BaseHotRodQueryIT.class, Person.class)
            .add(new StringAsset(manifest), "META-INF/MANIFEST.MF");
   }

}
