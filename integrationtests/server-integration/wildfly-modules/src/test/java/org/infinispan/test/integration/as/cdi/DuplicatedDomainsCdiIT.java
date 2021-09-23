package org.infinispan.test.integration.as.cdi;

import org.infinispan.commons.util.Version;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.integration.cdi.AbstractDuplicatedDomainsCdiIT;
import org.infinispan.test.integration.cdi.CdiConfig;
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
 * Tests whether {@link DefaultCacheManager} sets custom Cache name to avoid JMX
 * name collision.
 *
 * @author Sebastian Laskawiec
 */
@RunWith(Arquillian.class)
public class DuplicatedDomainsCdiIT extends AbstractDuplicatedDomainsCdiIT {

   @Deployment
   public static Archive<?> deployment() {
      WebArchive webArchive = ShrinkWrap
            .create(WebArchive.class, "cdi-dd.war")
            .addClass(DuplicatedDomainsCdiIT.class)
            .addClass(AbstractDuplicatedDomainsCdiIT.class)
            .addClass(CdiConfig.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
      return webArchive;
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services meta-inf").exportAsString();
      return new StringAsset(manifest);
   }
}
