package org.infinispan.test.integration.as.cdi;

import org.infinispan.commons.util.Version;
import org.infinispan.test.integration.cdi.AbstractGreetingCacheManagerIT;
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
 * @author Kevin Pollet &lt;pollet.kevin@gmail.com&gt; (C) 2011
 */
@RunWith(Arquillian.class)
public class GreetingCacheManagerIT extends AbstractGreetingCacheManagerIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "cdi-cm.war")
            .addPackage(AbstractGreetingCacheManagerIT.class.getPackage())
            .add(manifest(), "META-INF/MANIFEST.MF")
            .addAsWebInfResource("beans.xml");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }
}
