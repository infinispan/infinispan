package org.infinispan.test.integration.as.embedded;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.commons.util.Version;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.integration.data.Person;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
public class ReplicatedExampleIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "simple.war")
            .addClass(ReplicatedExampleIT.class)
            .addClass(Person.class)
            .addAsResource("cache-config/replicated-example.xml", "infinispan.xml")
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class).attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testPut() throws IOException {
      try (EmbeddedCacheManager cm = new DefaultCacheManager("infinispan.xml")){
         Cache<String, Person> cache = cm.getCache("test1");
         cache.put("p1", new Person("diego", 1));
         assertEquals("diego", cache.get("p1").getName());
      }
   }
}
