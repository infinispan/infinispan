package org.infinispan.test.integration.as;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.server.infinispan.spi.CacheContainer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
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

/**
 * Test the infinispan extension can be loaded in WFLY
 *
 * @author Ryan Emerson
 * @since 9.2
 */
@ApplicationScoped
@RunWith(Arquillian.class)
public class InfinispanExtensionIT {

   @Resource(lookup = "java:jboss/datagrid-infinispan/container/infinispan_container")
   CacheContainer container;

   @Resource(lookup = "java:jboss/datagrid-infinispan/container/infinispan_container/cache/default")
   Cache cache;

   @Deployment(name = "dep1", order = 1)
   public static Archive<?> dep1() {
      return createDeployment("dep1.war");
   }

   @Deployment(name = "dep2", order = 2)
   public static Archive<?> dep2() {
      return createDeployment("dep2.war");
   }

   private static Archive<?> createDeployment(String name) {
      return ShrinkWrap.create(WebArchive.class, name)
            .addClass(InfinispanExtensionIT.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", createDepString("org.infinispan.extension", "org.infinispan.server.endpoint",
                  "org.jgroups.extension")).exportAsString();
      return new StringAsset(manifest);
   }

   private static String createDepString(String... dependencies) {
      StringBuilder sb = new StringBuilder();
      for (String dep : dependencies)
         sb.append(dep).append(":").append(Version.getModuleSlot()).append(" services,");
      return sb.toString();
   }

   @Test
   @OperateOnDeployment("dep1")
   public void testDep1() {
      assertNotNull(container);
      assertNotNull(cache);
      cache.put("1", 1);
      assertEquals(1, cache.get("1"));
   }

   @Test
   @OperateOnDeployment("dep2")
   public void testDep2() {
      assertNotNull(container);
      assertNotNull(cache);
      assertEquals(1, cache.get("1"));
   }
}
