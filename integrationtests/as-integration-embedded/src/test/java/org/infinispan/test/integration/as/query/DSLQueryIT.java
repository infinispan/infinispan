package org.infinispan.test.integration.as.query;

import org.infinispan.Version;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.util.List;

/**
 *
 * Test for DSL queries when using the wildfly modules
 *
 * @author gustavonalle
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class DSLQueryIT {

   @Inject
   private GridService service;

   @Deployment
   @SuppressWarnings("unused")
   private static Archive<?> deployment() {
      return ShrinkWrap.create(WebArchive.class, "dsl.war")
            .addClasses(DSLQueryIT.class, QueryConfiguration.class, Book.class, GridService.class)
            .add(manifest(), "META-INF/MANIFEST.MF")
            .addAsResource("dynamic-indexing-distribution.xml")
            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.MODULE_SLOT + " services, org.infinispan.query:" + Version.MODULE_SLOT + " services")
            .exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testDSLQuery() throws Exception {
      service.store("00123", new Book("Functional Programming in Scala","manning"), true);
      List<Object> results = service.findByPublisher("manning");
      Assert.assertEquals(1, results.size());
   }
}
