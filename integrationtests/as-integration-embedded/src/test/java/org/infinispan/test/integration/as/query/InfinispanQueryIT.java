package org.infinispan.test.integration.as.query;

import java.util.List;

import javax.inject.Inject;

import org.infinispan.Version;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
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

/**
 * Test the Infinispan AS module integration
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanQueryIT {

   @Inject
   private GridService service;

   @Deployment(name = "dep.active-1")
   @TargetsContainer("container.active-1")
   public static Archive<?> createTestDeploymentOne() {
      return deployment();
   }

   @Deployment(name = "dep.active-2")
   @TargetsContainer("container.active-2")
   public static Archive<?> createTestDeploymentTwo() {
      return deployment();
   }

   private static Archive<?> deployment() {
      return ShrinkWrap.create(WebArchive.class)
            .addClasses(InfinispanQueryIT.class, QueryConfiguration.class, Book.class, GridService.class)
            .add(manifest(), "META-INF/MANIFEST.MF")
            .addAsResource("dynamic-indexing-distribution.xml")
            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies",
                    "org.infinispan:"       + Version.MODULE_SLOT + " services, "
                  + "org.infinispan.query:" + Version.MODULE_SLOT + " services")
                  .exportAsString();
      return new StringAsset(manifest);
   }

   /**
    * This is the "@Before" phase, so we abuse of the test and sequence
    * annotations to get it pushed on a specific node before the actual
    * tests.
    */
   @Test @InSequence(value=1) @OperateOnDeployment("dep.active-1")
   public void loadData() {
      storeSamples(true);
   }

   private void storeSamples(boolean index) {
      service.store("AB1", new Book("Hibernate in Action", "manning"), index);
      service.store("AB2", new Book("Seam in Action", "manning"), index);
      service.store("AB3", new Book("Hibernate Search in Action", "manning"), index);
   }

   @Test @InSequence(value=2) @OperateOnDeployment("dep.active-1")
   public void testSimpleGetOnFirstNode() {
      Book book = service.findById( "AB1" );
      Assert.assertNotNull(book);
      Assert.assertEquals("Hibernate in Action", book.title);
   }

   @Test @InSequence(value=3) @OperateOnDeployment("dep.active-2")
   public void testSimpleGetOnSecondNode() {
      Book book = service.findById( "AB2" );
      Assert.assertNotNull(book);
      Assert.assertEquals("Seam in Action", book.title);
   }

   @Test @InSequence(value=4) @OperateOnDeployment("dep.active-1")
   public void testQueryOnFirstNode() {
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("Hibernate");
      Assert.assertEquals(2, secondMatches.size());
   }

   @Test @InSequence(value=5) @OperateOnDeployment("dep.active-2")
   public void testQueryOnSecondNode() {
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(2, secondMatches.size());
   }

   @Test @InSequence(value=6) @OperateOnDeployment("dep.active-1")
   public void testWipeIndex() {
      service.clear();
      storeSamples(false);
      List matches = service.findFullText("action");
      Assert.assertEquals(0, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(0, secondMatches.size());
   }

   @Test @InSequence(value=7) @OperateOnDeployment("dep.active-2")
   public void testIndexIsEmpty() {
      List matches = service.findFullText("action");
      Assert.assertEquals(0, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(0, secondMatches.size());
   }

   @Test @InSequence(value=8) @OperateOnDeployment("dep.active-1")
   public void testMassIndexer() {
      service.rebuildIndexes();
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(2, secondMatches.size());
   }

   @Test @InSequence(value=9) @OperateOnDeployment("dep.active-2")
   public void testMassIndexerResult() {
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(2, secondMatches.size());
   }

}
