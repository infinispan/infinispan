package org.infinispan.test.integration.as.query;

import java.util.List;

import javax.inject.Inject;

import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.InSequence;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @since 9.0
 */
public class BaseQueryTest {

   @Inject
   protected GridService service;

   /**
    * This is the "@Before" phase, so we abuse of the test and sequence
    * annotations to get it pushed on a specific node before the actual
    * tests.
    */
   @Test
   @InSequence(1)
   @OperateOnDeployment("dep.active-1")
   public void loadData() {
      storeSamples(true);
   }

   private void storeSamples(boolean index) {
      service.store("AB1", new Book("Hibernate in Action", "manning"), index);
      service.store("AB2", new Book("Seam in Action", "manning"), index);
      service.store("AB3", new Book("Hibernate Search in Action", "manning"), index);
   }

   @Test
   @InSequence(2)
   @OperateOnDeployment("dep.active-1")
   public void testSimpleGetOnFirstNode() {
      Book book = service.findById("AB1");
      Assert.assertNotNull(book);
      Assert.assertEquals("Hibernate in Action", book.title);
   }

   @Test
   @InSequence(3)
   @OperateOnDeployment("dep.active-2")
   public void testSimpleGetOnSecondNode() {
      Book book = service.findById("AB2");
      Assert.assertNotNull(book);
      Assert.assertEquals("Seam in Action", book.title);
   }

   @Test
   @InSequence(4)
   @OperateOnDeployment("dep.active-1")
   @Ignore(value = "Will be fixed by ISPN-5929")
   public void testQueryOnFirstNode() {
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("Hibernate");
      Assert.assertEquals(2, secondMatches.size());
   }

   @Test
   @InSequence(5)
   @OperateOnDeployment("dep.active-2")
   @Ignore(value = "Will be fixed by ISPN-5929")
   public void testQueryOnSecondNode() {
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(2, secondMatches.size());
   }

   @Test
   @InSequence(6)
   @OperateOnDeployment("dep.active-1")
   public void testWipeIndex() {
      service.clear();
      storeSamples(false);
      List matches = service.findFullText("action");
      Assert.assertEquals(0, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(0, secondMatches.size());
   }

   @Test
   @InSequence(7)
   @OperateOnDeployment("dep.active-2")
   public void testIndexIsEmpty() {
      List matches = service.findFullText("action");
      Assert.assertEquals(0, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(0, secondMatches.size());
   }

   @Test
   @InSequence(8)
   @OperateOnDeployment("dep.active-1")
   public void testMassIndexer() {
      service.rebuildIndexes();
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(2, secondMatches.size());
   }

   @Test
   @InSequence(9)
   @OperateOnDeployment("dep.active-2")
   public void testMassIndexerResult() {
      List matches = service.findFullText("action");
      Assert.assertEquals(3, matches.size());
      List secondMatches = service.findFullText("HibernatE");
      Assert.assertEquals(2, secondMatches.size());
   }

}
