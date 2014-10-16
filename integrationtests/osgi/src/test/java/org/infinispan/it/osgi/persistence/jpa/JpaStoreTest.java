package org.infinispan.it.osgi.persistence.jpa;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class JpaStoreTest extends org.infinispan.persistence.jpa.JpaStoreTest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Before
   @Override
   public void setUp() throws Exception {
      TestResourceTracker.backgroundTestStarted(this);
      super.setUp();
   }

   @After
   @Override
   public void tearDown() throws PersistenceException {
      super.tearDown();
   }

   @Test
   @Override
   public void testLoadAndStoreImmortal() throws PersistenceException {
      super.testLoadAndStoreImmortal();
   }

   @Test
   @Override
   public void testLoadAndStoreWithLifespan() throws Exception {
      super.testLoadAndStoreWithLifespan();
   }

   @Test
   @Override
   public void testLoadAndStoreWithIdle() throws Exception {
      super.testLoadAndStoreWithIdle();
   }

   @Test
   @Override
   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      super.testLoadAndStoreWithLifespanAndIdle();
   }

   @Test
   @Override
   public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException {
      super.testStopStartDoesNotNukeValues();
   }

   @Test
   @Override
   public void testPreload() throws Exception {
      super.testPreload();
   }

   @Test
   @Override
   public void testStoreAndRemove() throws PersistenceException {
      super.testStoreAndRemove();
   }

   @Test
   @Override
   public void testPurgeExpired() throws Exception {
      super.testPurgeExpired();
   }

   @Test
   @Override
   public void testReplaceExpiredEntry() throws Exception {
      super.testReplaceExpiredEntry();
   }

   @Test
   @Override
   public void testLoadAll() throws PersistenceException {
      super.testLoadAll();
   }
}
