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
public class JpaStorePersonEntityTest extends org.infinispan.persistence.jpa.JpaStorePersonEntityTest {
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
   public void stopMarshaller() {
      super.stopMarshaller();
   }

   @Test
   @Override
   public void testLoadAndStoreImmortal() {
      super.testLoadAndStoreImmortal();
   }

   @Test
   @Override
   public void testPreload() throws Exception {
      super.testPreload();
   }

   @Test
   @Override
   public void testStoreAndRemoveAll() {
      super.testStoreAndRemoveAll();
   }

   @Test(expected = PersistenceException.class)
   @Override
   public void testStoreNoJpa() {
      super.testStoreNoJpa();
   };

   @Test(expected = PersistenceException.class)
   @Override
   public void testStoreWithJpaBadKey() {
      super.testStoreWithJpaBadKey();
   }

   @Test
   @Override
   public void testStoreWithJpaGoodKey() {
      super.testStoreWithJpaGoodKey();
   }
}
