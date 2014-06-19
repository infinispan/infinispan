package org.infinispan.it.osgi.persistence.jpa;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class JpaStoreOOMETest extends org.infinispan.persistence.jpa.JpaStoreOOMETest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
   }

   @After
   @Override
   public void stopMarshaller() {
      super.stopMarshaller();
   }

   @Test
   @Override
   public void testProcessClear() {
      super.testProcessClear();
   }

   @Test
   @Override
   public void testPurge() {
      super.testPurge();
   }
}
