package org.infinispan.it.osgi.persistence.jpa;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.it.osgi.util.CustomPaxExamRunner;
import org.infinispan.commons.test.TestResourceTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

@RunWith(CustomPaxExamRunner.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class JpaStoreFunctionalTest extends org.infinispan.persistence.jpa.JpaStoreFunctionalTest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Before
   @Override
   public void setup() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      super.setup();
   }

   @After
   @Override
   public void teardown() {
      super.teardown();
   }

   @Test
   @Override
   public void testPreloadAndExpiry() {
      super.testPreloadAndExpiry();
   }
}
