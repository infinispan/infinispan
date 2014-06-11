package org.infinispan.it.osgi.persistence.jdbc.configuration;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.test.TestingUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class XmlFileParsingTest extends org.infinispan.persistence.jdbc.configuration.XmlFileParsingTest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @After
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Test
   public void testStringKeyedJdbcStore() throws Exception {
      super.testStringKeyedJdbcStore();
   }

   @Test
   public void testBinaryKeyedJdbcStore() throws Exception {
      super.testBinaryKeyedJdbcStore();
   }

   @Test
   public void testMixedKeyedJdbcStore() throws Exception {
      super.testMixedKeyedJdbcStore();
   }
}
