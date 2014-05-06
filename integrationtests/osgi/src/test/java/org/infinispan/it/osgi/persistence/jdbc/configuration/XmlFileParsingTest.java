package org.infinispan.it.osgi.persistence.jdbc.configuration;

import org.infinispan.it.osgi.Osgi;
import org.infinispan.test.TestingUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import static org.infinispan.it.osgi.util.IspnKarafOptions.*;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@Category(Osgi.class)
@ExamReactorStrategy(PerClass.class)
public class XmlFileParsingTest extends org.infinispan.persistence.jdbc.configuration.XmlFileParsingTest {

   @Configuration
   public Option[] config() throws Exception {
      return options(
            karafContainer(),
            featureIspnCoreDependencies(),
            featureIspnCore(),
            featureJdbcStore(),
            junitBundles(),
            keepRuntimeFolder()
      );
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
