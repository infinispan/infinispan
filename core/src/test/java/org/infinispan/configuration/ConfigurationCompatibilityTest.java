package org.infinispan.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName= "configuration.ConfigurationCompatibilityTest")
public class ConfigurationCompatibilityTest {

   public void testDocumentationPersistenceConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence()
         .passivation(false)
         .addSingleFileStore()
            .fetchPersistentState(true)
            .shared(false)
            .preload(true)
            .ignoreModifications(false)
            .purgeOnStartup(false)
            .location(System.getProperty("java.io.tmpdir"))
            .async()
               .enabled(true)
               .flushLockTimeout(15000)
               .threadPoolSize(5)
            .singleton()
              .enabled(true)
              .pushStateWhenCoordinator(true)
              .pushStateTimeout(20000);
   }

}
