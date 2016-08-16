package org.infinispan.query.config;

import static org.testng.AssertJUnit.fail;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "query.config.ProgrammaticConfigInheritanceTest", groups = "functional")
public class ProgrammaticConfigInheritanceTest {

   public void testPropertyMutabilityInInheritance() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().autoConfig(true).addProperty("key", "value");
      Configuration configuration = builder.build();
      try {
         configuration.indexing().properties().setProperty("anotherKey", "anotherValue");
         fail("Expecting unmodifiable properties");
      } catch (UnsupportedOperationException e) {
         // expected
      }

      ConfigurationBuilder derived = new ConfigurationBuilder();
      derived.read(configuration);
      builder.indexing().addProperty("anotherKey", "anotherValue");

   }
}
