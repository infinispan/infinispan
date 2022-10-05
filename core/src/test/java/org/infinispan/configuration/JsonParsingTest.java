package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.JsonParsingTest")
public class JsonParsingTest extends AbstractInfinispanTest {
   public void testSerializationAllowList() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/serialization-test.json");
      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      Set<String> classes = globalConfiguration.serialization().allowList().getClasses();
      assertEquals(3, classes.size());
      List<String> regexps = globalConfiguration.serialization().allowList().getRegexps();
      assertEquals(2, regexps.size());
   }
}
