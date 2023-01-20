package org.infinispan.configuration.parsing;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.YamlParsingTest")
public class YamlParsingTest extends AbstractInfinispanTest {
   public void testSerializationAllowList() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/serialization-test.yaml");
      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      Set<String> classes = globalConfiguration.serialization().allowList().getClasses();
      assertEquals(3, classes.size());
      List<String> regexps = globalConfiguration.serialization().allowList().getRegexps();
      assertEquals(2, regexps.size());
   }

   public void testErrorReporting() {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      Exceptions.expectException("^ISPN000327:.*broken.yaml\\[18,18\\].*", () -> parserRegistry.parseFile("configs/broken.yaml"), CacheConfigurationException.class);
   }
}
