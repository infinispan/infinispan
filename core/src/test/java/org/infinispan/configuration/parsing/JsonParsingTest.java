package org.infinispan.configuration.parsing;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.testing.Exceptions;
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

   public void testErrorReporting() {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      Exceptions.expectException("^ISPN000327:.*broken.json\\[23,15\\].*", () -> parserRegistry.parseFile("configs/broken.json"), CacheConfigurationException.class);
   }

   public void testInvalidTracingCollector() throws Exception {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/tracing-endpoint-wrong.json");
      Exceptions.expectException("^ISPN000699:.*Tracing collector endpoint 'sdjsd92k2..21232' is not valid.*", () -> holder.getGlobalConfigurationBuilder().build(), CacheConfigurationException.class);
   }

   public void testAliasTest() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/aliases-test.json");
      Configuration cache = holder.getNamedConfigurationBuilders().get("anotherRespCache").build();
      assertEquals(Set.of("1"), cache.aliases());
   }

   public void testNaming() {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      String configJson = """
            {
                "actionTokens": {
                    "distributed-cache": {
                        "owners": "2",
                        "mode": "SYNC",
                        "encoding": {
                            "key": {
                                "media-type": "application/x-java-object"
                            },
                            "value": {
                                "media-type": "application/x-java-object"
                            }
                        },
                        "expiration": {
                            "lifespan": "-1",
                            "max-idle": "-1",
                            "interval": "300000"
                        },
                        "memory": {
                            "max-count": "100"
                        }
                    }
                }
            }""";

      ConfigurationBuilderHolder configHolder = parserRegistry.parse(configJson, MediaType.APPLICATION_JSON);
      assertTrue(configHolder.getNamedConfigurationBuilders().containsKey("actionTokens"));
   }
}
