package org.infinispan.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 14.0
 */
@Test(groups = "functional", testName= "configuration.PersistentConfigurationMatchesTest")
public class PersistentConfigurationMatchesTest extends AbstractInfinispanTest {
   public void testOldConfigurationMatches() {
      String cacheName = "someCache";
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder oldHolder = parserRegistry.parse("<?xml version=\"1.0\"?>\n" +
            "<infinispan xmlns=\"urn:infinispan:config:13.0\">\n" +
            "    <cache-container>\n" +
            "        <caches>\n" +
            "            <distributed-cache name=\"someCache\" mode=\"SYNC\">\n" +
            "                <persistence>\n" +
            "                    <file-store/>\n" +
            "                </persistence>\n" +
            "            </distributed-cache>\n" +
            "        </caches>\n" +
            "    </cache-container>\n" +
            "</infinispan>\n");

      ConfigurationBuilderHolder newHolder = parserRegistry.parse("<?xml version=\"1.0\"?>\n" +
            "<infinispan xmlns=\"urn:infinispan:config:14.0\">\n" +
            "    <cache-container>\n" +
            "        <caches>\n" +
            "            <distributed-cache name=\"someCache\" mode=\"SYNC\">\n" +
            "                <persistence>\n" +
            "                    <file-store/>\n" +
            "                </persistence>\n" +
            "            </distributed-cache>\n" +
            "        </caches>\n" +
            "    </cache-container>\n" +
            "</infinispan>\n");
      Configuration oldConfig = oldHolder.getNamedConfigurationBuilders().get(cacheName).build();
      Configuration newConfig = newHolder.getNamedConfigurationBuilders().get(cacheName).build();
      AssertJUnit.assertTrue(oldConfig.matches(newConfig));
   }
}
