package org.infinispan.configuration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
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
      ConfigurationBuilderHolder oldHolder = parserRegistry.parse("""
            <?xml version="1.0"?>
            <infinispan xmlns="urn:infinispan:config:13.0">
                <cache-container>
                    <caches>
                        <distributed-cache name="someCache" mode="SYNC">
                            <persistence>
                                <file-store/>
                            </persistence>
                        </distributed-cache>
                    </caches>
                </cache-container>
            </infinispan>
            """);

      ConfigurationBuilderHolder newHolder = parserRegistry.parse("""
            <?xml version="1.0"?>
            <infinispan xmlns="urn:infinispan:config:14.0">
                <cache-container>
                    <caches>
                        <distributed-cache name="someCache" mode="SYNC">
                            <persistence>
                                <file-store/>
                            </persistence>
                        </distributed-cache>
                    </caches>
                </cache-container>
            </infinispan>
            """);
      Configuration oldConfig = oldHolder.getNamedConfigurationBuilders().get(cacheName).build();
      Configuration newConfig = newHolder.getNamedConfigurationBuilders().get(cacheName).build();
      assertTrue(oldConfig.matches(newConfig));
   }
}
