package org.infinispan.server.integration;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.jboss.arquillian.junit.ArquillianTest;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 *
 * @author Richard Achmatowicz
 * @author Martin Gencur
 * @author Jozef Vilkolak
 */
@InfinispanTest(config = "ispn-config/qe-infinispan.xml", numberOfServers = 1)
public class HotRodRemoteCacheIT extends BaseIT {

   @ClassRule
   public static InstrumentedArquillianTestClass instrumentedArquillianTestClass = new InstrumentedArquillianTestClass();

   @Rule
   public ArquillianTest arquillianTest = new ArquillianTest();

   @InfinispanResourceTest
   private static InfinispanServerTestMethodRule infinispanServerTestMethodRule;

   @Test
   public void testCacheManager() {
      RemoteCache remoteCache = infinispanServerTestMethodRule.hotrod().create();
      remoteCache.put("foo", "bar");
      assertEquals("bar", remoteCache.get("foo"));
   }
}
