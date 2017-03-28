package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Hot Rod server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodServerTest")
public class HotRodServerTest extends AbstractInfinispanTest {

   public void testValidateProtocolServerNullProperties() {
      Stoppable.useCacheManager(createCacheManager(hotRodCacheConfiguration()),
                                cm -> Stoppable.useServer(new HotRodServer(), server -> {
                                   server.start(new HotRodServerConfigurationBuilder().build(), cm);
                                   assertEquals(server.getHost(), "127.0.0.1");
                                   assertEquals(server.getPort(), 11222);
                                }));
   }
}
