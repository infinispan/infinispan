package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Tester for ConsistentHashFactory.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ConsistentHashFactoryTest", groups = "functional")
public class ConsistentHashFactoryTest extends AbstractInfinispanTest {

   public void testPropertyCorrectlyRead() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.consistentHashImpl(2, SomeCustomConsistentHashV2.class);
      ConsistentHashFactory chf = new ConsistentHashFactory();
      chf.init(builder.build());
      ConsistentHash hash = chf.newConsistentHash(2);
      assertNotNull(hash);
      assertEquals(hash.getClass(), SomeCustomConsistentHashV2.class);
   }

   public void testNoChDefined() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      ConsistentHashFactory chf = new ConsistentHashFactory();
      chf.init(builder.build());
      ConsistentHash hash = chf.newConsistentHash(2);
      assertNotNull(hash);
      assertEquals(hash.getClass(), ConsistentHashV2.class);
   }
}
