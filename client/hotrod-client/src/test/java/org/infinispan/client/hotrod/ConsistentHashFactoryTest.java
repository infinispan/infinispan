package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

/**
 * Tester for ConsistentHashFactory.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ConsistentHashFactoryTest", groups = "functional")
public class ConsistentHashFactoryTest {

   public void testPropertyCorrectlyRead() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.consistentHashImpl(1, SomeCustomConsistentHashV1.class);
      ConsistentHashFactory chf = new ConsistentHashFactory();
      chf.init(builder.build());
      ConsistentHash hash = chf.newConsistentHash(1);
      assertNotNull(hash);
      assertEquals(hash.getClass(), SomeCustomConsistentHashV1.class);
   }

   public void testNoChDefined() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      ConsistentHashFactory chf = new ConsistentHashFactory();
      chf.init(builder.build());
      ConsistentHash hash = chf.newConsistentHash(1);
      assertNotNull(hash);
      assertEquals(hash.getClass(), ConsistentHashV1.class);
   }
}
