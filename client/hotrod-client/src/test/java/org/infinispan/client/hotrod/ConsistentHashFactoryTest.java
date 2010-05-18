package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Tester for ConsistentHashFactory.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ConsistentHashFactoryTest", groups = "functional")
public class ConsistentHashFactoryTest {

   public void testPropertyCorrectlyRead() {
      Properties propos = new Properties();
      String value = "org.infinispan.client.hotrod.impl.consistenthash.SomeCustomConsitentHashV1";
      propos.put("consistent-hash.1", value);
      ConsistentHashFactory chf = new ConsistentHashFactory();
      chf.init(propos);
      String s = chf.getVersion2ConsistentHash().get(1);
      assert s != null;
      assert value.equals(s);
   }

   public void testNoChDefined() {
      ConsistentHashFactory chf = new ConsistentHashFactory();
      ConsistentHash hash = chf.newConsistentHash(1);
      assert hash != null;
      assert hash.getClass().equals(ConsistentHashV1.class);
   }
}
