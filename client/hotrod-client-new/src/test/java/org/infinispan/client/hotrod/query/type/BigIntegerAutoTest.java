package org.infinispan.client.hotrod.query.type;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.CalculusAuto;
import org.infinispan.client.hotrod.query.testdomain.protobuf.CalculusAutoSchemaImpl;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.protostream.SerializationContextInitializer;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.type.BigIntegerAutoTest")
public class BigIntegerAutoTest extends SingleHotRodServerTest {

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return new CalculusAutoSchemaImpl();
   }

   @Test
   public void test() {
      RemoteCache<String, CalculusAuto> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("1", new CalculusAuto(BigInteger.TEN));
      CalculusAuto calculus = remoteCache.get("1");
      assertThat(calculus.getPurchases()).isEqualTo(10);
   }
}
