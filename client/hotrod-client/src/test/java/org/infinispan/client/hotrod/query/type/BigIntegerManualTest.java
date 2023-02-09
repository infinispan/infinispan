package org.infinispan.client.hotrod.query.type;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.CalculusManual;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.CalculusManualSCI;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.protostream.SerializationContextInitializer;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.type.BigIntegerManualTest")
public class BigIntegerManualTest extends SingleHotRodServerTest {

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return new CalculusManualSCI();
   }

   @Test
   public void test() {
      RemoteCache<String, CalculusManual> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("1", new CalculusManual(BigInteger.TEN));
      CalculusManual calculus = remoteCache.get("1");
      assertThat(calculus.getPurchases()).isEqualTo(10);
   }
}
