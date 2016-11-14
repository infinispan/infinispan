package org.infinispan.marshaller.kryo;

import org.infinispan.marshaller.test.AbstractMarshallingTest;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "functional", testName = "marshaller.kryo.KryoMarshallingTest")
public class KryoMarshallingTest extends AbstractMarshallingTest {

   KryoMarshallingTest() {
      super(new KryoMarshaller());
   }

   protected void checkCustomSerializerCounters(int readCount, int writeCount) {
      assert UserSerializer.readCount.get() == readCount;
      assert UserSerializer.writeCount.get() == writeCount;
   }

   protected void resetCustomerSerializerCounters() {
      UserSerializer.readCount.set(0);
      UserSerializer.writeCount.set(0);
   }
}
