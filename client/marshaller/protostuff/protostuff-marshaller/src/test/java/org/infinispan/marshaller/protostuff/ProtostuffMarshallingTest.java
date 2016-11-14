package org.infinispan.marshaller.protostuff;

import org.infinispan.marshaller.test.AbstractMarshallingTest;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "functional", testName = "marshaller.protostuff.ProtostuffMarshallingTest")
public class ProtostuffMarshallingTest extends AbstractMarshallingTest {

   ProtostuffMarshallingTest() {
      super(new ProtostuffMarshaller());
   }

   protected void checkCustomSerializerCounters(int readCount, int writeCount) {
      assert UserSchema.mergeFromCount.get() == readCount;
      assert UserSchema.writeToCount.get() == writeCount;
   }

   protected void resetCustomerSerializerCounters() {
      UserSchema.mergeFromCount.set(0);
      UserSchema.writeToCount.set(0);
   }
}
