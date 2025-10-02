package org.infinispan.marshall.protostream.impl.adapters;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.ClassAdapterTest")
public class ClassAdapterTest extends AbstractAdapterTest {

   public void testClassIsMarshallable() throws Exception {
      Class<?> original = String.class;
      Class<?> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }
}
