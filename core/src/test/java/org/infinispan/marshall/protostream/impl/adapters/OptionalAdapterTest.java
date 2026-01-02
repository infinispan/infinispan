package org.infinispan.marshall.protostream.impl.adapters;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Optional;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.OptionalAdapterTest")
public class OptionalAdapterTest extends AbstractAdapterTest {

   public void testEmptyOptional() throws Exception {
      Optional<String> original = Optional.empty();
      Optional<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testPresentOptional() throws Exception {
      Optional<String> original = Optional.of("with value");
      Optional<String> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }
}
