package org.infinispan.commons.util;

import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;

@Test(testName="commons.util.MemoryUnitTest", groups="functional")
public class MemoryUnitTest {

   public void testMemoryUnitParser() {
      assertEquals(1000, MemoryUnit.parseBytes("1000"));
      assertEquals(1000, MemoryUnit.parseBytes("1K"));
      assertEquals(1024, MemoryUnit.parseBytes("1Ki"));
      assertEquals(1_000_000l, MemoryUnit.parseBytes("1M"));
      assertEquals(1_048_576l, MemoryUnit.parseBytes("1Mi"));
      assertEquals(1_000_000_000l, MemoryUnit.parseBytes("1G"));
      assertEquals(1_073_741_824l, MemoryUnit.parseBytes("1Gi"));
      assertEquals(1_000_000_000_000l, MemoryUnit.parseBytes("1T"));
      assertEquals(1_099_511_627_776l, MemoryUnit.parseBytes("1Ti"));
   }
}
