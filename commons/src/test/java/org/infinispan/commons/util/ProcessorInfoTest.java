package org.infinispan.commons.util;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class ProcessorInfoTest {

   @Test
   public void testCPUCount() {
      assertTrue(ProcessorInfo.availableProcessors() <= Runtime.getRuntime().availableProcessors());
   }
}
