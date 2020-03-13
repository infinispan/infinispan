package org.infinispan.commons.util;

import org.infinispan.commons.test.categories.Java11;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

public class ProcessorInfoTest {

   @Test
   @Category(Java11.class)
   public void testCPUCount() {
      assertTrue(ProcessorInfo.availableProcessors() <= Runtime.getRuntime().availableProcessors());
   }
}
