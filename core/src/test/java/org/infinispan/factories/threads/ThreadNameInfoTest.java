package org.infinispan.factories.threads;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "unit", testName = "factories.threads.ThreadNameInfoTest")
public class ThreadNameInfoTest {

   public void testThreadNamePatterns() {
      ThreadNameInfo name = new ThreadNameInfo(100, 1, 2, "nodeX", "eviction");
      String threadName = name.format(Thread.currentThread(), "infinispan-%n-%c-p%f-t%t");
      assertEquals("infinispan-nodeX-eviction-p2-t1", threadName);

      threadName = name.format(Thread.currentThread(), "%%-%g%p%");
      assertEquals("%-100system:main", threadName);
   }

}
