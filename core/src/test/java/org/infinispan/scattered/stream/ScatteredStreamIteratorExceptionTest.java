package org.infinispan.scattered.stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.stream.DistributedStreamIteratorExceptionTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "scattered.stream.ScatteredStreamIteratorExceptionTest")
public class ScatteredStreamIteratorExceptionTest extends DistributedStreamIteratorExceptionTest {
   public ScatteredStreamIteratorExceptionTest() {
      super(CacheMode.SCATTERED_SYNC);
   }

   @Override
   protected InternalDataContainer mockContainer(Throwable t) {
      return when(mock(InternalDataContainer.class).spliterator()).thenThrow(t).getMock();
   }
}
