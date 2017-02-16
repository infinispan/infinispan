package org.infinispan.commons.marshall;

import org.testng.annotations.Test;

/**
 * Tests that the adaptive buffer size predictor adjusts sizes
 * in different circumstances.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.AdaptiveBufferSizePredictorTest")
public class AdaptiveBufferSizePredictorTest {

   public void testAdaptivenesOfBufferSizeChanges() throws Exception {
      AdaptiveBufferSizePredictor predictor = new AdaptiveBufferSizePredictor();
      int size = 32;
      int nextSize;
      int prevNextSize = AdaptiveBufferSizePredictor.DEFAULT_INITIAL;
      for (int i = 0; i < 100; i++) {
         predictor.recordSize(size);
         nextSize = predictor.nextSize(null);
         if (i % 2 != 0) {
            if ((nextSize * 0.88) < size)
               break;
            else {
               assert nextSize < prevNextSize;
               prevNextSize = nextSize;
            }
         }
      }

      size = 32768;

      for (int i = 0; i < 100; i++) {
         predictor.recordSize(size);
         nextSize = predictor.nextSize(null);
         if ((nextSize * 0.89) > size) {
            break;
         } else {
            assert nextSize > prevNextSize;
            prevNextSize = nextSize;
         }
      }
   }

}
