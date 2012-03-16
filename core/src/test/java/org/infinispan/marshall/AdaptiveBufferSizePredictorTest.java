/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.marshall;

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
