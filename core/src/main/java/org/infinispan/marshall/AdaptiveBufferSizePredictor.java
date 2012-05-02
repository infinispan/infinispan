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

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link BufferSizePredictor} that automatically increases and
 * decreases the predicted buffer size on feed back.
 * <p>
 * It gradually increases the expected number of bytes if the previous buffer
 * fully filled the allocated buffer.  It gradually decreases the expected
 * number of bytes if the read operation was not able to fill a certain amount
 * of the allocated buffer two times consecutively.  Otherwise, it keeps
 * returning the same prediction.
 *
 * TODO: Object type hints could be useful at giving more type-specific predictions
 *
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Galder Zamarreño
 * @since 5.0
 */
public class AdaptiveBufferSizePredictor implements BufferSizePredictor {

   private static final Log log = LogFactory.getLog(MarshallableTypeHints.class);
   private static final boolean isTrace = log.isTraceEnabled();

   static final int DEFAULT_MINIMUM = 16;
   static final int DEFAULT_INITIAL = 512;
   static final int DEFAULT_MAXIMUM = 65536;

   private static final int INDEX_INCREMENT = 4;
   private static final int INDEX_DECREMENT = 1;

   private static final int[] SIZE_TABLE;

   private final int minIndex;
   private final int maxIndex;
   private int index;
   private int nextBufferSize;
   private boolean decreaseNow;

   static {
      List<Integer> sizeTable = new ArrayList<Integer>();

      // First, add the base 1 to 8 bytes sizes
      for (int i = 1; i <= 8; i++)
         sizeTable.add(i);

      for (int i = 4; i < 32; i++) {
         long v = 1L << i;
         long inc = v >>> 4;
         v -= inc << 3;
         // From 8 onwards, follow a 2^i progression of increments,
         // applying each increment 8 times. For example:
         // for incr=2^1 ->  9, 10, 11, 12, 13, 14, 15, 16
         // for incr=2^2 -> 18, 20, 22, 24, 26, 28, 30, 32
         // for incr=2^3 -> 36, 40, 44, 48 ...
         // ...
         // for incr=2^31 -> 1073741824, 1207959552...etc

         for (int j = 0; j < 8; j++) {
            v += inc;
            if (v > Integer.MAX_VALUE)
               sizeTable.add(Integer.MAX_VALUE);
            else
               sizeTable.add((int) v);
         }
      }

      SIZE_TABLE = new int[sizeTable.size()];
      for (int i = 0; i < SIZE_TABLE.length; i++)
         SIZE_TABLE[i] = sizeTable.get(i);
   }

   private static int getSizeTableIndex(final int size) {
       if (size <= 16)
           return size - 1;

       int bits = 0;
       int v = size;
       do {
           v >>>= 1;
           bits ++;
       } while (v != 0);

       final int baseIdx = bits << 3;
       final int startIdx = baseIdx - 18;
       final int endIdx = baseIdx - 25;

       for (int i = startIdx; i >= endIdx; i --)
           if (size >= SIZE_TABLE[i])
               return i;

       throw new RuntimeException("Shouldn't reach here; please file a bug report.");
   }

   /**
    * Creates a new predictor with the default parameters.  With the default
    * parameters, the expected buffer size starts from {@code 512}, does not
    * go down below {@code 16}, and does not go up above {@code 65536}.
    */
   public AdaptiveBufferSizePredictor() {
       this(DEFAULT_MINIMUM, DEFAULT_INITIAL, DEFAULT_MAXIMUM);
   }

   /**
    * Creates a new predictor with the specified parameters.
    *
    * @param minimum  the inclusive lower bound of the expected buffer size
    * @param initial  the initial buffer size when no feed back was received
    * @param maximum  the inclusive upper bound of the expected buffer size
    */
   public AdaptiveBufferSizePredictor(int minimum, int initial, int maximum) {
       if (minimum <= 0)
           throw new IllegalArgumentException("minimum: " + minimum);

       if (initial < minimum)
           throw new IllegalArgumentException("initial: " + initial);

       if (maximum < initial)
           throw new IllegalArgumentException("maximum: " + maximum);


       int minIndex = getSizeTableIndex(minimum);
       if (SIZE_TABLE[minIndex] < minimum)
           this.minIndex = minIndex + 1;
       else
           this.minIndex = minIndex;

       int maxIndex = getSizeTableIndex(maximum);
       if (SIZE_TABLE[maxIndex] > maximum)
           this.maxIndex = maxIndex - 1;
       else
           this.maxIndex = maxIndex;

       index = getSizeTableIndex(initial);
       nextBufferSize = SIZE_TABLE[index];
   }


   @Override
   public int nextSize(Object obj) {
      if (isTrace)
         log.tracef("Next predicted buffer size for object type '%s' will be %d",
               obj == null ? "Null" : obj.getClass().getName(), nextBufferSize);

      return nextBufferSize;
   }

   @Override
   public void recordSize(int previousSize) {
      if (previousSize <= SIZE_TABLE[Math.max(0, index - INDEX_DECREMENT - 1)]) {
          if (decreaseNow) {
              index = Math.max(index - INDEX_DECREMENT, minIndex);
              nextBufferSize = SIZE_TABLE[index];
              decreaseNow = false;
          } else {
              decreaseNow = true;
          }
      } else if (previousSize >= nextBufferSize) {
          index = Math.min(index + INDEX_INCREMENT, maxIndex);
          nextBufferSize = SIZE_TABLE[index];
          decreaseNow = false;
      }
   }
}
