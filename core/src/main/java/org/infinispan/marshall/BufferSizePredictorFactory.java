/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.marshall;

/**
 * Factory class for retrieving {@link BufferSizePredictor} instances.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class BufferSizePredictorFactory {

   private static final ThreadLocal<BufferSizePredictor> bufferSizePredictorTL =
      new ThreadLocal<BufferSizePredictor>() {
         @Override
         protected BufferSizePredictor initialValue() {
            return new AdaptiveBufferSizePredictor();
         }
      };

   public static BufferSizePredictor getBufferSizePredictor() {
      return bufferSizePredictorTL.get();
   }

}
