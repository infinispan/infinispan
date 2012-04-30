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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class providing hints about marshallable types, such as whether a particular
 * type is marshallable or not, or an accurate approach to the serialized
 * size of a particular type.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public final class MarshallableTypeHints {

   /**
    * Cache of classes that are considered to be marshallable alongside their
    * buffer size predictor. Since checking whether a type is marshallable
    * requires attempting to marshalling them, a cache for the types that are
    * known to be marshallable or not is advantageous.
    */
   private final ConcurrentMap<Class<?>, MarshallingType> typeHints =
         new ConcurrentHashMap<Class<?>, MarshallingType>();

   /**
    * Get the serialized form size predictor for a particular type.
    *
    * @param type Marshallable type for which serialized form size will be predicted
    * @return an instance of {@link BufferSizePredictor}
    */
   public BufferSizePredictor getBufferSizePredictor(Class<? extends Object> type) {
      MarshallingType marshallingType = typeHints.get(type);
      if (marshallingType == null) {
         marshallingType = new MarshallingType(false, new AdaptiveBufferSizePredictor());
         MarshallingType prev = typeHints.putIfAbsent(type, marshallingType);
         if (prev != null)
            marshallingType = prev;
      }
      return marshallingType.sizePredictor;
   }

   /**
    * Returns whether the hint on whether a particular type is marshallable or
    * not is available. This method can be used to avoid attempting to marshall
    * a type, if the hints for the type have already been calculated.
    *
    * @param type Marshallable type to check whether an attempt to mark it as
    *             marshallable has been made.
    * @return true if the type has been marked as marshallable at all, false
    * if no attempt has been made to mark the type as marshallable.
    */
   public boolean containsMarshallable(Class<? extends Object> type) {
      return typeHints.containsKey(type);
   }

   /**
    * Returns whether a type can be serialized.
    */
   public boolean isMarshallable(Class<? extends Object> type) {
      MarshallingType marshallingType = typeHints.get(type);
      if (marshallingType != null)
         return marshallingType.isMarshallable;

      return false;
   }

   /**
    * Marks a particular type as being marshallable or not being not marshallable.
    *
    * @param type Class to mark as serializable or non-serializable
    * @param isMarshallable Whether the type can be marshalled or not.
    */
   public void markMarshallable(Class<? extends Object> type, boolean isMarshallable) {
      MarshallingType marshallingType = typeHints.get(type);
      if (marshallingType != null && marshallingType.isMarshallable != isMarshallable) {
         typeHints.replace(type, new MarshallingType(
               isMarshallable, marshallingType.sizePredictor));
      } else {
         typeHints.putIfAbsent(type, new MarshallingType(
               isMarshallable, new AdaptiveBufferSizePredictor()));
      }
   }

   /**
    * Clear the cached marshallable type hints.
    */
   public void clear() {
      typeHints.clear();
   }

   private class MarshallingType {

      final boolean isMarshallable;
      final BufferSizePredictor sizePredictor;

      private MarshallingType(boolean marshallable, BufferSizePredictor sizePredictor) {
         isMarshallable = marshallable;
         this.sizePredictor = sizePredictor;
      }

   }

}
