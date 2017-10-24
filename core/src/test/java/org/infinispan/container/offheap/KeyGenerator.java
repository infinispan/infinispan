package org.infinispan.container.offheap;

import java.util.Random;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * Generates keys and values to be used during the container and cache operations. Currently these are limited to
 * {@link WrappedByteArray} instances primarily for use with off heap testing.
 * These instances are pre-created to minimize impact during measurements.
 */
public class KeyGenerator {

   private static final int randomSeed = 17;
   private static final int keySpaceSize = 1000;
   private static final int keyObjectSize = 10;
   private static final int valueSpaceSize = 100;
   private static final int valueObjectSize = 1000;

   private RandomSequence keySequence;
   private RandomSequence valueSequence;

   private Metadata metadata;

   public KeyGenerator() {
      Random random = new Random(randomSeed);
      keySequence = new RandomSequence( random, keySpaceSize, keyObjectSize);
      valueSequence = new RandomSequence( random, valueSpaceSize, valueObjectSize);
      metadata = new EmbeddedMetadata.Builder().build();
   }

   public WrappedByteArray getNextKey() {
      return keySequence.nextValue();
   }

   public WrappedByteArray getNextValue() {
      return valueSequence.nextValue();
   }

   public Metadata getMetadata() {
      return metadata;
   }

   private class RandomSequence {
      private final WrappedByteArray[] list;
      private final int spaceSize;

      private int idx = -1;

      RandomSequence(Random random, int spaceSize, int objectSize) {
         this.list = new WrappedByteArray[spaceSize];
         this.spaceSize = spaceSize;

         for (int i=0; i<spaceSize; i++) {
            byte[] bytes = new byte[objectSize];
            random.nextBytes(bytes);
            list[i] = toUserObject(bytes);
         }
      }

      WrappedByteArray nextValue() {
         idx++;
         if (idx==spaceSize) {
            idx = 0;
         }
         return list[idx];
      }

   }

   private WrappedByteArray toUserObject(byte[] bytes) {
      return new WrappedByteArray(bytes);
   }
}
