package org.infinispan.lucene.testutils;

/**
 * RepeatableLongByteSequence is a testing utility to get a source of bytes.
 * Use nextByte() to produce them.
 * The generated sequence is similar to a random generated sequence, but will always generate
 * the same sequence and avoid immediate repetitions of bytes and
 * close repetitive patterns (they might occur in large scale).
 *
 * After having written such a stream from one
 * instance, create a second instance to assert equality of contents (see test)
 * as the source is not random and will generate the same sequence.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public class RepeatableLongByteSequence {

   private byte lastUsedValue = -1;
   private byte currentMax = (byte) 1;
   private byte currentMin = (byte) -1;
   private boolean rising = true;

   public byte nextByte() {
      byte next;
      if (rising) {
         next = ++lastUsedValue;
         if (next == currentMax) {
            rising = false;
            currentMax++; // overflow might occur, not bad for our purposes.
         }
      }
      else {
         next = --lastUsedValue;
         if (next == currentMin) {
            rising = true;
            currentMin--; // as above: overflow allowed
         }
      }
      return next;
   }

   /**
    * @param buffer is going to be modified: a new series of bytes is going to be written into
    */
   public void nextBytes(byte[] buffer) {
      for(int i=0; i < buffer.length; i++) {
         buffer[i] = nextByte();
      }
   }

   public void reset() {
      lastUsedValue = -1;
      currentMax = (byte) 1;
      currentMin = (byte) -1;
      rising = true;
   }

}
