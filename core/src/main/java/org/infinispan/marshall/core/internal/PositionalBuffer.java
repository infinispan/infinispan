package org.infinispan.marshall.core.internal;

public interface PositionalBuffer {

   interface Output {

      /**
       * Create space for storing the position and
       * return the position before making the space.
       */
      int savePosition();

      /**
       * Write the current position of the output at the given offset.
       * This method is designed to take the output of {@link #savePosition()}
       * as input.
       *
       * @param offset within the output where the current position needs storing
       * @return the position writen
       */
      int writePosition(int offset);

   }

   interface Input {

      /**
       * Rewind input's position to the given value.
       *
       * @param pos value to which the position needs to set
       */
      void rewindPosition(int pos); // in.afterExternal

   }

//   int beforeExternal();
//
//   void afterExternal(int v)


}
