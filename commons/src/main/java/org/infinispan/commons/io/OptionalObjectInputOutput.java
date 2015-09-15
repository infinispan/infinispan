package org.infinispan.commons.io;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;

/**
 * OptionalObjectInputOutput.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class OptionalObjectInputOutput {

   public static void writeOptional(ObjectOutput output, Optional<String> s) throws IOException {
      if (s.isPresent()) {
         output.write(1);
         output.writeUTF(s.get());
      } else {
         output.write(0);
      }
   }

   public static Optional<String> readOptionalUTF(ObjectInput input) throws IOException {
      if(input.readByte() == 0) {
         return Optional.empty();
      } else {
         return Optional.of(input.readUTF());
      }
   }
}
