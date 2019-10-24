package org.infinispan.server;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.SerializeWith;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@SerializeWith(ExitStatus.Externalizer.class)
public class ExitStatus implements Serializable {
   public enum ExitMode {
      SERVER_SHUTDOWN,
      CLUSTER_SHUTDOWN,
      ERROR;

      private static final ExitMode[] CACHED_VALUES = values();

      static ExitMode valueOf(int ordinal) {
         return CACHED_VALUES[ordinal];
      }
   }

   public static final ExitStatus CLUSTER_SHUTDOWN = new ExitStatus(ExitMode.CLUSTER_SHUTDOWN, 0);
   public static final ExitStatus SERVER_SHUTDOWN = new ExitStatus(ExitMode.SERVER_SHUTDOWN, 0);

   final ExitMode mode;
   final int status;

   ExitStatus(ExitMode mode, int status) {
      this.mode = mode;
      this.status = status;
   }


   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<ExitStatus> {
      @Override
      public void writeObject(ObjectOutput output, ExitStatus object) throws IOException {
         MarshallUtil.marshallEnum(object.mode, output);
         output.writeInt(object.status);
      }

      @Override
      public ExitStatus readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ExitMode mode = MarshallUtil.unmarshallEnum(input, ExitMode::valueOf);
         int status = input.readInt();
         return new ExitStatus(mode, status);
      }
   }
}
