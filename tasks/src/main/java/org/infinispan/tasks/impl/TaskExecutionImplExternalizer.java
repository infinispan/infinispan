package org.infinispan.tasks.impl;

import static org.infinispan.commons.io.OptionalObjectInputOutput.readOptionalUTF;
import static org.infinispan.commons.io.OptionalObjectInputOutput.writeOptional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.commons.marshall.Externalizer;

/**
 * TaskEventImplExternalizer.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class TaskExecutionImplExternalizer implements Externalizer<TaskExecutionImpl> {

   @Override
   public void writeObject(ObjectOutput output, TaskExecutionImpl object) throws IOException {
      output.writeLong(object.uuid.getMostSignificantBits());
      output.writeLong(object.uuid.getLeastSignificantBits());
      output.writeUTF(object.name);
      writeOptional(output, object.what);
      output.writeUTF(object.where);
      writeOptional(output, object.who);
   }

   @Override
   public TaskExecutionImpl readObject(ObjectInput input) throws IOException {
      long uuidMSB = input.readLong();
      long uuidLSB = input.readLong();
      String name = input.readUTF();
      Optional<String> what = readOptionalUTF(input);
      String where = input.readUTF();
      Optional<String> who = readOptionalUTF(input);

      TaskExecutionImpl event = new TaskExecutionImpl(new UUID(uuidMSB, uuidLSB), name, what, where, who);

      return event;
   }

}
