package org.infinispan.tasks.impl;

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
      output.writeObject(object.what);
      output.writeUTF(object.where);
      output.writeObject(object.who);
   }

   @Override
   public TaskExecutionImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      long uuidMSB = input.readLong();
      long uuidLSB = input.readLong();
      String name = input.readUTF();
      Optional<String> what = (Optional<String>) input.readObject();
      String where = input.readUTF();
      Optional<String> who = (Optional<String>) input.readObject();

      TaskExecutionImpl event = new TaskExecutionImpl(new UUID(uuidMSB, uuidLSB), name, what, where, who);

      return event;
   }

}
