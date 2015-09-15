package org.infinispan.tasks.impl.history;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.commons.marshall.Externalizer;
import static org.infinispan.commons.io.OptionalObjectInputOutput.*;
import org.infinispan.tasks.TaskEventStatus;

/**
 * TaskEventImplExternalizer.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class TaskEventImplExternalizer implements Externalizer<TaskEventImpl> {

   @Override
   public void writeObject(ObjectOutput output, TaskEventImpl object) throws IOException {
      output.writeLong(object.uuid.getMostSignificantBits());
      output.writeLong(object.uuid.getLeastSignificantBits());
      output.writeUTF(object.name);
      writeOptional(output, object.what);
      output.writeUTF(object.where);
      writeOptional(output, object.who);
      output.writeUTF(object.status.name());
      switch(object.status) {
      case PENDING:
         break;
      case RUNNING:
         output.writeLong(object.start.getEpochSecond());
         break;
      case ERROR:
      case SUCCESS:
         output.writeLong(object.start.getEpochSecond());
         output.writeLong(object.finish.getEpochSecond());
         break;
      }
      writeOptional(output, object.log);
   }

   @Override
   public TaskEventImpl readObject(ObjectInput input) throws IOException {
      long uuidMSB = input.readLong();
      long uuidLSB = input.readLong();
      String name = input.readUTF();
      Optional<String> what = readOptionalUTF(input);
      String where = input.readUTF();
      Optional<String> who = readOptionalUTF(input);
      TaskEventStatus status = TaskEventStatus.valueOf(input.readUTF());

      TaskEventImpl event = new TaskEventImpl(new UUID(uuidMSB, uuidLSB), name, what, where, who);
      event.setStatus(status);

      switch(status) {
      case PENDING:
         break;
      case RUNNING:
         event.setStart(Instant.ofEpochSecond(input.readLong()));
         break;
      case ERROR:
      case SUCCESS:
         event.setStart(Instant.ofEpochSecond(input.readLong()));
         event.setFinish(Instant.ofEpochSecond(input.readLong()));
         break;
      }
      event.setLog(readOptionalUTF(input));


      return event;
   }

}
