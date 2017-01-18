package org.infinispan.marshall.exts;

import static org.infinispan.commons.marshall.Ids.TRIANGLE_ACK_EXTERNALIZER;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PrimaryMultiKeyAckCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;

/**
 * Externalizer for the triangle acknowledges.
 * <p>
 * It doesn't use the {@link org.infinispan.marshall.DeltaAwareObjectOutput} like the {@link
 * CacheRpcCommandExternalizer}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleAckExternalizer implements AdvancedExternalizer<ReplicableCommand> {

   private final RemoteCommandsFactory remoteCommandsFactory;

   public TriangleAckExternalizer(RemoteCommandsFactory remoteCommandsFactory) {
      this.remoteCommandsFactory = remoteCommandsFactory;
   }

   public Set<Class<? extends ReplicableCommand>> getTypeClasses() {
      //noinspection unchecked
      return Util.asSet(PrimaryAckCommand.class, BackupAckCommand.class, ExceptionAckCommand.class,
            PrimaryMultiKeyAckCommand.class, BackupMultiKeyAckCommand.class);
   }

   public Integer getId() {
      return TRIANGLE_ACK_EXTERNALIZER;
   }

   public void writeObject(ObjectOutput output, ReplicableCommand object) throws IOException {
      output.writeByte(object.getCommandId());
      object.writeTo(output);
   }

   public ReplicableCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      ReplicableCommand command = remoteCommandsFactory.fromStream(input.readByte(), (byte) 0);
      command.readFrom(input);
      return command;
   }
}
