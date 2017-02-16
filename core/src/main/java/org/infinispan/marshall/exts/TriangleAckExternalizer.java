package org.infinispan.marshall.exts;

import static org.infinispan.commons.marshall.Ids.TRIANGLE_ACK_EXTERNALIZER;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PrimaryMultiKeyAckCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.util.ByteString;

/**
 * Externalizer for the triangle acknowledges.
 * <p>
 * It doesn't use the {@link org.infinispan.marshall.DeltaAwareObjectOutput} like the {@link
 * CacheRpcCommandExternalizer}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleAckExternalizer implements AdvancedExternalizer<CacheRpcCommand> {

   private final RemoteCommandsFactory remoteCommandsFactory;

   public TriangleAckExternalizer(RemoteCommandsFactory remoteCommandsFactory) {
      this.remoteCommandsFactory = remoteCommandsFactory;
   }

   public Set<Class<? extends CacheRpcCommand>> getTypeClasses() {
      //noinspection unchecked
      return Util.asSet(PrimaryAckCommand.class, BackupAckCommand.class, ExceptionAckCommand.class,
            PrimaryMultiKeyAckCommand.class, BackupMultiKeyAckCommand.class);
   }

   public Integer getId() {
      return TRIANGLE_ACK_EXTERNALIZER;
   }

   public void writeObject(ObjectOutput output, CacheRpcCommand object) throws IOException {
      output.writeByte(object.getCommandId());
      ByteString.writeObject(output, object.getCacheName());
      object.writeTo(output);
   }

   public CacheRpcCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      CacheRpcCommand command = remoteCommandsFactory
            .fromStream(input.readByte(), (byte) 0, ByteString.readObject(input));
      command.readFrom(input);
      return command;
   }
}
