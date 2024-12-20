package org.infinispan.commands.triangle;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.functional.AbstractWriteKeyCommand;
import org.infinispan.commands.functional.AbstractWriteManyCommand;
import org.infinispan.commands.functional.FunctionalCommand;
import org.infinispan.encoding.DataConversion;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * A base {@link BackupWriteCommand} used by {@link FunctionalCommand}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
abstract class FunctionalBackupWriteCommand extends BackupWriteCommand {

   @ProtoField(number = 7)
   final Params params;

   @ProtoField(number = 8)
   final DataConversion keyDataConversion;

   @ProtoField(number = 9)
   final DataConversion valueDataConversion;

   final Object function;

   @ProtoField(number = 10)
   MarshallableObject<?> getFunction() {
      return MarshallableObject.create(function);
   }

   // Used by ProtoFactory implementations
   protected FunctionalBackupWriteCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId,
                                          long flags, long sequence, int segmentId, MarshallableObject<?> function,
                                          Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(cacheName, commandInvocationId, topologyId, flags, sequence, segmentId);
      this.function = MarshallableObject.unwrap(function);
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   protected FunctionalBackupWriteCommand(ByteString cacheName, AbstractWriteKeyCommand<?, ?> command, long sequence,
                                          int segmentId, Object function) {
      super(cacheName, command, sequence, segmentId);
      this.params = command.getParams();
      this.keyDataConversion = command.getKeyDataConversion();
      this.valueDataConversion = command.getValueDataConversion();
      this.function = function;
   }

   protected FunctionalBackupWriteCommand(ByteString cacheName, AbstractWriteManyCommand<?, ?> command, long sequence,
                                          int segmentId, Object function) {
      super(cacheName, command, sequence, segmentId);
      this.params = command.getParams();
      this.keyDataConversion = command.getKeyDataConversion();
      this.valueDataConversion = command.getValueDataConversion();
      this.function = function;
   }
}
