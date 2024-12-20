package org.infinispan.commands.triangle;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.functional.AbstractWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.util.TriangleFunctionsUtil;

/**
 * A multi-key {@link BackupWriteCommand} for {@link WriteOnlyManyEntriesCommand} and {@link
 * ReadWriteManyEntriesCommand}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTI_ENTRIES_FUNCTIONAL_BACKUP_WRITE_COMMAND)
public class MultiEntriesFunctionalBackupWriteCommand extends FunctionalBackupWriteCommand {

   public static final byte COMMAND_ID = 79;

   @ProtoField(number = 11, defaultValue = "false")
   final boolean writeOnly;
   final Map<?, ?> entries;

   public static <K, V, T> MultiEntriesFunctionalBackupWriteCommand create(ByteString cacheName, WriteOnlyManyEntriesCommand<K, V, T> command,
                                                                           Collection<Object> keys, long sequence, int segmentId) {
      Map<?, ?> entries = TriangleFunctionsUtil.filterEntries(command.getArguments(), keys);
      return new MultiEntriesFunctionalBackupWriteCommand(cacheName, command, sequence, segmentId, command.getBiConsumer(), true, entries);
   }

   public static <K, V, T, R> MultiEntriesFunctionalBackupWriteCommand create(ByteString cacheName, ReadWriteManyEntriesCommand<K, V, T, R> command,
                                                                              Collection<Object> keys, long sequence, int segmentId) {
      Map<?, ?> entries = TriangleFunctionsUtil.filterEntries(command.getArguments(), keys);
      return new MultiEntriesFunctionalBackupWriteCommand(cacheName, command, sequence, segmentId, command.getBiFunction(), false, entries);
   }

   @ProtoFactory
   MultiEntriesFunctionalBackupWriteCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId,
                                            long flags, long sequence, int segmentId, MarshallableObject<?> function,
                                            Params params, DataConversion keyDataConversion, DataConversion valueDataConversion,
                                            boolean writeOnly, MarshallableMap<?, ?> wrappedEntries) {
      super(cacheName, commandInvocationId, topologyId, flags, sequence, segmentId, function, params, keyDataConversion, valueDataConversion);
      this.writeOnly = writeOnly;
      this.entries = MarshallableMap.unwrap(wrappedEntries);
   }

   private MultiEntriesFunctionalBackupWriteCommand(ByteString cacheName, AbstractWriteManyCommand<?, ?> command, long sequence,
                                                    int segmentId, Object function, boolean writeOnly, Map<?, ?> entries) {
      super(cacheName, command, sequence, segmentId, function);
      this.writeOnly = writeOnly;
      this.entries = entries;
   }

   @ProtoField(number = 12)
   MarshallableMap<?, ?> getWrappedEntries() {
      return MarshallableMap.create(entries);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   WriteCommand createWriteCommand() {
      //noinspection unchecked
      AbstractWriteManyCommand cmd = writeOnly ?
            new WriteOnlyManyEntriesCommand(entries, (BiConsumer) function, params, getCommandInvocationId(),
                  keyDataConversion, valueDataConversion) :
            new ReadWriteManyEntriesCommand(entries, (BiFunction) function, params, getCommandInvocationId(),
                  keyDataConversion, valueDataConversion);
      cmd.setForwarded(true);
      return cmd;
   }
}
