package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * A command writing multiple key/value pairs with the same metadata.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutMapCommand extends AbstractTopologyAffectedCommand implements WriteCommand, MetadataAwareCommand, RemoteLockCommand {
   public static final byte COMMAND_ID = 9;

   private Map<Object, Object> map;
   private Metadata metadata;
   private boolean isForwarded = false;
   private Map<Object, PrivateMetadata> internalMetadataMap;

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   private CommandInvocationId commandInvocationId;

   public PutMapCommand() {
   }

   @SuppressWarnings("unchecked")
   public PutMapCommand(Map<?, ?> map, Metadata metadata, long flagsBitSet, CommandInvocationId commandInvocationId) {
      this.map = (Map<Object, Object>) map;
      this.metadata = metadata;
      this.commandInvocationId = commandInvocationId;
      setFlagsBitSet(flagsBitSet);
      this.internalMetadataMap = new HashMap<>();
   }

   public PutMapCommand(PutMapCommand command) {
      this.map = command.map;
      this.metadata = command.metadata;
      this.isForwarded = command.isForwarded;
      this.commandInvocationId = command.commandInvocationId;
      setFlagsBitSet(command.getFlagsBitSet());
      this.internalMetadataMap = new HashMap<>(command.internalMetadataMap);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutMapCommand(ctx, this);
   }

   @Override
   public Collection<?> getKeysToLock() {
      return isForwarded ? Collections.emptyList() : Collections.unmodifiableCollection(map.keySet());
   }

   @Override
   public Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }

   public Map<Object, Object> getMap() {
      return map;
   }

   public void setMap(Map<Object, Object> map) {
      this.map = map;
      this.internalMetadataMap.keySet().retainAll(map.keySet());
   }

   public final PutMapCommand withMap(Map<Object, Object> map) {
      setMap(map);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallMap(map, output);
      output.writeObject(metadata);
      output.writeBoolean(isForwarded);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallMap(internalMetadataMap, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      map = MarshallUtil.unmarshallMap(input, LinkedHashMap::new);
      metadata = (Metadata) input.readObject();
      isForwarded = input.readBoolean();
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      internalMetadataMap = MarshallUtil.unmarshallMap(input, HashMap::new);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PutMapCommand that = (PutMapCommand) o;

      return Objects.equals(metadata, that.metadata) &&
            Objects.equals(map, that.map);

   }

   @Override
   public int hashCode() {
      int result = map != null ? map.hashCode() : 0;
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PutMapCommand{map={");
      if (!map.isEmpty()) {
         Iterator<Entry<Object, Object>> it = map.entrySet().iterator();
         int i = 0;
         for (;;) {
            Entry<Object, Object> e = it.next();
            sb.append(toStr(e.getKey())).append('=').append(toStr(e.getValue()));
            if (!it.hasNext()) {
               break;
            }
            if (i > 100) {
               sb.append(" ...");
               break;
            }
            sb.append(", ");
            i++;
         }
      }
      sb.append("}, flags=").append(printFlags())
         .append(", metadata=").append(metadata)
         .append(", internalMetadata=").append(internalMetadataMap)
         .append(", isForwarded=").append(isForwarded)
         .append("}");
      return sb.toString();
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // Do nothing
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return map.keySet();
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isReturnValueExpected() {
      return !hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES);
   }

   @Override
   public LoadType loadType() {
      return hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? LoadType.DONT_LOAD : LoadType.PRIMARY;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   /**
    * For non transactional caches that support concurrent writes (default), the commands are forwarded between nodes,
    * e.g.:
    * - commands is executed on node A, but some of the keys should be locked on node B
    * - the command is send to the main owner (B)
    * - B tries to acquire lock on the keys it owns, then forwards the commands to the other owners as well
    * - at this last stage, the command has the "isForwarded" flag set to true.
    */
   public boolean isForwarded() {
      return isForwarded;
   }

   /**
    * @see #isForwarded()
    */
   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public PrivateMetadata getInternalMetadata(Object key) {
      return internalMetadataMap.get(key);
   }

   @Override
   public void setInternalMetadata(Object key, PrivateMetadata internalMetadata) {
      this.internalMetadataMap.put(key, internalMetadata);
   }

}
