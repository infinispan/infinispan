package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.marshall.core.UserAwareObjectOutput;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * @author Mircea.Markus@jboss.com
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
public class RemoveCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {
   private static final Log log = LogFactory.getLog(RemoveCommand.class);
   public static final byte COMMAND_ID = 10;
   protected CacheNotifier<Object, Object> notifier;
   protected boolean successful = true;
   private boolean nonExistent = false;

   protected Metadata metadata;
   protected ValueMatcher valueMatcher;

   /**
    * When not null, value indicates that the entry should only be removed if the key is mapped to this value.
    * When null, the entry should be removed regardless of what value it is mapped to.
    */
   protected Object value;

   public RemoveCommand(Object key, Object value, CacheNotifier notifier, int segment, long flagsBitSet,
                        CommandInvocationId commandInvocationId) {
      super(key, segment, flagsBitSet, commandInvocationId);
      this.value = value;
      //noinspection unchecked
      this.notifier = notifier;
      this.valueMatcher = value != null ? ValueMatcher.MATCH_EXPECTED : ValueMatcher.MATCH_ALWAYS;
   }

   public void init(CacheNotifier notifier) {
      //noinspection unchecked
      this.notifier = notifier;
   }

   public RemoveCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // It's not worth looking up the entry if we're never going to apply the change.
      if (valueMatcher == ValueMatcher.MATCH_NEVER) {
         successful = false;
         return null;
      }
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      Object prevValue = e.getValue();
      if (prevValue == null) {
         nonExistent = true;
         if (valueMatcher.matches(null, value, null)) {
            e.setChanged(true);
            e.setRemoved(true);
            e.setCreated(false);
            if (this instanceof EvictCommand) {
               e.setEvicted(true);
            }
            e.setValue(null);
            return isConditional() ? true : null;
         } else {
            log.trace("Nothing to remove since the entry doesn't exist in the context or it is null");
            successful = false;
            return false;
         }
      }

      if (!valueMatcher.matches(prevValue, value, null)) {
         successful = false;
         return false;
      }

      if (this instanceof EvictCommand) {
         e.setEvicted(true);
      }

      return performRemove(e, prevValue, ctx);
   }

   public void notify(InvocationContext ctx, Object removedValue, Metadata removedMetadata, boolean isPre) {
      if (removedValue != null)
         notifier.notifyCacheEntryRemoved(key, removedValue, removedMetadata, isPre, ctx, this);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public boolean equals(Object o) {
      if (!super.equals(o)) {
         return false;
      }

      RemoveCommand that = (RemoveCommand) o;

      return Objects.equals(value, that.value);
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
   }


   @Override
   public String toString() {
      return new StringBuilder()
         .append("RemoveCommand{key=")
         .append(toStr(key))
         .append(", value=").append(toStr(value))
         .append(", metadata=").append(metadata)
         .append(", flags=").append(printFlags())
         .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
         .append(", valueMatcher=").append(valueMatcher)
         .append(", topologyId=").append(getTopologyId())
         .append("}")
         .toString();
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return value != null;
   }

   public boolean isNonExistent() {
      return nonExistent;
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeEntry(key, value, metadata);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      MarshallUtil.marshallEnum(valueMatcher, output);
      CommandInvocationId.writeTo(output, commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      MarshalledEntryImpl me = MarshalledEntryUtil.read(input);
      key = me.getKey();
      value = me.getValue();
      metadata = me.metadata();
      segment = UnsignedNumeric.readUnsignedInt(input);
      setFlagsBitSet(input.readLong());
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      commandInvocationId = CommandInvocationId.readFrom(input);
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      this.valueMatcher = valueMatcher;
   }

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public LoadType loadType() {
      return isConditional() || !hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? LoadType.PRIMARY : LoadType.DONT_LOAD;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   @Override
   public final boolean isReturnValueExpected() {
      // IGNORE_RETURN_VALUES ignored for conditional remove
      return isConditional() || super.isReturnValueExpected();
   }

   protected Object performRemove(MVCCEntry e, Object prevValue, InvocationContext ctx) {
      notify(ctx, prevValue, e.getMetadata(), true);

      e.setRemoved(true);
      e.setChanged(true);
      e.setValue(null);
      if (metadata != null) {
         e.setMetadata(metadata);
      }

      if (valueMatcher != ValueMatcher.MATCH_EXPECTED_OR_NEW) {
         return isConditional() ? true : prevValue;
      } else {
         // Return the expected value when retrying
         return isConditional() ? true : value;
      }
   }
}
