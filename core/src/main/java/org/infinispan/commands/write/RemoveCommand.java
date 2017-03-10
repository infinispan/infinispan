package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
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

   /**
    * When not null, value indicates that the entry should only be removed if the key is mapped to this value.
    * When null, the entry should be removed regardless of what value it is mapped to.
    */
   protected Object value;

   public RemoveCommand(Object key, Object value, CacheNotifier notifier, long flagsBitSet,
                        CommandInvocationId commandInvocationId, InvocationManager invocationManager) {
      super(key, flagsBitSet, commandInvocationId, invocationManager);
      this.value = value;
      //noinspection unchecked
      this.notifier = notifier;
   }

   public void init(CacheNotifier notifier, InvocationManager invocationManager) {
      //noinspection unchecked
      this.notifier = notifier;
      this.invocationManager = invocationManager;
   }

   public RemoveCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      Object prevValue = e.getValue();
      if (prevValue == null) {
         nonExistent = true;
         if (value == null) {
            // TODO ISPN-8204 It would be more correct to set successful = false here
            return performRemove(e, null, ctx);
         } else {
            log.trace("Nothing to remove since the entry doesn't exist in the context or it is null");
            successful = false;
            return Boolean.FALSE;
         }
      }

      if (value != null && !value.equals(prevValue)) {
         successful = false;
         return Boolean.FALSE;
      }

      notify(ctx, prevValue, e.getMetadata(), true);
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
         .append("RemoveCommand{key=").append(toStr(key))
         .append(", value=").append(toStr(value))
         .append(", metadata=").append(metadata)
         .append(", flags=").append(printFlags())
         .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
         .append(", lastInvocationId=").append(lastInvocationId)
         .append(", topologyId=").append(getTopologyId())
         .append('}').toString();
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeObject(metadata);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = input.readObject();
      metadata = (Metadata) input.readObject();
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
   }

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public LoadType loadType() {
      return isConditional() || !hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? LoadType.OWNER : LoadType.DONT_LOAD;
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
      e.setChanged(true);
      e.setRemoved(true);
      e.setCreated(false);
      e.setValue(null);
      if (metadata != null) {
         e.setMetadata(metadata);
      }

      Object result;
      if (isConditional()) {
         result = Boolean.TRUE;
      } else {
         result = prevValue;
      }
      recordInvocation(ctx, e, prevValue, e.getMetadata());
      return result;
   }
}
