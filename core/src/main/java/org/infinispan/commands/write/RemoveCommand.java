package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
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
public class RemoveCommand extends AbstractDataWriteCommand {
   private static final Log log = LogFactory.getLog(RemoveCommand.class);
   public static final byte COMMAND_ID = 10;
   protected CacheNotifier<Object, Object> notifier;
   protected boolean successful = true;
   private boolean nonExistent = false;

   /**
    * When not null, value indicates that the entry should only be removed if the key is mapped to this value.
    * When null, the entry should be removed regardless of what value it is mapped to.
    */
   protected Object value;

   public RemoveCommand(Object key, Object value, CacheNotifier notifier, long flagsBitSet,
                        CommandInvocationId commandInvocationId, Object providedResult, InvocationManager invocationManager, boolean synchronous) {
      super(key, flagsBitSet, commandInvocationId, providedResult, invocationManager, synchronous);
      this.value = value;
      //noinspection unchecked
      this.notifier = notifier;
   }

   public void init(CacheNotifier notifier, InvocationManager invocationManager, boolean isCacheAsync) {
      //noinspection unchecked
      this.notifier = notifier;
      this.invocationManager = invocationManager;
      this.synchronous = isCacheAsync;
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
         if (hasAnyFlag(FlagBitSets.DISABLE_CONDITION) || value == null) {
            return performRemove(e, null, ctx);
         } else {
            log.trace("Nothing to remove since the entry doesn't exist in the context or it is null");
            successful = false;
            return hasAnyFlag(FlagBitSets.PROVIDED_RESULT) ? providedResult : Boolean.FALSE;
         }
      }

      if (!hasAnyFlag(FlagBitSets.DISABLE_CONDITION) && value != null && !value.equals(prevValue)) {
         successful = false;
         return hasAnyFlag(FlagBitSets.PROVIDED_RESULT) ? providedResult : Boolean.FALSE;
      }

      notify(ctx, prevValue, e.getMetadata(), true);
      return performRemove(e, prevValue, ctx);
   }

   public void notify(InvocationContext ctx, Object removedValue, Metadata removedMetadata, boolean isPre) {
      notifier.notifyCacheEntryRemoved(key, removedValue, removedMetadata, isPre, ctx, this);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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
      StringBuilder sb = new StringBuilder()
         .append("RemoveCommand{key=").append(toStr(key))
         .append(", value=").append(toStr(value))
         .append(", flags=").append(printFlags())
         .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
         .append(", topologyId=").append(getTopologyId());
      if (hasAnyFlag(FlagBitSets.PROVIDED_RESULT)) {
         sb.append(", result=").append(providedResult);
      }
      return sb.append('}')
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = input.readObject();
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
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

   @Override
   public void initBackupWriteRpcCommand(BackupWriteRpcCommand command) {
      command.setRemove(commandInvocationId, key, getFlagsBitSet(), getTopologyId());
   }

   protected Object performRemove(MVCCEntry e, Object prevValue, InvocationContext ctx) {
      e.setChanged(true);
      e.setRemoved(true);
      e.setCreated(false);
      e.setValid(true);
      e.setValue(null);

      Object result;
      if (hasAnyFlag(FlagBitSets.PROVIDED_RESULT)) {
         result = providedResult;
      } else if (isConditional()) {
         result = successful;
      } else {
         result = prevValue;
      }
      recordInvocation(ctx, e, result);
      return result;
   }
}
