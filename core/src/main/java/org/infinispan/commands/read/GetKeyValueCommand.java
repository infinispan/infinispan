package org.infinispan.commands.read;

import org.infinispan.commands.Visitor;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;

import java.util.Set;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#get(Object)} and
 * {@link org.infinispan.Cache#containsKey(Object)} operations
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class GetKeyValueCommand extends AbstractDataCommand {
   public static final byte COMMAND_ID = 4;
   private InternalCacheEntry remotelyFetchedValue;
   // TODO: These two instance variables specific to getCacheEntry, optimise
   private boolean returnEntry;
   private InternalEntryFactory entryFactory;

   public GetKeyValueCommand(Object key, Set<Flag> flags, boolean returnEntry, InternalEntryFactory entryFactory) {
      this.key = key;
      this.flags = flags;
      this.returnEntry = returnEntry;
      this.entryFactory = entryFactory;
   }

   public GetKeyValueCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetKeyValueCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry == null || entry.isNull()) {
         return null;
      }
      if (entry.isRemoved()) {
         return null;
      }

      // Get cache entry instead of value
      if (returnEntry) {
         CacheEntry copy = entryFactory.copy(entry);
         return copy;
      }

      final Object value = entry.getValue();
      return value;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      flags = (Set<Flag>) parameters[1];
      returnEntry = (Boolean) parameters[2];
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, Flag.copyWithoutRemotableFlags(flags), returnEntry};
   }

   /**
    * @see #getRemotelyFetchedValue()
    */
   public void setRemotelyFetchedValue(InternalCacheEntry remotelyFetchedValue) {
      this.remotelyFetchedValue = remotelyFetchedValue;
   }

   /**
    * If the cache needs to go remotely in order to obtain the value associated to this key, then the remote value
    * is stored in this field.
    * TODO: this method should be able to removed with the refactoring from ISPN-2177
    */
   public InternalCacheEntry getRemotelyFetchedValue() {
      return remotelyFetchedValue;
   }

   public boolean isReturnEntry() {
      return returnEntry;
   }

   public String toString() {
      return new StringBuilder()
            .append("GetKeyValueCommand {key=")
            .append(toStr(key))
            .append(", flags=").append(flags)
            .append(", returnEntry=").append(returnEntry)
            .append("}")
            .toString();
   }

}
