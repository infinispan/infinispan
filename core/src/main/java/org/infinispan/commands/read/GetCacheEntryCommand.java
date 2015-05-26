package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Set;

import org.infinispan.commands.Visitor;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;

/**
 * Used to fetch a full CacheEntry rather than just the value.
 * This functionality was originally incorporated into GetKeyValueCommand.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.1
 */
public final class GetCacheEntryCommand extends AbstractDataCommand implements RemoteFetchingCommand {

   public static final byte COMMAND_ID = 45;

   private InternalEntryFactory entryFactory;
   private InternalCacheEntry remotelyFetchedValue;

   public GetCacheEntryCommand(Object key, Set<Flag> flags, InternalEntryFactory entryFactory) {
      this.key = key;
      this.flags = flags;
      this.entryFactory = entryFactory;
   }

   public GetCacheEntryCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetCacheEntryCommand(ctx, this);
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

      return entryFactory.copy(entry);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      flags = (Set<Flag>) parameters[1];
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

   @Override
   public Object[] getParameters() {
      return new Object[]{key, Flag.copyWithoutRemotableFlags(flags)};
   }

   public String toString() {
      return new StringBuilder()
            .append("GetCacheEntryCommand {key=")
            .append(toStr(key))
            .append(", flags=").append(flags)
            .append("}")
            .toString();
   }

}
