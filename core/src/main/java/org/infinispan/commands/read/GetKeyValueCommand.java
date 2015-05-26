package org.infinispan.commands.read;

import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#get(Object)} and
 * {@link org.infinispan.Cache#containsKey(Object)} operations
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class GetKeyValueCommand extends AbstractDataCommand implements RemoteFetchingCommand {

   public static final byte COMMAND_ID = 4;
   private static final Log log = LogFactory.getLog(GetKeyValueCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private InternalCacheEntry remotelyFetchedValue;

   public GetKeyValueCommand(Object key, Set<Flag> flags) {
      this.key = key;
      this.flags = flags;
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
         if (trace) {
            log.trace("Entry not found");
         }
         return null;
      }
      if (entry.isRemoved()) {
         if (trace) {
            log.tracef("Entry has been deleted and is of type %s", entry.getClass().getSimpleName());
         }
         return null;
      }

      return entry.getValue();
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
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, Flag.copyWithoutRemotableFlags(flags)};
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

   public String toString() {
      return new StringBuilder()
            .append("GetKeyValueCommand {key=")
            .append(toStr(key))
            .append(", flags=").append(flags)
            .append("}")
            .toString();
   }

}
