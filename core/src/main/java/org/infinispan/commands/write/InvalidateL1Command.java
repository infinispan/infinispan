package org.infinispan.commands.write;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DataLocality;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Invalidates an entry in a L1 cache (used with DIST mode)
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class InvalidateL1Command extends InvalidateCommand {
   public static final int COMMAND_ID = 7;
   private static final Log log = LogFactory.getLog(InvalidateL1Command.class);
   private DistributionManager dm;
   private DataContainer dataContainer;
   private Address writeOrigin;

   public InvalidateL1Command() {
      writeOrigin = null;
   }

   public InvalidateL1Command(DataContainer dc, DistributionManager dm, CacheNotifier notifier, Set<Flag> flags,
                              CommandInvocationId commandInvocationId, Object... keys) {
      super(notifier, flags, commandInvocationId, keys);
      writeOrigin = null;
      this.dm = dm;
      this.dataContainer = dc;
   }

   public InvalidateL1Command(DataContainer dc, DistributionManager dm, CacheNotifier notifier, Set<Flag> flags,
                              Collection<Object> keys, CommandInvocationId commandInvocationId) {
      this(null, dc, dm, notifier, flags, keys, commandInvocationId);
   }

   public InvalidateL1Command(Address writeOrigin, DataContainer dc, DistributionManager dm, CacheNotifier notifier,
                              Set<Flag> flags, Collection<Object> keys, CommandInvocationId commandInvocationId) {
      super(notifier, flags, keys, commandInvocationId);
      this.writeOrigin = writeOrigin;
      this.dm = dm;
      this.dataContainer = dc;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void init(Configuration config, DistributionManager dm, CacheNotifier n, DataContainer dc) {
      super.init(n, config);
      this.dm = dm;
      this.dataContainer = dc;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      if (trace) log.tracef("Preparing to invalidate keys %s", Arrays.asList(keys));
      for (Object k : getKeys()) {
         InternalCacheEntry ice = dataContainer.get(k);
         if (ice != null) {
            DataLocality locality = dm.getLocality(k);

            if (!locality.isLocal()) {
               if (trace) log.tracef("Invalidating key %s.", k);
               invalidate(ctx, k);
            } else {
               log.tracef("Not invalidating key %s as it is local now", k);
            }
         }
      }
      return null;
   }

   public void setKeys(Object[] keys) {
      this.keys = keys;
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      if (ctx.isOriginLocal()) return true;
      for (Object k : getKeys()) {
         // If any key in the set of keys to invalidate is not local, or we are uncertain due to a rehash, then we
         // process this command.
         DataLocality locality = dm.getLocality(k);
         if (!locality.isLocal() || locality.isUncertain()) return true;
      }
      return false;
   }

   @Override
   public Object[] getParameters() {
      if (keys == null || keys.length == 0) {
         return new Object[]{commandInvocationId, writeOrigin, 0};
      } else if (keys.length == 1) {
         return new Object[]{commandInvocationId, writeOrigin, 1,  keys[0]};
      } else {
         Object[] retval = new Object[keys.length + 3];
         int i = 0;
         retval[i++] = commandInvocationId;
         retval[i++] = writeOrigin;
         retval[i++] = keys.length;
         System.arraycopy(keys, 0, retval, i, keys.length);
         return retval;
      }
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      int i = 0;
      commandInvocationId = (CommandInvocationId) args[i++];
      writeOrigin = (Address) args[i++];
      int size = (Integer) args[i++];
      keys = new Object[size];
      if (size == 1) {
         keys[0] = args[i];
      } else if (size > 0) {
         System.arraycopy(args, i, keys, 0, size);
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitInvalidateL1Command(ctx, this);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "num keys=" + (keys == null ? 0 : keys.length) +
            ", origin=" + writeOrigin +
            '}';
   }

   /**
    * Returns true if the write that caused the invalidation was performed on this node.
    * More formal, if a put(k) happens on node A and ch(A)={B}, then an invalidation message
    * might be multicasted by B to all cluster members including A. This method returns true
    * if and only if the node where it is invoked is A.
    */
   public boolean isCausedByALocalWrite(Address address) {
      return writeOrigin != null && writeOrigin.equals(address);
   }
}
