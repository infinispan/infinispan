package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
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

   public InvalidateL1Command(DataContainer dc, DistributionManager dm, CacheNotifier notifier, long flagsBitSet,
                              CommandInvocationId commandInvocationId, Object... keys) {
      super(notifier, flagsBitSet, commandInvocationId, keys);
      writeOrigin = null;
      this.dm = dm;
      this.dataContainer = dc;
   }

   public InvalidateL1Command(DataContainer dc, DistributionManager dm, CacheNotifier notifier, long flagsBitSet,
                              Collection<Object> keys, CommandInvocationId commandInvocationId) {
      this(null, dc, dm, notifier, flagsBitSet, keys, commandInvocationId);
   }

   public InvalidateL1Command(Address writeOrigin, DataContainer dc, DistributionManager dm, CacheNotifier notifier,
                              long flagsBitSet, Collection<Object> keys, CommandInvocationId commandInvocationId) {
      super(notifier, flagsBitSet, keys, commandInvocationId);
      this.writeOrigin = writeOrigin;
      this.dm = dm;
      this.dataContainer = dc;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void init(DistributionManager dm, CacheNotifier n, DataContainer dc) {
      super.init(n);
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
            boolean isLocal = dm.getCacheTopology().isWriteOwner(k);
            if (!isLocal) {
               if (trace) log.tracef("Invalidating key %s.", k);
               MVCCEntry e = (MVCCEntry) ctx.lookupEntry(k);
               notify(ctx, e, true);
               e.setValue(null);
               e.setMetadata(null);
               e.setRemoved(true);
               e.setChanged(true);
               e.setCreated(false);
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
   public Collection<?> getKeysToLock() {
      //no keys to lock
      return Collections.emptyList();
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output); //command invocation id + keys
      output.writeObject(writeOrigin);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      writeOrigin = (Address) input.readObject();
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
