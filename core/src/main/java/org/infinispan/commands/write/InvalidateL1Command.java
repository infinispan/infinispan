package org.infinispan.commands.write;

import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * Invalidates an entry in a L1 cache (used with DIST mode)
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.INVALIDATE_L1_COMMAND)
public class InvalidateL1Command extends InvalidateCommand {
   public static final int COMMAND_ID = 7;
   private DistributionManager dm;

   public InvalidateL1Command() {
   }

   public InvalidateL1Command(DistributionManager dm, CacheNotifier notifier, Object... keys) {
      super(notifier, keys);
      this.dm = dm;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void init(DistributionManager dm, CacheNotifier n) {
      super.init(n);
      this.dm = dm;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (Object k : getKeys()) {
         if (!dm.isLocal(k)) invalidate(ctx, k);
      }
      return null;
   }

   @Override
   public boolean equals(Object o) {
      return this == o || o instanceof InvalidateL1Command && super.equals(o);
   }
}
