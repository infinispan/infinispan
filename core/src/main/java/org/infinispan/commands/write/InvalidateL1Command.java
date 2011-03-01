package org.infinispan.commands.write;

import org.infinispan.commands.Visitor;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DataLocality;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Invalidates an entry in a L1 cache (used with DIST mode)
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.INVALIDATE_L1_COMMAND)
public class InvalidateL1Command extends InvalidateCommand {
   public static final int COMMAND_ID = 7;
   private static final Log log = LogFactory.getLog(InvalidateL1Command.class);
   private DistributionManager dm;
   private DataContainer dataContainer;
   private Configuration config;
   private boolean forRehash;

   public InvalidateL1Command() {
   }

   public InvalidateL1Command(boolean forRehash, DataContainer dc, Configuration config, DistributionManager dm,
                              CacheNotifier notifier, Object... keys) {
      super(notifier, keys);
      this.dm = dm;
      this.forRehash = forRehash;
      this.dataContainer = dc;
      this.config = config;
   }

   public InvalidateL1Command(boolean forRehash, DataContainer dc, Configuration config, DistributionManager dm,
                              CacheNotifier notifier, Collection<Object> keys) {
      super(notifier, keys);
      this.dm = dm;
      this.forRehash = forRehash;
      this.dataContainer = dc;
      this.config = config;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void init(Configuration config, DistributionManager dm, CacheNotifier n, DataContainer dc) {
      super.init(n);
      this.dm = dm;
      this.config = config;
      this.dataContainer = dc;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {

      for (Object k : getKeys()) {
         InternalCacheEntry ice = dataContainer.get(k);
         if (ice != null) {
            DataLocality locality = dm.getLocality(k);

            while (locality.isUncertain() && dm.isRehashInProgress()) {
               LockSupport.parkNanos(MILLISECONDS.toNanos(50));
               locality = dm.getLocality(k);
            }

            if (!locality.isLocal()) {
               if (forRehash && config.isL1OnRehash()) {
                  if (log.isTraceEnabled()) log.trace("Not removing, instead putting entry into L1.");
                  dataContainer.put(k, ice.getValue(), config.getL1Lifespan(), config.getExpirationMaxIdle());
               } else {
                  invalidate(ctx, k);
               }
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
      if (ctx.isOriginLocal() || forRehash) return true;
      for (Object k : getKeys()) {
         // If any key in the set of keys to invalidate is not local, or we are uncertain due to a rehash, then we
         // process this command.
         DataLocality locality = dm.getLocality(k);
         if (!locality.isLocal() || locality.isUncertain()) return true;
      }
      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      InvalidateL1Command that = (InvalidateL1Command) o;

      if (forRehash != that.forRehash) return false;

      return true;
   }

   @Override
   public Object[] getParameters() {
      if (keys == null || keys.length == 0) {
         return new Object[]{forRehash};
      } else if (keys.length == 1) {
         return new Object[]{forRehash, 1, keys[0]};
      } else {
         Object[] retval = new Object[keys.length + 2];
         retval[0] = forRehash;
         retval[1] = keys.length;
         System.arraycopy(keys, 0, retval, 2, keys.length);
         return retval;
      }
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      forRehash = (Boolean) args[0];
      int size = (Integer) args[1];
      keys = new Object[size];
      if (size == 1) {
         keys[0] = args[2];
      } else if (size > 0) {
         System.arraycopy(args, 2, keys, 0, size);
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitInvalidateL1Command(ctx, this);
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (forRehash ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "num keys=" + (keys == null ? 0 : keys.length) +
            ", forRehash=" + forRehash +
            '}';
   }

}
