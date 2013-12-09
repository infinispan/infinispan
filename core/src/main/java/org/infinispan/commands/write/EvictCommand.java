package org.infinispan.commands.write;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class EvictCommand extends RemoveCommand implements LocalCommand {

   private static final Log log = LogFactory.getLog(EvictCommand.class);

   public EvictCommand(Object key, CacheNotifier notifier, Set<Flag> flags) {
      super(key, null, notifier, flags, null);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEvictCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (key == null) {
         throw new NullPointerException("Key is null!!");
      }
      super.perform(ctx);
      return null;
   }

   @Override
   public void notify(InvocationContext ctx, Object value, boolean isPre) {
      if (!isPre) {
         if (log.isTraceEnabled())
            log.tracef("Notify eviction listeners for key=%", key);

         notifier.notifyCacheEntryEvicted(key, value, ctx, this);
      }
   }

   @Override
   public byte getCommandId() {
      return -1; // these are not meant for replication!
   }
   
   @Override
   public String toString() {
      return new StringBuilder()
         .append("EvictCommand{key=")
         .append(key)
         .append(", value=").append(value)
         .append(", flags=").append(flags)
         .append("}")
         .toString();
   }
}
