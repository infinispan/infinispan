package org.infinispan.commands.read;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;

/**
 * Command to calculate the size of the cache
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class SizeCommand extends AbstractLocalCommand implements VisitableCommand {
   private final AdvancedCache<?, ?> cache;

   public SizeCommand(Cache<Object, ?> cache, long flags) {
      setFlagsBitSet(flags);
      AdvancedCache<Object, ?> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache();
      if (flags != EnumUtil.EMPTY_BIT_SET) {
         this.cache = advancedCache.withFlags(EnumUtil.enumArrayOf(flags, Flag.class));
      } else {
         this.cache = advancedCache;
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitSizeCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Integer perform(InvocationContext ctx) throws Throwable {
      long size = cache.keySet().stream().count();
      if (size > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      } else {
         return (int) size;
      }
   }

   @Override
   public String toString() {
      return "SizeCommand{}";
   }
}
