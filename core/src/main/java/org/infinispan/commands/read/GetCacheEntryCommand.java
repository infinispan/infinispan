package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.ByteString;

/**
 * Used to fetch a full CacheEntry rather than just the value.
 * This functionality was originally incorporated into GetKeyValueCommand.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.1
 */
public final class GetCacheEntryCommand extends AbstractDataCommand implements LocalCommand {

   public GetCacheEntryCommand(ByteString cacheName, Object key, int segment, long flagsBitSet) {
      super(cacheName, key, segment, flagsBitSet);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetCacheEntryCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   public String toString() {
      return "GetCacheEntryCommand {key=" +
            toStr(key) +
            ", flags=" + printFlags() +
            "}";
   }
}
