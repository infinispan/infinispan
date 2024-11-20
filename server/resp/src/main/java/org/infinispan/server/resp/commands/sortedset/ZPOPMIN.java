package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.sortedset.internal.POP;

/**
 * ZPOPMIN
 *
 * @see <a href="https://redis.io/commands/zpopmin/">ZPOPMIN</a>
 * @since 15.0
 */
public class ZPOPMIN extends POP {

   public ZPOPMIN() {
      super(true);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.SORTEDSET | AclCategory.FAST;
   }
}
