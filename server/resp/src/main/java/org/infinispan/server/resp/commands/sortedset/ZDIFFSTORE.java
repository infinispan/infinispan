package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.sortedset.internal.DIFF;

/**
 * ZDIFFSTORE
 *
 * @see <a href="https://redis.io/commands/zdiffstore/">ZDIFFSTORE</a>
 * @since 15.0
 */
public class ZDIFFSTORE extends DIFF {
   public ZDIFFSTORE() {
      super(-4, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.SORTEDSET | AclCategory.SLOW;
   }
}
