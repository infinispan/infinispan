package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.AclCategory;

/**
 * @see <a href="https://redis.io/commands/zrangestore/">ZRANGESTORE</a>
 * This command is like {@link ZRANGE}, but stores the result in the &lt;dst&gt; destination key.
 * @since 15.0
 */
public class ZRANGESTORE extends ZRANGE {
   public ZRANGESTORE() {
      super(-5, 1, 2, 1, AclCategory.WRITE.mask() | AclCategory.SORTEDSET.mask() | AclCategory.SLOW.mask());
   }
}
