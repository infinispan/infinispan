package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * ZUNION
 *
 * @see <a href="https://redis.io/commands/zunion/">ZUNION</a>
 * @since 15.0
 */
public class ZUNION extends AGGCommand {
   public ZUNION() {
      super(-3, 0, 0, 0, AGGCommandType.UNION, AclCategory.READ.mask() | AclCategory.SORTEDSET.mask() | AclCategory.SLOW.mask());
   }
}
