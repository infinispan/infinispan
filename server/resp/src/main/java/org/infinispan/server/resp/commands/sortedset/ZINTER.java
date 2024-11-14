package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * ZINTER
 *
 * @see <a href="https://redis.io/commands/zinter/">ZINTER</a>
 * @since 15.0
 */
public class ZINTER extends AGGCommand {
   public ZINTER() {
      super(-3, 0, 0, 0, AGGCommandType.INTER);
   }

   @Override
   public long aclMask() {
      return AclCategory.READ | AclCategory.SORTEDSET | AclCategory.SLOW;
   }
}
