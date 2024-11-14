package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.list.internal.POP;

/**
 * LPOP
 *
 * @see <a href="https://redis.io/commands/lpop/">LPOP</a>
 * @since 15.0
 */
public class LPOP extends POP {
   public LPOP() {
      super(true);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.LIST | AclCategory.FAST;
   }
}
