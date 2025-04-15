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
      super(true, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.FAST.mask());
   }
}
