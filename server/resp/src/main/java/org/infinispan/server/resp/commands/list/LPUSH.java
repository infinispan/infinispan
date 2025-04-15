package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.internal.PUSH;

/**
 * LPUSH
 *
 * @see <a href="https://redis.io/commands/lpush/">LPUSH</a>
 * @since 15.0
 */
public class LPUSH extends PUSH implements Resp3Command {
   public LPUSH() {
      super(true, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.FAST.mask());
   }
}
