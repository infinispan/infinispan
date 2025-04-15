package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.internal.PUSHX;

/**
 * RPUSHX
 *
 * @see <a href="https://redis.io/commands/rpushx/">RPUSHX</a>
 * @since 15.0
 */
public class RPUSHX extends PUSHX implements Resp3Command {
   public RPUSHX() {
      super(false, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.FAST.mask());
   }
}
