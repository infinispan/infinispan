package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.internal.PUSH;

/**
 * RPUSH
 *
 * @see <a href="https://redis.io/commands/rpush/">RPUSH</a>
 * @since 15.0
 */
public class RPUSH extends PUSH implements Resp3Command {
   public RPUSH() {
      super(false);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.LIST | AclCategory.FAST;
   }
}
