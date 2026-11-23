package org.infinispan.server.resp.commands.list.blocking;

import java.util.Collection;

import org.infinispan.server.resp.AclCategory;

/**
 * BLPOP
 *
 * @see <a href="https://redis.io/commands/blpop/">BLPOP</a>
 * @since 15.0
 */
public class BLPOP extends SingleBlockingPop<Collection<byte[]>> {
   public BLPOP() {
      super(LIST, true, -3, 1, -2, 1, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask() | AclCategory.BLOCKING.mask());
   }
}
