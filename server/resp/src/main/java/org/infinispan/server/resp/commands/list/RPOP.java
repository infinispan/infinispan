package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.commands.list.internal.POP;

/**
 * RPOP
 *
 * @see <a href="https://redis.io/commands/rpop/">RPOP</a>
 * @since 15.0
 */
public class RPOP extends POP {
   public RPOP() {
      super(false);
   }
}
