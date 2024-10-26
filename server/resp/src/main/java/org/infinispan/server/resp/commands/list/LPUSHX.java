package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.internal.PUSHX;

/**
 * LPUSHX
 *
 * @see <a href="https://redis.io/commands/lpushx/">LPUSHX</a>
 * @since 15.0
 */
public class LPUSHX extends PUSHX implements Resp3Command {
   public LPUSHX() {
      super(true);
   }
}
