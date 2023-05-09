package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.internal.PUSHX;

/**
 * @link https://redis.io/commands/lpushx/
 * Inserts specified values at the head of the list stored at key,
 * only if key already exists and holds a list. In contrary to LPUSH,
 * no operation will be performed when key does not yet exist.
 * @since 15.0
 */
public class LPUSHX extends PUSHX implements Resp3Command {
   public LPUSHX() {
      super(true);
   }
}
