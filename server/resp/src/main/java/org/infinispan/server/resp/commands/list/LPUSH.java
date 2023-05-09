package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.internal.PUSH;

/**
 * @link https://redis.io/commands/lpush/
 * Insert all the specified values at the head of the list stored at key. If key does not exist,
 * it is created as empty list before performing the push operations.
 * When key holds a value that is not a list, an error is returned.
 * Integer reply: the length of the list after the push operation.
 * @since 15.0
 */
public class LPUSH extends PUSH implements Resp3Command {
   public LPUSH() {
      super(true);
   }
}
