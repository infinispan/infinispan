package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.ZREMRANGE;

/**
 * Removes all elements in the sorted set stored at key with rank between start and stop.
 * Both start and stop are 0 -based indexes with 0 being the element with the lowest score.
 * These indexes can be negative numbers, where they indicate offsets starting at the element with
 * the highest score. For example: -1 is the element with the highest score, -2 the
 * element with the second highest score and so forth.
 *
 * Integer reply: the number of elements removed.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zremrangebyrank/">Redis Documentation</a>
 */
public class ZREMRANGEBYRANK extends ZREMRANGE {
   public ZREMRANGEBYRANK() {
      super(4, Type.RANK);
   }
}
