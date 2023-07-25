package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.ZREMRANGE;

/**
 * When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering,
 * this command removes all elements in the sorted set stored at key between the lexicographical range
 * specified by min and max.
 *
 * The meaning of min and max are the same of the {@link ZRANGEBYLEX} command.
 * Similarly, this command actually removes the same elements that {@link ZRANGEBYLEX} would return
 * if called with the same min and max arguments.
 *
 * Integer reply: the number of elements removed.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zremrangebylex/">Redis Documentation</a>
 */
public class ZREMRANGEBYLEX extends ZREMRANGE {
   public ZREMRANGEBYLEX() {
      super(4, Type.LEX);
   }
}
