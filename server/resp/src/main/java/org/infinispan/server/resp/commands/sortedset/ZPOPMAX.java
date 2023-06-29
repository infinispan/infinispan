package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.POP;

/**
 * ZPOPMAX key [count]
 * Removes and returns up to count members with the highest scores in the sorted set stored at key.
 *
 * When left unspecified, the default value for count is 1.
 * Specifying a count value that is higher than the sorted set's cardinality will not produce an error.
 * When returning multiple elements, the one with the highest score will be the first, followed
 * by the elements with lower scores.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zpopmax/">Redis Documentation</a>
 */
public class ZPOPMAX extends POP {

   public ZPOPMAX() {
      super(false);
   }
}
