package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * Computes the intersection of numkeys sorted sets given by the specified keys,
 * and stores the result in destination. It is mandatory to provide the number of input keys (numkeys)
 * before passing the input keys and the other (optional) arguments.
 *
 * By default, the resulting score of an element is the sum of its scores in the sorted sets where it exists.
 * Because intersection requires an element to be a member of every given sorted set, this results in the
 * score of every element in the resulting sorted set to be equal to the number of input sorted sets.
 *
 * For a description of the WEIGHTS and AGGREGATE options, see {@link ZUNIONSTORE}.
 *
 * If destination already exists, it is overwritten.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zinterstore/">Redis Documentation</a>
 */
public class ZINTERSTORE extends AGGCommand {
   public ZINTERSTORE() {
      super(-4, 1, 1, 1, AGGCommandType.INTER);
   }
}
