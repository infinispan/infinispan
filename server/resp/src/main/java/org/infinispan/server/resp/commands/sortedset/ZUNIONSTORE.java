package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * Computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination.
 * It is mandatory to provide the number of input keys (numkeys) before passing the input keys
 * and the other (optional) arguments.
 *
 * By default, the resulting score of an element is the sum of its scores in the
 * sorted sets where it exists.
 * <ul>
 *    <li>
 *       WEIGHTS: it is possible to specify a multiplication factor for each input sorted set.
 *       This means that the score of every element in every input sorted set is multiplied
 *       by this factor before being passed to the aggregation function. Default to 1
 *    </li>
 *    <li>
 *       AGGREGATE: it is possible to specify how the results of the union are aggregated.
 *       This option defaults to SUM, where the score of an element is summed across the inputs where
 *       it exists. When this option is set to either MIN or MAX, the resulting
 *       set will contain the minimum or maximum score of an element across the inputs where it exists.
 *    </li>
 * </ul>
 *
 * If destination already exists, it is overwritten.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zunionstore/">Redis Documentation</a>
 */
public class ZUNIONSTORE extends AGGCommand {
   public ZUNIONSTORE() {
      super(-4, 1, 1, 1, AGGCommandType.UNION);
   }
}
