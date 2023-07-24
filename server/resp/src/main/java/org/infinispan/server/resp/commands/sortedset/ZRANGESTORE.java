package org.infinispan.server.resp.commands.sortedset;

/**
 * This command is like {@link ZRANGE}, but stores the result in the <dst> destination key.
 * @since 15.0
 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation</a>
 */
public class ZRANGESTORE extends ZRANGE {
   public ZRANGESTORE() {
      super(-5);
   }
}
