package org.infinispan.server.resp.commands.stream;

/**
 * XREVRANGE
 *
 * @see <a href="https://redis.io/commands/xrevrange/">XREVRANGE</a>
 * @since 16.4
 */
public class XREVRANGE extends AbstractRange {

   public XREVRANGE() {
      super(true);
   }
}
