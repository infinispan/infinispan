package org.infinispan.server.resp.commands.stream;

/**
 * XRANGE
 *
 * @see <a href="https://redis.io/commands/xrange/">XRANGE</a>
 * @since 16.4
 */
public class XRANGE extends AbstractRange {

   public XRANGE() {
      super(false);
   }
}
