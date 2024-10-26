package org.infinispan.server.resp.commands.list.blocking;

/**
 * BRPOP
 * <p>
 * Derogating to the above documentation, when multiple client are blocked
 * on a BRPOP, the order in which they will be served is unspecified.
 * </p>
 *
 * @see <a href="https://redis.io/commands/brpop/">BRPOP</a>
 * @since 15.0
 */
public class BRPOP extends SingleBlockingPop {

   public BRPOP() {
      super(false, -3, 1, -2, 1);
   }
}
