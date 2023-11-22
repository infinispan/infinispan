package org.infinispan.server.resp.commands.list.blocking;

/**
 * @link https://redis.io/commands/brpop/
 *       Derogating to the above documentation, when multiple client are blocked
 *       on a BRPOP, the order in which they will be served is unspecified.
 * @since 15.0
 */
public class BRPOP extends BPOP {

   public BRPOP() {
      super(false);
   }
}
