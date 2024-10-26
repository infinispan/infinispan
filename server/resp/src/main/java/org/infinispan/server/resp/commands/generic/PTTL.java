package org.infinispan.server.resp.commands.generic;

/**
 * PTTL
 *
 * @see <a href="https://redis.io/commands/pttl/">PTTL</a>
 * @since 15.0
 */
public class PTTL extends TTL {

   public PTTL() {
      super(ExpirationOption.REMAINING);
   }
}
