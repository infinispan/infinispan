package org.infinispan.server.resp.commands.generic;

/**
 * EXPIREAT
 *
 * @see <a href="https://redis.io/commands/expireat/">EXPIREAT</a>
 * @since 15.0
 */
public class EXPIREAT extends EXPIRE {
   public EXPIREAT() {
      super(true, true);
   }
}
