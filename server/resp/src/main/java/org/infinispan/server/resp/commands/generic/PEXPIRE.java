package org.infinispan.server.resp.commands.generic;

/**
 * `<code>PEXPIRE key milliseconds [NX | XX | GT | LT]</code>` command.
 *
 * <p>
 * This command works exactly like {@link EXPIRE} but the time to live of the key is specified in milliseconds
 * instead of seconds.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/pexpire/">Redis Documentation</a>
 */
public class PEXPIRE extends EXPIRE {

   public PEXPIRE() {
      super(false, false);
   }
}
