package org.infinispan.server.resp.commands.json;

/**
 * JSON.ARRLEN
 *
 * @see <a href="https://redis.io/commands/json.arrlen/">JSON.ARRLEN</a>
 * @since 15.2
 */
public class JSONARRLEN extends JSONLEN {
   public JSONARRLEN() {
      super("JSON.ARRLEN");
   }
}
