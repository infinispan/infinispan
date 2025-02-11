package org.infinispan.server.resp.commands.json;

/**
 * JSON.OBJLEN
 *
 * @see <a href="https://redis.io/commands/json.objlen/">JSON.OBJLEN</a>
 *
 * @since 15.2
 */
public class JSONOBJLEN extends JSONLEN {
   public JSONOBJLEN() {
      super("JSON.OBJLEN", true);
   }
}
