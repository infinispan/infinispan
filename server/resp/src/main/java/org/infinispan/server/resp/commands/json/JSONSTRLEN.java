package org.infinispan.server.resp.commands.json;

import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.json.EmbeddedJsonCache;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * JSON.STRLEN
 *
 * @see <a href="https://redis.io/commands/json.strlen/">JSON.STRLEN</a>
 * @since 15.2
 */
public class JSONSTRLEN extends JSONLEN {
   public JSONSTRLEN() {
      super("JSON.STRLEN");
   }

   @Override
   protected CompletionStage<List<Long>> len(EmbeddedJsonCache ejc, byte[] key, byte[] path) {
      return ejc.srtLen(key, path);
   }

   @Override
   protected void raiseTypeError(byte[] path) {
      throw new RuntimeException("Path '" + RespUtil.ascii(path) + "' does not exist or not a string");
   }
}
