package org.infinispan.server.resp.commands.generic;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.iteration.IterationManager;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.iteration.BaseIterationCommand;

import java.util.ArrayList;
import java.util.List;

/**
 * Returns all keys matching pattern.
 *
 * @see <a href="https://redis.io/commands/keys">Redis Documentation</a>
 * @since 15.0
 */
public class KEYS extends BaseIterationCommand {

   public KEYS() {
      super(2, 0, 0, 0);
   }

   @Override
   protected boolean writeCursor() {
      return false;
   }

   @Override
   protected IterationManager retrieveIterationManager(Resp3Handler handler) {
      return handler.respServer().getIterationManager();
   }

   @Override
   protected String cursor(List<byte[]> raw) {
      return "0";
   }

   @Override
   protected byte[] getMatch(List<byte[]> arguments) {
      return arguments.get(0);
   }

   @Override
   protected List<byte[]> writeResponse(List<CacheEntry> response) {
      List<byte[]> output = new ArrayList<>(response.size());
      for (CacheEntry<?, ?> entry : response) {
         output.add((byte[]) entry.getKey());
      }
      return output;
   }
}
