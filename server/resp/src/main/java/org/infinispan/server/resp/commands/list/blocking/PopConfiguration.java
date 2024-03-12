package org.infinispan.server.resp.commands.list.blocking;

import java.util.List;

class PopConfiguration {

   private final boolean head;
   private final int count;
   private final long timeout;
   private final List<byte[]> keys;

   PopConfiguration(boolean head, int count, long timeout, List<byte[]> keys) {
      this.head = head;
      this.count = count;
      this.timeout = timeout;
      this.keys = keys;
   }

   public byte[] key(int idx) {
      return keys.get(idx);
   }

   public int count() {
      return count;
   }

   public boolean isHead() {
      return head;
   }

   public long timeout() {
      return timeout;
   }

   public List<byte[]> keys() {
      return keys;
   }
}
