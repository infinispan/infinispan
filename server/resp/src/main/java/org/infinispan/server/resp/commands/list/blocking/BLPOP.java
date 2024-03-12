package org.infinispan.server.resp.commands.list.blocking;

public class BLPOP extends SingleBlockingPop {
   public BLPOP() {
      super(true, -3, 1, -2, 1);
   }
}
