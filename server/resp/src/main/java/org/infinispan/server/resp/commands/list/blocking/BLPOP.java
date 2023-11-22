package org.infinispan.server.resp.commands.list.blocking;

public class BLPOP extends BPOP {
   public BLPOP() {
      super(true);
   }
}
