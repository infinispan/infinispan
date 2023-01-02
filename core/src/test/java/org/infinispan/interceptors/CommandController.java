package org.infinispan.interceptors;

public interface CommandController {

   void awaitCommandBlocked() throws InterruptedException;

   void unblockCommand();

}
