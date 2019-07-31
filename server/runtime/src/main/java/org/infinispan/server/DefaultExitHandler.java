package org.infinispan.server;

import org.infinispan.commons.util.Version;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class DefaultExitHandler extends ExitHandler {
   @Override
   public void exit(int exitCode) {
      Server.log.serverStopping(Version.getBrandName());
      exitFuture.complete(exitCode);
      Server.log.serverStopped(Version.getBrandName());
   }
}
