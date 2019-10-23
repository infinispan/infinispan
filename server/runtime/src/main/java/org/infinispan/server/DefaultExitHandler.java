package org.infinispan.server;

import org.infinispan.commons.util.Version;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class DefaultExitHandler extends ExitHandler {
   @Override
   public void exit(ExitStatus exitStatus) {
      Server.log.serverStopping(Version.getBrandName());
      exitFuture.complete(exitStatus);
      Server.log.serverStopped(Version.getBrandName());
   }
}
