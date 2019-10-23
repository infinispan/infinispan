package org.infinispan.server;

import java.io.Serializable;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ExitStatus implements Serializable {
   public enum ExitMode { SERVER_SHUTDOWN, CLUSTER_SHUTDOWN, ERROR }

   public static final ExitStatus CLUSTER_SHUTDOWN = new ExitStatus(ExitMode.CLUSTER_SHUTDOWN, 0);
   public static final ExitStatus SERVER_SHUTDOWN = new ExitStatus(ExitMode.SERVER_SHUTDOWN, 0);

   final ExitMode mode;
   final int status;

   public ExitStatus(ExitMode mode, int status) {
      this.mode = mode;
      this.status = status;
   }
}
