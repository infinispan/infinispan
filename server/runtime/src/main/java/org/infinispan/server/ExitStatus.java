package org.infinispan.server;

import java.io.Serializable;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@ProtoTypeId(ProtoStreamTypeIds.SERVER_RUNTIME_EXIT_STATUS)
public class ExitStatus implements Serializable {

   @Proto
   @ProtoTypeId(ProtoStreamTypeIds.SERVER_RUNTIME_EXIT_MODE)
   public enum ExitMode {
      SERVER_SHUTDOWN,
      CLUSTER_SHUTDOWN,
      ERROR
   }

   public static final ExitStatus CLUSTER_SHUTDOWN = new ExitStatus(ExitMode.CLUSTER_SHUTDOWN, 0);
   public static final ExitStatus SERVER_SHUTDOWN = new ExitStatus(ExitMode.SERVER_SHUTDOWN, 0);

   @ProtoField(1)
   final ExitMode mode;

   @ProtoField(value = 2, defaultValue = "-1")
   final int status;

   @ProtoFactory
   ExitStatus(ExitMode mode, int status) {
      this.mode = mode;
      this.status = status;
   }

   @Override
   public String toString() {
      return "ExitStatus{" +
            "mode=" + mode +
            ", status=" + status +
            '}';
   }
}
