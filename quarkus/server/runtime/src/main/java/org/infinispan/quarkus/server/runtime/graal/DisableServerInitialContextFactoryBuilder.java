package org.infinispan.quarkus.server.runtime.graal;

import org.infinispan.server.Server;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class DisableServerInitialContextFactoryBuilder {
}

@TargetClass(Server.class)
final class Target_org_infinispan_server_Server {
   @Substitute
   private void registerInitialContextFactoryBuilder() {
      // Do nothing
   }
}
