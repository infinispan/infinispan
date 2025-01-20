package org.infinispan.quarkus.server.runtime.graal;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.server.resp.RespServer;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;


public class SubstituteLua {
}

@TargetClass(RespServer.class)
final class Substitute_RespServer {

   @Substitute
   private void initializeLuaTaskEngine(GlobalComponentRegistry gcr) {
   }
}
