package org.infinispan.client.hotrod.graalvm.substitutions;

import org.infinispan.server.functional.ClusteredIT;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class SubstituteClusteredIT {
}

@TargetClass(ClusteredIT.class)
final class Target_ClusteredIT {

   @Substitute
   public static JavaArchive[] artifacts() {
      // ShrinkWrap does not work in native mode so we return an empty array. Consequently it's not possible to run
      // tests that rely on additional server side dependencies.
      return new JavaArchive[0];
   }
}
