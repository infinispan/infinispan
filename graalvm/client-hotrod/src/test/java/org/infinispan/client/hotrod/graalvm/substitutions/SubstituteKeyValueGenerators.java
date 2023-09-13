package org.infinispan.client.hotrod.graalvm.substitutions;

import java.util.UUID;

import org.infinispan.server.functional.hotrod.KeyValueGenerators;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class SubstituteKeyValueGenerators {
}

@TargetClass(KeyValueGenerators.class)
interface Target_SubstituteKeyValueGenerators {

   @Substitute
   static String getCallerMethodName(int ignore) {
      // We're no longer returning the method name, but a UUID will ensure that key/values are unique
      return UUID.randomUUID().toString();
   }
}
