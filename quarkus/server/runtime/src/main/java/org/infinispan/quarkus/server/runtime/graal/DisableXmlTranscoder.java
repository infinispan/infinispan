package org.infinispan.quarkus.server.runtime.graal;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.server.core.LifecycleCallbacks;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class DisableXmlTranscoder {
}

@TargetClass(LifecycleCallbacks.class)
final class Target_LifecycleCallbacks {
   @Substitute
   private void registerXmlTranscoder(EncoderRegistry encoderRegistry, ClassLoader classLoader, ClassAllowList classAllowList) {
      // Do nothing
   }
}
