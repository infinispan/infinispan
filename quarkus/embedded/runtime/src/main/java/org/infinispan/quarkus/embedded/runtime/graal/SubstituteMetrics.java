package org.infinispan.quarkus.embedded.runtime.graal;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.metrics.impl.BaseOperatingSystemAdditionalMetrics;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

class SubstituteMetrics {
}

@TargetClass(BaseOperatingSystemAdditionalMetrics.class)
final class Target_BaseOperatingSystemAdditionalMetrics implements MeterBinder {

   @Substitute
   @Override
   public void bindTo(MeterRegistry ignore) {
      // no-op
   }

   @Substitute
   public Json cpuReport() {
      return Json.nil();
   }
}
