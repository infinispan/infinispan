package org.infinispan.quarkus.embedded.runtime;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import io.prometheus.metrics.core.exemplars.ExemplarSampler;

public class SubstituteMicrometerMetrics {
}

@TargetClass(ExemplarSampler.class)
final class SubstituteExemplarSampler {
    @Substitute
    private long doObserve(double value) {
        return 0L;
    }

}
