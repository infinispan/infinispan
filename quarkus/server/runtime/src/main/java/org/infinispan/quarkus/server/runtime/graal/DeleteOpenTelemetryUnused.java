package org.infinispan.quarkus.server.runtime.graal;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import io.opentelemetry.sdk.internal.RandomSupplier;

@Delete
@TargetClass(className = "io.opentelemetry.sdk.internal.AndroidFriendlyRandomHolder")
final class Delete_ioopentelemetrysdkinternalAndroidFriendlyRandomHolder {
}

@TargetClass(RandomSupplier.class)
final class SubstituteRandomSupplier {

   @Substitute
   public static Supplier<Random> platformDefault() {
      return ThreadLocalRandom::current;
   }
}

public final class DeleteOpenTelemetryUnused {
}
