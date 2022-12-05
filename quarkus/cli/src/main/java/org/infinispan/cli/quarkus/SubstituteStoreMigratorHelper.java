package org.infinispan.cli.quarkus;

import java.util.Properties;

import org.infinispan.cli.impl.StoreMigratorHelper;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

/**
 * @since 15.0
 **/
@TargetClass(StoreMigratorHelper.class)
@Substitute
public final class SubstituteStoreMigratorHelper {

   @Substitute
   public static void run(Properties props, boolean verbose) throws Exception {
      throw new UnsupportedOperationException("The native CLI doesn't implement store migration. Use the JVM version instead.");
   }
}
