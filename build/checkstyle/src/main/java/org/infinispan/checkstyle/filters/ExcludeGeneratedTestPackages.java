package org.infinispan.checkstyle.filters;

import java.io.File;

import com.puppycrawl.tools.checkstyle.api.AuditEvent;
import com.puppycrawl.tools.checkstyle.api.Filter;

/**
 * Excludes generated packages.
 */
public class ExcludeGeneratedTestPackages implements Filter {

   private static final String SUB_PATH = File.separator + "target" + File.separator + "generated-test-sources";

   @Override
   public boolean accept(AuditEvent aEvent) {
      final String fileName = aEvent.getFileName();
      return !fileName.contains(SUB_PATH);
   }
}
