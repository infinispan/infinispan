package org.infinispan.checkstyle.filters;

import java.io.File;
import java.util.List;

import com.puppycrawl.tools.checkstyle.api.AbstractFileSetCheck;

/**
 * Use a simple CheckStyle rule to make sure no copyright templates are being used:
 * Infinispan uses a single copyright file which can be found in the root of the project.
 *
 * @author Sanne Grinovero
 */
public class HeadersNoCopyrightCheck extends AbstractFileSetCheck {

   @Override
   protected void processFiltered(File aFile, List<String> aLines) {
      final String fileName = aFile.getName();
      if (fileName != null && !fileName.endsWith(".java")) {
         //Not a Java source file, skip it.
         return;
      }
      else if ("package-info.java".equals(fileName)) {
         //package-info files don't necessarily start with "package"
         return;
      }
      else if (!aLines.isEmpty()) {
         final String firstLine = aLines.get(0);
         if (firstLine!=null && !firstLine.startsWith("package ")) {
            log(1, "Java files should start with \''package \''. Infinispan doesn\''t use bulky copyright headers!");
         }
      }
   }

}
