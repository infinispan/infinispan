package org.infinispan.checkstyle.filters;

import java.io.File;

import com.puppycrawl.tools.checkstyle.api.AbstractFileSetCheck;
import com.puppycrawl.tools.checkstyle.api.FileText;

/**
 * Use a simple CheckStyle rule to make sure no copyright templates are being used:
 * Infinispan uses a single copyright file which can be found in the root of the project.
 *
 * @author Sanne Grinovero
 */
public class HeadersNoCopyrightCheck extends AbstractFileSetCheck {

   @Override
   protected void processFiltered(File file, FileText fileText) {
      final String fileName = file.getName();
      if (fileName != null && !fileName.endsWith(".java")) {
         //Not a Java source file, skip it.
         return;
      }
      else if ("package-info.java".equals(fileName)) {
         //package-info files don't necessarily start with "package"
         return;
      }
      else if (fileText.size() != 0) {
         final String firstLine = fileText.get(0);
         if (firstLine!=null && !firstLine.startsWith("package ")) {
            log(1, "Java files should start with \''package \''. Infinispan doesn\''t use bulky copyright headers!");
         }
      }
   }

}
