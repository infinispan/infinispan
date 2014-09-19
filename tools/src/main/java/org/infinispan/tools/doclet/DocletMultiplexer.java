package org.infinispan.tools.doclet;

import java.lang.reflect.Method;

import org.infinispan.tools.doclet.jmx.JmxDoclet;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;
import com.sun.tools.javadoc.Main;

public class DocletMultiplexer {
   private static final Object doclets[] = {
         new PublicAPIDoclet(),
         new JmxDoclet()
   };

   public static void main(String[] args) {
      String name = DocletMultiplexer.class.getName();
      Main.execute(name, name, args);
   }

   public static boolean validOptions(String[][] options, DocErrorReporter reporter) throws java.io.IOException {
      boolean validOptions = true;
      for(Object doclet : doclets) {
         try {
            Method method = doclet.getClass().getMethod("validOptions", String[][].class, DocErrorReporter.class);
            validOptions = validOptions && ((Boolean)method.invoke(doclet, options, reporter));
         } catch (Exception e) {
            reporter.printWarning(e.getMessage());
         }
      }
      return validOptions;
   }

   public static LanguageVersion languageVersion() {
      return LanguageVersion.JAVA_1_5;
   }

   public static int optionLength(String option) {
      for(Object doclet : doclets) {
         try {
            Method method = doclet.getClass().getMethod("optionLength", String.class);
            int l = (Integer)method.invoke(doclet, option);
            if (l > 0)
               return l;
         } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
         }
      }
      return Standard.optionLength(option);
   }

   public static boolean start(RootDoc root) throws java.io.IOException {
      boolean start = true;
      for(Object doclet : doclets) {
         try {
            Method method = doclet.getClass().getMethod("start", RootDoc.class);
            start = start && ((Boolean)method.invoke(doclet, root));
         } catch (Exception e) {
            root.printWarning(e.getMessage());
         }
      }
      return start;
   }
}
