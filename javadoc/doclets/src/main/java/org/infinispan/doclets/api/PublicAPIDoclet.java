package org.infinispan.doclets.api;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.formats.html.HtmlDoclet;
import com.sun.tools.javadoc.Main;

public class PublicAPIDoclet {

   public static void main(String[] args) {
      String name = PublicAPIDoclet.class.getName();
      Main.execute(name, name, args);
   }

   public static boolean validOptions(String[][] options, DocErrorReporter reporter) throws java.io.IOException {
      return HtmlDoclet.validOptions(options, reporter);
   }

   public static LanguageVersion languageVersion() {
      return LanguageVersion.JAVA_1_5;
   }

   public static int optionLength(String option) {
      return HtmlDoclet.optionLength(option);
   }

   public static boolean start(RootDoc root) throws java.io.IOException {
      return HtmlDoclet.start((RootDoc) PublicAPIFilterHandler.filter(root, RootDoc.class));
   }
}
