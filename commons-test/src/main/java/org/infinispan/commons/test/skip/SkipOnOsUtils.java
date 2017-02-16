package org.infinispan.commons.test.skip;

class SkipOnOsUtils {

   static SkipOnOs.OS getOs() {
      String os = System.getProperty("os.name").toLowerCase();
      if (os.indexOf("win") >= 0) {
         return SkipOnOs.OS.WINDOWS;
      } else if (os.indexOf("sunos") >= 0) {
         return SkipOnOs.OS.SOLARIS;
      } else {
         return SkipOnOs.OS.UNIX;
      }
   }

}
