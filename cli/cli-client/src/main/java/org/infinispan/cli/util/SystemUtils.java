package org.infinispan.cli.util;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * SystemUtils.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class SystemUtils {
   /**
    * Returns an appropriate system-dependent folder for storing application-specific data. The logic in this method
    * uses the os.name to decide which is best. Currently it uses: ~/.config/${appName} on Unix/Linux (as per
    * Freedesktop.org) %APPDATA%/Sun/Java/${appName} on Windows ~/Library/Java/${appName} on Mac OS X
    *
    * @param appName
    * @return
    */
   public static String getAppConfigFolder(String appName) {
      Path configRoot = null;
      String osName = System.getProperty("os.name");
      if ("Mac OS X".equals(osName)) {
         configRoot = Paths.get(System.getProperty("user.home"), "Library", "Java");
      } else if (osName.startsWith("Windows")) {
         // If on Windows, use the APPDATA environment
         try {
            String appData = System.getenv("APPDATA");
            if (appData != null) {
               configRoot = Paths.get(appData).resolve("Sun").resolve("Java");
            }
         } catch (SecurityException e) {
            // We may be wrapped by a SecurityManager, ignore the exception
         }
      }
      if (configRoot == null) {
         // Use the user.home
         configRoot = Paths.get(System.getProperty("user.home"), ".config");
      }
      return configRoot.resolve(appName).toString();
   }

}
