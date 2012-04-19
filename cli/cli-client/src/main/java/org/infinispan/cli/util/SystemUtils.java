/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.util;

import java.io.File;

/**
 * SystemUtils.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class SystemUtils {
   /**
    * Returns an appropriate system-dependent folder for storing application-specific data. The
    * logic in this method uses the os.name to decide which is best. Currently it uses:
    * ~/.config/${appName} on Unix/Linux (as per Freedesktop.org) %APPDATA%/Sun/Java/${appName} on
    * Windows ~/Library/Java/${appName} on Mac OS X
    *
    * @param appName
    * @return
    */
   public static String getAppConfigFolder(String appName) {
      String configRoot = null;
      String osName = System.getProperty("os.name");
      if ("Mac OS X".equals(osName)) {
         configRoot = System.getProperty("user.home") + File.separator + "Library" + File.separator + "Java";
      } else if (osName.startsWith("Windows")) {
         // If on Windows, use the APPDATA environment
         try {
            configRoot = System.getenv("APPDATA");
            if (configRoot != null) {
               configRoot = configRoot + File.separator + "Sun" + File.separator + "Java"; // FIXME: should be different if using other JVMs from other vendors
            }
         } catch (SecurityException e) {
            // We may be wrapped by a SecurityManager, ignore the exception
         }
      }
      if (configRoot == null) {
         // Use the user.home
         configRoot = System.getProperty("user.home") + File.separator + ".config";
      }
      return configRoot + File.separator + appName;
   }

}
