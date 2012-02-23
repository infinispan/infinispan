/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A wrapper around system properties that supports legacy keys
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class LegacyKeySupportSystemProperties {

   private static final Log log = LogFactory.getLog(LegacyKeySupportSystemProperties.class);

   private static void warnLegacy(String oldKey, String newKey) {
      if (log.isInfoEnabled())
         log.infof("Could not find value for key %1$s, but did find value under deprecated key %2$s. Please use %1$s as support for %2$s will eventually be discontinued.",
                  newKey, oldKey);
   }

   public static String getProperty(String key, String legacyKey) {
      String val = SysPropertyActions.getProperty(key);
      if (val == null) {
         val = SysPropertyActions.getProperty(legacyKey);
         if (val != null) warnLegacy(legacyKey, key);
      }
      return val;
   }

   public static String getProperty(String key, String legacyKey, String defaultValue) {
      String val = SysPropertyActions.getProperty(key);
      if (val == null) {
         val = SysPropertyActions.getProperty(legacyKey);
         if (val != null)
            warnLegacy(legacyKey, key);
         else
            val = defaultValue;
      }
      return val;
   }   
}
