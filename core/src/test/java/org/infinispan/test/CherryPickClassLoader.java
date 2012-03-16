/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.test;

import org.jboss.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A classloader where the classes that are included, excluded or not found can
 * individually configured to suit a particular test in a cherry pick fashion.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class CherryPickClassLoader extends ClassLoader {

   private static final Logger log = Logger.getLogger(CherryPickClassLoader.class);

   private String[] includedClasses;
   private String[] excludedClasses;
   private String[] notFoundClasses;

   private Map<String, Class> classes = new HashMap<String, Class>();

   public CherryPickClassLoader(String[] includedClasses,
                                String[] excludedClasses, ClassLoader parent) {
      this(includedClasses, excludedClasses, null, parent);
   }

   public CherryPickClassLoader(String[] includedClasses,
                                String[] excludedClasses,
                                String[] notFoundClasses, ClassLoader parent) {
      super(parent);
      this.includedClasses = includedClasses;
      this.excludedClasses = excludedClasses;
      this.notFoundClasses = notFoundClasses;
      log.debugf("Created %s", this);
   }

   @Override
   protected synchronized Class<?> loadClass(String name, boolean resolve)
         throws ClassNotFoundException {
      log.tracef("loadClass(%s,%b)", name, resolve);
      if (isIncluded(name) && (isExcluded(name) == false)) {
         Class c = findClass(name);
         if (resolve)
            resolveClass(c);
         return c;
      } else if (isNotFound(name)) {
         throw new ClassNotFoundException(name + " is discarded");
      } else {
         return super.loadClass(name, resolve);
      }
   }

   @Override
   protected Class<?> findClass(String name) throws ClassNotFoundException {
      log.tracef("findClass(%s)", name);
      Class result = classes.get(name);
      if (result != null)
         return result;

      if (isIncluded(name) && (isExcluded(name) == false)) {
         result = createClass(name);
      } else if (isNotFound(name)) {
         throw new ClassNotFoundException(name + " is discarded");
      } else {
         result = super.findClass(name);
      }

      classes.put(name, result);
      return result;
   }

   protected Class createClass(String name) throws ClassFormatError, ClassNotFoundException {
      log.infof("createClass(%s)", name);
      try {
         InputStream is = getResourceAsStream(name.replace('.', '/').concat(".class"));
         byte[] bytes = new byte[1024];
         ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         int read;
         while ((read = is.read(bytes)) > -1) {
            baos.write(bytes, 0, read);
         }
         bytes = baos.toByteArray();
         return this.defineClass(name, bytes, 0, bytes.length);
      } catch (FileNotFoundException e) {
         throw new ClassNotFoundException("cannot find " + name, e);
      } catch (IOException e) {
         throw new ClassNotFoundException("cannot read " + name, e);
      }
   }

   protected boolean isIncluded(String className) {
      if (includedClasses != null) {
         for (int i = 0; i < includedClasses.length; i++) {
            if (className.startsWith(includedClasses[i])) {
               return true;
            }
         }
      }
      return false;
   }

   protected boolean isExcluded(String className) {
      if (excludedClasses != null) {
         for (int i = 0; i < excludedClasses.length; i++) {
            if (className.startsWith(excludedClasses[i])) {
               return true;
            }
         }
      }
      return false;
   }

   protected boolean isNotFound(String className) {
      if (notFoundClasses != null) {
         for (int i = 0; i < notFoundClasses.length; i++) {
            if (className.startsWith(notFoundClasses[i])) {
               return true;
            }
         }
      }
     return false;
   }

   @Override
   public String toString() {
      String s = getClass().getName();
      s += "[includedClasses=";
      s += listClasses(includedClasses);
      s += ";excludedClasses=";
      s += listClasses(excludedClasses);
      s += ";notFoundClasses=";
      s += listClasses(notFoundClasses);
      s += ";parent=";
      s += getParent();
      s += "]";
      return s;
   }

   private static String listClasses(String[] classes) {
      if (classes == null) return null;
      String s = "";
      for (int i = 0; i < classes.length; i++) {
         if (i > 0) s += ",";
         s += classes[i];
      }
      return s;
   }

}
