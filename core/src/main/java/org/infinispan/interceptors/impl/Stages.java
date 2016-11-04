package org.infinispan.interceptors.impl;

/**
 * @author Dan Berindei
 * @since 9.0
 */
class Stages {
   static String className(Object o) {
      if (o == null) return "null";

      String fullName = o.getClass().getName();
      return fullName.substring(fullName.lastIndexOf('.') + 1);
   }

}
