package org.infinispan.objectfilter.impl.util;

import org.hibernate.hql.internal.util.Strings;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class StringHelper {

   private StringHelper() {
   }

   public static String join(String[] array, String separator) {
      return Strings.join(array, separator);
   }

   public static String join(Iterable<String> iterable, String separator) {
      return Strings.join(iterable, separator);
   }

   public static List<String> splitPropertyPath(String propertyPath) {
      List<String> pathAsList = new LinkedList<>();
      Collections.addAll(pathAsList, propertyPath.split("[.]"));
      return pathAsList;
   }
}
