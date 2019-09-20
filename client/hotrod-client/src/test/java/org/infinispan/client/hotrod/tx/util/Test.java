package org.infinispan.client.hotrod.tx.util;

import java.util.regex.Pattern;

import org.infinispan.commons.configuration.ClassWhiteList;

/**
 * // TODO: Document this
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class Test {
   public static void main(String[] args) {
      String name = Object[].class.getName();
      String pattern = "\\Q[\\ELjava.lang.Object;";
      ClassWhiteList wl = new ClassWhiteList();
      wl.addRegexps(pattern);
      System.err.println(Pattern.compile(pattern).matcher(name).find());
      System.err.println(wl.isSafeClass(name));
   }
}
