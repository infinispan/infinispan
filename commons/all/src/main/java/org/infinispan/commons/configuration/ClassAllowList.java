package org.infinispan.commons.configuration;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.KeyValueWithPrevious;

/**
 * The {@link ClassAllowList} maintains classes definitions either by name or regular expression and is used for
 * permissioning.
 * <p>
 * By default, it includes regular expressions from the system property "infinispan.deserialization.allowlist.regexps"
 * and fully qualified class names from "infinispan.deserialization.allowlist.classes".
 * <p>
 * Classes are checked first against the set of class names, and in case not present each of the regular expressions are
 * evaluated in the order supplied.
 *
 * @since 9.4
 */
public class ClassAllowList {

   private static final Log log = LogFactory.getLog(ClassAllowList.class);

   private static final String CLASSES_PROPERTY_NAME = "infinispan.deserialization.allowlist.classes";
   private static final String REGEXPS_PROPERTY_NAME = "infinispan.deserialization.allowlist.regexps";

   private static final Set<String> SYS_ALLOWED_CLASSES = new HashSet<>();
   private static final List<String> SYS_ALLOWED_REGEXP = new ArrayList<>();
   private static final List<String> DENIED_PREFIXES = List.of(
         "bsh.",
         "clojure.",
         "com.mchange.",
         "com.sun.rowset.",
         "groovy.",
         "java.rmi.",
         "javax.management.remote.",
         "javax.naming.InitialContext",
         "ognl.",
         "org.apache.commons.collections.functors.",
         "org.apache.commons.collections4.functors.",
         "org.apache.xalan.",
         "org.codehaus.groovy.",
         "org.hibernate.property.access.",
         "org.mozilla.javascript.",
         "org.springframework.core.",
         "org.springframework.aop."
   );

   static {
      // Classes always allowed
      // Primitive Arrays
      SYS_ALLOWED_CLASSES.add(byte[].class.getName());
      SYS_ALLOWED_CLASSES.add(short[].class.getName());
      SYS_ALLOWED_CLASSES.add(int[].class.getName());
      SYS_ALLOWED_CLASSES.add(long[].class.getName());
      SYS_ALLOWED_CLASSES.add(float[].class.getName());
      SYS_ALLOWED_CLASSES.add(double[].class.getName());
      SYS_ALLOWED_CLASSES.add(char[].class.getName());
      SYS_ALLOWED_CLASSES.add(boolean[].class.getName());

      // Boxed Primitives
      SYS_ALLOWED_CLASSES.add(Byte.class.getName());
      SYS_ALLOWED_CLASSES.add(Short.class.getName());
      SYS_ALLOWED_CLASSES.add(Integer.class.getName());
      SYS_ALLOWED_CLASSES.add(Long.class.getName());
      SYS_ALLOWED_CLASSES.add(Float.class.getName());
      SYS_ALLOWED_CLASSES.add(Double.class.getName());
      SYS_ALLOWED_CLASSES.add(Character.class.getName());
      SYS_ALLOWED_CLASSES.add(String.class.getName());
      SYS_ALLOWED_CLASSES.add(Boolean.class.getName());

      // Java.math
      SYS_ALLOWED_CLASSES.add(BigInteger.class.getName());
      SYS_ALLOWED_CLASSES.add(BigDecimal.class.getName());

      // Java.time
      SYS_ALLOWED_CLASSES.add(Instant.class.getName());
      SYS_ALLOWED_CLASSES.add("java.time.Ser");


      // Util
      SYS_ALLOWED_CLASSES.add(Date.class.getName());

      // Misc
      SYS_ALLOWED_CLASSES.add(Enum.class.getName());
      SYS_ALLOWED_CLASSES.add(Number.class.getName());

      // Array types are handled by stripping array prefixes in isSafeClass()

      // Infinispan classes
      // Used by client listeners
      SYS_ALLOWED_CLASSES.add(KeyValueWithPrevious.class.getName());
      String regexps = System.getProperty(REGEXPS_PROPERTY_NAME);
      if (regexps != null) {
         SYS_ALLOWED_REGEXP.addAll(asList(regexps.trim().split(",")));
      }
      String cls = System.getProperty(CLASSES_PROPERTY_NAME);
      if (cls != null) {
         SYS_ALLOWED_CLASSES.addAll(asList(cls.trim().split(",")));
      }
   }

   private final Set<String> classes = new CopyOnWriteArraySet<>(SYS_ALLOWED_CLASSES);
   private final List<String> regexps = new CopyOnWriteArrayList<>(SYS_ALLOWED_REGEXP);
   private final List<Pattern> compiled = new CopyOnWriteArrayList<>();
   private final ClassLoader classLoader;

   public ClassAllowList() {
      this(Collections.emptySet(), Collections.emptyList(), null);
   }

   public ClassAllowList(List<String> regexps) {
      this(Collections.emptySet(), regexps, null);
   }

   public ClassAllowList(Collection<String> classes, List<String> regexps, ClassLoader classLoader) {
      Collection<String> classList = requireNonNull(classes, "Classes must not be null");
      Collection<String> regexList = requireNonNull(regexps, "Regexps must not be null");
      this.classes.addAll(classList);
      this.regexps.addAll(regexList);
      this.compiled.addAll(this.regexps.stream().map(Pattern::compile).toList());
      this.classLoader = classLoader;
   }

   public boolean isSafeClass(String className) {
      String componentName = stripArrayPrefix(className);
      if (componentName != null) {
         return isSafeClass(componentName);
      }
      if (isDenied(className)) {
         log.tracef("Class '%s' matches deserialization deny-list", className);
         return false;
      }
      if (classes.contains(className)) return true;
      boolean regexMatch = compiled.stream().anyMatch(p -> p.matcher(className).matches());
      if (regexMatch) {
         classes.add(className);
         return true;
      }
      if (log.isTraceEnabled())
         log.tracef("Class '%s' not in allowlist", className);
      return false;
   }

   private static String stripArrayPrefix(String className) {
      if (className.startsWith("[")) {
         String inner = className.substring(1);
         if (inner.startsWith("[")) {
            return inner;
         }
         if (inner.startsWith("L") && inner.endsWith(";")) {
            return inner.substring(1, inner.length() - 1);
         }
      }
      return null;
   }

   private static boolean isDenied(String className) {
      for (String prefix : DENIED_PREFIXES) {
         if (className.startsWith(prefix)) return true;
      }
      return false;
   }

   public void addClasses(Class<?>... classes) {
      stream(classes).forEach(c -> this.classes.add(c.getName()));
   }

   public void addClasses(String... classes) {
      this.classes.addAll(Arrays.asList(classes));
   }

   public void addRegexps(String... regexps) {
      this.regexps.addAll(asList(regexps));
      this.compiled.addAll(stream(regexps).map(Pattern::compile).toList());
   }

   public void read(ClassAllowList allowList) {
      this.regexps.addAll(allowList.regexps);
      this.compiled.addAll(allowList.compiled);
      this.classes.addAll(allowList.classes);
   }

   public ClassLoader getClassLoader() {
      return classLoader;
   }
}
