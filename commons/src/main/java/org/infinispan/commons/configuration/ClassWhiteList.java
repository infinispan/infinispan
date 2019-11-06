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
import java.util.stream.Collectors;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * The {@link ClassWhiteList} maintains classes definitions either by name or regular expression and is used for
 * permissioning.
 * <p>
 * By default it includes regular expressions from the system property "infinispan.deserialization.whitelist.regexps"
 * and fully qualified class names from "infinispan.deserialization.whitelist.classes".
 * <p>
 * Classes are checked first against the set of class names, and in case not present each of the regular expressions are
 * evaluated in the order supplied.
 *
 * @since 9.4
 */
public final class ClassWhiteList {

   private static final Log log = LogFactory.getLog(ClassWhiteList.class);

   private static final String CLASSES_PROPERTY_NAME = "infinispan.deserialization.whitelist.classes";
   private static final String REGEXPS_PROPERTY_NAME = "infinispan.deserialization.whitelist.regexps";

   private static final Set<String> SYS_ALLOWED_CLASSES = new HashSet<>();
   private static final List<String> SYS_ALLOWED_REGEXP = new ArrayList<>();

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

      // Reference array regex, for arrray representations of allowed classes e.g '[Ljava.lang.Byte;'
      SYS_ALLOWED_REGEXP.add("^\\[[\\[L].*\\;$");

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

   public ClassWhiteList() {
      this(Collections.emptySet(), Collections.emptyList());
   }

   public ClassWhiteList(List<String> regexps) {
      this(Collections.emptySet(), regexps);
   }

   public ClassWhiteList(Collection<String> classes, List<String> regexps) {
      Collection<String> classList = requireNonNull(classes, "Classes must not be null");
      Collection<String> regexList = requireNonNull(regexps, "Regexps must not be null");
      this.classes.addAll(classList);
      this.regexps.addAll(regexList);
      this.compiled.addAll(this.regexps.stream().map(Pattern::compile).collect(Collectors.toList()));
   }

   public boolean isSafeClass(String className) {
      // Test for classes first (faster)
      boolean isClassAllowed = classes.contains(className);
      if (isClassAllowed) return true;
      boolean regexMatch = compiled.stream().anyMatch(p -> p.matcher(className).find());
      if (regexMatch) {
         // Add the class name to the classes set to avoid future regex checks
         classes.add(className);
         return true;
      }
      if (log.isTraceEnabled())
         log.tracef("Class '%s' not in whitelist", className);
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
      this.compiled.addAll(stream(regexps).map(Pattern::compile).collect(Collectors.toList()));
   }

   public void read(ClassWhiteList whiteList) {
      this.regexps.addAll(whiteList.regexps);
      this.compiled.addAll(whiteList.compiled);
      this.classes.addAll(whiteList.classes);
   }
}
