package org.infinispan.commons.configuration;

import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClassAllowList}.
 */
public class ClassAllowListTest {

   @Test
   public void testSystemAllowedClasses() {
      ClassAllowList allowList = new ClassAllowList();
      // boxed primitives and String
      assertTrue(allowList.isSafeClass("java.lang.String"));
      assertTrue(allowList.isSafeClass("java.lang.Boolean"));
      assertTrue(allowList.isSafeClass("java.lang.Byte"));
      assertTrue(allowList.isSafeClass("java.lang.Short"));
      assertTrue(allowList.isSafeClass("java.lang.Integer"));
      assertTrue(allowList.isSafeClass("java.lang.Long"));
      assertTrue(allowList.isSafeClass("java.lang.Float"));
      assertTrue(allowList.isSafeClass("java.lang.Double"));
      assertTrue(allowList.isSafeClass("java.lang.Character"));
      // java.math
      assertTrue(allowList.isSafeClass("java.math.BigInteger"));
      assertTrue(allowList.isSafeClass("java.math.BigDecimal"));
      // java.time
      assertTrue(allowList.isSafeClass("java.time.Instant"));
      assertTrue(allowList.isSafeClass("java.time.Ser"));
      // java.util
      assertTrue(allowList.isSafeClass("java.util.Date"));
      // misc
      assertTrue(allowList.isSafeClass("java.lang.Enum"));
      assertTrue(allowList.isSafeClass("java.lang.Number"));
      // primitive arrays are allowed by name, without array stripping involved
      assertTrue(allowList.isSafeClass("[B"));
      assertTrue(allowList.isSafeClass("[S"));
      assertTrue(allowList.isSafeClass("[C"));
      assertTrue(allowList.isSafeClass("[Z"));
      assertTrue(allowList.isSafeClass("[I"));
      assertTrue(allowList.isSafeClass("[J"));
      assertTrue(allowList.isSafeClass("[F"));
      assertTrue(allowList.isSafeClass("[D"));
      // Infinispan classes used by client listeners
      assertTrue(allowList.isSafeClass("org.infinispan.commons.util.KeyValueWithPrevious"));
   }

   @Test
   public void testMultiDimensionalPrimitiveArrays() {
      ClassAllowList defaults = new ClassAllowList();
      // each prefix level is stripped until the primitive array name remains
      assertTrue(defaults.isSafeClass("[[I"));
      assertTrue(defaults.isSafeClass("[[B"));
      assertTrue(defaults.isSafeClass("[[[I"));
      assertTrue(defaults.isSafeClass("[[[[D"));
   }

   @Test
   public void testDenyListBeatsWildcardRegexp() {
      ClassAllowList allowList = new ClassAllowList(of(".*"));
      assertFalse(allowList.isSafeClass("org.apache.commons.collections.functors.ChainedTransformer"));
      assertFalse(allowList.isSafeClass("bsh.Interpreter"));
   }

   @Test
   public void testDenyListBeatsExplicitlyAddedClasses() {
      ClassAllowList allowList = new ClassAllowList();
      allowList.addClasses("org.apache.commons.collections.functors.ChainedTransformer");
      assertFalse(allowList.isSafeClass("org.apache.commons.collections.functors.ChainedTransformer"));
   }

   @Test
   public void testRegexpMustMatchEntireClassName() {
      ClassAllowList allowList = new ClassAllowList(of("com\\.foo\\..*"));
      assertTrue(allowList.isSafeClass("com.foo.Bar"));
      // a partial substring match must not be sufficient (matches(), not find())
      assertFalse(allowList.isSafeClass("malicious.com.foo.Bar"));
   }

   @Test
   public void testReferenceArraysAreCheckedByComponentType() {
      ClassAllowList allowList = new ClassAllowList(of("org\\.example\\..*"));
      assertTrue(allowList.isSafeClass("[Lorg.example.Foo;"));
      // multi-dimensional arrays strip one prefix level at a time, to any depth
      assertTrue(allowList.isSafeClass("[[Lorg.example.Foo;"));
      assertTrue(allowList.isSafeClass("[[[Lorg.example.Foo;"));
      assertFalse(allowList.isSafeClass("[Ljava.lang.Object;"));
      // partial prefix match on the component type must be rejected
      assertFalse(allowList.isSafeClass("[Lmalicious.org.example.Foo;"));

      // system-allowed component types work without any user configuration
      ClassAllowList defaults = new ClassAllowList();
      assertTrue(defaults.isSafeClass("[[Ljava.lang.String;"));
   }

   @Test
   public void testDeniedClassesAreRejectedAsArrayComponents() {
      ClassAllowList allowList = new ClassAllowList(of(".*"));
      assertFalse(allowList.isSafeClass("[Lorg.apache.commons.collections.functors.ChainedTransformer;"));
      assertFalse(allowList.isSafeClass("[[Ljava.rmi.ServerStub;"));
   }

   @Test
   public void testInvalidRegexpIsRejected() {
      // sanity check that user regexps are compiled eagerly, not per lookup
      assertThrows(IllegalArgumentException.class,
            () -> new ClassAllowList(Collections.emptyList(), List.of("[invalid"), (ClassLoader) null));
   }

   @Test
   public void testReadMergesExplicitClassesAndRegexps() {
      ClassAllowList source = new ClassAllowList(of("com\\.foo\\..*"));
      source.addClasses("com.bar.Baz");

      ClassAllowList target = new ClassAllowList();
      target.read(source);

      // explicitly added classes are merged
      assertTrue(target.isSafeClass("com.bar.Baz"));
      // the regexp itself is merged, so new matches are safe on the target
      assertTrue(target.isSafeClass("com.foo.Qux"));
   }

   @Test
   public void testAddClasses() {
      ClassAllowList allowList = new ClassAllowList();
      // not in the system allow-list, so this only passes if addClasses(Class...) works
      assertFalse(allowList.isSafeClass("java.util.ArrayList"));
      allowList.addClasses(java.util.ArrayList.class);
      assertTrue(allowList.isSafeClass("java.util.ArrayList"));

      // the String... overload must also allow a valid, non-denied class
      assertFalse(allowList.isSafeClass("com.example.MyService"));
      allowList.addClasses("com.example.MyService");
      assertTrue(allowList.isSafeClass("com.example.MyService"));
   }

   @Test
   public void testAddRegexps() {
      ClassAllowList allowList = new ClassAllowList();
      // not allowed before the regexp is added dynamically
      assertFalse(allowList.isSafeClass("org.mycustom.MyService"));
      allowList.addRegexps("org\\.mycustom\\..*");
      assertTrue(allowList.isSafeClass("org.mycustom.MyService"));
      // a non-matching class is still rejected
      assertFalse(allowList.isSafeClass("org.other.MyService"));
   }

   @Test
   public void testInnerClassNames() {
      // exact-name matching handles the $ inner-class notation
      ClassAllowList allowList = new ClassAllowList();
      assertFalse(allowList.isSafeClass("org.example.Outer$Inner"));
      allowList.addClasses("org.example.Outer$Inner");
      assertTrue(allowList.isSafeClass("org.example.Outer$Inner"));

      // regex matching handles inner and anonymous class notations
      ClassAllowList regexList = new ClassAllowList(of("org\\.example\\..*"));
      assertTrue(regexList.isSafeClass("org.example.Outer$Inner"));
      assertTrue(regexList.isSafeClass("org.example.Outer$1"));
   }

   @Test
   public void testMalformedArrayDescriptorsAreRejected() {
      ClassAllowList empty = new ClassAllowList(Collections.emptySet(), Collections.emptyList(), null);
      assertFalse(empty.isSafeClass("[L"));    // reference array missing the closing ';'
      assertFalse(empty.isSafeClass("["));     // bare opening bracket
      assertFalse(empty.isSafeClass("Lfoo;")); // reference array missing the leading '['
   }

   @Test
   public void testNullAndEmptyClassNames() {
      ClassAllowList allowList = new ClassAllowList();
      assertThrows(NullPointerException.class, () -> allowList.isSafeClass(null));
      assertFalse(allowList.isSafeClass(""));
   }

}
