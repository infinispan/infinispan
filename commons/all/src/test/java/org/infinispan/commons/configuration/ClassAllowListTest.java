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
      assertTrue(allowList.isSafeClass("java.lang.String"));
      // primitive arrays are allowed by name, without array stripping involved
      assertTrue(allowList.isSafeClass("[B"));
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

      allowList.addRegexps(".*");
      assertFalse(allowList.isSafeClass("java.rmi.ServerStub"));
   }

   @Test
   public void testRegexpMustMatchEntireClassName() {
      ClassAllowList allowList = new ClassAllowList(of("com\\.foo\\..*"));
      assertTrue(allowList.isSafeClass("com.foo.Bar"));
      // a partial substring match must not be sufficient (matches(), not find())
      assertFalse(allowList.isSafeClass("malicious.com.foo.Bar"));

      allowList = new ClassAllowList(of("org\\.example\\..*"));
      assertTrue(allowList.isSafeClass("org.example.Foo"));
      assertFalse(allowList.isSafeClass("evil.org.example.Foo"));
   }

   @Test
   public void testReferenceArraysAreCheckedByComponentType() {
      ClassAllowList allowList = new ClassAllowList(of("org\\.example\\..*"));
      assertTrue(allowList.isSafeClass("[Lorg.example.Foo;"));
      // multi-dimensional arrays strip one prefix level at a time
      assertTrue(allowList.isSafeClass("[[Lorg.example.Foo;"));
      assertFalse(allowList.isSafeClass("[Ljava.lang.Object;"));

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
   public void testArrayOfNonAllowedReferenceTypeIsRejected() {
      // the previous implementation allowed every reference array via a single over-permissive regexp;
      // arrays must now be rejected unless their component type is explicitly safe
      ClassAllowList allowList = new ClassAllowList(Collections.emptySet(), Collections.emptyList(), null);
      assertFalse(allowList.isSafeClass("[Ljava.lang.Object;"));
   }

   @Test
   public void testInvalidRegexpIsRejected() {
      // sanity check that user regexps are compiled eagerly, not per lookup
      assertThrows(IllegalArgumentException.class,
            () -> new ClassAllowList(Collections.emptyList(), List.of("[invalid"), (ClassLoader) null));
   }

}
