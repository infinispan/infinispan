package org.infinispan.commons.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

import org.junit.Test;

public class ClassFinderTest {

   @Test
   public void testFindWithAnnotations() {
      @ClassAnnotation class With { }
      @ClassAnnotation2 class With2 { }
      class Without { }

      assertThat(ClassFinder.withAnnotationPresent(List.of(With.class, With2.class, Without.class), ClassAnnotation.class))
            .hasSize(1)
            .contains(With.class);

      assertThat(ClassFinder.withAnnotationDeclared(List.of(With.class, With2.class, Without.class), ClassAnnotation.class))
            .hasSize(1)
            .contains(With.class);
   }

   @Test
   public void testIsAssignable() {
      class A { }
      class B extends A { }
      class C { }

      assertThat(ClassFinder.isAssignableFrom(List.of(A.class, B.class, C.class), A.class))
            .hasSize(2)
            .contains(A.class, B.class);

      assertThat(ClassFinder.isAssignableFrom(List.of(A.class, B.class, C.class), B.class))
            .hasSize(1)
            .contains(B.class);
   }

   @Test
   public void testInfinispanClasses() throws Throwable {
      assertThat(ClassFinder.infinispanClasses()).hasSizeGreaterThan(100);
      assertThat(ClassFinder.infinispanClasses("")).isEmpty();
   }

   @Target(ElementType.TYPE)
   @Retention(RetentionPolicy.RUNTIME)
   private @interface ClassAnnotation { }

   @Target(ElementType.TYPE)
   @Retention(RetentionPolicy.RUNTIME)
   private @interface ClassAnnotation2 { }
}
