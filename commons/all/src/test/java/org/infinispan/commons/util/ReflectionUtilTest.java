package org.infinispan.commons.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.infinispan.commons.CacheException;
import org.junit.Test;

public class ReflectionUtilTest {

   @Test
   public void testGetAllMethods() {
      class T {
         @MethodAnnotation
         public void a() { }

         @MethodAnnotation
         public void b() { }

         public void c() { }
      }

      assertThat(ReflectionUtil.getAllMethods(T.class, MethodAnnotation.class)).hasSize(2);
      assertThat(ReflectionUtil.getAllMethods(T.class, FieldAnnotation.class)).isEmpty();
   }

   @Test
   public void testGetValue() {
      class T {
         private String field;
      }

      T t = new T();
      assertThat(ReflectionUtil.getValue(t, "field")).isNull();

      t.field = "value";
      assertThat(ReflectionUtil.getValue(t, "field")).isEqualTo("value");

      assertThatThrownBy(() -> assertThat(ReflectionUtil.getValue(t, "something")))
            .isInstanceOf(CacheException.class);
   }

   @Test
   public void testGetAnnotation() {
      @ClassAnnotation
      class T { }

      assertThat(ReflectionUtil.getAnnotation(T.class, ClassAnnotation.class)).isNotNull();
      assertThat(ReflectionUtil.getAnnotation(T.class, FieldAnnotation.class)).isNull();
   }

   @Test
   public void testGetAnnotationSuper() {
      @ClassAnnotation
      class A {}
      class B extends A { }

      assertThat(ReflectionUtil.getAnnotation(A.class, ClassAnnotation.class)).isNotNull();
      assertThat(ReflectionUtil.isAnnotationPresent(A.class, ClassAnnotation.class)).isTrue();
      assertThat(ReflectionUtil.getAnnotation(B.class, ClassAnnotation.class)).isNotNull();
      assertThat(ReflectionUtil.isAnnotationPresent(B.class, ClassAnnotation.class)).isTrue();
      assertThat(ReflectionUtil.getAnnotation(B.class, FieldAnnotation.class)).isNull();
      assertThat(ReflectionUtil.isAnnotationPresent(B.class, FieldAnnotation.class)).isFalse();
   }

   @Test
   public void testGetAnnotationInterface() {
      @ClassAnnotation
      interface A { }
      class B implements A {}

      assertThat(ReflectionUtil.getAnnotation(A.class, ClassAnnotation.class)).isNotNull();
      assertThat(ReflectionUtil.isAnnotationPresent(A.class, ClassAnnotation.class)).isTrue();
      assertThat(ReflectionUtil.getAnnotation(B.class, ClassAnnotation.class)).isNotNull();
      assertThat(ReflectionUtil.isAnnotationPresent(B.class, ClassAnnotation.class)).isTrue();
      assertThat(ReflectionUtil.getAnnotation(B.class, FieldAnnotation.class)).isNull();
      assertThat(ReflectionUtil.isAnnotationPresent(B.class, FieldAnnotation.class)).isFalse();
   }

   @Test
   public void testGetClass() throws Throwable {
      class A { }

      assertThat(ReflectionUtil.getClassForName(A.class.getName(), A.class.getClassLoader())).isEqualTo(A.class);
      assertThatThrownBy(() -> ReflectionUtil.getClassForName("org.infinispan.commons.util.C", ReflectionUtil.class.getClassLoader()))
            .isInstanceOf(ClassNotFoundException.class);

      assertThat(ReflectionUtil.getClassForName(float.class.getName(), A.class.getClassLoader())).isEqualTo(float.class);
      assertThat(ReflectionUtil.getClassForName(byte[].class.getName(), A.class.getClassLoader())).isEqualTo(byte[].class);
   }

   @Test
   public void testSetGetField() {
      class A {
         String field;
         private String other;
      }

      Field field = ReflectionUtil.getField("field", A.class);
      assertThat(field).isNotNull();
      assertThat(field.getName()).isEqualTo("field");
      assertThat(field.getType()).isEqualTo(String.class);

      assertThat(ReflectionUtil.getField("unknown", A.class)).isNull();

      A a = new A();
      a.field = "before";
      ReflectionUtil.setField(a, field, "updated");
      assertThat(a.field).isEqualTo("updated");

      // Next, try with private fields.
      Field other = ReflectionUtil.getField("other", A.class);
      assertThat(other).isNotNull();
      a.other = "before";
      assertThatThrownBy(() -> ReflectionUtil.setField(a, other, "updated"))
            .isInstanceOf(CacheException.class);
   }

   @Test
   public void testUnwrap() {
      String value = "value";

      assertThat(ReflectionUtil.unwrap(value, String.class)).isEqualTo(value);

      class A {
         protected String field;
      }
      class B extends A { }

      B b = new B();
      b.field = value;
      assertThat(ReflectionUtil.unwrap(b, B.class)).isEqualTo(b);

      A a = ReflectionUtil.unwrap(b, A.class);
      assertThat(a).isNotNull()
            .satisfies(ignore -> assertThat(a).isEqualTo(b))
            .satisfies(ignore -> assertThat(a.field).isEqualTo(value));

      assertThat(ReflectionUtil.unwrapAny(String.class, a, b, value))
            .isEqualTo(value);

      class C {}
      assertThatThrownBy(() -> ReflectionUtil.unwrap(b, C.class))
            .isInstanceOf(IllegalArgumentException.class);

      assertThatThrownBy(() -> ReflectionUtil.unwrapAny(C.class, a, b, value))
            .isInstanceOf(IllegalArgumentException.class);
   }

   @Test
   public void testGetInvokeMethod() {
      class A {
         public void a() { }

         public int b(Integer x, Integer y) {
            return x + y;
         }
      }

      A a = new A();
      assertThat(ReflectionUtil.findMethod(A.class, "a")).isNotNull();

      Method add = ReflectionUtil.findMethod(A.class, "b", Integer.class, Integer.class);
      assertThat(add).isNotNull();
      assertThat(ReflectionUtil.invokeMethod(a, add, new Object[] { 1, 1 })).isEqualTo(2);
      assertThatThrownBy(() -> ReflectionUtil.invokeMethod(a, add, new Object[]{ 1, null }))
            .isInstanceOf(CacheException.class);
      assertThatThrownBy(() -> ReflectionUtil.invokeMethod(a, add, new Object[]{ "1", "1" }))
            .isInstanceOf(CacheException.class);

      assertThatThrownBy(() -> ReflectionUtil.findMethod(A.class, "unknown"))
            .isInstanceOf(CacheException.class);
      assertThatThrownBy(() -> ReflectionUtil.findMethod(A.class, "b", float.class, float.class))
            .isInstanceOf(CacheException.class);
   }

   @Test
   public void testGetterAndSetter() {
      class A {
         private String field;
         private boolean flag;

         public void setField(String field) {
            this.field = field;
         }

         public void setFlag(boolean flag) {
            this.flag = flag;
         }

         public String getField() {
            return field;
         }

         public boolean isFlag() {
            return flag;
         }
      }
      class B extends A { }

      Method setter = ReflectionUtil.findSetterForField(A.class, "field");
      assertThat(setter).isNotNull();

      Method getter = ReflectionUtil.findGetterForField(A.class, "field");
      assertThat(getter).isNotNull();

      assertThat(ReflectionUtil.findGetterForField(A.class, "flag")).isNotNull();
      assertThat(ReflectionUtil.findSetterForField(A.class, "flag")).isNotNull();

      assertThat(ReflectionUtil.findSetterForField(B.class, "field")).isNotNull();
      assertThat(ReflectionUtil.findGetterForField(B.class, "field")).isNotNull();

      assertThat(ReflectionUtil.findSetterForField(A.class, "unknown")).isNull();
      assertThat(ReflectionUtil.findSetterForField(B.class, "unknown")).isNull();
      assertThat(ReflectionUtil.findGetterForField(A.class, "unknown")).isNull();
      assertThat(ReflectionUtil.findGetterForField(B.class, "unknown")).isNull();
   }

   @Test
   public void testStringify() throws Throwable {
      Class<?>[] classes = new Class[]{int.class, String.class};
      String[] stringfied = ReflectionUtil.toStringArray(classes);
      assertThat(stringfied).hasSize(2).containsExactly("int", "java.lang.String");
      assertThat(ReflectionUtil.toStringArray(null)).isEmpty();

      assertThat(ReflectionUtil.extractFieldName("getField")).isEqualTo("field");
      assertThat(ReflectionUtil.extractFieldName("setField")).isEqualTo("field");
      assertThat(ReflectionUtil.extractFieldName("isFlag")).isEqualTo("flag");
      assertThat(ReflectionUtil.extractFieldName("flag")).isNull();

      Class<?>[] actual = ReflectionUtil.toClassArray(stringfied, ReflectionUtil.class.getClassLoader());
      assertThat(actual).isEqualTo(classes);
      assertThat(ReflectionUtil.toClassArray(null, ReflectionUtil.class.getClassLoader())).isEmpty();
   }

   @Target(ElementType.TYPE)
   @Retention(RetentionPolicy.RUNTIME)
   public @interface ClassAnnotation { }

   @Target(ElementType.METHOD)
   @Retention(RetentionPolicy.RUNTIME)
   public @interface MethodAnnotation { }

   @Target(ElementType.FIELD)
   @Retention(RetentionPolicy.RUNTIME)
   public @interface FieldAnnotation { }
}
