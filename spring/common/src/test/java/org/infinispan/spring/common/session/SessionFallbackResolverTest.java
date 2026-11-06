package org.infinispan.spring.common.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.infinispan.spring.common.marshalling.SpringJavaSerializationMarshaller;
import org.testng.annotations.Test;

@Test(testName = "spring.common.session.SessionFallbackResolverTest", groups = "unit")
public class SessionFallbackResolverTest {

   public void testResolveNull() {
      assertThat(SessionFallbackResolver.resolve(null, getClass().getClassLoader()))
            .isInstanceOf(SpringJavaSerializationMarshaller.class);
   }

   public void testResolveEmpty() {
      assertThat(SessionFallbackResolver.resolve("", getClass().getClassLoader()))
            .isInstanceOf(SpringJavaSerializationMarshaller.class);
   }

   public void testResolveJava() {
      assertThat(SessionFallbackResolver.resolve("java", getClass().getClassLoader()))
            .isInstanceOf(SpringJavaSerializationMarshaller.class);
   }

   public void testResolveInvalidClassName() {
      assertThatThrownBy(() -> SessionFallbackResolver.resolve("com.example.NonExistentMarshaller", getClass().getClassLoader()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot instantiate session fallback serializer class");
   }
}
