package org.infinispan.spring.common.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.spring.common.marshalling.MarshallerResolver;
import org.infinispan.spring.common.marshalling.SpringJavaSerializationMarshaller;
import org.testng.annotations.Test;

@Test(testName = "spring.common.session.MarshallerResolverTest", groups = "unit")
public class MarshallerResolverTest {

   @Test
   public void testDefaultReturnsJavaSerialization() {
      assertThat(MarshallerResolver.resolve(null)).isInstanceOf(SpringJavaSerializationMarshaller.class);
   }

   @Test
   public void testEmptyStringReturnsJavaSerialization() {
      assertThat(MarshallerResolver.resolve("")).isInstanceOf(SpringJavaSerializationMarshaller.class);
   }

   @Test
   public void testJavaShortcut() {
      assertThat(MarshallerResolver.resolve("java")).isInstanceOf(SpringJavaSerializationMarshaller.class);
   }

   @Test
   public void testProtostreamShortcut() {
      assertThat(MarshallerResolver.resolve("protostream")).isInstanceOf(ProtoStreamMarshaller.class);
   }

   @Test
   public void testInvalidClassName() {
      assertThatThrownBy(() -> MarshallerResolver.resolve("com.nonexistent.FakeMarshaller"))
            .isInstanceOf(IllegalArgumentException.class);
   }

   @Test
   public void testValidFQCN() {
      assertThat(MarshallerResolver.resolve("org.infinispan.commons.marshall.ProtoStreamMarshaller"))
            .isInstanceOf(ProtoStreamMarshaller.class);
   }
}
