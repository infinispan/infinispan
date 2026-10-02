package org.infinispan.spring.common.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.spring.common.marshalling.SpringJavaSerializationMarshaller;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "spring.common.session.SpringJavaSerializationMarshallerTest")
public class SpringJavaSerializationMarshallerTest {

   public void testRoundTrip() throws Exception {
      ClassAllowList allowList = new ClassAllowList(Collections.emptyList());
      allowList.addRegexps("java.util\\..*", "java.lang\\..*");
      SpringJavaSerializationMarshaller marshaller = new SpringJavaSerializationMarshaller(allowList);
      String original = "test-value";
      byte[] bytes = marshaller.objectToByteBuffer(original);
      Object result = marshaller.objectFromByteBuffer(bytes);
      assertThat(result).isEqualTo(original);
   }

   public void testMediaType() {
      SpringJavaSerializationMarshaller marshaller = new SpringJavaSerializationMarshaller();
      assertThat(marshaller.mediaType()).isEqualTo(MediaType.APPLICATION_SERIALIZED_OBJECT);
   }

   public void testIsMarshallable() {
      SpringJavaSerializationMarshaller marshaller = new SpringJavaSerializationMarshaller();
      assertThat(marshaller.isMarshallable("hello")).isTrue();
   }
}
