package org.infinispan.server.websocket;

import org.infinispan.server.websocket.json.JsonObject;
import org.testng.annotations.Test;

import static org.infinispan.assertions.JsonPayloadAssertion.assertThat;
import static org.testng.Assert.fail;

/**
 * Tests Channel Utils class.
 *
 * @author Sebastian Laskawiec
 */
@Test(testName = "websocket.ChannelUtilsTest", groups = "unit")
public class ChannelUtilsTest {

   public void shouldReturnJsonObjectOnStringValue() throws Exception {
      //given
      String value = "new val";

      //when
      JsonObject jsonObject = ChannelUtils.toJSON("key", value, "cacheName");

      //then
      assertThat(jsonObject).hasValue("new val");
   }

   public void shouldReturnJsonObjectOnNumberValue() throws Exception {
      //given
      Integer value = 1;

      //when
      JsonObject jsonObject = ChannelUtils.toJSON("key", value, "cacheName");

      //then
      assertThat(jsonObject).hasValue(1);
   }

   public void shouldReturnJsonObjectOnCharacterValue() throws Exception {
      //given
      Character value = 'a';

      //when
      JsonObject jsonObject = ChannelUtils.toJSON("key", value, "cacheName");

      //then
      assertThat(jsonObject).hasValue('a');
   }

   public void shouldReturnJsonObjectOnCustomClassValue() throws Exception {
      //given
      class CustomValue {
         String field1;

         public String getField1() {
            return field1;
         }

         public void setField1(String field1) {
            this.field1 = field1;
         }
      }

      CustomValue value = new CustomValue();
      value.field1 = "value";

      //when
      JsonObject jsonObject = ChannelUtils.toJSON("key", value, "cacheName");

      //then
      assertThat(jsonObject).hasValue("{\"field1\":\"value\"}");
   }

   public void shouldFailOnSerializingCustomClassWithoutGetters() throws Exception {
      //given
      class CustomValue {
         String field1;
      }

      CustomValue value = new CustomValue();
      value.field1 = "value";

      //when //then
      try {
         ChannelUtils.toJSON("key", value, "cacheName");
         fail();
      } catch (IllegalStateException e) {
      }
   }
}