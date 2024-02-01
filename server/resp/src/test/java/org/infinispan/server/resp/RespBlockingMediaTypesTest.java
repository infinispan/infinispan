package org.infinispan.server.resp;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.resp.RespBlockingMediaTypesTest")
public class RespBlockingMediaTypesTest extends RespBxPOPTest {

   private MediaType valueType;

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.encoding()
            .key().mediaType(MediaType.APPLICATION_OCTET_STREAM)
            .encoding()
            .value().mediaType(valueType.toString());
   }

   private RespBlockingMediaTypesTest withValueType(MediaType type) {
      this.valueType = type;
      return this;
   }

   @Override
   public Object[] factory() {
      List<RespBlockingMediaTypesTest> instances = new ArrayList<>();
      MediaType[] types = new MediaType[] {
            MediaType.APPLICATION_PROTOSTREAM,
            MediaType.APPLICATION_OCTET_STREAM,
            MediaType.APPLICATION_OBJECT,
            MediaType.TEXT_PLAIN,
      };
      for (MediaType value : types) {
         instances.add(new RespBlockingMediaTypesTest().withValueType(value));
      }
      return instances.toArray();
   }

   @Override
   protected String parameters() {
      return super.parameters() + " + " + "[value=" + valueType + "]";
   }
}
