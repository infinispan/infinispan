package org.infinispan.rest.dataconversion;

import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "rest.TextObjectTranscoderTest")
public class TextObjectTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);

      transcoder = new DefaultTranscoder(TestingUtil.createProtoStreamMarshaller(RestTestSCI.INSTANCE));
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

}
