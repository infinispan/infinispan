package org.infinispan.dataconversion.impl;

import java.util.Collections;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.encoding.impl.JavaSerializationTranscoder;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.JavaSerializationTranscoderTest")
public class JavaSerializationTranscoderTest extends AbstractTranscoderTest {
   protected Person dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = new Person("Joe");
      Address address = new Address();
      address.setCity("London");
      dataSrc.setAddress(address);
      transcoder = new JavaSerializationTranscoder(new ClassAllowList(Collections.singletonList(".*")));
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

}
