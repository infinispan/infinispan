package org.infinispan.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.tools.config.ConfigurationConverter;
import org.testng.annotations.Test;

@Test(testName = "tools.ConfigurationConverterTest", groups = "functional")
public class ConfigurationConverterTest {

   public void testConversionFrom60() throws Exception {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ConfigurationConverter.convert(ConfigurationConverterTest.class.getResourceAsStream("/6.0.xml"), baos);
      ParserRegistry pr = new ParserRegistry();
      pr.parse(new ByteArrayInputStream(baos.toByteArray()));
   }
}
