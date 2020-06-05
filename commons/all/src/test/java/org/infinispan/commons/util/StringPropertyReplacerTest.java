package org.infinispan.commons.util;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

public class StringPropertyReplacerTest {

   @Test
   public void testReplaceProperties() {
      Properties properties = new Properties();
      properties.put("one", "1");
      properties.put("two", "2");
      assertEquals("V1", StringPropertyReplacer.replaceProperties("V${one}", properties));
      assertEquals("VX", StringPropertyReplacer.replaceProperties("V${void:X}", properties));
      assertEquals("V1", StringPropertyReplacer.replaceProperties("V${void,one}", properties));
      assertEquals("VX", StringPropertyReplacer.replaceProperties("V${void1,void2:X}", properties));
      assertEquals(System.getenv("PATH"), StringPropertyReplacer.replaceProperties("${env.PATH}", properties));
      assertEquals(File.separator, StringPropertyReplacer.replaceProperties("${/}"));
      assertEquals(File.pathSeparator, StringPropertyReplacer.replaceProperties("${:}"));
   }
}
