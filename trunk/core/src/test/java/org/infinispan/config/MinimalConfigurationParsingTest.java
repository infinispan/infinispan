package org.infinispan.config;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@Test(groups = "unit", testName = "config.MinimalConfigurationParsingTest")
public class MinimalConfigurationParsingTest {

   public void testGlobalAndDefaultSection() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\">\n" +
            "    <global />\n" +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"10000\" isolationLevel=\"READ_COMMITTED\" />\n" +
            "    </default>\n" +
            "</infinispan>";
      testXml(xml);
   }

   public void testNoGlobalSection() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\">\n" +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"10000\" isolationLevel=\"READ_COMMITTED\" />\n" +
            "    </default>\n" +
            "</infinispan>";
      testXml(xml);
   }

   public void testNoDefaultSection() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\">\n" +
            "    <global />\n" +
            "</infinispan>";
      testXml(xml);
   }

   public void testNoSections() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\">\n" +
            "</infinispan>";
      testXml(xml);
   }

   private void testXml(String xml) throws IOException {
      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      InfinispanConfiguration ic = InfinispanConfiguration.newInfinispanConfiguration(stream);
      assert ic != null;
   }
}
