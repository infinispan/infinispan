package org.infinispan.config.parsing;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.marshall.MarshallerImpl;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.testng.annotations.Test;
import org.w3c.dom.Element;


@Test(groups = "unit", testName = "config.parsing.GlobalConfigurationParserTest")
public class GlobalConfigurationParserTest {

   public void testTransport() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String transportClass = "org.blah.Blah";
      String xml = "<transport transportClass=\"" + transportClass + "\"><property name=\"something\" value=\"value\"/></transport>";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureTransport(e, gc);

      assert gc.getTransportClass().equals(transportClass);
      assert gc.getTransportProperties().size() == 1;
      assert gc.getTransportProperties().getProperty("something").equals("value");
   }

   public void testDefaultTransport() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<transport />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureTransport(e, gc);

      assert gc.getTransportClass().equals(JGroupsTransport.class.getName());
      assert gc.getTransportProperties().size() == 0;
   }

   public void testGlobalJmxStatistics() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<globalJmxStatistics enabled=\"true\" jmxDomain=\"horizons\" mBeanServerLookup=\"org.infinispan.jmx.PerThreadMBeanServerLookup\" allowDuplicateDomains=\"true\"/>";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration c = new GlobalConfiguration();
      parser.configureGlobalJmxStatistics(e, c);

      assert c.isExposeGlobalJmxStatistics();
      assert c.getJmxDomain().equals("horizons");
      assert c.getMBeanServerLookup().equals("org.infinispan.jmx.PerThreadMBeanServerLookup");
      assert c.isAllowDuplicateDomains();
   }

   public void testShutdown() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<shutdown hookBehavior=\"REGISTER\" />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureShutdown(e, gc);

      assert gc.getShutdownHookBehavior() == GlobalConfiguration.ShutdownHookBehavior.REGISTER;
   }

   public void testDefaultShutdown() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<shutdown />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureShutdown(e, gc);

      assert gc.getShutdownHookBehavior() == GlobalConfiguration.ShutdownHookBehavior.DEFAULT;
   }

   public void testMarshalling() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<serialization marshallerClass=\"org.infinispan.marshall.MarshallerImpl\" version=\"9.2\"\n" +
            "                     objectInputStreamPoolSize=\"100\" objectOutputStreamPoolSize=\"100\"/>";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureSerialization(e, gc);

      assert gc.getMarshallerClass().equals(MarshallerImpl.class.getName());
      assert gc.getMarshallVersionString().equals("9.2");
      assert gc.getObjectInputStreamPoolSize() == 100;
      assert gc.getObjectOutputStreamPoolSize() == 100;
   }

   public void testMarshallingDefaults() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<serialization />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureSerialization(e, gc);

      assert gc.getMarshallerClass().equals(VersionAwareMarshaller.class.getName());
      assert gc.getMarshallVersionString().equals("4.0");
      assert gc.getObjectInputStreamPoolSize() == 50;
      assert gc.getObjectOutputStreamPoolSize() == 50;
   }

   public void testAsyncListenerExecutor() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<asyncListenerExecutor factory=\"com.mycompany.Factory\">\n" +
            "         <property name=\"maxThreads\" value=\"5\" />" +
            "      </asyncListenerExecutor>";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureAsyncListenerExecutor(e, gc);

      assert gc.getAsyncListenerExecutorFactoryClass().equals("com.mycompany.Factory");
      assert gc.getAsyncListenerExecutorProperties().size() == 1;
      assert gc.getAsyncListenerExecutorProperties().get("maxThreads").equals("5");
   }

   public void testAsyncSerializationExecutor() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<asyncSerializationExecutor factory=\"com.mycompany.Factory\">\n" +
            "         <property name=\"maxThreads\" value=\"5\" />" +
            "      </asyncSerializationExecutor>";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureAsyncSerializationExecutor(e, gc);

      assert gc.getAsyncSerializationExecutorFactoryClass().equals("com.mycompany.Factory");
      assert gc.getAsyncSerializationExecutorProperties().size() == 1;
      assert gc.getAsyncSerializationExecutorProperties().get("maxThreads").equals("5");
   }

   public void testEvictionScheduledExecutor() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<evictionScheduledExecutor factory=\"com.mycompany.Factory\">\n" +
            "         <property name=\"maxThreads\" value=\"5\" />" +
            "      </evictionScheduledExecutor>";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureEvictionScheduledExecutor(e, gc);

      assert gc.getEvictionScheduledExecutorFactoryClass().equals("com.mycompany.Factory");
      assert gc.getEvictionScheduledExecutorProperties().size() == 1;
      assert gc.getEvictionScheduledExecutorProperties().get("maxThreads").equals("5");
   }

   public void testReplicationQueueScheduledExecutor() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<replicationQueueScheduledExecutor factory=\"com.mycompany.Factory\">\n" +
            "         <property name=\"maxThreads\" value=\"5\" />" +
            "      </replicationQueueScheduledExecutor>";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureReplicationQueueScheduledExecutor(e, gc);

      assert gc.getReplicationQueueScheduledExecutorFactoryClass().equals("com.mycompany.Factory");
      assert gc.getReplicationQueueScheduledExecutorProperties().size() == 1;
      assert gc.getReplicationQueueScheduledExecutorProperties().get("maxThreads").equals("5");
   }

   public void testAsyncListenerExecutorDefaults() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<asyncListenerExecutor />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureAsyncListenerExecutor(e, gc);

      assert gc.getAsyncListenerExecutorFactoryClass().equals(DefaultExecutorFactory.class.getName());
      assert gc.getAsyncListenerExecutorProperties().size() == 0;
   }

   public void testAsyncSerializationExecutorDefaults() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<asyncSerializationExecutor />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureAsyncSerializationExecutor(e, gc);

      assert gc.getAsyncSerializationExecutorFactoryClass().equals(DefaultExecutorFactory.class.getName());
      assert gc.getAsyncSerializationExecutorProperties().size() == 0;
   }

   public void testEvictionScheduledExecutorDefaults() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<evictionScheduledExecutor />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureEvictionScheduledExecutor(e, gc);

      assert gc.getEvictionScheduledExecutorFactoryClass().equals(DefaultScheduledExecutorFactory.class.getName());
      assert gc.getEvictionScheduledExecutorProperties().size() == 0;
   }

   public void testReplicationQueueScheduledExecutorDefaults() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml = "<replicationQueueScheduledExecutor />";
      Element e = XmlConfigHelper.stringToElement(xml);

      GlobalConfiguration gc = new GlobalConfiguration();
      parser.configureReplicationQueueScheduledExecutor(e, gc);

      assert gc.getReplicationQueueScheduledExecutorFactoryClass().equals(DefaultScheduledExecutorFactory.class.getName());
      assert gc.getReplicationQueueScheduledExecutorProperties().size() == 0;
   }
}
