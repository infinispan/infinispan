package org.infinispan.query.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.config.Configuration.ModuleConfigurationBean;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "config.parsing.QueryParsingTest")
public class QueryParsingTest extends AbstractInfinispanTest {


   public void testQueryConfig() throws Exception {
      String config = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\" xmlns:query=\"urn:infinispan:config:query:4.0\">\n" +
            "   <global>\n" +
            "      <transport clusterName=\"demoCluster\"/>\n" +
            "   </global>\n" +
            "\n" +
            "   <default>\n" +
            "      <clustering mode=\"replication\">\n" +
            "      </clustering>\n" +
            "   <modules>\n" +
            "     <module name=\"query\">\n"+
            "           <query:indexing enabled=\"true\" indexLocalOnly=\"true\"/>\n" +
            "     </module>\n" +
            "   </modules>\n" +
            "   </default>\n" +
            "</infinispan>";
      
      System.out.println(config);

      InputStream is = new ByteArrayInputStream(config.getBytes());
      InputStream schema = InfinispanConfiguration.findSchemaInputStream();
      assert schema != null;
      InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(is,schema);
      GlobalConfiguration gc = c.parseGlobalConfiguration();
      assert gc.getTransportClass().equals(JGroupsTransport.class.getName());
      assert gc.getClusterName().equals("demoCluster");

      Configuration def = c.parseDefaultConfiguration();
      ModuleConfigurationBean extensionConfig = def.getModuleConfigurationBean("query");
      QueryConfigurationBean bean = (QueryConfigurationBean) extensionConfig.getConfigurationBean();
      assert bean.isEnabled();
      assert bean.isIndexLocalOnly();
   }
  }