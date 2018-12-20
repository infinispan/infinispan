package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.infinispan.commons.util.FileLookupFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class BuiltinJGroupsChannelConfigurator extends FileJGroupsChannelConfigurator {

   public static BuiltinJGroupsChannelConfigurator TCP(Properties properties) {
      return loadBuiltIn("tcp", "default-configs/default-jgroups-tcp.xml", properties);
   }

   public static BuiltinJGroupsChannelConfigurator UDP(Properties properties) {
      return loadBuiltIn("udp", "default-configs/default-jgroups-udp.xml", properties);
   }

   private static BuiltinJGroupsChannelConfigurator loadBuiltIn(String name, String path, Properties properties) {
      try (InputStream xml = FileLookupFactory.newInstance().lookupFileStrict(path, BuiltinJGroupsChannelConfigurator.class.getClassLoader())) {
         return new BuiltinJGroupsChannelConfigurator(name, path, xml, properties);
      } catch (IOException e) {
         throw JGroupsTransport.log.jgroupsConfigurationNotFound(path);
      }
   }

   BuiltinJGroupsChannelConfigurator(String name, String path, InputStream is, Properties properties) throws IOException {
      super(name, path, is, properties);
   }
}
