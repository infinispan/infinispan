package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.util.logging.Log.CONFIG;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.infinispan.commons.util.FileLookupFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class BuiltinJGroupsChannelConfigurator extends FileJGroupsChannelConfigurator {

   public static final String TCP_STACK_NAME = "tcp";

   public static BuiltinJGroupsChannelConfigurator TCP(Properties properties) {
      return loadBuiltIn(TCP_STACK_NAME, "default-configs/default-jgroups-tcp.xml", properties);
   }

   public static BuiltinJGroupsChannelConfigurator UDP(Properties properties) {
      return loadBuiltIn("udp", "default-configs/default-jgroups-udp.xml", properties);
   }

   public static BuiltinJGroupsChannelConfigurator KUBERNETES(Properties properties) {
      return loadBuiltIn("kubernetes", "default-configs/default-jgroups-kubernetes.xml", properties);
   }

   public static BuiltinJGroupsChannelConfigurator EC2(Properties properties) {
      return loadBuiltIn("ec2", "default-configs/default-jgroups-ec2.xml", properties);
   }

   public static BuiltinJGroupsChannelConfigurator GOOGLE(Properties properties) {
      return loadBuiltIn("google", "default-configs/default-jgroups-google.xml", properties);
   }

   public static BuiltinJGroupsChannelConfigurator AZURE(Properties properties) {
      return loadBuiltIn("azure", "default-configs/default-jgroups-azure.xml", properties);
   }

   private static BuiltinJGroupsChannelConfigurator loadBuiltIn(String name, String path, Properties properties) {
      try (InputStream xml = FileLookupFactory.newInstance().lookupFileStrict(path, BuiltinJGroupsChannelConfigurator.class.getClassLoader())) {
         return new BuiltinJGroupsChannelConfigurator(name, path, xml, properties);
      } catch (IOException e) {
         throw CONFIG.jgroupsConfigurationNotFound(path);
      }
   }

   BuiltinJGroupsChannelConfigurator(String name, String path, InputStream is, Properties properties) throws IOException {
      super(name, path, is, properties);
   }
}
