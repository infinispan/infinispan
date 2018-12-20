package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.util.StringPropertyReplacer;
import org.jgroups.JChannel;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.XmlConfigurator;

/**
 * A JGroups {@link JGroupsChannelConfigurator} which loads configuration from an XML file supplied as an {@link InputStream}
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class FileJGroupsChannelConfigurator implements JGroupsChannelConfigurator {
   private final String name;
   private final String path;
   final List<ProtocolConfiguration> stack;

   public FileJGroupsChannelConfigurator(String name, String path, InputStream is, Properties properties) throws IOException {
      this.name = name;
      this.path = path;
      this.stack = XmlConfigurator.getInstance(is).getProtocolStack();
      this.stack.forEach(c -> StringPropertyReplacer.replaceProperties(c.getProperties(), properties));
   }

   @Override
   public String getProtocolStackString() {
      return stack.toString();
   }

   @Override
   public List<ProtocolConfiguration> getProtocolStack() {
      return stack;
   }

   public String getName() {
      return name;
   }

   @Override
   public JChannel createChannel() throws Exception {
      return new JChannel(this);
   }

   public String getPath() {
      return path;
   }
}
