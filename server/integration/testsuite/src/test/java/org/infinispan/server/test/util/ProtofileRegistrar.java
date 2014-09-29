package org.infinispan.server.test.util;

import org.infinispan.commons.util.Util;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

/**
 * Utility class for registering arbitrary .proto files on the server.
 *
 * @author mgencur
 */
public class ProtofileRegistrar {

   public static void main(String[] args) {
      if (args.length != 3) {
         System.err.println("There are 3 parameters required:\n" +
                 "1) path to one or more proto classpath resources, comma separated \n" +
                 "2) server host\n" +
                 "3) server JMX port");
         System.exit(1);
      }

      final String protofilesPath = args[0];
      final String ser = args[1];
      final int jmxPort = Integer.parseInt(args[2]);

      ProtofileRegistrar registrar = new ProtofileRegistrar();
      try {
         registrar.registerProtofile(protofilesPath, ser, jmxPort);
      } catch (Exception e) {
         e.printStackTrace();
         System.exit(1);
      }
   }

   private void registerProtofile(String protofilesPath, String host, int jmxPort) throws Exception {
      JMXConnector jmxConnector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:http-remoting-jmx://" + host + ":" + jmxPort));
      MBeanServerConnection jmxConnection = jmxConnector.getMBeanServerConnection();

      ObjectName objName = new ObjectName("jboss.infinispan:type=RemoteQuery,name="
              + ObjectName.quote("clustered") + ",component=ProtobufMetadataManager");

      LinkedList<String> fileNames = new LinkedList<>();
      LinkedList<String> fileContents = new LinkedList<>();

      //initialize client-side serialization context via JMX
      for (String protofile : protofilesPath.split(",")) {
         String absPath = protofile.startsWith("/") ? protofile : "/" + protofile;
         String descriptor = readClasspathResource(absPath);
         String name = protofile.startsWith("/") ? protofile.substring(1) : protofile;
         fileNames.add(name);
         fileContents.add(descriptor);
      }
      String[] names = fileNames.toArray(new String[fileNames.size()]);
      String[] contents = fileContents.toArray(new String[fileContents.size()]);

      jmxConnection.invoke(objName, "registerProtofiles", new Object[]{names, contents}, new String[]{String[].class.getName(), String[].class.getName()});

      System.out.printf("Successfully registered protofile(s) %s at %s/%d\n", protofilesPath, host, jmxPort);
   }

   private String readClasspathResource(String c) throws IOException {
      InputStream is = getClass().getResourceAsStream(c);
      if (is == null) {
         throw new IOException("Resource not found in classpath : " + c);
      }
      return Util.read(is);
   }
}
