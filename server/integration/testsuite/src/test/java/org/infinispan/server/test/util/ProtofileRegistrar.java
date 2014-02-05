package org.infinispan.server.test.util;

import java.io.IOException;
import java.io.InputStream;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.infinispan.commons.util.Util;

/**
 * Utility class for registering arbitrary .protobin file on the server.
 *
 * @author mgencur
 */
public class ProtofileRegistrar {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("There are 3 parameters required:\n" +
                    "1) path to a protofile\n" +
                    "2) server host\n" +
                    "3) server JMX port");
            System.exit(1);
        }

        final String PROTOFILE_PATH = args[0];
        final String SERVER_HOST = args[1];
        final int JMX_PORT = Integer.parseInt(args[2]);

        ProtofileRegistrar registrar = new ProtofileRegistrar();
        try {
            registrar.registerProtofile(PROTOFILE_PATH, SERVER_HOST, JMX_PORT);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void registerProtofile(String protofilePath, String host, int jmxPort) throws Exception {
        JMXConnector jmxConnector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:remoting-jmx://" + host + ":" + jmxPort));
        MBeanServerConnection jmxConnection = jmxConnector.getMBeanServerConnection();

        ObjectName objName = new ObjectName("jboss.infinispan:type=RemoteQuery,name="
                + ObjectName.quote("local") + ",component=ProtobufMetadataManager");

        //initialize client-side serialization context via JMX
        byte[] descriptor = readClasspathResource(protofilePath);
        jmxConnection.invoke(objName, "registerProtofile", new Object[]{descriptor}, new String[]{byte[].class.getName()});
        System.out.printf("Successfully registered protofile %s at %s/%d\n", protofilePath, host, jmxPort);
    }

    private byte[] readClasspathResource(String c) throws IOException {
        InputStream is = getClass().getResourceAsStream(c);
        try {
            return Util.readStream(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }
}
