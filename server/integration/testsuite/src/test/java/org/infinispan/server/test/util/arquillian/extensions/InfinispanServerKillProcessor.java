package org.infinispan.server.test.util.arquillian.extensions;

import org.jboss.arquillian.container.spi.Container;
import org.jboss.arquillian.container.spi.ServerKillProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Logger;

/**
 * Handles calls to  {@link org.jboss.arquillian.container.test.api.ContainerController#kill(String)}
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:tsykora@redhat.com">Tomas Sykora</a>
 */
public class InfinispanServerKillProcessor implements ServerKillProcessor {

    private final Logger log = Logger.getLogger(InfinispanServerKillProcessor.class.getName());

    private static final long TIMEOUT = 1000;

    public void kill(Container container) throws Exception {

        String osName = System.getProperty("os.name", "");
        String killSequence = null;
        Process p = null;

        if (osName.startsWith("Linux")) {
            killSequence = "kill -9 `ps aux | grep -v 'grep' | grep 'jboss.home.dir=[jbossHome] ' | sed -re '1,$s/[ \\t]+/ /g' | cut -d ' ' -f 2`";
            killSequence = killSequence.replace("[jbossHome]", container.getContainerConfiguration().getContainerProperties()
                    .get("jbossHome"));
            p = Runtime.getRuntime().exec(new String[]{"sh", "-c", killSequence});
            executeKill(p, killSequence);
        } else if (osName.startsWith("Mac OS X")) {
            killSequence = "ps aux | grep -v grep | grep 'jboss.home.dir=[jbossHome]' | awk '{ print $2 }' |xargs kill -9";
            killSequence = killSequence.replace("[jbossHome]", container.getContainerConfiguration().getContainerProperties()
                    .get("jbossHome"));
            p = Runtime.getRuntime().exec(new String[]{"sh", "-c", killSequence});
            executeKill(p, killSequence);
        } else if (osName.startsWith("SunOS")) {
            killSequence = "jps | grep jboss-modules.jar | cut -f 1 -d ' ' | head -1 | xargs kill -9";
            p = Runtime.getRuntime().exec(new String[]{"sh", "-c", killSequence});
            executeKill(p, killSequence);
        } else if (osName.startsWith("HP-UX")) {
            // port-offset: node0=0, node0=100, node0=200
            String port = "8080";
            String vmArguments = container.getContainerConfiguration().getContainerProperties().get("javaVmArguments");
            if (vmArguments.contains("jboss.node.name=node1")) {
                port = "8180";
            }
            if (vmArguments.contains("jboss.node.name=node2")) {
                port = "8280";
            }
            killSequence = "lsof | grep '" + port + " (LISTEN)' | awk '{print $2}' | xargs kill -9";
            p = Runtime.getRuntime().exec(new String[]{"sh", "-c", killSequence});
            executeKill(p, killSequence);
        } else if (osName.startsWith("Windows")) {
            // port-offset: node0=0, node0=100, node0=200
            String port = "8080";
            String vmArguments = container.getContainerConfiguration().getContainerProperties().get("javaVmArguments");
            if (vmArguments.contains("jboss.node.name=node1")) {
                port = "8180";
            }
            if (vmArguments.contains("jboss.node.name=node2")) {
                port = "8280";
            }
            Process ptemp = Runtime.getRuntime().exec(
                    new String[]{"cmd", "/c", "netstat -aon | findstr LISTENING | findstr 127.0.0.1:" + port});
            ptemp.waitFor();
            log.info("Exit value of finding process (0 = ok, else num. unexpected): " + ptemp.exitValue());
            if (ptemp.exitValue() == 0) {
                String idForKill = getPidForKill(ptemp.getInputStream());
                log.info("Process ID for kill: " + idForKill + " (port: " + port + ")");
                killSequence = "taskkill /F /T /PID " + idForKill;
                p = Runtime.getRuntime().exec(new String[]{"cmd", "/c", killSequence});
                executeKill(p, killSequence);
            } else {
                throw new RuntimeException("Finding sequence failed => server not killed. (Exit value of finding process: "
                        + p.exitValue() + " OS=" + System.getProperty("os.name") + ")");
            }
        } else {
            throw new RuntimeException("System was probably NOT properly decided. Property os.name = "
                    + System.getProperty("os.name"));
        }
    }

    private void executeKill(Process p, String logKillSequence) throws Exception {
        log.info("Issuing kill sequence (on " + System.getProperty("os.name") + "): " + logKillSequence);
        p.waitFor();
        if (p.exitValue() != 0) {
            throw new RuntimeException("Kill sequence failed => server not killed. (Exit value of killing process: "
                    + p.exitValue() + " OS=" + System.getProperty("os.name") + ")");
        }
        Thread.sleep(TIMEOUT);
        log.info("Kill sequence successfully completed. \n");
    }

    /**
     * Get ID of process listening on 127.0.0.1:8080 (or 8180 or 8280). Used on Windows.
     * Note! This method is closing InputStream.
     */
    private String getPidForKill(InputStream is) throws IOException {
        String line = null;
        String lineForSplit = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        lineForSplit = in.readLine(); // returns null if end of stream was reached
        log.info("Getting process id from: " + lineForSplit);
        while ((line = in.readLine()) != null) {
            log.warning("There is another line here! : " + line);
        }
        in.close();
        // Expected: [white spaces] TCP 127.0.0.1:8080 0.0.0.0:0 LISTENING 3468
        if (lineForSplit != null) {
            String[] output = lineForSplit.split("\\s+");
            return output[5];
        } else {
            throw new RuntimeException(
                    "Finding sequence failed => server not killed. (Unexpected problem in reading process InputStream)");
        }
    }
}