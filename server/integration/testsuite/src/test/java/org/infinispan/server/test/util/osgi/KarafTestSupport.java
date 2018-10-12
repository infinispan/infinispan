package org.infinispan.server.test.util.osgi;

import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.felix.service.command.CommandProcessor;
import org.apache.felix.service.command.CommandSession;
import org.apache.karaf.features.FeaturesService;
import org.junit.Assert;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;

/**
 * Class copied from JBoss Fuse project (FuseTestSupport class) and modified.
 */
public class KarafTestSupport {

    public static final Long DEFAULT_TIMEOUT = 30000L;
    public static final Long SYSTEM_TIMEOUT = 30000L;
    public static final Long DEFAULT_WAIT = 10000L;
    public static final Long PROVISION_TIMEOUT = 300000L;
    public static final Long COMMAND_TIMEOUT = 70000L;

    protected ExecutorService executor = Executors.newCachedThreadPool();

    @Inject
    protected BundleContext bundleContext;

    protected Bundle installBundle(String groupId, String artifactId) throws Exception {
        MavenArtifactProvisionOption mvnUrl = CoreOptions.mavenBundle(groupId, artifactId).versionAsInProject();
        return bundleContext.installBundle(mvnUrl.getURL());
    }

    protected Bundle getInstalledBundle(String symbolicName) {
        for (Bundle b : bundleContext.getBundles()) {
            if (b.getSymbolicName().equals(symbolicName)) {
                return b;
            }
        }
        for (Bundle b : bundleContext.getBundles()) {
            System.err.println("Bundle: " + b.getSymbolicName());
        }
        throw new RuntimeException("Bundle " + symbolicName + " does not exist");
    }

    /**
     * Make available system properties that are configured for the test, to the test container.
     * <p><b>Note:</b> If not obvious the container runs in in forked mode and thus system properties passed
     * form command line or surefire plugin are not available to the container without an approach like this.
     */
    public static Option copySystemProperty(String propertyName) {
        return KarafDistributionOption.editConfigurationFilePut("etc/system.properties", propertyName, System.getProperty(propertyName) != null ? System.getProperty(propertyName) : "");
    }

    /**
     * Create an provisioning option for the specified maven artifact
     * (groupId and artifactId), using the version found in the list
     * of dependencies of this maven project.
     *
     * @param groupId    the groupId of the maven bundle
     * @param artifactId the artifactId of the maven bundle
     * @return the provisioning option for the given bundle
     */
    protected static MavenArtifactProvisionOption mavenBundle(String groupId, String artifactId) {
        return CoreOptions.mavenBundle(groupId, artifactId).versionAsInProject();
    }

    /**
     * Create an provisioning option for the specified maven artifact
     * (groupId and artifactId), using the version found in the list
     * of dependencies of this maven project.
     *
     * @param groupId    the groupId of the maven bundle
     * @param artifactId the artifactId of the maven bundle
     * @param version    the version of the maven bundle
     * @return the provisioning option for the given bundle
     */
    protected static MavenArtifactProvisionOption mavenBundle(String groupId, String artifactId, String version) {
        return CoreOptions.mavenBundle(groupId, artifactId).version(version);
    }

    /**
     * Executes a shell command and returns output as a String.
     * Commands have a default timeout of 10 seconds.
     */
    protected String executeCommand(final String command) {
        return executeCommands(COMMAND_TIMEOUT, false, command);
    }

    /**
     * Executes a shell command and returns output as a String.
     * Commands have a default timeout of 10 seconds.
     */
    protected String executeCommands(final String... commands) {
        return executeCommands(COMMAND_TIMEOUT, false, commands);
    }

    /**
     * Executes a shell command and returns output as a String.
     * Commands have a default timeout of 10 seconds.
     * @param command The command to execute.
     * @param timeout The amount of time in millis to wait for the command to execute.
     * @param silent  Specifies if the command should be displayed in the screen.
     */
    protected String executeCommand(final String command, final long timeout, final boolean silent) {
        return executeCommands(timeout, silent, command);
    }

    /**
    * Executes a shell command and returns output as a String.
    * Commands have a default timeout of 10 seconds.
    * @param timeout The amount of time in millis to wait for the command to execute.
    * @param silent  Specifies if the command should be displayed in the screen.
    * @param commands The command to execute.
    */
    protected String executeCommands(final long timeout, final boolean silent, final String... commands) {
        String response = null;
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(byteArrayOutputStream);
        final CommandProcessor commandProcessor = ServiceLocator.getOsgiService(CommandProcessor.class);
        final CommandSession commandSession = commandProcessor.createSession(System.in, printStream, printStream);
        commandSession.put("APPLICATION", System.getProperty("karaf.name", "root"));
        commandSession.put("USER", "karaf");
        FutureTask<String> commandFuture = new FutureTask<String>(new Callable<String>() {
            public String call() throws Exception {
                for (String command : commands) {
                    boolean keepRunning = true;

                    if (!silent) {
                        System.out.println(command);
                        System.out.flush();
                    }

                    while (!Thread.currentThread().isInterrupted() && keepRunning) {
                        try {
                            commandSession.execute(command);
                            keepRunning = false;
                        } catch (Exception e) {
                            if (retryException(e)) {
                                keepRunning = true;
                                sleep(1000);
                            } else {
                                throw new CommandExecutionException(e);
                            }
                        }
                    }
                }
                printStream.flush();
                return byteArrayOutputStream.toString();
            }

        });

        try {
            executor.submit(commandFuture);
            response = commandFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw CommandExecutionException.launderThrowable(e.getCause());
        } catch (Exception e) {
            throw CommandExecutionException.launderThrowable(e);
        }

        return response;
    }

    private static boolean retryException(Exception e) {
        //The gogo runtime package is not exported, so we are just checking against the class name.
        return e.getClass().getName().equals("org.apache.felix.gogo.runtime.CommandNotFoundException");
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Installs a feature and checks that feature is properly installed.
     */
    public void installAndCheckFeature(String feature, String featureUrl) throws Exception {
        System.err.println(executeCommand("features:addUrl " + featureUrl));
        System.err.println(executeCommand("features:install " + feature));
        FeaturesService featuresService = ServiceLocator.getOsgiService(FeaturesService.class);
        System.err.println(executeCommand("osgi:list -t 0"));
        Assert.assertTrue("Expected " + feature + " feature to be installed.", featuresService.isInstalled(featuresService.getFeature(feature)));
    }

    /**
     * Uninstalls a feature and checks that feature is properly uninstalled.
     */
    public void unInstallAndCheckFeature(String feature) throws Exception {
        System.err.println(executeCommand("features:uninstall " + feature));
        FeaturesService featuresService = ServiceLocator.getOsgiService(FeaturesService.class);
        System.err.println(executeCommand("osgi:list -t 0"));
        Assert.assertFalse("Expected " + feature + " feature to be installed.", featuresService.isInstalled(featuresService.getFeature(feature)));
    }

    /**
     * This is used to customize the Probe that will contain the test.
     * We need to enable dynamic import of provisional bundles, to use the Console.
     */
    @ProbeBuilder
    public TestProbeBuilder probeConfiguration(TestProbeBuilder probe) {
        probe.setHeader(Constants.DYNAMICIMPORT_PACKAGE, "*,org.apache.felix.service.*;status=provisional");
        probe.setHeader(Constants.EXPORT_PACKAGE, "org.infinispan.server.test.client.hotrod.osgi");
        return probe;
    }

    /**
     * If custom Maven local repositories are used PAX URL needs to know about it.
     *
     * This method will return a composit option with the settings required for PAX EXAM to find
     * the custom Maven local repo.
     */
    public static Option localRepoForPAXUrl() throws Exception {
        String localRepo = System.getProperty("localRepository");

        if (localRepo == null) {
           return null;
        }

        return composite(systemProperty("org.ops4j.pax.url.mvn.localRepository").value(localRepo),
                         editConfigurationFilePut("etc/org.ops4j.pax.url.mvn.cfg", "org.ops4j.pax.url.mvn.localRepository", localRepo));
    }
}
