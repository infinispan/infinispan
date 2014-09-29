package org.infinispan.it.osgi.util;

import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.streamBundle;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.CoreOptions.vmOptions;
import static org.ops4j.pax.exam.CoreOptions.wrappedBundle;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.LogLevelOption.LogLevel;
import org.ops4j.pax.exam.options.AbstractUrlProvisionOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption;

public class IspnKarafOptions {
   private static final String PROP_VERSION_KARAF = "version.karaf";
   private static final String PROP_VERSION_PAX_EXAM = "version.pax.exam";
   private static final String PROP_VERSION_MOCKITO = "version.mockito";
   private static final String PROP_VERSION_OBJENESIS = "version.mockito_dep.objenesis";
   private static final String PROP_VERBOSE_KARAF = "verbose.karaf";

   private static Pattern PATTERN_HEADER = Pattern.compile("(.+)=(.+)");

   static {
      /* PAX URL calls are made in the junit driver in addition to the container
       * and this is required for the in-driver calls to find custom local repo
       * locations. */
      PaxURLUtils.configureLocalMavenRepo();
   }

   public static String karafVersion() throws Exception {
      String karafVersion = System.getProperty(PROP_VERSION_KARAF);
      if (karafVersion == null) {
         karafVersion = MavenUtils.getProperties().getProperty(PROP_VERSION_KARAF);
      }
      return karafVersion;
   }

   public static Option verboseKaraf() {
      Option result = null;
      if (Boolean.parseBoolean(System.getProperty(PROP_VERBOSE_KARAF))) {
         result = logLevel(LogLevel.TRACE);
      };
      return result;
   }

   public static Option karafContainer() throws Exception {
      String karafVersion = karafVersion();
      return karafDistributionConfiguration()
            .frameworkUrl(
                  maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("tar.gz").version(karafVersion))
                  /* The deploy folder doesn't guarantee that dependencies will be available before the probe runs. */
                  .useDeployFolder(false)
                  .karafVersion(karafVersion).unpackDirectory(new File("target/pax"));
   }

   public static Option featureIspnCore() {
      return mvnFeature("org.infinispan", "infinispan-core", "infinispan-core");
   }

   public static Option featureIspnCoreAndTests() throws Exception {
      return composite(featureIspnCore(),
            mvnTestsAsFragmentBundle("org.infinispan", "infinispan-core", "org.infinispan.core"));
   }

   public static Option featureLevelDbJni() {
      return mvnFeature("org.infinispan", "infinispan-cachestore-leveldb", "infinispan-cachestore-leveldb-jni");
   }

   public static Option featureRemoteStore() {
      return mvnFeature("org.infinispan", "infinispan-cachestore-remote", "infinispan-cachestore-remote");
   }

   public static Option featureJdbcStore() {
      return mvnFeature("org.infinispan", "infinispan-cachestore-jdbc", "infinispan-cachestore-jdbc");
   }

   public static Option featureJdbcStoreAndTests() throws Exception {
      return composite(featureJdbcStore(),
            mavenBundle().groupId("org.testng").artifactId("testng").versionAsInProject(),
            mvnTestsAsFragmentBundle("org.infinispan", "infinispan-cachestore-jdbc", "org.infinispan.cachestore-jdbc"));
   }

   public static Option featureJpaStore() {
      return mvnFeature("org.infinispan", "infinispan-cachestore-jpa", "infinispan-cachestore-jpa");
   }

   public static Option featureJpaStoreAndTests() throws Exception {
      return composite(featureJpaStore(),
            bundleMockito(),
            mavenBundle().groupId("org.testng").artifactId("testng").versionAsInProject(),
            mvnTestsAsFragmentBundle("org.infinispan", "infinispan-cachestore-jpa", "org.infinispan.cachestore-jpa"));
   }

   public static Option bundleMockito() throws Exception {
      String versionMockito = MavenUtils.getProperties().getProperty(PROP_VERSION_MOCKITO);
      String versionObjenesis = MavenUtils.getProperties().getProperty(PROP_VERSION_OBJENESIS);
      return composite(
            mavenBundle().groupId("org.objenesis").artifactId("objenesis").version(versionObjenesis),
            mavenBundle().groupId("org.mockito").artifactId("mockito-core").version(versionMockito));
   }

   public static Option featureKarafJNDI() throws Exception {
      String karafVersion = karafVersion();
      String groupId = String.format("org.apache.karaf.%sfeatures", karafVersion.startsWith("2") ? "assemblies." : "");

      return features(maven().groupId(groupId).artifactId("enterprise").type("xml")
            .classifier("features").version(karafVersion), "jndi");
   }

   public static Option bundleH2Database() {
      return composite(
            mavenBundle().groupId("com.h2database").artifactId("h2").versionAsInProject(),
            mavenBundle().groupId("org.osgi").artifactId("org.osgi.enterprise").version("4.2.0"));
   }

   public static Option mvnFeature(String groupId, String artifactId, String feature) {
      return features(maven().groupId(groupId).artifactId(artifactId).type("xml")
            .classifier("features").versionAsInProject(), feature);
   }

   private static UrlProvisionOption testJarAsStreamBundle(String groupId, String artifactId) throws Exception {
      return asStreamBundle(maven().groupId(groupId).artifactId(artifactId).type("jar").classifier("tests").versionAsInProject().getURL());
   }

   /**
    * Wraps the specified test jars as bundles fragments and attaches them to the specified host bundle.
    * The host bundle must be the one exporting the packages contained in the test jar.
    * 
    * @param groupId
    * @param artifactId
    * @param hostBundle
    * @return
    * @throws Exception 
    */
   public static WrappedUrlProvisionOption mvnTestsAsFragmentBundle(String groupId, String artifactId, String hostBundle, String... instructions) throws Exception {
      PaxURLUtils.registerURLHandlers();

      UrlProvisionOption testBundle = asStreamBundle(testJarAsStreamBundle(groupId, artifactId), "assembly:%s!/!org/infinispan/test/fwk/**");

      String[] allInstructions = Arrays.copyOf(instructions, instructions.length + 1);
      allInstructions[instructions.length] = String.format("Fragment-Host=%s", hostBundle); 

      return wrappedBundle(testBundle).instructions(allInstructions);
   }

   public static UrlProvisionOption emptyBundle(String... headers) throws IOException {
      Manifest manifest = new Manifest();
      Attributes mainAttributes = manifest.getMainAttributes();

      mainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
      mainAttributes.putValue("Bundle-ManifestVersion", "2");

      for (String header : headers) {
         Matcher matcher = PATTERN_HEADER.matcher(header);
         if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format("Invalid header: %s (expecting '%s')", header, PATTERN_HEADER.pattern()));
         }
         mainAttributes.putValue(matcher.group(1), matcher.group(2));
      }
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      JarOutputStream jar = new JarOutputStream(buf, manifest);
      jar.close();
      return streamBundle(new ByteArrayInputStream(buf.toByteArray()));
   }

   /**
    * Some test packages are split across several Maven modules this option repackages them
    * and exposes them through a single bundle.
    * 
    * @return
    * @throws Exception 
    */
   public static Option bundleSplitTestPackages() throws Exception {
      PaxURLUtils.registerURLHandlers();

      UrlProvisionOption splitCoreBundle = asStreamBundle(
            testJarAsStreamBundle("org.infinispan", "infinispan-core"),
            "assembly:%s!/org/infinispan/test/fwk/**");
      UrlProvisionOption splitJDBCBundle = asStreamBundle(
            testJarAsStreamBundle("org.infinispan", "infinispan-cachestore-jdbc"),
            "assembly:%s!/org/infinispan/test/fwk/**");

      WrappedUrlProvisionOption wrappedSplitCoreBundle = wrappedBundle(splitCoreBundle)
            .bundleSymbolicName("split-test-core")
            .instructions("Export-Package=org.infinispan.test.fwk;partial=true;mandatory:=partial");
      WrappedUrlProvisionOption wrappedSplitJDBCBundle = wrappedBundle(splitJDBCBundle)
            .bundleSymbolicName("split-test-jdbc")
            .instructions("Export-Package=org.infinispan.test.fwk;partial=true;mandatory:=partial");

      UrlProvisionOption wrappedSplitTestBundle = emptyBundle(
            "Bundle-SymbolicName=split-test",
            "Export-Package=org.infinispan.test.fwk",
            "Require-Bundle=split-test-core,split-test-jdbc");

      return composite(wrappedSplitCoreBundle,
            wrappedSplitJDBCBundle,
            wrappedSplitTestBundle);
   }

   public static UrlProvisionOption asStreamBundle(AbstractUrlProvisionOption<?> option, String newURLFormat, String... args) throws MalformedURLException, IOException {
      return asStreamBundle(option.getURL(), newURLFormat, args);
   }

   public static UrlProvisionOption asStreamBundle(String url) throws MalformedURLException, IOException {
      return asStreamBundle(url, "%s");
   }

   /**
    * 
    * Some PAX-URL protocols are not supported by Karaf. This method can be used when one of the unsupported protocol
    * is required. The URLs are resolved outside Karaf and the bundles are provided as stream bundles.
    * 
    * @param option
    * @param newURLFormat
    * @param args
    * @return 
    * @return
    * @throws MalformedURLException
    * @throws IOException
    */
   public static UrlProvisionOption asStreamBundle(String url, String newURLFormat, String... args) throws MalformedURLException, IOException {
      InputStream in = new URL(String.format(newURLFormat, url, args)).openStream();
      try {
         return streamBundle(in);
      } finally {
         try {
            in.close();
         } catch (IOException ex) {
         }
      }
   }

   /**
    * PAX URL needs to know the location of the local maven repo to resolve mvn: URLs. When
    * running the tests on the CI machine TeamCity passes a custom local repo location using
    * -Dmaven.repo.local to isolate the build targets and PAX URL is not aware there's a
    * custom repo to be used and tries to load from the default local repo location.
    * 
    * This option will pass the location specified using -Dmaven.repo.local to the appropriate
    * system property of the container.
    * 
    * @return an Option or null if no custom repo location is specified by the maven build.
    * @throws Exception 
    */
   public static Option localRepoForPAXUrl() throws Exception {
      String localRepo = MavenUtils.getLocalRepository();
      if (localRepo == null) {
         return null;
      }

      return composite(systemProperty(PaxURLUtils.PROP_PAX_URL_LOCAL_REPO).value(localRepo),
            editConfigurationFilePut("etc/org.ops4j.pax.url.mvn.cfg", PaxURLUtils.PROP_PAX_URL_LOCAL_REPO, localRepo));
   }

   /**
    * Sets the system variables used inside persistence.xml to use H2.
    */
   public static Option hibernatePersistenceH2() {
      return composite(systemProperty("connection.url").value("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"),
            systemProperty("driver.class").value("org.h2.Driver"));
   }

   public static Option bundlePaxExamSpi() throws Exception {
      String version = MavenUtils.getProperties().getProperty(PROP_VERSION_PAX_EXAM);
      return wrappedBundle(mavenBundle().groupId("org.ops4j.pax.exam").artifactId("pax-exam-spi").version(version));
   }

   public static Option commonOptions() throws Exception {
      return composite(karafContainer(),
            vmOptions("-Djava.net.preferIPv4Stack=true", "-Djgroups.bind_addr=127.0.0.1"),
            verboseKaraf(),
            junitBundles(),
            keepRuntimeFolder(),
            /* Required for the @Category(Per{Suite,Class,Method}) annotations. */
            bundlePaxExamSpi(),
            localRepoForPAXUrl());
   }

   public static Option perSuiteOptions() throws Exception {
      return composite(commonOptions(),
            featureKarafJNDI(),
            featureIspnCoreAndTests(),
            featureJdbcStoreAndTests(),
            featureJpaStoreAndTests(),
            featureLevelDbJni(),
            featureRemoteStore(),
            bundleH2Database(),
            hibernatePersistenceH2(),
            bundleSplitTestPackages());
   }
}
