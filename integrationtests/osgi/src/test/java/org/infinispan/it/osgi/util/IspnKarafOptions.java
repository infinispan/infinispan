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
import java.net.URL;
import java.util.Arrays;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.KarafDistributionConfigurationConsoleOption;
import org.ops4j.pax.exam.karaf.options.LogLevelOption.LogLevel;
import org.ops4j.pax.exam.options.AbstractUrlProvisionOption;
import org.ops4j.pax.exam.options.MavenUrlReference;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption;

public class IspnKarafOptions {
   private static final String PROP_VERSION_KARAF = "version.karaf";
   private static final String PROP_VERSION_PAX_EXAM = "version.pax.exam";
   private static final String PROP_VERSION_MOCKITO = "version.mockito";
   private static final String PROP_VERSION_OBJENESIS = "version.mockito_dep.objenesis";
   private static final String PROP_VERSION_BYTEBUDDY = "version.mockito_dep.bytebuddy";
   private static final String PROP_VERBOSE_KARAF = "verbose.karaf";
   private static final String PROP_UBER_JAR = "uberjar";

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
      LogLevel logLevel = Boolean.getBoolean(PROP_VERBOSE_KARAF) ? LogLevel.TRACE : LogLevel.INFO;

      return composite(logLevel(logLevel),
                       vmOptions("-Dorg.ops4j.pax.logging.DefaultServiceLog.level=" + logLevel,
                                 "-Dorg.apache.logging.log4j.level=" + logLevel));
   }

   public static Option runWithoutConsole() {
      return new KarafDistributionConfigurationConsoleOption(false, false);
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

   public static Option featureRocksDBJNI() {
      return mvnFeature("org.infinispan", "infinispan-cachestore-rocksdb", "infinispan-cachestore-rocksdb");
   }

   public static Option featureRemoteStore() {
      return mvnFeature("org.infinispan", "infinispan-cachestore-remote", "infinispan-cachestore-remote");
   }

   public static Option featureJdbcStore() {
      return mvnFeature("org.infinispan", "infinispan-cachestore-jdbc", "infinispan-cachestore-jdbc");
   }

   public static Option featureJdbcStoreAndTests() throws Exception {
      return composite(featureJdbcStore(),
                       bundleTestNG(),
                       mvnTestsAsFragmentBundle("org.infinispan", "infinispan-cachestore-jdbc", "org.infinispan.cachestore-jdbc"));
   }

   public static Option bundleTestNG() {
      return mavenBundle().groupId("org.testng").artifactId("testng").versionAsInProject();
   }

   public static Option bundleLog4J() {
      return composite(
            mavenBundle().groupId("org.apache.logging.log4j").artifactId("log4j-api").versionAsInProject(),
            mavenBundle().groupId("org.apache.logging.log4j").artifactId("log4j-core").versionAsInProject()
      );
   }

   public static Option bundleTestAnnotations() {
      return wrappedBundle(mavenBundle().groupId("org.infinispan").artifactId("infinispan-commons-test").versionAsInProject().getURL());
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

   public static Option featureEmbeddedUberJarAndTests() throws Exception {
      return composite(mvnFeature("org.infinispan", "infinispan-embedded", "infinispan-embedded"),
                       mvnFeature("org.infinispan", "infinispan-embedded", "c3p0"),
                       mvnFeature("org.infinispan", "infinispan-embedded", "hikaricp"),
                       mvnFeature("org.infinispan", "infinispan-embedded", "hibernate"),
                       mvnFeature("org.infinispan", "infinispan-embedded", "rocksdb"),
                       mvnTestsAsFragmentBundle("org.infinispan", "infinispan-core", "org.infinispan.embedded"),
                       mvnTestsAsFragmentBundle("org.infinispan", "infinispan-cachestore-jdbc", "org.infinispan.embedded"),
                       mvnTestsAsFragmentBundle("org.infinispan", "infinispan-cachestore-jpa", "org.infinispan.embedded"));
   }

   public static Option bundleMockito() throws Exception {
      String versionMockito = MavenUtils.getProperties().getProperty(PROP_VERSION_MOCKITO);
      String versionObjenesis = MavenUtils.getProperties().getProperty(PROP_VERSION_OBJENESIS);
      String versionByteBuddy = MavenUtils.getProperties().getProperty(PROP_VERSION_BYTEBUDDY);
      return composite(
            mavenBundle().groupId("org.objenesis").artifactId("objenesis").version(versionObjenesis),
            mavenBundle().groupId("org.mockito").artifactId("mockito-core").version(versionMockito),
            mavenBundle().groupId("net.bytebuddy").artifactId("byte-buddy").version(versionByteBuddy),
      mavenBundle().groupId("net.bytebuddy").artifactId("byte-buddy-agent").version(versionByteBuddy));
   }

   public static Option featureKarafJNDI() throws Exception {
      String karafVersion = karafVersion();
      String groupId = String.format("org.apache.karaf.%sfeatures", karafVersion.startsWith("2") ? "assemblies." : "");

      return features(maven().groupId(groupId).artifactId("enterprise").type("xml")
                            .classifier("features").version(karafVersion), "jndi");
   }

   public static Option bundleH2Database() {
      return mavenBundle().groupId("com.h2database").artifactId("h2").versionAsInProject();
   }

   public static Option mvnFeature(String groupId, String artifactId, String feature) {
      return features(maven().groupId(groupId).artifactId(artifactId).type("xml")
                            .classifier("features").versionAsInProject(), feature);
   }

   private static UrlProvisionOption testJarAsStreamBundle(String groupId, String artifactId) throws Exception {
      return asStreamBundle(maven().groupId(groupId).artifactId(artifactId).type("jar").classifier("tests").versionAsInProject().getURL());
   }

   /**
    * Wraps the specified test jars as bundles fragments and attaches them to the specified host bundle. The host bundle
    * must be the one exporting the packages contained in the test jar.
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
    * Some test packages are split across several Maven modules this option repackages them and exposes them through a
    * single bundle.
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

   public static UrlProvisionOption asStreamBundle(AbstractUrlProvisionOption<?> option, String newURLFormat, String... args) throws IOException {
      return asStreamBundle(option.getURL(), newURLFormat);
   }

   public static UrlProvisionOption asStreamBundle(String url) throws IOException {
      return asStreamBundle(url, "%s");
   }

   /**
    * Some PAX-URL protocols are not supported by Karaf. This method can be used when one of the unsupported protocol is
    * required. The URLs are resolved outside Karaf and the bundles are provided as stream bundles.
    */
   public static UrlProvisionOption asStreamBundle(String url, String newURLFormat) throws IOException {
      InputStream in = new URL(String.format(newURLFormat, url)).openStream();
      try {
         return streamBundle(in);
      } finally {
         try {
            in.close();
         } catch (IOException ignored) {
         }
      }
   }

   /**
    * PAX URL needs to know the location of the local maven repo to resolve mvn: URLs. When running the tests on the CI
    * machine TeamCity passes a custom local repo location using -Dmaven.repo.local to isolate the build targets and PAX
    * URL is not aware there's a custom repo to be used and tries to load from the default local repo location.
    * <p/>
    * This option will pass the location specified using -Dmaven.repo.local to the appropriate system property of the
    * container.
    *
    * @return an Option or null if no custom repo location is specified by the maven build.
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

   public static Option featurePaxUrlWrap() throws Exception {
      MavenUrlReference karafStandardRepo = maven()
            .groupId("org.apache.karaf.features")
            .artifactId("standard")
            .version(karafVersion())
            .classifier("features")
            .type("xml");
      return features(karafStandardRepo, "(wrap)");
   }

   public static Option commonOptions() throws Exception {
      return composite(karafContainer(),
                       vmOptions("-Djava.net.preferIPv4Stack=true", "-Djgroups.bind_addr=127.0.0.1"),
                       vmOptions("-Xmx500m", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=" + System.getProperty("user.dir")),
//                       vmOptions("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=5006"),
                       verboseKaraf(),
                       runWithoutConsole(),
                       junitBundles(),
                       keepRuntimeFolder(),
                       /* For the wrap: protocol used for non-OSGi dependencies */
                       /* Infinispan features already that need it depend on it, but the test-dependencies feature doesn't have any dependencies */
                       featurePaxUrlWrap(),
                       /* Required for the @ExamReactorStrategy, @Category(Per{Suite,Class,Method}.class) annotations. */
                       bundlePaxExamSpi(),
                       bundleTestAnnotations(),
                       localRepoForPAXUrl());
   }

   public static Option perSuiteOptions() throws Exception {
      if (!useUberJar()) {
         return composite(commonOptions(),
                          featureKarafJNDI(),
                          featureIspnCoreAndTests(),
                          featureJdbcStoreAndTests(),
                          featureJpaStoreAndTests(),
                          featureRocksDBJNI(),
                          featureRemoteStore(),
                          bundleH2Database(),
                          bundleLog4J(),
                          hibernatePersistenceH2(),
                          bundleSplitTestPackages());
      } else {
         return composite(commonOptions(),
                          featureKarafJNDI(),
                          featureEmbeddedUberJarAndTests(),
                          bundleSplitTestPackages(),
                          bundleH2Database(),
                          hibernatePersistenceH2(),
                          bundleLog4J(),
                          bundleTestNG(),
                          bundleMockito());
      }
   }

   /* Run tests with uberjar by default */
   private static boolean useUberJar() {
      String uberJar = System.getProperty(PROP_UBER_JAR);
      return uberJar == null || Boolean.parseBoolean(uberJar);
   }

}
