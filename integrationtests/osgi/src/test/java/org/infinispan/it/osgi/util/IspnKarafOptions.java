package org.infinispan.it.osgi.util;

import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

import java.io.File;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.RawUrlReference;

public class IspnKarafOptions {

   private static final String KARAF_VERSION = System.getProperty("version.karaf", "2.3.3");
   private static final String TEST_UTILS_FEATURE_FILE = "file:///" + System.getProperty("basedir").replace("\\", "/")
         + "/target/test-classes/test-features.xml";

   public static Option karafContainer() {
      return karafDistributionConfiguration()
            .frameworkUrl(
                  maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("zip").version(KARAF_VERSION))
            .karafVersion(KARAF_VERSION).unpackDirectory(new File("target/pax"));
   }

   public static Option featureIspnCore() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-core");
   }

   public static Option featureLevelDbJni() {
      return features(maven().groupId("org.infinispan").artifactId("infinispan-cachestore-leveldb").type("xml")
            .classifier("features").versionAsInProject(), "infinispan-cachestore-leveldb-jni");
   }

   public static Option featureRemoteStore() {
      return features(maven().groupId("org.infinispan").artifactId("infinispan-cachestore-remote").type("xml")
            .classifier("features").versionAsInProject(), "infinispan-cachestore-remote");
   }

   public static Option featureJdbcStore() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-cachestore");
   }

   public static Option featureIspnCoreDependencies() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-core-deps");
   }

   public static Option[] allOptions() {
      return options(karafContainer(), featureIspnCoreDependencies(), featureIspnCore(), featureRemoteStore(),
            featureJdbcStore(), featureLevelDbJni(), junitBundles(), keepRuntimeFolder());
   }

}
