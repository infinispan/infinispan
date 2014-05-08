package org.infinispan.it.osgi.util;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.RawUrlReference;
import org.ops4j.pax.exam.options.UrlProvisionOption;

import java.io.File;

import static org.ops4j.pax.exam.CoreOptions.*;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;


public class IspnKarafOptions {

   private static final String KARAF_VERSION = System.getProperty("version.karaf", "2.3.3");
   private static final String TEST_UTILS_FEATURE_FILE = "file:///" + System.getProperty("basedir").replace("\\", "/") + "/target/test-classes/test-features.xml";

   public static Option karafContainer() {
      return karafDistributionConfiguration()
            .frameworkUrl(
                  maven()
                        .groupId("org.apache.karaf")
                        .artifactId("apache-karaf")
                        .type("zip")
                        .version(KARAF_VERSION))
            .karafVersion(KARAF_VERSION)
            .unpackDirectory(new File("target/pax"));
   }

   public static Option featureIspnCore() {
      return features(maven().groupId("org.infinispan")
                            .artifactId("infinispan-core")
                            .type("xml")
                            .classifier("features")
                            .versionAsInProject(), "infinispan-core");
   }

   public static Option featureIspnCorePlusTests() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-core-plus-tests");
   }

   public static Option featureLevelDbJni() {
      return features(maven().groupId("org.infinispan")
                            .artifactId("infinispan-cachestore-leveldb")
                            .type("xml")
                            .classifier("features")
                            .versionAsInProject(), "infinispan-cachestore-leveldb-jni");
   }

   public static Option featureRemoteStore() {
      return features(maven().groupId("org.infinispan")
                            .artifactId("infinispan-cachestore-remote")
                            .type("xml")
                            .classifier("features")
                            .versionAsInProject(), "infinispan-cachestore-remote");
   }

   public static Option featureIspnCoreTests() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-core-tests");
   }

   public static Option featureJdbcStorePooled() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-cachestore-jdbc-pooled");
   }

   public static Option featureJdbcStoreManaged() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-cachestore-jdbc-managed");
   }

   public static Option featureIspnCoreDependencies() {
      return features(new RawUrlReference(TEST_UTILS_FEATURE_FILE), "infinispan-core-deps");
   }

}
