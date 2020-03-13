package org.infinispan.commons.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Features allow conditional enabling/disabling of Infinispan's functionality.
 * They are represented as named properties of the form 'org.infinispan.feature.*' and can either be set via one or
 * more 'META-INF/infinispan-features.properties' file on the classpath or by setting a system property,
 * e.g. -Dorg.infinispan.feature.A=true
 */
public class Features {
   private static final Log log = LogFactory.getLog(Features.class);
   public static final String FEATURE_PREFIX = "org.infinispan.feature.";
   static final String FEATURES_FILE = "META-INF/infinispan-features.properties";
   private final Properties features;

   public Features(ClassLoader classLoader) {
      features = new Properties();
      try {
         Collection<URL> featureFiles = FileLookupFactory.newInstance().lookupFileLocations(FEATURES_FILE, classLoader);
         for (URL url : featureFiles) {
            try (InputStream is = url.openStream()) {
               features.load(is);
            }
         }
         if (log.isDebugEnabled()) {
            features.forEach((key, value) -> log.debugf("Feature %s=%s", key, value));
         }
      } catch (IOException e) {
         log.debugf(e, "Error while attempting to obtain `%s` resources from the classpath", FEATURES_FILE);
         throw new CacheConfigurationException(e);
      }
   }

   public Features() {
      this(Features.class.getClassLoader());
   }

   public boolean isAvailable(String featureName) {
      String name = FEATURE_PREFIX + featureName;
      String sysprop = SecurityActions.getProperty(name);
      if (sysprop != null) {
         return Boolean.parseBoolean(sysprop);
      } else {
         return Boolean.parseBoolean(features.getProperty(name, "true"));
      }
   }
}
