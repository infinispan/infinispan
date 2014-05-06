
package org.infinispan.it.osgi.util;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

public class MavenUtils {
   private static final String MAVEN_PROPERTIES_FILENAME = "maven.properties";
   private static final String PROP_MAVEN_LOCAL_REPO = "maven.repo.local";

   /**
    * Get the Maven properties defined in the project.
    * 
    * @return a Properties object containing the Maven project properties.
    * @throws Exception
    */
   public static Properties getProperties() throws Exception {
      Bundle bundle = FrameworkUtil.getBundle(MavenUtils.class);

      URL resource;
      if (bundle != null) {
         /* OSGi */
         resource = bundle.getResource(MAVEN_PROPERTIES_FILENAME);
      } else {
         resource = MavenUtils.class.getClassLoader().getResource(MAVEN_PROPERTIES_FILENAME);
      }

      InputStream stream = resource.openStream();
      Properties props = new Properties();
      try {
         props.load(stream);
      } finally {
         stream.close();
      }

      /* Set to null properties which were not set in Maven. */
      for (String prop : props.stringPropertyNames()) {
         String propValue = props.getProperty(prop);
         if (String.format("${%s}", prop).equals(propValue.trim())) {
            props.remove(prop);
         }
      }
      return props;
   }

   public static String getLocalRepository() throws Exception {
      String localRepo = System.getProperty(PROP_MAVEN_LOCAL_REPO);
      if (localRepo == null) {
         localRepo = getProperties().getProperty(PROP_MAVEN_LOCAL_REPO);
      }
      if (localRepo == null) {
         return null;
      }
      return (new File(localRepo)).getAbsolutePath();
   }
}
