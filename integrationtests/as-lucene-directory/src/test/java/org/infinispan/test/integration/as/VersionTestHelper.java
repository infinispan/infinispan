package org.infinispan.test.integration.as;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;

/**
 * Helper class for integration testing of the generated JBoss Modules.
 * <p/>
 * The slot version is set as a property in the parent pom, and passed to the JVM of Arquillian as a system property of
 * the Maven maven-failsafe-plugin.
 * Loading it as a Properties makes these settings available to tests run withing the IDE as well.
 *
 * @author Sanne Grinovero
 * @since 7.2
 */
public class VersionTestHelper {

   private VersionTestHelper() {
      //not meant to be created
   }

   public static void addHibernateSearchManifestDependencies(Archive<?> archive) {
      archive.add( manifestDependencies( "org.hibernate.search.orm:${hibernate-search.module.slot} services"), "META-INF/MANIFEST.MF" );
   }

   public static Asset manifestDependencies(String moduleDependencies) {
      return manifest( injectVariables( moduleDependencies ) );
   }

   private static Asset manifest(String dependencies) {
      String manifest = Descriptors.create( ManifestDescriptor.class )
            .attribute( "Dependencies", dependencies )
            .exportAsString();
      return new StringAsset( manifest );
   }

   private static String injectVariables(String dependencies) {
      Properties projectCompilationProperties = new Properties();
      final InputStream resourceAsStream = VersionTestHelper.class.getClassLoader().getResourceAsStream( "module-versions.properties" );
      try {
         projectCompilationProperties.load( resourceAsStream );
      }
      catch (IOException e) {
         throw new RuntimeException( e );
      }
      finally {
         try {
            resourceAsStream.close();
         }
         catch (IOException e) {
            throw new RuntimeException( e );
         }
      }
      Set<Entry<Object,Object>> entrySet = projectCompilationProperties.entrySet();
      for (Entry<Object,Object> entry : entrySet) {
         String key = (String) entry.getKey();
         String value = (String) entry.getValue();
         dependencies = dependencies.replace("${" + key + "}", value);
      }
      return dependencies;
   }

}
