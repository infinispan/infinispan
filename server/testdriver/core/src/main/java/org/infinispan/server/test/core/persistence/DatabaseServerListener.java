package org.infinispan.server.test.core.persistence;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.jboss.logging.Logger;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
public class DatabaseServerListener implements InfinispanServerListener {
   private static final Logger log = Logger.getLogger(DatabaseServerListener.class);
   public static final String DATABASES = "org.infinispan.test.server.database.types";
   public static final String DATABASE_PROPERTIES_PATH = "org.infinispan.test.server.database.properties.path";
   private final String[] databaseTypes;
   public Map<String, Database> databases;

   public DatabaseServerListener(String... databaseTypes) {
      String property = System.getProperty(DATABASES);
      if (property != null) {
         this.databaseTypes = property.split(",");
         log.infof("Overriding databases: %s", this.databaseTypes);
      } else {
         this.databaseTypes = databaseTypes;
      }
   }

   @Override
   public void before(InfinispanServerDriver driver) {
      databases = new LinkedHashMap<>(databaseTypes.length);
      for (String dbType : databaseTypes) {
         Database database = initDatabase(dbType);
         log.infof("Starting database: %s", database.getType());
         database.start();
         log.infof("Started database: %s", database.getType());
         if (databases.putIfAbsent(dbType, database) != null) {
            throw new RuntimeException("Duplicate database type " + dbType);
         }
         driver.getConfiguration().properties().put("org.infinispan.server.test." + database.getType() + ".jdbcUrl", database.jdbcUrl());
      }
   }

   @Override
   public void after(InfinispanServerDriver driver) {
      log.info("Stopping databases");
      databases.values().forEach(Database::stop);
      log.info("Stopped databases");
   }

   public Database getDatabase(String databaseType) {
      return databases.get(databaseType);
   }

   public String[] getDatabaseTypes() {
      return databaseTypes;
   }

   private Database initDatabase(String databaseType) {
      String property = System.getProperty(DATABASE_PROPERTIES_PATH);
      try (InputStream inputStream = property != null ? Files.newInputStream(Paths.get(property).resolve(databaseType + ".properties")) : getClass().getResourceAsStream(String.format("/database/%s.properties", databaseType))) {
         Properties properties = new Properties();
         properties.load(inputStream);
         return Database.fromProperties(databaseType, properties);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
