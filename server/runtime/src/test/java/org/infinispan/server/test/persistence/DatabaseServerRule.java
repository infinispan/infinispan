package org.infinispan.server.test.persistence;

import java.io.InputStream;
import java.util.Properties;

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.server.test.InfinispanServerRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
public class DatabaseServerRule implements TestRule {
   public static final String DATABASES = "org.infinispan.test.server.jdbc.databases";
   private final InfinispanServerRule infinispanServerRule;
   public Database database;

   public DatabaseServerRule(InfinispanServerRule infinispanServerRule) {
      this.infinispanServerRule = infinispanServerRule;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            before();
            try {
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before() {
      if (infinispanServerRule.getServerDriver().getStatus() != ComponentStatus.RUNNING) {
         throw new IllegalStateException("Infinispan Server should be running");
      }
   }

   private void after() {
      database.stop();
   }

   public void setDatabaseType(String databaseType) {
      if (database != null) {
         if (database.getType().equals(databaseType)) {
            return; // Reuse
         }
         database.stop();
      }
      database = initDatabase(databaseType);
      database.start();
   }

   public Database getDatabase() {
      return database;
   }

   private Database initDatabase(String databaseType) {
      String filename = String.format("/database/%s.properties", databaseType);
      try (InputStream inputStream = getClass().getResourceAsStream(filename)) {
         Properties properties = new Properties();
         properties.load(inputStream);
         return Database.fromProperties(databaseType, properties);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static String[] getDatabaseTypes(String... defaults) {
      String property = System.getProperty(DATABASES);
      if (property != null) {
         return property.split(",");
      } else {
         return defaults;
      }
   }
}
