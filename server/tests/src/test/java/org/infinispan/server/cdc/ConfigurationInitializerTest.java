package org.infinispan.server.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.cdc.ChangeDataCaptureIT.CDC_TABLE_NAME;

import java.sql.SQLException;

import org.infinispan.cdc.configuration.ChangeDataCaptureConfiguration;
import org.infinispan.cdc.configuration.ChangeDataCaptureConfigurationBuilder;
import org.infinispan.cdc.configuration.TableConfigurationBuilder;
import org.infinispan.cdc.internal.configuration.CompleteConfiguration;
import org.infinispan.cdc.internal.configuration.ConnectionParameters;
import org.infinispan.cdc.internal.configuration.Table;
import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.persistence.ContainerDatabase;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit5.DatabaseExtension;
import org.junit.jupiter.api.extension.RegisterExtension;

@org.infinispan.server.test.core.tags.Database
public class ConfigurationInitializerTest {

   @RegisterExtension
   public static DatabaseExtension DATABASES = new DatabaseExtension(ChangeDataCaptureIT.DATABASE_LISTENER);

   @DatabaseTest
   public void testSimpleConfiguration(Database database) throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      ChangeDataCaptureConfigurationBuilder builder = cb.addModule(ChangeDataCaptureConfigurationBuilder.class);
      // Enable and adds only database configuration.
      builder.enabled(true);
      builder.connectionPool()
            .username(database.username())
            .password(database.password())
            .connectionUrl(database.jdbcUrl())
            .driverClass(database.driverClassName());
      // Set table name since cache name is custom.
      builder.table().name(CDC_TABLE_NAME);
      CompleteConfiguration configuration = createConfiguration(builder);

      ContainerDatabase container = (ContainerDatabase) database;
      assertDatabaseConfiguration(container, configuration.connection());

      Table table = configuration.table();
      assertThat(table.name()).isEqualTo(CDC_TABLE_NAME);
      assertThat(table.primaryKey().columns())
            .hasSize(1)
            .containsExactly(databaseFormat(database, "id"));
      // No FK since we didn't require any.
      assertThat(table.foreignKeys()).isEmpty();

      // Empty columns means to add all columns in the record.p
      assertThat(table.columns()).isEmpty();
   }

   @DatabaseTest
   public void testCompleteConfiguration(Database database) throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      ChangeDataCaptureConfigurationBuilder builder = cb.addModule(ChangeDataCaptureConfigurationBuilder.class);
      builder.enabled(true);
      builder.connectionPool()
            .username(database.username())
            .password(database.password())
            .connectionUrl(database.jdbcUrl())
            .driverClass(database.driverClassName());
      // Should resolve any foreign key.
      builder.addForeignKey("*");
      // Set table name since cache name is custom.
      TableConfigurationBuilder tcb = builder.table();
      tcb.name(CDC_TABLE_NAME);
      tcb.primaryKey().name(databaseFormat(database, "id"));
      tcb.addColumn().name("name");

      CompleteConfiguration configuration = createConfiguration(builder);
      assertDatabaseConfiguration((ContainerDatabase) database, configuration.connection());

      Table table = configuration.table();
      assertThat(table.name()).isEqualTo(CDC_TABLE_NAME);
      assertThat(table.primaryKey().columns())
            .hasSize(1)
            .containsExactly(databaseFormat(database, "id"));

      assertThat(table.foreignKeys())
            .hasSize(1)
            .allSatisfy(fk -> {
               assertThat(fk.columns()).containsExactly(databaseFormat(database, "major_id"));
               assertThat(fk.refTable()).isEqualTo(databaseFormat(database, "department"));
               assertThat(fk.refColumns()).containsExactly(databaseFormat(database, "id"));
            });

      assertThat(table.columns())
            .hasSize(1)
            .containsExactly("name");
   }

   @DatabaseTest
   public void testInvalidPrimaryKey(Database database) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      ChangeDataCaptureConfigurationBuilder builder = cb.addModule(ChangeDataCaptureConfigurationBuilder.class);
      // Enable and adds only database configuration.
      builder.enabled(true);
      builder.connectionPool()
            .username(database.username())
            .password(database.password())
            .connectionUrl(database.jdbcUrl())
            .driverClass(database.driverClassName());
      // Set table name since cache name is custom.
      TableConfigurationBuilder tcb = builder.table();
      tcb.name(CDC_TABLE_NAME);
      tcb.primaryKey().name("not_exists");

      assertThatThrownBy(() -> createConfiguration(builder))
            .hasMessageContaining("Primary key colum for table");
   }

   @DatabaseTest
   public void testInvalidForeignKeys(Database database) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      ChangeDataCaptureConfigurationBuilder builder = cb.addModule(ChangeDataCaptureConfigurationBuilder.class);
      // Enable and adds only database configuration.
      builder.enabled(true);
      builder.connectionPool()
            .username(database.username())
            .password(database.password())
            .connectionUrl(database.jdbcUrl())
            .driverClass(database.driverClassName());
      // Set table name since cache name is custom.
      TableConfigurationBuilder tcb = builder.table();
      tcb.name(CDC_TABLE_NAME);

      builder.addForeignKey("not_exists");

      assertThatThrownBy(() -> createConfiguration(builder))
            .hasMessageContaining("Provided foreign keys do not exist");
   }

   static CompleteConfiguration createConfiguration(ChangeDataCaptureConfigurationBuilder builder) throws SQLException {
      ChangeDataCaptureConfiguration cdc = builder.build().module(ChangeDataCaptureConfiguration.class);
      return CompleteConfiguration.create(CDC_TABLE_NAME, cdc);
   }

   static void assertDatabaseConfiguration(ContainerDatabase database, ConnectionParameters cp) {
      assertThat(cp.username()).isEqualTo(database.username());
      assertThat(cp.password()).isEqualTo(database.password());
      assertThat(cp.url().host()).isEqualTo(database.getHost());
      assertThat(cp.url().port()).isEqualTo(String.valueOf(database.getInternalPort()));
      assertThat(cp.url().vendor()).isEqualTo(DatabaseVendor.valueOf(database.getType().toUpperCase()));

      // SQL Server only connects *after* the CDC database exist. So we need to start the container without the schema name.
      // Oracle uses the default database name.
      if (cp.url().vendor() != DatabaseVendor.MSSQL && cp.url().vendor() != DatabaseVendor.ORACLE)
         assertThat(cp.url().schema()).isEqualTo("cdc");
   }

   static String databaseFormat(Database database, String value) {
      return switch (database.getType()) {
         case "db2", "oracle" -> value.toUpperCase();
         default -> value;
      };
   }
}
