package org.infinispan.cdc.internal.url;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor.DB2;
import static org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor.MYSQL;
import static org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor.ORACLE;
import static org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor.POSTGRES;
import static org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor.MSSQL;
import static org.infinispan.cdc.internal.url.DatabaseConstants.DATABASE_DEFAULT_HOST;
import static org.infinispan.cdc.internal.url.DatabaseConstants.MYSQL_DEFAULT_PORT;
import static org.infinispan.cdc.internal.url.DatabaseConstants.POSTGRES_DEFAULT_PORT;
import static org.infinispan.cdc.internal.url.DatabaseConstants.SQLSERVER_DEFAULT_PORT;

import java.util.stream.Stream;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DatabaseURLTest {

   @ParameterizedTest
   @MethodSource("databaseUrlsArguments")
   public void testUrlParsing(String param, DatabaseVendor vendor, String host, String port, String schema) {
      DatabaseURL url = DatabaseURL.create(param);
      assertThat(url.vendor()).isEqualTo(vendor);
      assertThat(url.host()).isEqualTo(host);
      assertThat(url.port()).isEqualTo(port);
      assertThat(url.database()).isEqualTo(schema);
   }

   @Test
   public void testNotAcceptedDatabase() {
      String h2 = "jdbc:h2:mem:CDC";
      assertThatThrownBy(() -> DatabaseURL.create(h2))
            .isInstanceOf(IllegalArgumentException.class);
   }

   static Stream<Arguments> databaseUrlsArguments() {
      return Stream.of(
            Arguments.arguments("jdbc:mysql://username:password@127.0.0.1:1234/change-data-capture", MYSQL, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:mysql://127.0.0.1:1234/change-data-capture", MYSQL, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:mysql://127.0.0.1/change-data-capture", MYSQL, "127.0.0.1", MYSQL_DEFAULT_PORT, "change-data-capture"),
            Arguments.arguments("jdbc:postgres://127.0.0.1:1234/change-data-capture", POSTGRES, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:postgres://username:password@127.0.0.1:1234/change-data-capture?param=true", POSTGRES, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:postgres:/", POSTGRES, DATABASE_DEFAULT_HOST, POSTGRES_DEFAULT_PORT, ""),
            Arguments.arguments("jdbc:postgres://127.0.0.1/change-data-capture", POSTGRES, "127.0.0.1", POSTGRES_DEFAULT_PORT, "change-data-capture"),
            Arguments.arguments("jdbc:oracle:thin:@127.0.0.1:1234:change-data-capture", ORACLE, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:oracle:thin:username/password@127.0.0.1:1234:change-data-capture", ORACLE, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:oracle:thin:username/password@//127.0.0.1:1234/change-data-capture", ORACLE, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:oracle:thin:@//127.0.0.1:1234/change-data-capture", ORACLE, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:sqlserver://127.0.0.1:1234;encrypt=true;databaseName=change-data-capture;", MSSQL, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:sqlserver://127.0.0.1;encrypt=true;databaseName=change-data-capture;portNumber=1234", MSSQL, "127.0.0.1", "1234", "change-data-capture"),
            Arguments.arguments("jdbc:sqlserver://;encrypt=true;databaseName=change-data-capture;serverName=localhost", MSSQL, DATABASE_DEFAULT_HOST, SQLSERVER_DEFAULT_PORT, "change-data-capture"),
            Arguments.arguments("jdbc:db2://127.0.0.1:1234/change-data-capture", DB2, "127.0.0.1", "1234", "change-data-capture")
      );
   }
}
