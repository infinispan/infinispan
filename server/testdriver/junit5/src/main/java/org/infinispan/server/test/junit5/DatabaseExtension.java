package org.infinispan.server.test.junit5;

import org.infinispan.server.test.core.persistence.DatabaseServerListener;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * <a href="https://junit.org/junit5">JUnit 5</a> extension to use databases during tests.
 *
 * <p>
 * This extension can be utilized to start a database for tests. The argument requires an instance of
 * {@link DatabaseServerListener} to handle the start and stop. For example:
 *
 * <pre>
 * {@code
 * private static final String[] DEFAULT_DATABASES = { "mysql", "postgres" };
 * public static final DatabaseServerListener DATABASE_LISTENER = new DatabaseServerListener(DEFAULT_DATABASES);
 *
 * @RegisterExtension
 * public static DatabaseExtension DATABASES = new DatabaseExtension(DATABASE_LISTENER);
 * }
 * </pre>
 *
 * Then utilize an argument provider to inject the database into the test method.
 *
 * <pre>
 * {@code
 * public static class ArgumentsProvider implements org.junit.jupiter.params.provider.ArgumentsProvider {
 *    @Override
 *    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
 *       return Arrays.stream(DATABASE_LISTENER.getDatabaseTypes())
 *                .map(DATABASE_LISTENER::getDatabase)
 *                .map(Arguments::of);
 *    }
 * }
 *
 * @ParameterizedTest
 * @ArgumentsSource(ArgumentsProvider.class)
 * public void testInjectingDatabase(Database database) { }
 * }
 * </pre>
 * </p>
 *
 * @since 15.2
 * @author José Bolina
 */
public class DatabaseExtension implements BeforeAllCallback, AfterAllCallback {

   private final DatabaseServerListener handler;

   public DatabaseExtension(DatabaseServerListener handler) {
      this.handler = handler;
   }

   @Override
   public void afterAll(ExtensionContext context) {
      handler.after(null);
   }

   @Override
   public void beforeAll(ExtensionContext context) {
      handler.before(null);
   }

}
