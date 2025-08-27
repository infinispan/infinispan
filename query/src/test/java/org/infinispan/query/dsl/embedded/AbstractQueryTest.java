package org.infinispan.query.dsl.embedded;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.TimeZone;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.ModelFactoryHS;
import org.infinispan.query.objectfilter.impl.util.DateHelper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;

/**
 * Base for query tests.
 *
 * @author rvansa@redhat.com
 * @author anistor@redhat.com
 * @since 6.0
 */
@CleanupAfterTest
public abstract class AbstractQueryTest extends MultipleCacheManagersTest {

   protected final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

   protected AbstractQueryTest() {
      DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
   }

   protected Date makeDate(String dateStr) throws ParseException {
      return DATE_FORMAT.parse(dateStr);
   }

   protected String instant(Instant instant) {
      return "'" + instant.toString() + "'";
   }

   protected String queryDate(Temporal temporal) {
      if (temporal instanceof LocalDate date) {
         return DateHelper.JPA_DATETIME_FORMATTER.format(date.atStartOfDay());
      } else if (temporal instanceof Instant i) {
         return DateHelper.JPA_DATETIME_FORMATTER.format(i.atOffset(ZoneOffset.UTC).toLocalDateTime());
      } else {
         return DateHelper.JPA_DATETIME_FORMATTER.format(temporal);
      }
   }

   protected Date toDate(Object o) {
      if (o instanceof Date d) {
         return d;
      } else if (o instanceof LocalDate ld) {
         return Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC));
      } else if (o instanceof LocalDateTime ldt) {
         return Date.from(ldt.toInstant(ZoneOffset.UTC));
      } else if (o instanceof Long l) {
         return new Date(l);
      } else if (o instanceof Instant i) {
         return new Date(i.toEpochMilli());
      } else {
         throw new IllegalArgumentException(o.getClass().getName());
      }
   }

   protected int compareDate(Object a, Object b) {
      return toDate(a).compareTo(toDate(b));
   }

   /**
    * To be overridden by subclasses.
    */
   protected BasicCache<Object, Object> getCacheForWrite() {
      return getCacheForQuery();
   }

   /**
    * To be overridden by subclasses.
    */
   protected BasicCache<Object, Object> getCacheForQuery() {
      return cache(0);
   }

   protected <T> Query<T> queryCache(String query) {
      return getCacheForQuery().query(query);
   }

   protected <T> Query<T> queryCache(String query, Object... args) {
      return getCacheForQuery().query(query.formatted(args));
   }

   /**
    * To be overridden by subclasses if they need to use a different model implementation.
    */
   protected ModelFactory getModelFactory() {
      return ModelFactoryHS.INSTANCE;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(getModelFactory().getUserImplClass())
            .addIndexedEntity(getModelFactory().getAccountImplClass())
            .addIndexedEntity(getModelFactory().getTransactionImplClass());
      createClusteredCaches(1, cfg);
   }

   @Override
   protected void clearContent() {
      // Don't clear, this is destroying the index
   }
}
