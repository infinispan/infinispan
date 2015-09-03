package org.infinispan.hibernate.search;

import org.hibernate.search.annotations.DocumentId;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.cfg.spi.DirectoryProviderService;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.spi.SearchIntegratorBuilder;
import org.hibernate.search.testsupport.setup.SearchConfigurationForTest;
import org.infinispan.hibernate.search.impl.DefaultCacheManagerService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;


/**
 * Test to verify the configuration property {@link DefaultCacheManagerService#INFINISPAN_TRANSPORT_OVERRIDE_RESOURCENAME}
 * is not ignored.
 *
 * @author Sanne Grinovero
 * @since 5.0
 */
public class OverrideConfigurationAppliedTest {

   @Rule
   public ExpectedException exceptions = ExpectedException.none();

   @Test
   public void testOverrideOptionGetsApplied() throws IOException {
      SearchConfigurationForTest cfg = new SearchConfigurationForTest()
            .addProperty("hibernate.search.default.directory_provider", "infinispan")
            .addProperty(DefaultCacheManagerService.INFINISPAN_TRANSPORT_OVERRIDE_RESOURCENAME, "not existing")
            .addClass(Dvd.class);

      //The most practical way to figure out if the property was applied is to provide it with
      //an illegal value to then verify the failure.
      exceptions.expect(SearchException.class);
      exceptions.expectMessage("HSEARCH000103");
      new SearchIntegratorBuilder().configuration(cfg).buildSearchIntegrator();
   }

   @Indexed(index = "index1")
   public static final class Dvd {
      @DocumentId
      long id;
      @Field
      String title;
   }

}
