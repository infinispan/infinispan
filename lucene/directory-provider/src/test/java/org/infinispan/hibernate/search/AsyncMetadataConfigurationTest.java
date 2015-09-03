package org.infinispan.hibernate.search;

import org.hibernate.search.spi.SearchIntegratorBuilder;
import org.hibernate.search.test.util.HibernateManualConfiguration;
import org.hibernate.search.testsupport.BytemanHelper;
import org.hibernate.search.testsupport.setup.SearchConfigurationForTest;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the {@link org.infinispan.hibernate.search.spi.InfinispanIntegration#WRITE_METADATA_ASYNC} setting is
 * correctly applied to the Infinispan directory.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @author Gunnar Morling
 * @since 5.0
 */
@RunWith(BMUnitRunner.class)
public class AsyncMetadataConfigurationTest {

   @Test
   @BMRule(targetClass = "org.infinispan.lucene.impl.DirectoryBuilderImpl",
         targetMethod = "create",
         helper = "org.hibernate.search.testsupport.BytemanHelper",
         action = "assertBooleanValue($0.writeFileListAsync, true); countInvocation();",
         name = "verifyAsyncMetadataOptionApplied")
   public void verifyAsyncMetadataOptionApplied() throws Exception {
      buildSearchFactoryWithAsyncOption(false, true);
   }

   @Test
   @BMRule(targetClass = "org.infinispan.lucene.impl.DirectoryBuilderImpl",
         targetMethod = "create",
         helper = "org.hibernate.search.testsupport.BytemanHelper",
         action = "assertBooleanValue($0.writeFileListAsync, false); countInvocation();",
         name = "verifyAsyncMetadataDisabledByDefault")
   public void verifyAsyncMetadataDisabledByDefault() throws Exception {
      buildSearchFactoryWithAsyncOption(false, null);
   }

   @Test
   @BMRule(targetClass = "org.infinispan.lucene.impl.DirectoryBuilderImpl",
         targetMethod = "create",
         helper = "org.hibernate.search.testsupport.BytemanHelper",
         action = "assertBooleanValue($0.writeFileListAsync, false); countInvocation();",
         name = "verifyAsyncMetadataOptionExplicitDisabled")
   public void verifyAsyncMetadataOptionExplicitDisabled() throws Exception {
      buildSearchFactoryWithAsyncOption(false, false);
   }

   @Test
   @BMRule(targetClass = "org.infinispan.lucene.impl.DirectoryBuilderImpl",
         targetMethod = "create",
         helper = "org.hibernate.search.testsupport.BytemanHelper",
         action = "assertBooleanValue($0.writeFileListAsync, true); countInvocation();",
         name = "verifyAsyncMetadataEnabledByDefaultForAsyncBackend")
   public void verifyAsyncMetadataOptionEnabledByDefaultForAsyncBackend() throws Exception {
      buildSearchFactoryWithAsyncOption(true, null);
   }

   @Test
   @BMRule(targetClass = "org.infinispan.lucene.impl.DirectoryBuilderImpl",
         targetMethod = "create",
         helper = "org.hibernate.search.testsupport.BytemanHelper",
         action = "assertBooleanValue($0.writeFileListAsync, false); countInvocation();",
         name = "verifyAsyncMetadataOptionExplicitlyDisabledForAsyncBackend")
   public void verifyAsyncMetadataOptionExplicitlyDisabledForAsyncBackend() throws Exception {
      buildSearchFactoryWithAsyncOption(true, false);
   }

   private void buildSearchFactoryWithAsyncOption(Boolean backendAsync, Boolean async) {
      SearchConfigurationForTest configuration = new HibernateManualConfiguration()
            .addClass(SimpleEmail.class)
            .addProperty("hibernate.search.default.directory_provider", "infinispan")
            .addProperty("hibernate.search.infinispan.configuration_resourcename", "localonly-infinispan.xml");

      if (backendAsync != null) {
         configuration.addProperty("hibernate.search.default.worker.execution", backendAsync ? "async" : "sync");
      }

      if (async != null) {
         configuration.addProperty("hibernate.search.default.write_metadata_async", async.toString());
      }

      new SearchIntegratorBuilder().configuration(configuration).buildSearchIntegrator();
      assertEquals("The directory provider was not started", 1, BytemanHelper.getAndResetInvocationCount());
   }
}
