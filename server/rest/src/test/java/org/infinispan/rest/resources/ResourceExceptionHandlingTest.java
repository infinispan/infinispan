package org.infinispan.rest.resources;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestResponse;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.ResourceExceptionHandlingTest")
public class ResourceExceptionHandlingTest extends AbstractRestResourceTest {

   public void testHeadRequestHasContentLengthZero() {
      // All Backup/Restore operations will fail with a NPE as ServerManagement#getBackupManager returns null
      RestCacheManagerClient cacheManager = client.cacheManager("default");
      String backupName = "failure";
      // GET request, expect content
      CompletionStage<RestResponse> response = cacheManager.getBackup(backupName, false);
      assertThat(response).bodyNotEmpty();

      // HEAD request, no content should be returned and content-length should be 0
      response = cacheManager.getBackup(backupName, true);
      assertThat(response)
            .isError()
            .hasNoContent()
            .hasContentLength(0);
   }
}
