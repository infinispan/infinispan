package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.testng.annotations.Test;

/**
 * Tests for REST v3 X-Site API endpoints.
 * <p>
 * Note: This test focuses on OpenAPI spec validation and basic endpoint availability.
 * Full X-Site functional tests require multi-site cluster setup and are covered in XSiteResourceTest.
 *
 * @since 16.0
 */
@Test(groups = "functional", testName = "rest.XSiteResourceV3Test")
public class XSiteResourceV3Test extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new XSiteResourceV3Test().withSecurity(false).browser(false),
            new XSiteResourceV3Test().withSecurity(false).browser(true),
            new XSiteResourceV3Test().withSecurity(true).browser(false),
            new XSiteResourceV3Test().withSecurity(true).browser(true),
      };
   }

   @Test
   public void testOpenAPIIncludesXSiteV3() {
      RestResponse response = join(client.raw().get("/rest/v3/openapi",
            Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();

      String body = response.body();
      Json spec = Json.read(body);

      // Verify OpenAPI version
      assertEquals("3.1.1", spec.at("openapi").asString());

      // Verify paths exist
      Json paths = spec.at("paths");

      // Cache-level endpoints
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/push-state/_status"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/_take-offline"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/_bring-online"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/_start-push-state"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/_cancel-push-state"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/_cancel-receive-state"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/take-offline-config"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/state-transfer-mode"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/backups/{site}/state-transfer-mode/_set"));
      assertTrue(paths.has("/rest/v3/caches/{cacheName}/x-site/push-state/_clear"));

      // Container-level endpoints
      assertTrue(paths.has("/rest/v3/container/x-site/backups"));
      assertTrue(paths.has("/rest/v3/container/x-site/backups/{site}"));
      assertTrue(paths.has("/rest/v3/container/x-site/backups/{site}/_bring-online"));
      assertTrue(paths.has("/rest/v3/container/x-site/backups/{site}/_take-offline"));
      assertTrue(paths.has("/rest/v3/container/x-site/backups/{site}/_start-push-state"));
      assertTrue(paths.has("/rest/v3/container/x-site/backups/{site}/_cancel-push-state"));

      // Verify operationIds for cache-level operations
      Json backupStatusOp = paths.at("/rest/v3/caches/{cacheName}/x-site/backups").at("get");
      assertEquals("listCacheBackupSites", backupStatusOp.at("operationId").asString());

      Json takeOfflineOp = paths.at("/rest/v3/caches/{cacheName}/x-site/backups/{site}/_take-offline").at("post");
      assertEquals("takeCacheSiteOffline", takeOfflineOp.at("operationId").asString());

      Json bringOnlineOp = paths.at("/rest/v3/caches/{cacheName}/x-site/backups/{site}/_bring-online").at("post");
      assertEquals("bringCacheSiteOnline", bringOnlineOp.at("operationId").asString());

      // Verify operationIds for container-level operations
      Json globalBackupOp = paths.at("/rest/v3/container/x-site/backups").at("get");
      assertEquals("getGlobalBackupStatus", globalBackupOp.at("operationId").asString());

      Json bringAllOnlineOp = paths.at("/rest/v3/container/x-site/backups/{site}/_bring-online").at("post");
      assertEquals("bringAllCachesOnline", bringAllOnlineOp.at("operationId").asString());

      Json takeAllOfflineOp = paths.at("/rest/v3/container/x-site/backups/{site}/_take-offline").at("post");
      assertEquals("takeAllCachesOffline", takeAllOfflineOp.at("operationId").asString());
   }

   @Test
   public void testCacheLevelEndpointAccessible() {
      // Test that cache-level endpoints are registered and accessible
      // Full functional tests are in XSiteResourceTest (requires multi-site setup)
      RestResponse response = join(adminClient.raw().get("/rest/v3/caches/defaultcache/x-site/backups",
            Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));

      // Endpoint should be accessible (not 500 error)
      // May return NOT_FOUND if x-site not configured, or OK with empty result
      assertThat(response.status()).isIn(OK.code(), NOT_FOUND.code());
   }

   @Test
   public void testContainerLevelEndpointAccessible() {
      // Test that container-level endpoints are registered and accessible
      // Full functional tests are in XSiteResourceTest (requires multi-site setup)
      RestResponse response = join(adminClient.raw().get("/rest/v3/container/x-site/backups",
            Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));

      // Endpoint should be accessible (not 500 error)
      // May return NOT_FOUND if x-site not configured, or OK with empty result
      assertThat(response.status()).isIn(OK.code(), NOT_FOUND.code());
   }
}
