package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponseInfo.OK;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.rest.helper.RestResponses.assertStatus;
import static org.infinispan.testing.TestResourceTracker.getCurrentTestShortName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "rest.RaftResourceTest")
public class RaftResourceTest extends AbstractRestResourceTest {

   private final List<String> allRaftMembers = new ArrayList<>();

   @Override
   public Object[] factory() {
      return new Object[] {
            new RaftResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(false),
            new RaftResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(true),
            new RaftResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
            new RaftResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(true),
            new RaftResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(false),
            new RaftResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(true),
            new RaftResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(false),
            new RaftResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(true),
      };
   }

   @Override
   protected void createCacheManagers() throws Exception {
      allRaftMembers.clear();

      for (int i = 0; i < NUM_SERVERS; i++) {
         allRaftMembers.add("node-" + getCurrentTestShortName() + Character.toString('A' + i));
      }
      super.createCacheManagers();
   }

   @Override
   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder builder = super.getGlobalConfigForNode(id);
      builder.transport().nodeName(allRaftMembers.get(id));
      builder.transport().raftMembers(allRaftMembers);
      return builder;
   }

   @Test
   public void testMembershipChanges() {
      // Contains only the initial members.
      assertMembership(allRaftMembers);

      // Add new member and verify.
      String raftId = "my-new-member";
      assertStatus(OK, adminClient.raft().addMember(raftId));
      assertMembership(allRaftMembers, raftId);

      // Remove the member and verify its back to the initial configuration.
      assertStatus(OK, adminClient.raft().removeMember(raftId));
      assertMembership(allRaftMembers);
   }

   private void assertMembership(List<String> members, String ... additional) {
      List<String> allMembers = new ArrayList<>(members);
      allMembers.addAll(List.of(additional));

      CompletionStage<RestResponse> res = adminClient.raft().currentMembers();
      try (RestResponse r = await(res)) {
         assertThat(r.status()).isEqualTo(OK);
         List<Json> actual = Json.read(r.body()).asJsonList();
         assertThat(actual).hasSameSizeAs(allMembers);

         for (Json json : actual) {
            assertThat(allMembers).contains(json.asString());
         }
      }
   }
}
