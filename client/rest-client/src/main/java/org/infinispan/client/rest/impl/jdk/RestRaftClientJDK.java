package org.infinispan.client.rest.impl.jdk;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestRaftClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @since 15.1
 */
final class RestRaftClientJDK implements RestRaftClient {
   private final RestRawClientJDK client;
   private final String path;

   RestRaftClientJDK(RestRawClientJDK client) {
      this.client = client;
      this.path = client.getConfiguration().contextPath() + "/v2/cluster/raft";
   }

   @Override
   public CompletionStage<RestResponse> addMember(String raftId) {
      return client.post(String.format("%s/%s", path, validateAndSanitize(raftId)));
   }

   @Override
   public CompletionStage<RestResponse> removeMember(String raftId) {
      return client.delete(String.format("%s/%s", path, validateAndSanitize(raftId)));
   }

   @Override
   public CompletionStage<RestResponse> currentMembers() {
      return client.get(path);
   }

   private static String validateAndSanitize(String raftId) {
      Objects.requireNonNull(raftId, "Raft ID must be non null");
      return Util.sanitize(raftId);
   }
}
