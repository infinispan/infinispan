package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * Interacts with the Raft configuration.
 *
 * @author José Bolina
 * @since 15.1
 */
public interface RestRaftClient {

   CompletionStage<RestResponse> addMember(String raftId);

   CompletionStage<RestResponse> removeMember(String raftId);

   CompletionStage<RestResponse> currentMembers();
}
