package org.infinispan.client.hotrod.marshall;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.Collections;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.company.FootballSchemaImpl;
import org.infinispan.client.hotrod.query.testdomain.protobuf.company.FootballTeam;
import org.infinispan.client.hotrod.query.testdomain.protobuf.company.Player;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.exception.ProtoStreamException;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.marshall.CircularReferencesMarshallTest")
@TestForIssue(jiraKey = "ISPN-14687")
public class CircularReferencesMarshallTest extends SingleHotRodServerTest {

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return new FootballSchemaImpl();
   }

   @Test
   public void testCircularity() {
      FootballTeam footBallTeam = new FootballTeam();
      footBallTeam.setName("New-Team");
      Player player = new Player("fax4ever", footBallTeam);
      footBallTeam.setPlayers(Collections.singletonList(player));

      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();

      assertThatThrownBy(() -> remoteCache.put("ciao", footBallTeam)).isInstanceOf(ProtoStreamException.class)
            .hasMessageContaining("IPROTO000008");
   }
}
