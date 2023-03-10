package org.infinispan.cli.commands.kubernetes;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import io.fabric8.kubernetes.api.model.Secret;

/**
 * @since 15.0
 **/
public class KubeTest {

   @Test
   public void testKubeSecrets() {
      Secret secret = new Secret();
      secret.setData(Collections.singletonMap("identities.yaml", "Y3JlZGVudGlhbHM6Ci0gdXNlcm5hbWU6IGFkbWluCiAgcGFzc3dvcmQ6IHBhc3N3b3JkCgo="));
      Map<String, String> map = Kube.decodeOpaqueSecrets(secret);
      assertEquals(1, map.size());
      Map.Entry<String, String> next = map.entrySet().iterator().next();
      assertEquals("admin", next.getKey());
      assertEquals("password", next.getValue());
   }
}
