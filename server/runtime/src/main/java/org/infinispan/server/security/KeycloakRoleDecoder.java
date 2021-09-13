package org.infinispan.server.security;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.dataconversion.internal.Json;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.Attributes.Entry;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.RoleDecoder;
import org.wildfly.security.authz.Roles;

public class KeycloakRoleDecoder implements RoleDecoder {

   private static final String CLAIM_REALM_ACCESS = "realm_access";
   private static final String CLAIM_RESOURCE_ACCESS = "resource_access";
   private static final String CLAIM_ROLES = "roles";

   @Override
   public Roles decodeRoles(AuthorizationIdentity identity) {
      Attributes attributes = identity.getAttributes();
      Entry realmAccess = attributes.get(CLAIM_REALM_ACCESS);
      Set<String> roleSet = new HashSet<>();

      if (realmAccess != null && !realmAccess.isEmpty()) {
         String realmAccessValue = realmAccess.get(0);
         Json json = Json.read(realmAccessValue);
         Json rolesValue = json.at(CLAIM_ROLES);
         if (rolesValue != null) {
            for (Object role : rolesValue.asList()) {
               roleSet.add(role.toString());
            }
         }
      }

      Entry resourceAccess = attributes.get(CLAIM_RESOURCE_ACCESS);

      if (resourceAccess != null) {
         for (String resource : resourceAccess) {
            Map<String, Json> resources = Json.read(resource).asJsonMap();

            for (String resourceKey : resources.keySet()) {
               Json roles = resources.get(resourceKey).at("roles");

               if (roles != null) {
                  for (Object role : roles.asList()) {
                     roleSet.add(role.toString());
                  }
               }
            }
         }
      }
      return Roles.fromSet(roleSet);
   }
}
