package org.infinispan.server.security;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.Attributes.Entry;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.RoleDecoder;
import org.wildfly.security.authz.Roles;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KeycloakRoleDecoder implements RoleDecoder {

    private static final String CLAIM_REALM_ACCESS = "realm_access";
    private static final String CLAIM_RESOURCE_ACCESS = "resource_access";
    private static final String CLAIM_ROLES = "roles";
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Roles decodeRoles(AuthorizationIdentity identity) {
        Attributes attributes = identity.getAttributes();
        Entry realmAccess = attributes.get(CLAIM_REALM_ACCESS);
        Set<String> roleSet = new HashSet<>();

        if (realmAccess != null) {
            String realmAccessValue = realmAccess.get(0);

            try {
                Map<String, List<String>> jsonNode = mapper.readValue(realmAccessValue, Map.class);
                List<String> roles = jsonNode.get(CLAIM_ROLES);

                if (roles != null) {
                    roleSet.addAll(roles);
                }
            } catch (IOException cause) {
                throw new RuntimeException("Failed to decode realm access roles", cause);
            }
        }

        Entry resourceAccess = attributes.get(CLAIM_RESOURCE_ACCESS);

        if (resourceAccess != null) {
            for(String resource : resourceAccess) {
                try {
                    Map<String, Map<String, List<String>>> resources = mapper.readValue(resource, Map.class);

                    for (String resourceKey : resources.keySet()) {
                        List<String> roles = resources.get(resourceKey).get("roles");

                        if (roles != null) {
                            roleSet.addAll(roles);
                        }
                    }
                } catch (IOException cause) {
                    throw new RuntimeException("Failed to decode resource access roles", cause);
                }
            }
        }

        return Roles.fromSet(roleSet);
    }
}
