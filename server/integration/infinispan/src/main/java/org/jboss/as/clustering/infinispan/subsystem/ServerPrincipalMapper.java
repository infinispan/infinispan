package org.jboss.as.clustering.infinispan.subsystem;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapperContext;

/**
 * ServerPrincipalMapper.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class ServerPrincipalMapper implements PrincipalRoleMapper {
    private EmbeddedCacheManager cacheManager;
    private Cache<String, Set<String>> mappingCache;
    public static final String SERVER_ROLE_MAPPING_CACHENAME = "___serverRoleMappingCache";

    @Override
    public Set<String> principalToRoles(Principal principal) {
        if (mappingCache != null) {
            return mappingCache.get(principal.getName());
        } else {
            return Collections.singleton(principal.getName());
        }
    }

    @Override
    public void setContext(PrincipalRoleMapperContext context) {
        this.cacheManager = context.getCacheManager();
        this.mappingCache = this.cacheManager.getCache(SERVER_ROLE_MAPPING_CACHENAME);
    }

}
