package org.jboss.as.clustering.infinispan.subsystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.AuthorizationConfiguration;
import org.jboss.msc.value.Value;

/**
 * AuthorizationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class AuthorizationConfigurationBuilder implements Value<AuthorizationConfiguration>, AuthorizationConfiguration {
    private String principalMapper;
    private String auditLogger;
    private Map<String, List<String>> roles = new HashMap<>();

    public void setPrincipalMapper(String principalMapper) {
        this.principalMapper = principalMapper;
    }

    public void setAuditLogger(String auditLogger) {
       this.auditLogger = auditLogger;
    }

    public void setRoles(Map<String, List<String>> roles) {
        this.roles = roles;
    }

    @Override
    public String getPrincipalMapper() {
        return principalMapper;
    }

    public String getAuditLogger(){
       return auditLogger;
    }

    @Override
    public Map<String, List<String>> getRoles() {
        return roles;
    }

    @Override
    public AuthorizationConfiguration getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }
}
