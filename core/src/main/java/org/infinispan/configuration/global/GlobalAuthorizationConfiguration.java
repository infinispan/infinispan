package org.infinispan.configuration.global;

import java.util.Map;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.impl.NullAuditLogger;

/**
 * GlobalAuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<AuditLogger> AUDIT_LOGGER = AttributeDefinition.builder("auditLogger", (AuditLogger)new NullAuditLogger()).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   static final AttributeDefinition<PrincipalRoleMapper> PRINCIPAL_ROLE_MAPPER = AttributeDefinition.builder("principalRoleMapper", null, PrincipalRoleMapper.class).immutable().build();
   static final AttributeDefinition<Map> ROLES = AttributeDefinition.builder("roles", null, Map.class).build();

   static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalAuthorizationConfiguration.class, ENABLED, AUDIT_LOGGER, PRINCIPAL_ROLE_MAPPER, ROLES);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<AuditLogger> auditLogger;
   private final Attribute<PrincipalRoleMapper> principalRoleMapper;
   private final Attribute<Map> roles;

   private final AttributeSet attributes;

   public GlobalAuthorizationConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      auditLogger = attributes.attribute(AUDIT_LOGGER);
      principalRoleMapper = attributes.attribute(PRINCIPAL_ROLE_MAPPER);
      roles = attributes.attribute(ROLES);
   }

   public boolean enabled() {
      return enabled.get();
   }


   public AuditLogger auditLogger() {
      return auditLogger.get();
   }

   public PrincipalRoleMapper principalRoleMapper() {
      return principalRoleMapper.get();
   }

   public Map<String, Role> roles() {
      return roles.get();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "GlobalAuthorizationConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      GlobalAuthorizationConfiguration other = (GlobalAuthorizationConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }
}
