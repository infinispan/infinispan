package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ClassAttributeSerializer;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.audit.NullAuditLogger;
import org.infinispan.security.impl.CacheRoleImpl;

/**
 * GlobalAuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfiguration implements ConfigurationInfo {
   private static final Map<String, Role> DEFAULT_ROLES;
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<Long> CACHE_SIZE = AttributeDefinition.builder("cacheSize", 1_000l).immutable().build();
   public static final AttributeDefinition<AuditLogger> AUDIT_LOGGER = AttributeDefinition.builder("auditLogger", (AuditLogger) new NullAuditLogger()).copier(IdentityAttributeCopier.INSTANCE).serializer(ClassAttributeSerializer.INSTANCE).immutable().build();
   public static final AttributeDefinition<Map<String, Role>> ROLES = AttributeDefinition.<Map<String, Role>>builder("roles", new HashMap<>())
         .serializer(new AttributeSerializer<Map<String, Role>, GlobalAuthorizationConfiguration, ConfigurationBuilderInfo>() {
            @Override
            public Object getSerializationValue(Attribute<Map<String, Role>> attribute, GlobalAuthorizationConfiguration configurationElement) {
               if (!configurationElement.enabled()) return null;
               return attribute.get().entrySet().stream()
                     .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPermissions()));
            }
         }).initializer(new AttributeInitializer<Map<String, Role>>() {
            @Override
            public Map<String, Role> initialize() {
               return DEFAULT_ROLES;
            }
         }).build();
   private final Attribute<Long> cacheSize;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalAuthorizationConfiguration.class, ENABLED, CACHE_SIZE, AUDIT_LOGGER, ROLES);
   }

   static {
      Map<String, Role> roles = new HashMap<>();
      roles.put("admin", new CacheRoleImpl("admin", true, AuthorizationPermission.ALL));
      roles.put("application", new CacheRoleImpl("application", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.ALL_WRITE,
            AuthorizationPermission.LISTEN,
            AuthorizationPermission.EXEC,
            AuthorizationPermission.MONITOR
      ));
      roles.put("deployer", new CacheRoleImpl("deployer", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.ALL_WRITE,
            AuthorizationPermission.LISTEN,
            AuthorizationPermission.EXEC,
            AuthorizationPermission.CREATE,
            AuthorizationPermission.MONITOR
      ));
      roles.put("observer", new CacheRoleImpl("observer", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.MONITOR
      ));
      roles.put("monitor", new CacheRoleImpl("monitor", true,
            AuthorizationPermission.MONITOR
      ));
      // Deprecated roles. Will be removed in Infinispan 16.0
      roles.put("___schema_manager", new CacheRoleImpl("___schema_manager", false,
            AuthorizationPermission.CREATE
      ));
      roles.put("___script_manager", new CacheRoleImpl("___script_manager", false,
            AuthorizationPermission.CREATE
      ));
      DEFAULT_ROLES = Collections.unmodifiableMap(roles);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.AUTHORIZATION.getLocalName());

   private final Attribute<Boolean> enabled;
   private final Attribute<AuditLogger> auditLogger;
   private final Attribute<Map<String, Role>> roles;
   private final PrincipalRoleMapperConfiguration roleMapperConfiguration;

   private final AttributeSet attributes;
   private final List<ConfigurationInfo> subElements;

   public GlobalAuthorizationConfiguration(AttributeSet attributes, PrincipalRoleMapperConfiguration roleMapperConfiguration) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.cacheSize = attributes.attribute(CACHE_SIZE);
      this.auditLogger = attributes.attribute(AUDIT_LOGGER);
      this.roles = attributes.attribute(ROLES);
      this.roleMapperConfiguration = roleMapperConfiguration;
      this.subElements = Collections.singletonList(roleMapperConfiguration);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public boolean enabled() {
      return enabled.get();
   }

   public long cacheSize() {
      return cacheSize.get();
   }

   public AuditLogger auditLogger() {
      return auditLogger.get();
   }

   public PrincipalRoleMapper principalRoleMapper() {
      return roleMapperConfiguration.roleMapper();
   }

   public PrincipalRoleMapperConfiguration roleMapperConfiguration() {
      return roleMapperConfiguration;
   }

   public Map<String, Role> roles() {
      return roles.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "GlobalAuthorizationConfiguration{" +
            "roleMapperConfiguration=" + roleMapperConfiguration +
            ", attributes=" + attributes +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalAuthorizationConfiguration that = (GlobalAuthorizationConfiguration) o;

      if (roleMapperConfiguration != null ? !roleMapperConfiguration.equals(that.roleMapperConfiguration) : that.roleMapperConfiguration != null)
         return false;
      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      int result = roleMapperConfiguration != null ? roleMapperConfiguration.hashCode() : 0;
      result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
      return result;
   }
}
