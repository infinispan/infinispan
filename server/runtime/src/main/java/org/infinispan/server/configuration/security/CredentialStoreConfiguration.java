package org.infinispan.server.configuration.security;

import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.CredentialStore;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.CredentialStoreSpi;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoreConfiguration extends ConfigurationElement<CredentialStoresConfiguration> {
   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   public static final AttributeDefinition<String> TYPE = AttributeDefinition.builder(Attribute.TYPE, "pkcs12", String.class).build();
   static final AttributeDefinition<Supplier<char[]>> CREDENTIAL = AttributeDefinition.builder(Attribute.CREDENTIAL, null, (Class<Supplier<char[]>>) (Class<?>) Supplier.class)
         .serializer(AttributeSerializer.SECRET).build();

   static AttributeSet attributeDefinitionSet() {
      KeyStore.getDefaultType();
      return new AttributeSet(CredentialStoreConfiguration.class, NAME, PATH, RELATIVE_TO, TYPE, CREDENTIAL);
   }

   private CredentialStoreSpi credentialStore;

   CredentialStoreConfiguration(AttributeSet attributes) {
      super(Element.CREDENTIAL_STORE, attributes);
   }

   void init(Properties properties) {
      if (credentialStore == null) {
         if (attributes.attribute(PATH).isNull()) {
            throw new IllegalStateException("file has to be specified");
         }
         String path = attributes.attribute(PATH).get();
         String relativeTo = properties.getProperty(attributes.attribute(RELATIVE_TO).get());
         String location = ParseUtils.resolvePath(path, relativeTo);
         credentialStore = new KeyStoreCredentialStore();
         final Map<String, String> map = new HashMap<>();
         map.put("location", location);
         map.put("create", "false");
         map.put("keyStoreType", attributes.attribute(TYPE).get());
         char[] credential = attributes.attribute(CREDENTIAL).get().get();
         try {
            credentialStore.initialize(
                  map,
                  new CredentialStore.CredentialSourceProtectionParameter(
                        IdentityCredentials.NONE.withCredential(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, credential)))),
                  null
            );
         } catch (CredentialStoreException e) {
            // We ignore the exception if it's about automatic creation
            if (!e.getMessage().startsWith("ELY09518")) {
               throw new CacheConfigurationException(e);
            }
         }
      }
   }

   public <C extends Credential> C getCredential(String alias, Class<C> type) {
      try {
         if (alias == null) {
            if (credentialStore.getAliases().size() == 1) {
               alias = credentialStore.getAliases().iterator().next();
            } else {
               throw Server.log.unspecifiedCredentialAlias();
            }
         }
         return credentialStore.retrieve(alias, type, null, null, null);
      } catch (CredentialStoreException e) {
         throw new CacheConfigurationException(e);
      }
   }
}
