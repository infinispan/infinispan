package org.infinispan.server.configuration.security;

import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.spec.AlgorithmParameterSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.ElytronPasswordProviderSupplier;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.source.CredentialSource;
import org.wildfly.security.credential.source.impl.CommandCredentialSource;
import org.wildfly.security.credential.store.CredentialStore;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.CredentialStoreSpi;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.WildFlyElytronPasswordProvider;
import org.wildfly.security.password.interfaces.ClearPassword;
import org.wildfly.security.util.PasswordBasedEncryptionUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoreConfiguration extends ConfigurationElement<CredentialStoresConfiguration> {
   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   public static final AttributeDefinition<String> TYPE = AttributeDefinition.builder(Attribute.TYPE, "pkcs12", String.class).build();
   static final AttributeDefinition<Supplier<CredentialSource>> CREDENTIAL = AttributeDefinition.builder(Attribute.CREDENTIAL, null, (Class<Supplier<CredentialSource>>) (Class<?>) Supplier.class)
         .serializer((writer, name, value) -> {
            ((AttributeSerializer<CredentialSource>) value).serialize(writer, name, null);
         }).build();

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
         try {
            CredentialSource credential = attributes.attribute(CREDENTIAL).get().get();
            credentialStore.initialize(
                  map,
                  new CredentialStore.CredentialSourceProtectionParameter(
                        IdentityCredentials.NONE.withCredential(credential.getCredential(PasswordCredential.class))),
                  ElytronPasswordProviderSupplier.PROVIDERS
            );
         } catch (Exception e) {
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

   public static class ClearTextCredentialSource implements CredentialSource {

      final char[] secret;

      public ClearTextCredentialSource(final char[] secret) {
         this.secret = secret;
      }

      @Override
      public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
         return credentialType == PasswordCredential.class ? SupportLevel.SUPPORTED : SupportLevel.UNSUPPORTED;
      }

      @Override
      public <C extends Credential> C getCredential(Class<C> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
         return credentialType.cast(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, secret)));
      }
   }

   public static class ClearTextCredentialSupplier implements Supplier<CredentialSource>, AttributeSerializer<ClearTextCredentialSupplier> {
      private final ClearTextCredentialSource credential;

      public ClearTextCredentialSupplier(char[] credential) {
         this.credential = new ClearTextCredentialSource(credential);
      }

      @Override
      public CredentialSource get() {
         return credential;
      }

      @Override
      public void serialize(ConfigurationWriter writer, String name, ClearTextCredentialSupplier value) {
         writer.writeStartElement(Element.CLEAR_TEXT_CREDENTIAL);
         if (writer.clearTextSecrets()) {
            writer.writeAttribute(Attribute.CLEAR_TEXT, new String(credential.secret));
         } else {
            writer.writeAttribute(name, "***");
         }
         writer.writeEndElement();
      }
   }

   public static class MaskedCredentialSupplier implements Supplier<CredentialSource>, AttributeSerializer<MaskedCredentialSupplier> {
      private final CredentialSource credential;
      private final String masked;

      public MaskedCredentialSupplier(String masked) {
         this.masked = masked;
         String[] part = masked.split(";");
         if (part.length != 3) {
            throw Server.log.wrongMaskedPasswordFormat();
         }
         String salt = part[1];
         final int iterationCount;
         try {
            iterationCount = Integer.parseInt(part[2]);
         } catch (NumberFormatException e) {
            throw Server.log.wrongMaskedPasswordFormat();
         }
         try {
            PasswordBasedEncryptionUtil pbe = new PasswordBasedEncryptionUtil.Builder()
                  .picketBoxCompatibility()
                  .salt(salt)
                  .iteration(iterationCount)
                  .decryptMode()
                  .build();
            credential = new ClearTextCredentialSource(pbe.decodeAndDecrypt(part[0]));
         } catch (GeneralSecurityException e) {
            throw new CacheConfigurationException(e);
         }
      }

      @Override
      public CredentialSource get() {
         return credential;
      }

      @Override
      public void serialize(ConfigurationWriter writer, String name, MaskedCredentialSupplier value) {
         writer.writeStartElement(Element.MASKED_CREDENTIAL);
         writer.writeAttribute(Attribute.MASKED, masked);
         writer.writeEndElement();
      }
   }

   public static class CommandCredentialSupplier implements Supplier<CredentialSource>, AttributeSerializer<CommandCredentialSupplier> {
      private final String command;
      private final CommandCredentialSource source;

      public CommandCredentialSupplier(String command) {
         this.command = command;
         CommandCredentialSource.Builder builder = CommandCredentialSource.builder();
         builder.setPasswordFactoryProvider(WildFlyElytronPasswordProvider.getInstance());
         // comma can be back slashed
         final String[] parsedCommands = command.split("(?<!\\\\) ");
         for (String parsedCommand : parsedCommands) {
            if (parsedCommand.indexOf('\\') != -1) {
               builder.addCommand(parsedCommand.replaceAll("\\\\ ", " "));
            } else {
               builder.addCommand(parsedCommand);
            }
         }
         try {
            this.source = builder.build();
         } catch (GeneralSecurityException e) {
            throw new CacheConfigurationException(e);
         }
      }

      @Override
      public CredentialSource get() {
         return source;
      }

      @Override
      public void serialize(ConfigurationWriter writer, String name, CommandCredentialSupplier value) {
         writer.writeStartElement(Element.COMMAND_CREDENTIAL);
         writer.writeAttribute(Attribute.COMMAND, command);
         writer.writeEndElement();
      }
   }

}
