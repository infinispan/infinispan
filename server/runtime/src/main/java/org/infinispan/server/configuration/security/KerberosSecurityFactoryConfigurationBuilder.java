package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.DEBUG;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.FAIL_CACHE;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.KEYTAB_PATH;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.MECHANISM_NAMES;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.MECHANISM_OIDS;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.MINIMUM_REMAINING_LIFETIME;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.OBTAIN_KERBEROS_TICKET;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.OPTIONS;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.PRINCIPAL;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.RELATIVE_TO;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.REQUEST_LIFETIME;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.REQUIRED;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.SERVER;
import static org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration.WRAP_GSS_CREDENTIAL;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Properties;

import org.ietf.jgss.Oid;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;
import org.wildfly.common.Assert;
import org.wildfly.security.SecurityFactory;
import org.wildfly.security.asn1.OidsUtil;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.server._private.ElytronMessages;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.source.CredentialSource;
import org.wildfly.security.mechanism.gssapi.GSSCredentialSecurityFactory;

/**
 * KerberosSecurityFactoryConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.1
 */
public class KerberosSecurityFactoryConfigurationBuilder implements Builder<KerberosSecurityFactoryConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;
   private CredentialSource credentialSource;

   KerberosSecurityFactoryConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = KerberosSecurityFactoryConfiguration.attributeDefinitionSet();
   }

   public KerberosSecurityFactoryConfigurationBuilder principal(String principal) {
      attributes.attribute(PRINCIPAL).set(principal);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder keyTabPath(String path) {
      attributes.attribute(KEYTAB_PATH).set(path);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder addOption(String name, String value) {
      attributes.attribute(OPTIONS).get().put(name, value);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder failCache(long failCache) {
      attributes.attribute(FAIL_CACHE).set(failCache);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder minimumRemainingLifetime(int minimumRemainingLifetime) {
      attributes.attribute(MINIMUM_REMAINING_LIFETIME).set(minimumRemainingLifetime);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder requestLifetime(int requestLifetime) {
      attributes.attribute(REQUEST_LIFETIME).set(requestLifetime);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder server(boolean server) {
      attributes.attribute(SERVER).set(server);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder checkKeyTab(boolean checkKeyTab) {
      attributes.attribute(REQUIRED).set(checkKeyTab);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder obtainKerberosTicket(boolean obtainKerberosTicket) {
      attributes.attribute(OBTAIN_KERBEROS_TICKET).set(obtainKerberosTicket);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder debug(boolean debug) {
      attributes.attribute(DEBUG).set(debug);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder wrapGssCredential(boolean wrapGssCredential) {
      attributes.attribute(WRAP_GSS_CREDENTIAL).set(wrapGssCredential);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder addMechanismName(String mechanismName) {
      attributes.attribute(MECHANISM_NAMES).get().add(mechanismName);
      return this;
   }

   public KerberosSecurityFactoryConfigurationBuilder addMechanismOid(String mechanismOid) {
      attributes.attribute(MECHANISM_OIDS).get().add(mechanismOid);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public KerberosSecurityFactoryConfiguration create() {
      return new KerberosSecurityFactoryConfiguration(attributes.protect(), credentialSource);
   }

   @Override
   public KerberosSecurityFactoryConfigurationBuilder read(KerberosSecurityFactoryConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   public CredentialSource build(Properties properties) {
      if (credentialSource == null) {
         String path = attributes.attribute(KEYTAB_PATH).get();
         String relativeTo = properties.getProperty(attributes.attribute(RELATIVE_TO).get());
         File keyTab = new File(ParseUtils.resolvePath(path, relativeTo));
         GSSCredentialSecurityFactory.Builder builder = GSSCredentialSecurityFactory.builder();
         builder
               .setKeyTab(keyTab)
               .setPrincipal(attributes.attribute(PRINCIPAL).get())
               .setCheckKeyTab(attributes.attribute(REQUIRED).get())
               .setDebug(attributes.attribute(DEBUG).get())
               .setIsServer(attributes.attribute(SERVER).get())
               .setObtainKerberosTicket(attributes.attribute(OBTAIN_KERBEROS_TICKET).get())
               .setWrapGssCredential(attributes.attribute(WRAP_GSS_CREDENTIAL).get())
               .setOptions(attributes.attribute(OPTIONS).get())
               .setFailCache(attributes.attribute(FAIL_CACHE).get())
               .setRequestLifetime(attributes.attribute(REQUEST_LIFETIME).get())
               .setMinimumRemainingLifetime(attributes.attribute(MINIMUM_REMAINING_LIFETIME).get())
         ;
         try {
            for (String name : attributes.attribute(MECHANISM_NAMES).get()) {
               String oid = OidsUtil.attributeNameToOid(OidsUtil.Category.GSS, name);
               builder.addMechanismOid(new Oid(oid));

            }
            for (String oid : attributes.attribute(MECHANISM_OIDS).get()) {

               builder.addMechanismOid(new Oid(oid));
            }
            credentialSource = fromSecurityFactory(builder.build());
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      }
      return credentialSource;
   }

   // Copy of Elytron's CredentialSource.fromSecurityFactory() to workaround casting
   CredentialSource fromSecurityFactory(SecurityFactory<? extends Credential> credentialFactory) {
      Assert.checkNotNullParam("credentialFactory", credentialFactory);
      return new CredentialSource() {
         public SupportLevel getCredentialAcquireSupport(final Class<? extends Credential> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws IOException {
            return SupportLevel.POSSIBLY_SUPPORTED;
         }

         public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws IOException {
            final Credential credential;
            try {
               credential = credentialFactory.create();
            } catch (GeneralSecurityException e) {
               throw ElytronMessages.log.cannotObtainCredentialFromFactory(e);
            }
            return credential.matches(credentialType, algorithmName, parameterSpec) ? credentialType.cast(credential) : null;
         }
      };
   }
}
