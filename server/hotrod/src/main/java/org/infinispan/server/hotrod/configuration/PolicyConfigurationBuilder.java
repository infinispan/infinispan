package org.infinispan.server.hotrod.configuration;

import javax.security.sasl.Sasl;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class PolicyConfigurationBuilder implements Builder<PolicyConfiguration> {
   private BooleanElementConfigurationBuilder forwardSecrecy, noActive, noAnonymous, noDictionary, noPlainText, passCredentials;

   PolicyConfigurationBuilder(SaslConfigurationBuilder sasl) {
      forwardSecrecy = new BooleanElementConfigurationBuilder("forward-secrecy", sasl, Sasl.POLICY_FORWARD_SECRECY);
      noActive = new BooleanElementConfigurationBuilder("no-active", sasl, Sasl.POLICY_NOACTIVE);
      noAnonymous = new BooleanElementConfigurationBuilder("no-anonymous", sasl, Sasl.POLICY_NOANONYMOUS);
      noDictionary = new BooleanElementConfigurationBuilder("no-dictionary", sasl, Sasl.POLICY_NODICTIONARY);
      noPlainText = new BooleanElementConfigurationBuilder("no-plain-text", sasl, Sasl.POLICY_NOPLAINTEXT);
      passCredentials = new BooleanElementConfigurationBuilder("pass-credentials", sasl, Sasl.POLICY_PASS_CREDENTIALS);
   }

   public BooleanElementConfigurationBuilder forwardSecrecy() {
      return forwardSecrecy;
   }

   public BooleanElementConfigurationBuilder noActive() {
      return noActive;
   }

   public BooleanElementConfigurationBuilder noAnonymous() {
      return noAnonymous;
   }

   public BooleanElementConfigurationBuilder noDictionary() {
      return noDictionary;
   }

   public BooleanElementConfigurationBuilder noPlainText() {
      return noPlainText;
   }

   public BooleanElementConfigurationBuilder passCredentials() {
      return passCredentials;
   }

   @Override
   public void validate() {
   }

   @Override
   public PolicyConfiguration create() {
      return new PolicyConfiguration(forwardSecrecy.create(), noActive.create(), noAnonymous.create(), noDictionary.create(), noPlainText.create(), passCredentials.create());
   }

   @Override
   public PolicyConfigurationBuilder read(PolicyConfiguration template) {
      forwardSecrecy.read(template.forwardSecrecy());
      noActive.read(template.noActive());
      noAnonymous.read(template.noAnonymous());
      noDictionary.read(template.noDictionary());
      noPlainText.read(template.noPlainText());
      passCredentials.read(template.passCredentials());
      return this;
   }
}
