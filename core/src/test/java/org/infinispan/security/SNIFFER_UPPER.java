package org.infinispan.security;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.ENCRYPT;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;

import static org.jgroups.util.Util.assertTrue;

/**
 * Class SNIFFER_UPPER is new helper Protocol to inject above ENCRYPT protocol in JGroups configuration file. After the
 * message is decrypted with lower protocol(ENCRYPT) should be checked if message is decrypted properly and is valid
 */
public class SNIFFER_UPPER extends Protocol {

   public static final String KEYSTORE_TYPE;
   public static final String KEYSTORE_PATH;
   private static short encryptID;
   private SecretKey secretKey;
   private Cipher symDecodingCipher;
   private String symAlgorithm;
   private static int msg_counter;

   static {
      ClassConfigurator.addProtocol((short) 1027, SNIFFER_UPPER.class);
      KEYSTORE_TYPE = "JCEKS";
      KEYSTORE_PATH = System.getProperty("basedir") + "/target/test-classes/stacks/security/keystores/encrypt.keystore";
      encryptID = ClassConfigurator.getProtocolId(ENCRYPT.class);
      msg_counter = 0;
   }

   public SNIFFER_UPPER() {
      name = getClass().getSimpleName();
   }

   private void initConfiguredKey(String keyStoreName, String storePassword, String keyPassword, String alias) throws Exception {
      InputStream inputStream = null;
      // must not use default keystore type - as does not support secret keys
      KeyStore store = KeyStore.getInstance(KEYSTORE_TYPE);

      SecretKey tempKey;
      try {
         // load in keystore using this thread's classloader
         inputStream = Thread.currentThread()
               .getContextClassLoader()
               .getResourceAsStream(keyStoreName);
         if (inputStream == null)
            inputStream = new FileInputStream(keyStoreName);
         // we can't find a keystore here -
         if (inputStream == null) {
            throw new Exception("Unable to load keystore " + keyStoreName
                                      + " ensure file is on classpath");
         }
         // we have located a file lets load the keystore
         try {
            store.load(inputStream, storePassword.toCharArray());
            // loaded keystore - get the key
            tempKey = (SecretKey) store.getKey(alias, keyPassword.toCharArray());
         } catch (IOException e) {
            throw new Exception("Unable to load keystore " + keyStoreName + ": " + e);
         } catch (NoSuchAlgorithmException e) {
            throw new Exception("No Such algorithm " + keyStoreName + ": " + e);
         } catch (CertificateException e) {
            throw new Exception("Certificate exception " + keyStoreName + ": " + e);
         }

         if (tempKey == null) {
            throw new Exception("Unable to retrieve key '" + alias
                                      + "' from keystore "
                                      + keyStoreName);
         }
         //set the key here
         secretKey = tempKey;
         symAlgorithm = tempKey.getAlgorithm();
      } finally {
         Util.close(inputStream);
      }

   }

   private void initSymCiphers(String algorithm, SecretKey secret) throws Exception {

      if (log.isDebugEnabled())
         log.debug(" Initializing symmetric ciphers");

      symDecodingCipher = Cipher.getInstance(algorithm);
      symDecodingCipher.init(Cipher.DECRYPT_MODE, secret);
   }

   @Override
   public void init() throws Exception {
      super.init();
      initConfiguredKey(KEYSTORE_PATH, "secret", "secret", "memcached");
      initSymCiphers(symAlgorithm, secretKey);
   }

   private byte[] decrypt(Cipher cipher, Message msg) throws Exception {
      byte[] decrypted_msg = cipher.doFinal(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
      return decrypted_msg;
   }

   public Object down(Event evt) {
      if (evt.getType() == Event.MSG) {
         Message msg = (Message) evt.getArg();
         if (msg.getLength() > 0) {
            if (log.isTraceEnabled())
               log.trace(String.format("down(): %d bytes", msg.getLength()));
            return down_prot.down(new Event(Event.MSG, msg));
         }
      }
      return down_prot.down(evt);
   }

   public Object up(Event evt) {
      if (evt.getType() == Event.MSG) {
         Message msg = (Message) evt.getArg();
         if (msg.getHeader(encryptID) != null) {
            msg_counter++;
            if (SNIFFER_LOWER.msgCounter == msg_counter) {
               byte[] decrypted_msg = null;
               try {
                  decrypted_msg = decrypt(symDecodingCipher, SNIFFER_LOWER.messageUP);
               } catch (Exception e) {
                  log.warn("up(): Exception thrown when trying to decode message", e);
               }
               assertTrue("up(): Messages are not equal!!!", Arrays.equals(decrypted_msg, msg.getRawBuffer()));
               if (log.isTraceEnabled())
                  log.trace(String.format("up(): %d bytes", msg.getLength()));
               return up_prot.up(new Event(Event.MSG, msg));
            }
         }
      }
      return up_prot.up(evt);
   }

   public void up(MessageBatch batch) {
      for (Message msg : batch) {
         if (msg.getHeader(encryptID) != null) {
            if (log.isTraceEnabled())
               log.trace(String.format("up() batch: %d bytes", msg.getLength()));
         }
      }

      if (!batch.isEmpty())
         up_prot.up(batch);
   }
}


