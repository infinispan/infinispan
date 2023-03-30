package org.infinispan.quarkus.server.runtime.graal;

public class FixAlpnClasses {
}

// The following requires https://github.com/quarkusio/quarkus/pull/3628 to be integrated

//@Delete
//// We use AlpnHackedJdkApplicationProtocolNegotiator, so no need to use this one
//@TargetClass(className = "io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator")
//final class Delete_JdkAlpnApplicationProtocolNegotiator { }
//
//// Have to make name unique - as quarkus has this same class
//@TargetClass(className = "io.netty.handler.ssl.JdkDefaultApplicationProtocolNegotiator")
//final class Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator {
//
//   @Alias
//   public static Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator INSTANCE;
//}
//
//@TargetClass(className = "io.netty.handler.ssl.JdkSslContext")
//final class Target_io_netty_handler_ssl_JdkSslContext {
//
//   @Substitute
//   static JdkApplicationProtocolNegotiator toNegotiator(ApplicationProtocolConfig config, boolean isServer) {
//      if (config == null) {
//         return (JdkApplicationProtocolNegotiator) (Object) Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator.INSTANCE;
//      }
//
//      switch (config.protocol()) {
//         case NONE:
//            return (JdkApplicationProtocolNegotiator) (Object) Target_io_netty_handler_ssl_JdkDefaultApplicationProtocolNegotiator.INSTANCE;
//         case ALPN:
//            if (isServer) {
//               // GRAAL RC9 bug: https://github.com/oracle/graal/issues/813
//               //                switch(config.selectorFailureBehavior()) {
//               //                case FATAL_ALERT:
//               //                    return new JdkAlpnApplicationProtocolNegotiator(true, config.supportedProtocols());
//               //                case NO_ADVERTISE:
//               //                    return new JdkAlpnApplicationProtocolNegotiator(false, config.supportedProtocols());
//               //                default:
//               //                    throw new UnsupportedOperationException(new StringBuilder("JDK provider does not support ")
//               //                    .append(config.selectorFailureBehavior()).append(" failure behavior").toString());
//               //                }
//               ApplicationProtocolConfig.SelectorFailureBehavior behavior = config.selectorFailureBehavior();
//               if (behavior == ApplicationProtocolConfig.SelectorFailureBehavior.FATAL_ALERT)
//                  return new AlpnHackedJdkApplicationProtocolNegotiator(true, config.supportedProtocols());
//               else if (behavior == ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE)
//                  return new AlpnHackedJdkApplicationProtocolNegotiator(false, config.supportedProtocols());
//               else {
//                  throw new UnsupportedOperationException(new StringBuilder("JDK provider does not support ")
//                        .append(config.selectorFailureBehavior()).append(" failure behavior").toString());
//               }
//            } else {
//               switch (config.selectedListenerFailureBehavior()) {
//                  case ACCEPT:
//                     return new AlpnHackedJdkApplicationProtocolNegotiator(false, config.supportedProtocols());
//                  case FATAL_ALERT:
//                     return new AlpnHackedJdkApplicationProtocolNegotiator(true, config.supportedProtocols());
//                  default:
//                     throw new UnsupportedOperationException(new StringBuilder("JDK provider does not support ")
//                           .append(config.selectedListenerFailureBehavior()).append(" failure behavior").toString());
//               }
//            }
//         default:
//            throw new UnsupportedOperationException(
//                  new StringBuilder("JDK provider does not support ").append(config.protocol()).append(" protocol")
//                        .toString());
//      }
//   }
//
//}
