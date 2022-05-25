package org.infinispan.remoting.transport.raft;

import java.util.Objects;

/**
 * A configuration object to configure {@link RaftChannel}.
 *
 * @since 14.0
 */
public final class RaftChannelConfiguration {

   private final RaftLogMode logMode;

   private RaftChannelConfiguration(RaftLogMode logMode) {
      this.logMode = logMode;
   }

   /**
    * @return The {@link RaftLogMode}.
    * @see RaftLogMode
    */
   public RaftLogMode logMode() {
      return logMode;
   }

   @Override
   public String toString() {
      return "RaftChannelConfiguration{" +
            "logMode=" + logMode +
            '}';
   }

   public static class Builder {

      private RaftLogMode logMode = RaftLogMode.PERSISTENT;

      /**
       * Sets the RAFT log mode.
       * <p>
       * The log mode can be {@link RaftLogMode#PERSISTENT} (default) or {@link RaftLogMode#VOLATILE}.
       *
       * @param logMode The log mode.
       * @return This instance.
       * @see RaftLogMode
       */
      public Builder logMode(RaftLogMode logMode) {
         this.logMode = Objects.requireNonNull(logMode);
         return this;
      }

      /**
       * @return The {@link RaftChannelConfiguration} created.
       */
      public RaftChannelConfiguration build() {
         return new RaftChannelConfiguration(logMode);
      }

   }

   public enum RaftLogMode {
      /**
       * The RAFT log entries are stored in memory only.
       * <p>
       * It improves the performance, but it can cause data lost.
       */
      VOLATILE,
      /**
       * The RAFT log entries are persisted before applying to the {@link RaftStateMachine}.
       * <p>
       * This is the default option.
       */
      PERSISTENT
   }
}
