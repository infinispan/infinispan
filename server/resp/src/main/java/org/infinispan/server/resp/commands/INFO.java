package org.infinispan.server.resp.commands;

import static org.infinispan.server.resp.Resp3Handler.handleBulkAsciiResult;
import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.jdkspecific.ProcessInfo;
import org.infinispan.commons.util.Version;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * <a href="https://redis.io/commands/info/">INFO</a>
 *
 * This implementation attempts to return all attributes that a real Redis server returns. However, in most
 * cases, the values are set to 0, because they cannot be retrieved or don't make any sense in Infinispan.
 *
 * @since 15.0
 */
public class INFO extends RespCommand implements Resp3Command {

   public INFO() {
      super(-1, 0, 0, 0);
   }


   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      handler.checkPermission(AuthorizationPermission.ADMIN);
      final EnumSet<Section> sections;
      if (arguments.isEmpty()) {
         sections = Section.DEFAULT.implies();
      } else {
         sections = EnumSet.noneOf(Section.class);
         for (byte[] arg : arguments) {
            String section = new String(arg, StandardCharsets.UTF_8);
            sections.addAll(Section.valueOf(section.toUpperCase()).implies());
         }
      }

      StringBuilder sb = new StringBuilder();
      if (sections.contains(Section.SERVER)) {
         sb.append("# Server\r\n");
         sb.append("redis_version:");
         sb.append(Version.getMajorMinor());
         sb.append(CRLF_STRING);
         sb.append("redis_git_sha1:");
         sb.append(Version.getProperty("infinispan.build.commitid"));
         sb.append(CRLF_STRING);
         sb.append("redis_git_dirty:0\r\n");
         sb.append("redis_build_id:");
         sb.append(Version.getVersion());
         sb.append(CRLF_STRING);
         sb.append("redis_mode:");
         switch (handler.cache().getCacheConfiguration().clustering().cacheMode()) {
            case LOCAL:
               sb.append("standalone\r\n");
               break;
            case REPL_ASYNC:
            case REPL_SYNC:
               sb.append("sentinel\r\n");
               break;
            case DIST_ASYNC:
            case DIST_SYNC:
               sb.append("cluster\r\n");
               break;
         }
         sb.append("os:");
         sb.append(System.getProperty("os.name"));
         sb.append(' ');
         sb.append(System.getProperty("os.version"));
         sb.append(' ');
         sb.append(System.getProperty("os.arch"));
         sb.append(CRLF_STRING);
         sb.append("arch_bits:");
         sb.append(System.getProperty("os.arch").endsWith("64") ? "64" : "32");
         sb.append(CRLF_STRING);
         sb.append("monotonic_clock:POSIX clock_gettime\r\n");
         sb.append("multiplexing_api:epoll\r\n"); // TODO: use server socket channel in use
         sb.append("atomicvar_api:c11 - builtin\r\n");
         sb.append("process_id:");
         sb.append(ProcessInfo.getInstance().getPid());
         sb.append(CRLF_STRING);
         sb.append("process_supervised:no\r\n");
         sb.append("run_id:0000000000000\r\n"); // get UUID
         sb.append("tcp_port:");
         sb.append(((InetSocketAddress) ctx.channel().localAddress()).getPort());
         sb.append(CRLF_STRING);
         sb.append("server_time_usec:");
         sb.append(System.currentTimeMillis() * 1000);
         sb.append(CRLF_STRING);
         sb.append("uptime_in_seconds:0\r\n");
         sb.append("uptime_in_days:0\r\n");
         sb.append("hz:10\r\n");
         sb.append("configured_hz:10\r\n");
         sb.append("lru_clock:0\r\n");
         sb.append("executable:server\r\n");
         sb.append("config_file:infinispan.xml\r\n");
         sb.append("io_threads_active:0\r\n");
         sb.append(CRLF_STRING);
      }
      if (sections.contains(Section.CLIENTS)) {
         sb.append("# Clients\r\n");
         sb.append("connected_clients:1\r\n");
         sb.append("cluster_connections:1\r\n");
         sb.append("maxclients:10000\r\n");
         sb.append("client_recent_max_input_buffer:0\r\n");
         sb.append("client_recent_max_output_buffer:0\r\n");
         sb.append("blocked_clients:0\r\n");
         sb.append("tracking_clients:0\r\n");
         sb.append("clients_in_timeout_table:0\r\n");
         sb.append(CRLF_STRING);
      }
      if (sections.contains(Section.MEMORY)) {
         sb.append("# Memory\r\n");
         sb.append("used_memory:0\r\n");
         sb.append("used_memory_human:0M\r\n");
         sb.append("used_memory_rss:0\r\n");
         sb.append("used_memory_rss_human:0M\r\n");
         sb.append("used_memory_peak:0\r\n");
         sb.append("used_memory_peak_human:0M\r\n");
         sb.append("used_memory_peak_perc:0%\r\n");
         sb.append("used_memory_overhead:0\r\n");
         sb.append("used_memory_startup:0\r\n");
         sb.append("used_memory_dataset:0\r\n");
         sb.append("used_memory_dataset_perc:0%\r\n");
         sb.append("allocator_allocated:0\r\n");
         sb.append("allocator_active:0\r\n");
         sb.append("allocator_resident:0\r\n");
         sb.append("total_system_memory:0\r\n");
         sb.append("total_system_memory_human:0G\r\n");
         sb.append("used_memory_lua:0\r\n");
         sb.append("used_memory_vm_eval:0\r\n");
         sb.append("used_memory_lua_human:0K\r\n");
         sb.append("used_memory_scripts_eval:0\r\n");
         sb.append("number_of_cached_scripts:0\r\n");
         sb.append("number_of_functions:0\r\n");
         sb.append("number_of_libraries:0\r\n");
         sb.append("used_memory_vm_functions:0\r\n");
         sb.append("used_memory_vm_total:0\r\n");
         sb.append("used_memory_vm_total_human:0K\r\n");
         sb.append("used_memory_functions:0\r\n");
         sb.append("used_memory_scripts:0\r\n");
         sb.append("used_memory_scripts_human:0\r\n");
         sb.append("maxmemory:0\r\n");
         sb.append("maxmemory_human:0G\r\n");
         sb.append("maxmemory_policy:allkeys-lru\r\n");
         sb.append("allocator_frag_ratio:0\r\n");
         sb.append("allocator_frag_bytes:0\r\n");
         sb.append("allocator_rss_ratio:0\r\n");
         sb.append("allocator_rss_bytes:0\r\n");
         sb.append("rss_overhead_ratio:0\r\n");
         sb.append("rss_overhead_bytes:0\r\n");
         sb.append("mem_fragmentation_ratio:0\r\n");
         sb.append("mem_fragmentation_bytes:0\r\n");
         sb.append("mem_not_counted_for_evict:0\r\n");
         sb.append("mem_replication_backlog:0\r\n");
         sb.append("mem_total_replication_buffers:0\r\n");
         sb.append("mem_clients_slaves:0\r\n");
         sb.append("mem_clients_normal:0\r\n");
         sb.append("mem_cluster_links:0\r\n");
         sb.append("mem_aof_buffer:0\r\n");
         sb.append("mem_allocator:none\r\n");
         sb.append("active_defrag_running:0\r\n");
         sb.append("lazyfree_pending_objects:0\r\n");
         sb.append("lazyfreed_objects:0\r\n");
         sb.append(CRLF_STRING);
      }

      if (sections.contains(Section.PERSISTENCE)) {
         sb.append("# Persistence\r\n");
         sb.append("loading:0\r\n");
         sb.append("async_loading:0\r\n");
         sb.append("current_cow_peak:0\r\n");
         sb.append("current_cow_size:0\r\n");
         sb.append("current_cow_size_age:0\r\n");
         sb.append("current_fork_perc:0.00\r\n");
         sb.append("current_save_keys_processed:0\r\n");
         sb.append("current_save_keys_total:0\r\n");
         sb.append("rdb_changes_since_last_save:0\r\n");
         sb.append("rdb_bgsave_in_progress:0\r\n");
         sb.append("rdb_last_save_time:0\r\n");
         sb.append("rdb_last_bgsave_status:ok\r\n");
         sb.append("rdb_last_bgsave_time_sec:-1\r\n");
         sb.append("rdb_current_bgsave_time_sec:-1\r\n");
         sb.append("rdb_saves:0\r\n");
         sb.append("rdb_last_cow_size:0\r\n");
         sb.append("rdb_last_load_keys_expired:0\r\n");
         sb.append("rdb_last_load_keys_loaded:0\r\n");
         sb.append("aof_enabled:0\r\n");
         sb.append("aof_rewrite_in_progress:0\r\n");
         sb.append("aof_rewrite_scheduled:0\r\n");
         sb.append("aof_last_rewrite_time_sec:-1\r\n");
         sb.append("aof_current_rewrite_time_sec:-1\r\n");
         sb.append("aof_last_bgrewrite_status:ok\r\n");
         sb.append("aof_rewrites:0\r\n");
         sb.append("aof_rewrites_consecutive_failures:0\r\n");
         sb.append("aof_last_write_status:ok\r\n");
         sb.append("aof_last_cow_size:0\r\n");
         sb.append("module_fork_in_progress:0\r\n");
         sb.append("module_fork_last_cow_size:0\r\n");
         sb.append(CRLF_STRING);
      }

      if (sections.contains(Section.STATS)) {
         sb.append("# Stats\r\n");
         sb.append("total_connections_received:0\r\n");
         sb.append("total_commands_processed:0\r\n");
         sb.append("instantaneous_ops_per_sec:0\r\n");
         sb.append("total_net_input_bytes:0\r\n");
         sb.append("total_net_output_bytes:0\r\n");
         sb.append("total_net_repl_input_bytes:0\r\n");
         sb.append("total_net_repl_output_bytes:0\r\n");
         sb.append("instantaneous_input_kbps:0.00\r\n");
         sb.append("instantaneous_output_kbps:0.00\r\n");
         sb.append("instantaneous_input_repl_kbps:0.00\r\n");
         sb.append("instantaneous_output_repl_kbps:0.00\r\n");
         sb.append("rejected_connections:0\r\n");
         sb.append("sync_full:0\r\n");
         sb.append("sync_partial_ok:0\r\n");
         sb.append("sync_partial_err:0\r\n");
         sb.append("expired_keys:0\r\n");
         sb.append("expired_stale_perc:0.00\r\n");
         sb.append("expired_time_cap_reached_count:0\r\n");
         sb.append("expire_cycle_cpu_milliseconds:0\r\n");
         sb.append("evicted_keys:0\r\n");
         sb.append("evicted_clients:0\r\n");
         sb.append("total_eviction_exceeded_time:0\r\n");
         sb.append("current_eviction_exceeded_time:0\r\n");
         sb.append("keyspace_hits:0\r\n");
         sb.append("keyspace_misses:0\r\n");
         sb.append("pubsub_channels:0\r\n");
         sb.append("pubsub_patterns:0\r\n");
         sb.append("pubsubshard_channels:0\r\n");
         sb.append("latest_fork_usec:0\r\n");
         sb.append("total_forks:0\r\n");
         sb.append("migrate_cached_sockets:0\r\n");
         sb.append("slave_expires_tracked_keys:0\r\n");
         sb.append("active_defrag_hits:0\r\n");
         sb.append("active_defrag_misses:0\r\n");
         sb.append("active_defrag_key_hits:0\r\n");
         sb.append("active_defrag_key_misses:0\r\n");
         sb.append("total_active_defrag_time:0\r\n");
         sb.append("current_active_defrag_time:0\r\n");
         sb.append("tracking_total_keys:0\r\n");
         sb.append("tracking_total_items:0\r\n");
         sb.append("tracking_total_prefixes:0\r\n");
         sb.append("unexpected_error_replies:0\r\n");
         sb.append("total_error_replies:0\r\n");
         sb.append("dump_payload_sanitizations:0\r\n");
         sb.append("total_reads_processed:0\r\n");
         sb.append("total_writes_processed:0\r\n");
         sb.append("io_threaded_reads_processed:0\r\n");
         sb.append("io_threaded_writes_processed:0\r\n");
         sb.append("reply_buffer_shrinks:0\r\n");
         sb.append("reply_buffer_expands:0\r\n");
         sb.append(CRLF_STRING);
      }

      if (sections.contains(Section.REPLICATION)) {
         sb.append("# Replication\r\n");
         sb.append("role:master\r\n");
         sb.append("connected_slaves:0\r\n");
         sb.append("master_failover_state:no-failover\r\n");
         sb.append("master_replid:0000000000000000000000000000000000000000\r\n");
         sb.append("master_replid2:0000000000000000000000000000000000000000\r\n");
         sb.append("master_repl_offset:0\r\n");
         sb.append("second_repl_offset:-1\r\n");
         sb.append("repl_backlog_active:0\r\n");
         sb.append("repl_backlog_size:0\r\n");
         sb.append("repl_backlog_first_byte_offset:0\r\n");
         sb.append("repl_backlog_histlen:0\r\n");
         sb.append(CRLF_STRING);
      }

      if (sections.contains(Section.CPU)) {
         sb.append("# CPU\r\n");
         sb.append("used_cpu_sys:0\r\n");
         sb.append("used_cpu_user:0\r\n");
         sb.append("used_cpu_sys_children:0.000000\r\n");
         sb.append("used_cpu_user_children:0.000000\r\n");
         sb.append("used_cpu_sys_main_thread:0.000000\r\n");
         sb.append("used_cpu_user_main_thread:0.000000\r\n");
         sb.append(CRLF_STRING);
      }

      if (sections.contains(Section.MODULES)) {
         sb.append("# Modules\r\n");
         sb.append(CRLF_STRING);
      }

      if (sections.contains(Section.ERRORSTATS)) {
         sb.append("# Errorstats\r\n");
         sb.append("errorstat_ERR:count=0\r\n");
         sb.append("errorstat_NOPERM:count=0\r\n");
         sb.append("errorstat_WRONGTYPE:count=0\r\n");
         sb.append(CRLF_STRING);
      }
      if (sections.contains(Section.CLUSTER)) {
         sb.append("# Cluster\r\n");
         sb.append("cluster_enabled:0\r\n");
         sb.append(CRLF_STRING);
      }
      if (sections.contains(Section.KEYSPACE)) {
         sb.append("# Keyspace\r\n");
         sb.append("db0:keys=0,expires=0,avg_ttl=0\r\n");
      }
      handleBulkAsciiResult(sb, handler.allocator());
      return handler.myStage();
   }

   enum Section {
      SERVER,
      CLIENTS,
      MEMORY,
      PERSISTENCE,
      STATS,
      REPLICATION,
      CPU,
      COMMANDSTATS,
      LATENCYSTATS,
      SENTINEL,
      CLUSTER,
      MODULES,
      KEYSPACE,
      ERRORSTATS,
      ALL {
         @Override
         EnumSet<Section> implies() {
            return EnumSet.complementOf(EnumSet.of(MODULES));
         }
      },
      DEFAULT {
         @Override
         EnumSet<Section> implies() {
            return EnumSet.of(SERVER, CLIENTS, MEMORY, PERSISTENCE, STATS, REPLICATION, CPU, MODULES, ERRORSTATS, CLUSTER, KEYSPACE);
         }
      },

      EVERYTHING {
         @Override
         EnumSet<Section> implies() {
            return EnumSet.allOf(Section.class);
         }
      };


      EnumSet<Section> implies() {
         return EnumSet.of(this);
      }
   }
}
