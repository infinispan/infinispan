#!/usr/bin/python3

import fileinput
import re
import sys

# Usage:
# * Add a breakpoint in Thread.start()
# * Action: new RuntimeException(String.format("Thread %s started thread %s", Thread.currentThread().getName(), name)).printStackTrace()
# * Condition: name.startsWith("<thread name prefix reported as thread leak>")
# * Run tests with the debugger attached and tee the output to build.log
# * Run report_thread_leaks.py build.log
# TODO Replace the conditional breakpoint with a Byteman script

re_leaked_threads_header = re.compile(r'.*java.lang.RuntimeException: Leaked threads:')
#  {HotRod-client-async-pool-88-15:  reported, possible sources [UNKNOWN]},
re_leaked_thread_line = re.compile(r'.*  \{(.*):.* possible sources \[(.*)\]\}.*')
# java.lang.RuntimeException: Thread main started thread HotRod-client-async-pool-92-1
re_started_thread_header = re.compile(r'.*java.lang.RuntimeException: Thread (.*) started thread (.*)')
# \tat java.base/java.lang.Thread.start(Thread.java:793)
re_stacktrace = re.compile('.*(\tat .*)')
#	at org.infinispan.server.test.client.hotrod.HotRodRemoteStreamingIT.RCMStopTest(HotRodRemoteStreamingIT.java:279)
re_interesting_stacktrace = re.compile('\tat .*(Test|IT).java')

lines = iter(fileinput.input(sys.argv[1:]))
leaks = []
threads = dict()


def advance():
   global line, lines
   line = next(lines, None)


advance()
while line:
   m = re_started_thread_header.match(line)
   if m:
      thread_name = m.group(2)
      thread_parent = m.group(1)
      parent_stacktrace = []
      advance()

      while line:
         m = re_stacktrace.match(line)
         if m:
            if re_interesting_stacktrace.match(m.group(1)):
               parent_stacktrace.append(m.group(1))

            advance()
         else:
            break

      threads[thread_name] = (thread_parent, parent_stacktrace)

   m = re_leaked_threads_header.match(line)
   if m:
      advance()
      while line:
         m = re_leaked_thread_line.match(line)
         if m:
            leaks.append(m.group(1))
            advance()
         else:
            break

      for leak in leaks:
         print("- Leaked thread: %s" % leak)
         thread = leak
         parent = threads.get(leak, None)
         if not parent:
            print("  - INFORMATION MISSING")
            continue

         while parent:
            if parent[1]:
               parentStack = "\n" + "\n".join(parent[1])
            else:
               parentStack = ""
            print("  - %s started by: %s%s" % (thread, parent[0], parentStack))

            thread = parent[0]
            parent = threads.get(thread, None)

         print("")

      leaks.clear()
      threads.clear()

   advance()

