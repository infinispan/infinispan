///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.6.3
//DEPS org.hdrhistogram:HdrHistogram:2.2.2


import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Utility script to parse a list of JFR files to extract tracing information.
 * <p>
 * The script is focused to parse the tracing information for internal Infinispan events. The script will read the
 * given JFR files and show metrics per operation, global metrics, and percentiles.
 * </p>
 * <p><b>This script requires JBang.</b></p>
 *
 * <p>
 * You can utilze it as:
 * <pre>{@code
 * ./JfrParser.java -c infinispan-category -files path/file1.jfr[ path/file2.jfr ... path/fileN.jfr] \
 *      [-p] \
 *      [-high N] \
 *      [-start "04/07/2024 08:48:45"] \
 *      [-end "04/07/2024 08:50:45"]
 * }</pre>
 *
 * The option `<code>-c</code>` filter the JFR category, see {@link org.infinispan.server.core.telemetry.jfr.JfrSpanProcessor$JfrSpan}.
 * The `<code>-files</code>` accept a list of space-separated path to JFR files. Option `<code>-p</code>` show percentiles.
 * Option `<code>-high</code>` receives a non-negative number to show the N requests which took longer to complete.
 * Option `<code>-start</code>` and `<code>-end</code>` receive a date in the `dd/MM/yyyy HH:mm:ss` format to restrict
 * the time interval to observe the events. All the results are written in the stdout.
 * </p>
 */
@Command(name = "JfrParser", mixinStandardHelpOptions = true, version = "JfrParser 0.1", description = "Parses a list of JFR files.")
class JfrParser implements Callable<Integer> {

    private static final String ROOT_SPAN = "0000000000000000";
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    @CommandLine.Option(names = "-files", split = " ", arity = "1..*")
    private String[] files;

    @CommandLine.Option(names = "-c", required = true)
    private String category;

    @CommandLine.Option(names = "-start")
    private Date after;

    @CommandLine.Option(names = "-end")
    private Date before;

    @CommandLine.Option(names = "-p", defaultValue = "false")
    private boolean percentiles;

    @CommandLine.Option(names = "-high", defaultValue = "0")
    private int highest;

    public static void main(String... args) {
        CommandLine cmd = new CommandLine(new JfrParser())
              .registerConverter(Date.class, SIMPLE_DATE_FORMAT::parse);
        int exitCode = cmd.execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        if (after != null)
            System.out.printf("Filter for after %s\n", after);

        if (before != null) {
            System.out.printf("Filter for before %s\n", before);
        }

        List<Span> spans = read().toList();
        Tree tree = createTree(spans);

        Histogram global = showRootMetrics(tree.roots);
        double avg = global.getMean();
        System.out.printf("\n\nGlobal metrics: %d entries\n", global.getTotalCount());
        System.out.printf("min/avg/max = %d/%f/%d\n", global.getMinValue(), avg, global.getMaxValue());

        if (percentiles) showPercentiles("\n\nGlobal percentiles", global);
        if (highest > 0) showHighest(tree, highest);

        return 0;
    }

    private Histogram showRootMetrics(Collection<Span> spans) {
        System.out.println("\n\nHistograms:");
        Map<String, List<Span>> byOperations = spans.stream()
              .collect(Collectors.groupingBy(Span::operation));

        Histogram histogram = null;
        for (Map.Entry<String, List<Span>> entry : byOperations.entrySet()) {
            System.out.printf("\noperation `%s` has %d entries\n", entry.getKey(), entry.getValue().size());

            Histogram h = showOperationMetrics(entry.getKey(), entry.getValue());
            if (histogram == null) {
                histogram = h;
            } else {
                histogram.add(h);
            }
        }

        return Objects.requireNonNull(histogram, "Histogram should not be null");
    }

    private Histogram showOperationMetrics(String operation, List<Span> spans) {
        Histogram histogram = spans.stream()
              .reduce(new Histogram(TimeUnit.MINUTES.toNanos(1), 5), (acc, curr) -> {
                  long v = curr.duration.toNanos();
                  acc.recordValue(v);
                  return acc;
              }, JfrParser::combineHistograms);

        double avg = histogram.getMean();
        System.out.printf("\t`%s` -> min/avg/max = %d/%f/%d\n", operation, histogram.getMinValue(), avg, histogram.getMaxValue());

        if (percentiles) showPercentiles(String.format("---- Percentiles `%s` ----", operation), histogram);

        return histogram;
    }

    private static Histogram combineHistograms(Histogram h1, Histogram h2) {
        h1.add(h2);
        return h1;
    }

    private static void showPercentiles(String message, Histogram histogram) {
        System.out.println(message);
        for (HistogramIterationValue hiv : histogram.percentiles(1)) {
            long ns = hiv.getValueIteratedTo();
            double ms = ns / 1e+6;
            System.out.printf("%fP: %d ns (%f ms) (%d entries)\n", hiv.getPercentile(), hiv.getValueIteratedTo(), ms, hiv.getCountAtValueIteratedTo());
        }
        System.out.println(" ---- ");
    }

    private static void showHighest(Tree tree, int highest) {
        System.out.printf("\n ----- Highest %d requests -----\n", highest);
        tree.roots.stream()
              .sorted((a1, a2) -> Long.compare(a2.duration.toNanos(), a1.duration.toNanos()))
              .limit(highest)
              .forEach(tree::showCompleteExecution);
    }

    private Stream<Span> read() {
        Stream<Span> stream = Stream.empty();
        for (String file : files) {
            stream = Stream.concat(stream, readFile(Path.of(file)));
        }
        return stream;
    }

    private Stream<Span> readFile(Path path) {
        return JfrParser.uncheckedReadAllEvents(path).stream().parallel()
              .filter(this::acceptEvent)
              .map(Span::from)
              .filter(s -> s.isAfter(after))
              .filter(s -> s.isBefore(before));
    }

    private boolean acceptEvent(RecordedEvent event) {
        return event.getEventType().getCategoryNames().contains(category);
    }

    private static List<RecordedEvent> uncheckedReadAllEvents(Path path) {
       try {
          return RecordingFile.readAllEvents(path);
       } catch (IOException e) {
          throw new RuntimeException(e);
       }
    }

    private Tree createTree(List<Span> spans) {
        Map<String, Span> index = new HashMap<>();
        Set<Span> roots = new HashSet<>();
        for (Span span : spans) {
            if (span.isRoot()) roots.add(span);

            index.put(span.trace, span);
        }

        return new Tree(index, roots);
    }

    private record Tree(Map<String, Span> index, Set<Span> roots) {

        private void showCompleteExecution(Span root) {
            assert roots.contains(root) : root + " is not a root element";
            StringBuilder sb = new StringBuilder();
            printTree(root, sb, 0);

            System.out.println(sb);
        }

        private void printTree(Span root, StringBuilder sb, int indentation) {
            print(root, sb, indentation);

            for (Span span : index.values()) {
                if (span.parent.equals(root.span)) {
                    printTree(span, sb, indentation + 2);
                }
            }
        }

        private void print(Span element, StringBuilder sb, int indentation) {
            if (indentation > 0) {
                sb.append(" ".repeat(indentation));
            }
            sb.append(element.toPrettyString())
                  .append(System.lineSeparator());
        }
    }

    private record Span(String node, String category, String cache, ZonedDateTime start, Duration duration,
                        String operation, String trace, String span, String parent) {

        public static Span from(RecordedEvent event) {
            ZonedDateTime start = event.getStartTime().atZone(ZoneId.systemDefault());
            Duration duration = event.getDuration();
            String node = event.getValue("node");
            String category = event.getValue("category");
            String cache = event.getValue("cache");
            String operation = event.getValue("operation");
            String trace = event.getValue("trace");
            String span = event.getValue("span");
            String parent = event.getValue("parent");

            String message = "Required property is null: " + event;
            return new Span(
                  node,
                  category,
                  cache,
                  start,
                  duration,
                  Objects.requireNonNull(operation, message),
                  Objects.requireNonNull(trace, message),
                  Objects.requireNonNull(span, message),
                  Objects.requireNonNull(parent, message));
        }

        public String toPrettyString() {
            long ns = duration.toNanos();
            double ms = ns / 1e+6;
            return String.format("%s (%s): %s -> %d ns (%f ms)", start, node, operation, ns, ms);
        }

        public boolean isRoot() {
            return parent.equals(ROOT_SPAN);
        }

        public boolean isAfter(Date date) {
            if (date == null) return true;

            ZonedDateTime i = date.toInstant().atZone(ZoneId.systemDefault());
            return start.equals(i) || start.isAfter(i);
        }

        public boolean isBefore(Date date) {
            if (date == null) return true;

            ZonedDateTime i = date.toInstant().atZone(ZoneId.systemDefault());
            return start.equals(i) || start.isBefore(i);
        }
    }
}
