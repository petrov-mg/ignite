import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.Tracing;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;

import static java.lang.String.join;
import static org.apache.ignite.spi.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/** */
public class SqlTracingPreview {
    /** */
    private static final String PERSON_SCHEMA = "person_schema";

    /** */
    private static final String PERSON_TABLE = PERSON_SCHEMA + '.' + Person.class.getSimpleName();

    /** */
    private static final String ORG_SCHEMA = "org_schema";

    /** */
    private static final String ORG_TABLE = ORG_SCHEMA + '.' + Organization.class.getSimpleName();

    /** */
    private static final int CACHE_ENTRIES_CNT = 100;

    /** */
    private static final int PAGE_SIZE = 20;

    /** */
    public static void main(String[] args) throws IOException, IgniteCheckedException {
        String cliName = "ignite-node-3";

        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
            .setV2Url("http://localhost:9411/api/v2/spans")
            .setServiceName(cliName)
            .build());

        ProcessManager procMgr = new ProcessManager();

        for (int i = 0; i < 3; i++)
            procMgr.startProcess("TracingNodeStartup", Integer.toString(i));

        try (
            IgniteEx cli = (IgniteEx) Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName(cliName)
                .setGridLogger(new Log4JLogger("modules/core/src/test/config/log4j-test.xml"))
                .setTracingSpi(new OpenCensusTracingSpi())
                .setClientMode(true)
                .setDiscoverySpi(new TcpDiscoverySpi()
                    .setIpFinder(new TcpDiscoveryVmIpFinder(false) {{
                        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
                    }})))
        ) {
            while (cli.cluster().nodes().size() != 4)
                U.sleep(10);

            cli.tracingConfiguration().set(
                new TracingConfigurationCoordinates.Builder(SQL).build(),
                new TracingConfigurationParameters.Builder()
                    .withSamplingRate(SAMPLING_RATE_ALWAYS)
                    //.withIncludedScopes(Stream.of(COMMUNICATION).collect(Collectors.toSet()))
                    .build());

            IgniteCache<Integer, Person> prsnCache = cli.createCache(
                new CacheConfiguration<Integer, Person>("person-cache")
                    .setIndexedTypes(Integer.class, Person.class)
                    .setSqlSchema(PERSON_SCHEMA)
            );

            IgniteCache<Integer, Organization> orgCache = cli.createCache(
                new CacheConfiguration<Integer, Organization>("org-cache")
                    .setIndexedTypes(Integer.class, Organization.class)
                    .setSqlSchema(ORG_SCHEMA)
            );

            int keyCntr = 0;

            for (int i = 0; i < CACHE_ENTRIES_CNT; i++) {
                prsnCache.put(keyCntr++, new Person(i, i));

                orgCache.put(keyCntr++, new Organization(i, i));
            }

            try(
                FieldsQueryCursor<List<?>> cursor = cli.context().query().querySqlFields(
                    new SqlFieldsQuery(
                        "SELECT * FROM " + PERSON_TABLE + " AS p JOIN " + ORG_TABLE + " AS o ON o.id = p.id")
                        .setPageSize(PAGE_SIZE)
                        .setDistributedJoins(true),
                    false)
            ) {
                U.sleep(100); // Emulation of user code work.

                Iterator<List<?>> iter = cursor.iterator();

                U.sleep(10);

                for (int i = 0; i < CACHE_ENTRIES_CNT - 1; i++) {
                    iter.next();

                    U.sleep(10);
                }
            }

            cli.context().query().querySqlFields(
                new SqlFieldsQueryEx(
                    "UPDATE " + PERSON_TABLE + " SET name=0 WHERE id < ?;", false)
                    .setSkipReducerOnUpdate(true)
                    .setPageSize(PAGE_SIZE)
                    .setArgs(CACHE_ENTRIES_CNT),
                false
            ).getAll();

            cli.context().query().querySqlFields(
                new SqlFieldsQuery(
                    "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR);"),
                false
            ).getAll();
        }
        finally {
            Tracing.getExportComponent().shutdown();

            ZipkinTraceExporter.unregister();

            U.sleep(5000); // See https://github.com/census-instrumentation/opencensus-java/issues/2031

            Runtime.getRuntime().halt(0);
        }
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private int name;

        /** */
        public Person(int id, int name) {
            this.id = id;
            this.name = name;
        }
    }

    /** */
    public static class Organization {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private int addr;

        /** */
        public Organization(int id, int addr) {
            this.id = id;
            this.addr = addr;
        }
    }

    /** */
    private static class ProcessManager {
        /** */
        private final List<Process> processes = new ArrayList<>();

        /** */
        public void startProcess(String mainCls, final String... args) throws IOException {
            String jvm = System.getenv("JAVA_HOME") + File.separator + "bin" + File.separator + "java";

            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();

            String cmd = jvm +
                " -classpath " + runtimeMxBean.getClassPath() +
                ' ' + mainCls +
                ' ' + join(" ", args);

            processes.add(Runtime.getRuntime().exec(cmd));
        }

        /** */
        public void shutdown() {
            for (Process process : processes)
                process.destroyForcibly();
        }
    }
}
