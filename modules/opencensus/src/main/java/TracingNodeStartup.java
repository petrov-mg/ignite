/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.Tracing;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;

/** */
public class TracingNodeStartup {
    /** */
    public static void main(String[] args) throws IgniteInterruptedCheckedException {
        int nodeIdx = Integer.parseInt(args[0]);

        String nodeName = "ignite-node-" + nodeIdx;

        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
            .setV2Url("http://localhost:9411/api/v2/spans")
            .setServiceName(nodeName)
            .build());

        try (
            Ignite srv = Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName(nodeName)
                .setDiscoverySpi(new TcpDiscoverySpi()
                    .setIpFinder(new TcpDiscoveryVmIpFinder(false) {{
                        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
                    }}))
                .setTracingSpi(new OpenCensusTracingSpi()))
        ) {
            while (srv.cluster().nodes().size() != 4)
                U.sleep(10);

            while (srv.cluster().nodes().size() == 4)
                U.sleep(10);
        }
        finally {
            Tracing.getExportComponent().shutdown();

            ZipkinTraceExporter.unregister();

            U.sleep(5000); // See https://github.com/census-instrumentation/opencensus-java/issues/2031

            Runtime.getRuntime().halt(0);
        }
    }
}
