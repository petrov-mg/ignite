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

package org.apache.ignite.internal.commandline.systemview;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTaskArg;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTaskResult;
import org.apache.ignite.spi.systemview.view.SystemView;

import static java.util.Collections.nCopies;
import static org.apache.ignite.internal.commandline.CommandList.SYSTEM_VIEW;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;

/**
 * Represents command for {@link SystemView} content printing.
 */
public class SystemViewCommand implements Command<Object> {
    /** Column separator. */
    public static final String COLUMN_SEPARATOR = "    ";

    /**
     * Argument for the system view content obtainig task.
     *
     * @see VisorSystemViewTask
     */
    private VisorSystemViewTaskArg taskArg;

    /** ID of the node to get the system view content from. */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try {
            VisorSystemViewTaskResult res;

            try (GridClient client = Command.startClient(clientCfg)) {
                res = executeTaskByNameOnNode(
                    client,
                    VisorSystemViewTask.class.getName(),
                    taskArg,
                    nodeId,
                    clientCfg
                );
            }

            if (res != null)
                printSystemViewContent(res, log);
            else
                log.info("No system view with specified name was found [name=" + taskArg.systemViewName() + "]");

            return res;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * Prints system view content obtained via {@link VisorSystemViewTask} execution.
     *
     * @param taskRes Result of {@link VisorSystemViewTask} execution.
     * @param log Logger.
     */
    private void printSystemViewContent(VisorSystemViewTaskResult taskRes, Logger log) {
        List<String> colTitles = taskRes.systemViewAttributes();

        List<? extends List<?>> sysViewRows = taskRes.systemViewContent();

        List<Integer> colLenghts = colTitles.stream().map(String::length).collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(sysViewRows.size());

        sysViewRows.forEach(sysViewRow -> {
            ListIterator<Integer> colLenIter = colLenghts.listIterator();

            rows.add(sysViewRow.stream().map(attr -> {
                String colVal = Objects.toString(attr);

                colLenIter.set(Math.max(colLenIter.next(), colVal.length()));

                return colVal;
            }).collect(Collectors.toList()));
        });

        printRow(colTitles, nCopies(colTitles.size(), String.class).iterator(), colLenghts.iterator(), log);

        rows.forEach(row -> printRow(row, taskRes.systemViewAttributeTypes().iterator(), colLenghts.iterator(), log));
    }

    /**
     * Prints row content with respect to column types and lengths.
     *
     * @param row Row which content should be printed.
     * @param typeIter Iterator that provides column types in sequential order for decent row formatting.
     * @param lenIter Iterator that provides column lengths in sequential order for decent row formatting.
     */
    private void printRow(List<String> row, Iterator<? extends Class<?>> typeIter, Iterator<Integer> lenIter, Logger log) {
        log.info(row.stream().map(colVal -> {
            Class<?> colType = typeIter.next();

            int colLen = lenIter.next();

            String format = Date.class.isAssignableFrom(colType) || Number.class.isAssignableFrom(colType) ?
                "%" + colLen + "s" :
                "%-" + colLen + "s";

            return String.format(format, colVal);
        }).collect(Collectors.joining(COLUMN_SEPARATOR)));
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        nodeId = null;

        String sysViewName = null;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            SystemViewCommandArg cmdArg = CommandArgUtils.of(arg, SystemViewCommandArg.class);

            if (cmdArg == NODE_ID) {
                String nodeIdArg = argIter.nextArg(
                    "ID of the node from which system view content should be obtained is expected.");

                try {
                    nodeId = UUID.fromString(nodeIdArg);
                }
                catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Failed to parse " + NODE_ID + " command argument." +
                        " String representation of \"java.util.UUID\" is exepected. For example:" +
                        " 123e4567-e89b-42d3-a456-556642440000", e);
                }
            }
            else {
                if (sysViewName != null)
                    throw new IllegalArgumentException("Multiple system view names are not supported.");

                sysViewName = arg;
            }
        }

        if (sysViewName == null) {
            throw new IllegalArgumentException(
                "The name of the system view for which its content should be printed is expected.");
        }

        taskArg = new VisorSystemViewTaskArg(sysViewName);
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new HashMap<>();

        params.put("node_id", "ID of the node to get the system view from. If not set, random node will be chosen.");
        params.put("system_view_name", "Name of the system view which content should be printed." +
            " Both \"SQL\" and \"Java\" styles of system view name are supported" +
            " (e.g. SQL_TABLES and sql.tables will be handled similarly).");

        Command.usage(log, "Print system view content:", SYSTEM_VIEW, params, optional(NODE_ID, "node_id"),
            "system_view_name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SYSTEM_VIEW.toCommandName();
    }
}
