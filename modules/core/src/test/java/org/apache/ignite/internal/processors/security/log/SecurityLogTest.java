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

package org.apache.ignite.internal.processors.security.log;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.TestSecurityData;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.junit.Test;

import static org.apache.ignite.internal.processors.security.SecurityLogMarker.AUTHENTICATION;
import static org.apache.ignite.internal.processors.security.SecurityLogMarker.AUTHORIZATION;
import static org.apache.ignite.internal.processors.security.log.SecurityLogTest.SecurityLogType.AUTHENTICATION_FAIL;
import static org.apache.ignite.internal.processors.security.log.SecurityLogTest.SecurityLogType.AUTHENTICATION_SUCCESS;
import static org.apache.ignite.internal.processors.security.log.SecurityLogTest.SecurityLogType.AUTHORIZATION_FAIL;
import static org.apache.ignite.internal.processors.security.log.SecurityLogTest.SecurityLogType.AUTHORIZATION_SUCCESS;
import static org.apache.ignite.internal.processors.security.log.SecurityLogTest.SecurityLogType.NODE_AUTHENTICATION_SUCCESS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** */
public class SecurityLogTest extends AbstractSecurityTest {
    /** Cache. */
    private static final String CACHE = "TEST_CACHE";

    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE = "FORBIDDEN_TEST_CACHE";

    /** Client. */
    private static final String CLIENT = "client";

    /** Client password. */
    private static final String VALID_CLIENT_PWD = "pwd";

    /** Log4j2 test config path. */
    private static final String CONFIG_PATH = "src/test/config/log4j2-security-test.xml";

    /** Default test log folder. */
    private static final String LOG_DEST = "/work/log/log4j2-security";

    /** */
    private static final String ALL_LOG = "/all.log";

    /** */
    private static final String AUTHENTICATION_INFO_LOG = "/authentication-info.log";

    /** */
    private static final String AUTHENTICATION_ERROR_LOG = "/authentication-error.log";

    /** */
    private static final String AUTHORIZATION_INFO_LOG = "/authorization-info.log";

    /** */
    private static final String AUTHORIZATION_ERROR_LOG = "/authorization-error.log";

    /** */
    private static final String[] TEST_LOGS = {
        ALL_LOG, AUTHENTICATION_ERROR_LOG, AUTHORIZATION_ERROR_LOG, AUTHENTICATION_INFO_LOG, AUTHORIZATION_INFO_LOG };

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        clearAllLogs();

        startGrid(
            getConfiguration(0,
                new TestSecurityData(
                    CLIENT,
                    builder()
                        .appendCachePermissions(CACHE, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                        .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
                        .build()
                ).setPwd(VALID_CLIENT_PWD))
        ).cluster().active(true);

        checkLogs(NODE_AUTHENTICATION_SUCCESS, ALL_LOG, AUTHENTICATION_INFO_LOG);
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        U.delete(new File(logDestination()));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testSecurityLog() throws Exception {
        try (IgniteClient client = checkOperationLog(() -> startClient(CLIENT, VALID_CLIENT_PWD),
            AUTHENTICATION_SUCCESS, ALL_LOG, AUTHENTICATION_INFO_LOG)) {

            checkOperationLog(() -> client.cache(CACHE).put("key", "value"),
                AUTHORIZATION_SUCCESS, ALL_LOG, AUTHORIZATION_INFO_LOG);

            checkOperationLog(() -> assertForbidden(() -> client.cache(FORBIDDEN_CACHE).put("key", "value"),
                ClientAuthorizationException.class),
                AUTHORIZATION_FAIL, ALL_LOG, AUTHORIZATION_ERROR_LOG);
        }

        checkOperationLog(() -> assertForbidden(() -> startClient(CLIENT, "invalid"),
            ClientAuthenticationException.class),
            AUTHENTICATION_FAIL, ALL_LOG, AUTHENTICATION_ERROR_LOG);
    }

    /**
     * @param idx Index.
     * @param clientData Array of client security data.
     */
    protected IgniteConfiguration getConfiguration(int idx, TestSecurityData... clientData) throws Exception {
        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(instanceName)
            .setGridLogger(new Log4J2Logger(CONFIG_PATH))
            .setAuthenticationEnabled(true)
            .setSecurityLogEnabled(true)
            .setPluginConfigurations(
                secPluginCfg("srv_" + instanceName, null, allowAllPermissionSet(), clientData))
            .setCacheConfiguration(
                new CacheConfiguration().setName(CACHE),
                new CacheConfiguration().setName(FORBIDDEN_CACHE)
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /**
     * @param logFile Log file.
     * @param types Security log types.
     * @return {@code true} if log contains messages only of specified types, otherwise {@code false}.
     */
    private boolean logContains(String logFile, SecurityLogType... types) {
        return isSpecified(NODE_AUTHENTICATION_SUCCESS, types) ==
                logFile.contains('[' + AUTHENTICATION + "] Node authentication succeed") &&
            isSpecified(AUTHENTICATION_SUCCESS, types) ==
                logFile.contains('[' + AUTHENTICATION + "] Authentication succeed") &&
            isSpecified(AUTHENTICATION_FAIL, types) ==
                logFile.contains('[' + AUTHENTICATION + "] Authentication failed") &&
            isSpecified(AUTHORIZATION_SUCCESS, types) ==
                logFile.contains('[' + AUTHORIZATION + "] Authorization succeed") &&
            isSpecified(AUTHORIZATION_FAIL, types) ==
                logFile.contains('[' + AUTHORIZATION + "] Authorization failed");
    }

    /** */
    private boolean isSpecified(SecurityLogType type, SecurityLogType... types) {
        return Arrays.asList(types).contains(type);
    }

    /**
     * Iterates over all test log files and checks
     * if specified log message type is presented only in passed log files.
     *
     * @param type Security log type.
     * @param logPaths Log paths.
     */
    private void checkLogs(SecurityLogType type, String... logPaths) throws Exception {
        for (String log : TEST_LOGS)
            assertThat(logContains(getLog(log), type), is(Arrays.asList(logPaths).contains(log)));
    }

    /**
     * Executes operation and checks if it has produced messege of expected type in expected log files.
     *
     * @param c Operation to execute.
     * @param type Security log message type.
     * @param logPaths Log paths.
     * @return Result of {@code c} execution.
     */
    private <T> T checkOperationLog(Callable<T> c, SecurityLogType type, String... logPaths) throws Exception {
        clearAllLogs();

        T res = c.call();

        checkLogs(type, logPaths);

        return res;
    }

    /**
     * Executes operation and checks if it has produced messege of expected type in expected log files.
     *
     * @param r Operation to execute.
     * @param type Security log message type.
     * @param logPaths Log paths.
     */
    private void checkOperationLog(Runnable r, SecurityLogType type, String... logPaths) throws Exception {
        clearAllLogs();

        r.run();

        checkLogs(type, logPaths);
    }

    /** Clear all test log files. */
    private void clearAllLogs() {
        Arrays.stream(TEST_LOGS).forEach(this::clearLog);
    }

    /** */
    private String logDestination() {
        return U.getIgniteHome() + LOG_DEST;
    }
    /**
     * @param path Log path relative to defaul test log folder destination.
     * @return String representation of log file whose path was passed.
     */
    private String getLog(String path) throws IOException {
        return U.readFileToString( logDestination() + path, Charset.defaultCharset().name());
    }

    /**
     * @param path Log path relative to defaul test log folder destination.
     */
    private void clearLog(String path) {
        try {
            FileUtils.write(new File(logDestination() + path), "", Charset.defaultCharset());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * @param userName User name.
     * @param pwd Password.
     */
    private IgniteClient startClient(String userName, String pwd) {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(userName)
                .setUserPassword(pwd)
        );
    }

    /** */
    public enum SecurityLogType {
        /** */
        NODE_AUTHENTICATION_SUCCESS,

        /** */
        AUTHENTICATION_SUCCESS,

        /** */
        AUTHENTICATION_FAIL,

        /** */
        AUTHORIZATION_SUCCESS,

        /** */
        AUTHORIZATION_FAIL
    }
}
