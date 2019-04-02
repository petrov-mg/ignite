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
import java.util.Arrays;
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
import org.apache.ignite.internal.processors.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.processors.security.SecurityLogMarker;
import org.apache.ignite.internal.processors.security.TestSecurityData;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.junit.Test;

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
public class SecurityLogTest extends AbstractCacheOperationPermissionCheckTest {
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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(
            getConfiguration(0,
                new TestSecurityData(
                    CLIENT,
                    builder()
                        .appendCachePermissions(CACHE_NAME, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                        .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
                        .build()
                ).setPwd(VALID_CLIENT_PWD)));
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        U.delete(new File(U.getIgniteHome(), LOG_DEST));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testSecurityLog() throws Exception {
        try (IgniteClient client = startClient(CLIENT, VALID_CLIENT_PWD)) {
            client.cache(CACHE_NAME).put("key", "value");

            assertForbidden(() -> client.cache(FORBIDDEN_CACHE).put("key", "value"),
                ClientAuthorizationException.class);
        }

        assertForbidden(() -> startClient(CLIENT, "invalid"), ClientAuthenticationException.class);

        checkLogContainsOnly(getLog(ALL_LOG), AUTHENTICATION_FAIL, AUTHENTICATION_SUCCESS,
            AUTHORIZATION_FAIL, AUTHORIZATION_SUCCESS, NODE_AUTHENTICATION_SUCCESS);

        checkLogContainsOnly(getLog(AUTHENTICATION_INFO_LOG), AUTHENTICATION_SUCCESS, NODE_AUTHENTICATION_SUCCESS);
        checkLogContainsOnly(getLog(AUTHENTICATION_ERROR_LOG), AUTHENTICATION_FAIL);
        checkLogContainsOnly(getLog(AUTHORIZATION_INFO_LOG), AUTHORIZATION_SUCCESS);
        checkLogContainsOnly(getLog(AUTHORIZATION_ERROR_LOG), AUTHORIZATION_FAIL);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(new Log4J2Logger(CONFIG_PATH));
    }

    /**
     * @param idx Index.
     * @param clientData Array of client security data.
     */
    protected IgniteConfiguration getConfiguration(int idx,
        TestSecurityData... clientData) throws Exception {
        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(instanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setPluginConfigurations(
                secPluginCfg("srv_" + instanceName, null, allowAllPermissionSet(), clientData))
            .setCacheConfiguration(
                new CacheConfiguration().setName(CACHE_NAME),
                new CacheConfiguration().setName(FORBIDDEN_CACHE)
            );
    }

    /**
     * Checks that log contains messages only of specified types.
     *
     * @param logFile Log file.
     * @param types Security log types.
     */
    private void checkLogContainsOnly(String logFile, SecurityLogType... types) {
        assertThat(logFile.contains('[' + SecurityLogMarker.AUTHENTICATION + "] Node authentication succeed"),
            is(specified(NODE_AUTHENTICATION_SUCCESS, types)));

        assertThat(logFile.contains('[' + SecurityLogMarker.AUTHENTICATION + "] Authentication succeed"),
            is(specified(AUTHENTICATION_SUCCESS, types)));

        assertThat(logFile.contains('[' + SecurityLogMarker.AUTHENTICATION + "] Authentication failed"),
            is(specified(AUTHENTICATION_FAIL, types)));

        assertThat(logFile.contains('[' + SecurityLogMarker.AUTHORIZATION + "] Authorization succeed"),
            is(specified(AUTHORIZATION_SUCCESS, types)));

        assertThat(logFile.contains('[' + SecurityLogMarker.AUTHORIZATION + "] Authorization failed"),
            is(specified(AUTHORIZATION_FAIL, types)));
    }

    /** */
    private boolean specified(SecurityLogType type, SecurityLogType... types) {
        return Arrays.asList(types).contains(type);
    }

    /**
     * @param path Log path relative to defaul test log folder destination.
     * @return String representation of log file whose path was passed.
     */
    private String getLog(String path) throws Exception {
        return U.readFileToString(U.getIgniteHome() + LOG_DEST + path, "UTF-8");
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
