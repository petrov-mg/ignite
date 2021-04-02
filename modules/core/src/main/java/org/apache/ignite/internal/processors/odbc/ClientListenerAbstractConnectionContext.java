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

package org.apache.ignite.internal.processors.odbc;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Base connection context.
 */
public abstract class ClientListenerAbstractConnectionContext implements ClientListenerConnectionContext {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Security context or {@code null} if security is disabled. */
    private SecurityContext secCtx;

    /** Connection ID. */
    private long connId;

    /** User attributes. */
    protected Map<String, String> userAttrs;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param connId Connection id.
     */
    protected ClientListenerAbstractConnectionContext(GridKernalContext ctx, long connId) {
        this.ctx = ctx;
        this.connId = connId;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public SecurityContext securityContext() {
        return secCtx;
    }

    /** {@inheritDoc} */
    @Override public long connectionId() {
        return connId;
    }

    /**
     * Perform authentication.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void authenticate(GridNioSession ses, String user, String pwd) throws IgniteCheckedException {
        if (!ctx.security().clientAuthenticationEnabled())
            return;

        SecurityCredentials cred = new SecurityCredentials(user, pwd);

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(UUID.randomUUID());
        authCtx.nodeAttributes(F.isEmpty(userAttrs) ? Collections.emptyMap() : userAttrs);
        authCtx.credentials(cred);
        authCtx.address(ses.remoteAddress());
        authCtx.certificates(ses.certificates());

        secCtx = ctx.security().authenticate(authCtx);

        if (secCtx == null) {
            throw new IgniteAccessControlException(
                String.format("The user name or password is incorrect [userName=%s]", user)
            );
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected() {
        if (ctx.security().clientAuthenticationEnabled())
            ctx.security().onSessionExpired(secCtx.subject().id());
    }
}
