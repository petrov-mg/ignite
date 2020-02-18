package org.apache.ignite.jdbc;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;

/**
 * Represents bunch of methods to test sql execution authorization.
 */
public abstract class AbstractSqlAuthorizationTest extends AbstractSecurityTest {
    /** */
    protected static final String NO_PERMISSIONS_USER = "no-permissions-user";

    /**
     * Executes {@code sql} statement on behalf of the user with the specified {@code login}.
     */
    abstract void execute(String login, String sql) throws Exception;

    /**
     * Returns {@link SecurityPermissionSet} containing specified system permissions.
     */
    protected SecurityPermissionSet systemPermissions(SecurityPermission... perms) {
        return create()
            .defaultAllowAll(false)
            .appendSystemPermissions(perms)
            .build();
    }

    /**
     * Returns {@link SecurityPermissionSet} containing specified cache permissions.
     */
    protected SecurityPermissionSet cachePermissions(String cache, SecurityPermission... perms) {
        return create()
            .defaultAllowAll(false)
            .appendCachePermissions(cache, perms)
            .build();
    }

    /**
     * Returns {@link TestSecurityData} that represnts user with system {@code perm} granted.
     */
    protected TestSecurityData systemPermissionsHolder(SecurityPermission perm) {
        return new TestSecurityData(
            toLogin(perm),
            systemPermissions(perm)
        );
    }

    /**
     * Returns {@link TestSecurityData} that represnts user with cache {@code perms} granted.
     */
    protected TestSecurityData cachePermissionsHolder(String cache, SecurityPermission... perms) {
        return new TestSecurityData(toLogin(cache, perms), cachePermissions(cache, perms));
    }

    /**
     * Returns {@link TestSecurityData} that represnts user with empty permissions.
     */
    protected TestSecurityData emptyPermissionsHolder() {
        return new TestSecurityData(
            NO_PERMISSIONS_USER,
            create().defaultAllowAll(false).build()
        );
    }

    /**
     * Creates login of permission holder.
     */
    protected String toLogin(SecurityPermission perm) {
        return perm.name() + "-user";
    }

    /**
     * Creates login of cache permissions holder.
     */
    String toLogin(String cache, SecurityPermission... perms) {
        return cache + Arrays.stream(perms)
            .sorted()
            .map(SecurityPermission::name)
            .collect(Collectors.joining("-", "-", ""));
    }

    /**
     * Checks that {@code sql} executes with no permisions granted.
     */
    protected void checkNoPermissionsRequired(String sql) throws Exception {
        execute(NO_PERMISSIONS_USER, sql);
    }

    /**
     * Checks that {@code sql} execution requires system {@code perms} be granted.
     */
    protected void checkSystemPermissionRequired(String sql, SecurityPermission perm) throws Exception {
        assertExecutionFailed(NO_PERMISSIONS_USER, sql);

        executeWithSystemPermission(sql, perm);
    }

    /**
     * Checks that {@code sql} execution requires {@code perms} for {@code cache} be granted.
     */
    protected void checkCachePermissionsRequired(String sql, String cache, SecurityPermission... perms) throws Exception {
        assertExecutionFailed(NO_PERMISSIONS_USER, sql);

        executeWithCachePermission(sql, cache, perms);
    }

    /**
     * Executes {@code sql} on behalf of the user with the required cache permissions.
     */
    protected void executeWithCachePermission(String sql, String cache, SecurityPermission... perms) throws Exception{
        execute(toLogin(cache, perms), sql);
    }

    /**
     * Executes {@code sql} on behalf of the user with the required system permissions.
     */
    protected void executeWithSystemPermission(String sql, SecurityPermission perm) throws Exception{
        execute(toLogin(perm), sql);
    }

    /**
     * Asserts that execution of {@code sql} statement on behalf of the user with the specified {@code login} fails.
     */
    protected void assertExecutionFailed(String login, String sql) {
        try {
            execute(login, sql);
        }
        catch (Throwable e) {
            assertTrue(e.getMessage().contains("Authorization failed"));

            return;
        }

        fail("SQL execution completed unexpectedly successfully [login=" + login + ", sql=" + sql + ']');
    }
}
