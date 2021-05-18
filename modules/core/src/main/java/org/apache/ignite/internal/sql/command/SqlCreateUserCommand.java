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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.sql.SqlKeyword.PASSWORD;
import static org.apache.ignite.internal.sql.SqlKeyword.USER_OPTIONS;
import static org.apache.ignite.internal.sql.SqlKeyword.WITH;
import static org.apache.ignite.internal.sql.SqlLexerTokenType.DEFAULT;
import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseString;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesOptionalKeyword;

/**
 * CREATE USER command.
 */
public class SqlCreateUserCommand implements SqlCommand {
    /** User name. */
    private String userName;

    /** User's password. */
    private String passwd;

    /** User options. */
    private String userOpts;

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /**
     * @return User name.
     */
    public String userName() {
        return userName;
    }

    /**
     * @return User's password.
     */
    public String password() {
        return passwd;
    }

    /** User options. */
    public String userOptions() {
        return userOpts;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        userName = SqlParserUtils.parseUsername(lex);

        skipIfMatchesKeyword(lex, WITH);

        do {
            if (!lex.shift() || lex.tokenType() != DEFAULT)
                break;

            switch (lex.token()) {
                case PASSWORD: {
                    if (passwd != null)
                        throw error(lex, PASSWORD + " query option specified multiple times");

                    passwd = parseString(lex);

                    break;
                }

                case USER_OPTIONS: {
                    if (userOpts != null)
                        throw error(lex, USER_OPTIONS + " query option specified multiple times");

                    userOpts = parseString(lex);

                    break;
                }

                default:
                    throw errorUnexpectedToken(lex, PASSWORD, USER_OPTIONS);
            }
        }
        while (skipIfMatchesOptionalKeyword(lex, WITH));

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateUserCommand.class, this);
    }
}
