package org.apache.flink.connector.jdbc.dialect.dameng;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

/**
 * Factory for {@link DamengDialect}.
 */
public class DamengDialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:dm:");
    }

    @Override
    public JdbcDialect create() {
        return new DamengDialect();
    }
}
