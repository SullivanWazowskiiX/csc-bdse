package ru.csc.bdse.kv.storage;

import org.postgresql.PGProperty;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

public class Context {

    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE_NAME = "storage";
    private static final String DATABASE_CONTAINER_NETWORK_ALIAS = "database";
    private static final String DATABASE_PORT = "5432";

    private PGSimpleDataSource dataSource;

    private JdbcTemplate jdbcTemplate;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private DataSourceTransactionManager dataSourceTransactionManager;
    private TransactionTemplate transactionTemplate;

    public DataSource getDataSource() {
        if (dataSource == null) {
            dataSource = new PGSimpleDataSource();
            dataSource.setProperty(PGProperty.USER, USER);
            dataSource.setProperty(PGProperty.PASSWORD, PASSWORD);
            dataSource.setProperty(PGProperty.PG_HOST, DATABASE_CONTAINER_NETWORK_ALIAS);
            dataSource.setProperty(PGProperty.PG_PORT, DATABASE_PORT);
            dataSource.setProperty(PGProperty.PG_DBNAME, DATABASE_NAME);
        }
        return dataSource;
    }

    public JdbcTemplate getJdbcTemplate() {
        if (jdbcTemplate == null) {
            this.jdbcTemplate = new JdbcTemplate(getDataSource());
        }
        return jdbcTemplate;
    }

    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
        if (namedParameterJdbcTemplate == null) {
            this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(getJdbcTemplate());
        }
        return namedParameterJdbcTemplate;
    }

    public DataSourceTransactionManager getDataSourceTransactionManager() {
        if (dataSourceTransactionManager == null) {
            this.dataSourceTransactionManager = new DataSourceTransactionManager(getDataSource());
        }
        return dataSourceTransactionManager;
    }

    public TransactionTemplate getTransactionTemplate() {
        if (transactionTemplate == null) {
            this.transactionTemplate = new TransactionTemplate(getDataSourceTransactionManager());
        }
        return transactionTemplate;
    }

    public boolean isValid() {
        try {
            return getDataSource().getConnection().isValid(10);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


}
