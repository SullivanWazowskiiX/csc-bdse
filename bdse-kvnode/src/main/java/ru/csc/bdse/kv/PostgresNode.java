package ru.csc.bdse.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import org.springframework.transaction.support.TransactionTemplate;
import ru.csc.bdse.kv.storage.StorageContainer;
import ru.csc.bdse.util.Require;


import java.util.*;

import static java.lang.Thread.sleep;

public class PostgresNode implements KeyValueApi {

    private static final Logger logger = LoggerFactory.getLogger(PostgresNode.class);

    private final String name;
    private final StorageContainer storageContainer;

    private NodeStatus nodeStatus;

    public PostgresNode(String name,
                        StorageContainer storageContainer) {
        Require.nonEmpty(name, "name");
        Require.nonNull(storageContainer, "storageContainer");

        this.name = name;
        this.storageContainer = storageContainer;
        this.nodeStatus = NodeStatus.DOWN;
    }

    @Override
    public void put(String key, byte[] value) {
        throwExceptionIfStorageShutdown();

        getTransactionTemplate().execute(status -> {

            String sql = "INSERT INTO key_value(key, value) VALUES (:key, :value) " +
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value";

            SqlParameterSource parameters = new MapSqlParameterSource()
                    .addValue("key", key)
                    .addValue("value", value);

            logger.info("Put : key = {}", key);
            getJdbcTemplate().update(sql, parameters);
            return null;
        });
    }

    @Override
    public Optional<byte[]> get(String key) {
        throwExceptionIfStorageShutdown();

        String sql = "SELECT value FROM key_value WHERE key = :key";

        SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("key", key);

        logger.info("Get : key = {}", key);
        List<byte[]> values = getJdbcTemplate().query(sql, parameters, (resultSet, i) -> resultSet.getBytes("value"));


        logger.info("Values : count = {}", values.size());
        if (values.size() > 1) {
            logger.error("Values count more than one: valuesCount = {}", values.size());
        }

        return values.isEmpty() ? Optional.empty() : Optional.ofNullable(values.get(0));

    }

    @Override
    public Set<String> getKeys(String prefix) {
        throwExceptionIfStorageShutdown();

        String sql = "SELECT key FROM key_value WHERE key LIKE :prefix";

        SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("prefix", prefix + '%');

        logger.info("Get keys : prefix = {}", prefix);
        List<String> keys = getJdbcTemplate().queryForList(sql, parameters, String.class);
        logger.info("Get keys : keys = {}", keys);

        return new HashSet<>(keys);
    }

    @Override
    public void delete(String key) {
        throwExceptionIfStorageShutdown();

        getTransactionTemplate().execute(status -> {

            String sql = "DELETE FROM key_value WHERE  key = :key";

            SqlParameterSource parameters = new MapSqlParameterSource()
                    .addValue("key", key);

            logger.info("Delete : key = {}", key);
            int rowsCount = getJdbcTemplate().update(sql, parameters);
            logger.info("Delete : deletedRowsCount = {}", rowsCount);
            return null;
        });
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(new NodeInfo(name, nodeStatus));
    }

    @Override
    public void action(String node, NodeAction action) {
        logger.info("Action: node = {}, action = {}", node, action);

        switch (action) {
            case UP:
                onNodeUpAction();
                break;
            case DOWN:
                onNodeDownAction();
                break;
            default:
                throw new IllegalStateException("Unexpected action " + action);
        }
    }


    private void throwExceptionIfStorageShutdown() {
        if (nodeStatus == NodeStatus.DOWN) {
            throw new RuntimeException("Node is shutdown.");
        }
    }

    public void init() {
        logger.info("Init");
        onNodeUpAction();
    }

    public void destroy() {
        logger.info("Destroy");
        onNodeDownAction();
    }

    private void onNodeUpAction() {
        logger.info("Try startup storage container");
        if (nodeStatus == NodeStatus.UP) {
            logger.info("Storage container already up");
            return;
        }
        storageContainer.startup();
        if (storageContainer.getState() == StorageContainer.State.UP) {
            nodeStatus = NodeStatus.UP;
        }
    }

    private void onNodeDownAction() {
        logger.info("Try shutdown storage container");
        if (nodeStatus == NodeStatus.DOWN) {
            logger.info("Storage container already down");
            return;
        }
        nodeStatus = NodeStatus.DOWN;
        storageContainer.shutdown();
    }

    public String getName() {
        return name;
    }

    private TransactionTemplate getTransactionTemplate() {
        return storageContainer.getContext().getTransactionTemplate();
    }

    private NamedParameterJdbcTemplate getJdbcTemplate() {
        return storageContainer.getContext().getNamedParameterJdbcTemplate();
    }

}
