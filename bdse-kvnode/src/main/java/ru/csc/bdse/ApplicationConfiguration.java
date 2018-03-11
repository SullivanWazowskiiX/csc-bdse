package ru.csc.bdse;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.csc.bdse.kv.storage.StorageContainer;
import ru.csc.bdse.kv.PostgresNode;
import ru.csc.bdse.util.Env;

import java.util.UUID;
import java.util.function.Supplier;

@Configuration
public class ApplicationConfiguration {


    @Bean(initMethod = "init", destroyMethod = "destroy")
    public StorageContainer postgresInit() {
        return new StorageContainer();
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public PostgresNode postgresNode(StorageContainer postgresInit) {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(randomNodeName());
        return new PostgresNode(nodeName, postgresInit);
    }

    private Supplier<? extends String> randomNodeName() {
        return (Supplier<String>) () -> "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

}
