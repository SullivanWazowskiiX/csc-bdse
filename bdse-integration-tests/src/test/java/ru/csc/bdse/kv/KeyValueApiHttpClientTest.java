package ru.csc.bdse.kv;

import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * @author semkagtn
 */
public class KeyValueApiHttpClientTest extends AbstractKeyValueApiTest {

    @ClassRule
    public static GenericContainer node = (GenericContainer) new GenericContainer(new ImageFromDockerfile()
            .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar",
                    new File("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
            .withFileFromFile("kv/kvstorage",
                    new File("../bdse-kvnode/src/main/resources/kv/kvstorage"))
            .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
            .withFileSystemBind("/var/run/docker.sock","/var/run/docker.sock")
            .withEnv(Env.KVNODE_NAME, "node-0")
            .withExposedPorts(8080)
            .withStartupTimeout(Duration.of(120, SECONDS));

    @Override
    protected KeyValueApi newKeyValueApi() {
        final String baseUrl = "http://localhost:" + node.getMappedPort(8080);
        return new KeyValueApiHttpClient(baseUrl);
    }
}
