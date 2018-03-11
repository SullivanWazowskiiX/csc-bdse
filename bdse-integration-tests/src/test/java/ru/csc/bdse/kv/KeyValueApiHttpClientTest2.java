package ru.csc.bdse.kv;

import org.assertj.core.api.SoftAssertions;
import org.junit.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.HttpServerErrorException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Env;
import ru.csc.bdse.util.Random;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Thread.sleep;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Test have to be implemented
 *
 * @author alesavinx
 */
public class KeyValueApiHttpClientTest2 {

    private static Logger logger = LoggerFactory.getLogger(KeyValueApiHttpClientTest2.class);

    private static final String BASE_URL = "http://localhost:";
    private static final int FIVE_SECONDS = 5000;
    private static final String NODE_NAME = "node-0";

    private final ExecutorService executorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @ClassRule
    public static GenericContainer node = (GenericContainer) new GenericContainer(new ImageFromDockerfile()
            .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar",
                    new File("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
            .withFileFromFile("kv/kvstorage",
                    new File("../bdse-kvnode/src/main/resources/kv/kvstorage"))
            .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
            .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock")
            .withEnv(Env.KVNODE_NAME, NODE_NAME)
            .withExposedPorts(8080)
            .withStartupTimeout(Duration.of(120, SECONDS));


    private KeyValueApi api = newKeyValueApi();

    private KeyValueApi newKeyValueApi() {
        final String baseUrl = BASE_URL + node.getMappedPort(8080);
        return new KeyValueApiHttpClient(baseUrl);
    }

    @Before
    public void beforeTestMethod() {
        logger.info("Before test method. StartUp");
        api.action(NODE_NAME, NodeAction.UP);
    }

    @After
    public void afterTestMethod() {
        logger.info("After test method. StartUp");
        api.action(NODE_NAME, NodeAction.DOWN);
    }


    @Test
    public void concurrentPuts() {
        String key = Random.nextKey();
        byte[] value = Random.nextValue();
        SoftAssertions softAssert = new SoftAssertions();
        for (int i = 0; i < 100; i++) {
            simultaniousActions(() -> api.put(key, value), () -> api.put(key, value));
            Optional<byte[]> currentValue = api.get(key);
            softAssert.assertThat(currentValue.isPresent()).as("value").isFalse();
        }
        softAssert.assertAll();
    }

    @Test
    public void concurrentDeleteAndKeys() {
        int ITERATION_COUNT = 100;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < ITERATION_COUNT; i++) {
            String key = Random.nextKey();
            byte[] value = Random.nextValue();

            keys.add(key);
            api.put(key, value);
        }

        SoftAssertions softAssert = new SoftAssertions();
        for (int i = 0; i < ITERATION_COUNT; i++) {
            String key = keys.remove(keys.size() - 1);
            simultaniousActions(() -> api.delete(key), () -> api.getKeys(key.substring(0, 1)));
            Optional<byte[]> currentValue = api.get(key);
            softAssert.assertThat(!currentValue.isPresent());
        }
        softAssert.assertAll();
    }

    @Test
    public void actionUpDown() throws InterruptedException {
        SoftAssertions softAssert = new SoftAssertions();

        api.action(NODE_NAME, NodeAction.UP);
        Set<NodeInfo> upInfo = api.getInfo();
        softAssert.assertThat(upInfo).as("size").hasSize(1);
        softAssert.assertThat(upInfo.iterator().next().getStatus()).as("status").isEqualTo(NodeStatus.UP);

        api.action(NODE_NAME, NodeAction.DOWN);
        Set<NodeInfo> downInfo = api.getInfo();
        softAssert.assertThat(downInfo).as("size").hasSize(1);
        softAssert.assertThat(downInfo.iterator().next().getStatus()).as("status").isEqualTo(NodeStatus.DOWN);

        sleep(FIVE_SECONDS);

        api.action(NODE_NAME, NodeAction.UP);
        upInfo = api.getInfo();
        softAssert.assertThat(upInfo).as("size").hasSize(1);
        softAssert.assertThat(upInfo.iterator().next().getStatus()).as("status").isEqualTo(NodeStatus.UP);
        softAssert.assertAll();
    }

    @Test(expected = HttpServerErrorException.class)
    public void putWithStoppedNode() {
        api.action(NODE_NAME, NodeAction.DOWN);
        api.put("key", "value".getBytes());
    }

    @Test(expected = HttpServerErrorException.class)
    public void getWithStoppedNode() {
        api.action(NODE_NAME, NodeAction.DOWN);
        api.get("key");
    }

    @Test(expected = HttpServerErrorException.class)
    public void getKeysByPrefixWithStoppedNode() {
        api.action(NODE_NAME, NodeAction.DOWN);
        api.getKeys("prefix");
    }

    @Test
    public void deleteByTombstone() {
        // TODO use tombstones to mark as deleted (optional)
    }

    @Test
    public void loadMillionKeys() {
        //TODO load too many data (optional)
    }

    @AfterClass
    public static void afterClass() {
        System.out.print("After class method. Debug point.");
    }


    private void simultaniousActions(Action firstAction, Action secondAction) {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        executorService.submit(createRunnable(countDownLatch, firstAction));
        executorService.submit(createRunnable(countDownLatch, secondAction));
    }

    private Runnable createRunnable(CountDownLatch countDownLatch, Action action) {
        return () -> {
            try {
                countDownLatch.countDown();
                countDownLatch.await();
                action.perform();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }

    private interface Action {
        void perform();
    }

}


