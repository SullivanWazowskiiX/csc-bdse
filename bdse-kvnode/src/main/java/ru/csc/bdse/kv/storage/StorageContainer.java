package ru.csc.bdse.kv.storage;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.*;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static java.lang.Thread.sleep;

public class StorageContainer {

    private static Logger logger = LoggerFactory.getLogger(StorageContainer.class);

    /**
     * Environment variable with current container id.
     */
    private static final String HOSTNAME = "HOSTNAME";

    private static final String BRIDGE_DRIVER = "bridge";
    private static final String INTERNAL_NETWORK_NAME = "my-net";

    private static final String KVSTORAGE_PATH = "/kvstorage";
    private static final String PATH_TO_INIT_SQL_SCRIPT = "kv/kvstorage/content/initdb.sql";

    private static final int SECONDS_TO_WAIT_BEFORE_KILLING = 10;
    private static final int MILLIS_TIME_BETWEEN_INIT_CONTEXT = 1000;

    private static final String MOUNTPOINT = "/var/lib/postgresql/data/pgdata";
    private static final String DB_VOLUME_NAME = "pgdata";

    private static final String DATABASE_CONTAINER_NETWORK_ALIAS = "database";
    private static final String DATABASE_PORT = "5432";


    private DockerClient dockerClient;

    private String currentContainerId;
    private String storageContainerId;
    private String storageImageId;
    private String internalNetworkId;
    private String volumeName;

    private State state;

    private Context context;

    private synchronized void init() {
        logger.info("init");
        try {
            this.state = State.DOWN;
            this.dockerClient = DefaultDockerClient.fromEnv().build();
            this.currentContainerId = System.getenv(HOSTNAME);
            this.storageImageId = dockerClient.build(Paths.get(KVSTORAGE_PATH));

            NetworkCreation internalNetwork = createInternalNetwork();
            this.internalNetworkId = internalNetwork.id();

            Volume volume = createVolume();
            this.volumeName = volume.name();

            addCurrentContainerToInternalNetwork(internalNetworkId);

            this.context = new Context();
            viewDockerVersionForDebug();
        } catch (DockerCertificateException | InterruptedException | DockerException | IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void destroy() {
        logger.info("Destroy");
        shutdown();
    }

    public synchronized void startup() {
        logger.info("startup");
        if (state == State.UP) {
            logger.info("Node already up");
        }
        try {

            ContainerCreation storageContainer = createStorageContainer(storageImageId, internalNetworkId, volumeName);
            storageContainerId = storageContainer.id();
            dockerClient.startContainer(storageContainerId);
            ContainerInfo containerInfo = dockerClient.inspectContainer(storageContainerId);
            logger.info("Inspect container: info = {}", containerInfo);

            waitUntilPostgresStartUp();

            String intidbScript = getInitDbScript();
            context.getJdbcTemplate().execute(intidbScript);

            logger.info("Container up");
            state = State.UP;
        } catch (DockerException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void shutdown() {
        logger.info("shutdown");
        if (state == State.DOWN) {
            logger.info("Node already down");
        }
        state = State.DOWN;
        try {
            dockerClient.stopContainer(storageContainerId, SECONDS_TO_WAIT_BEFORE_KILLING);
        } catch (DockerException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public State getState() {
        return state;
    }

    public enum State {
        UP,
        DOWN
    }

    private void waitUntilPostgresStartUp() {
        boolean ready = false;
        while (!ready) {
            try {
                sleep(MILLIS_TIME_BETWEEN_INIT_CONTEXT);
                ready = context.isValid();
                logger.info("Is database is ready : result = {}", ready);
            } catch (Exception e) {
                // will try again
            }
        }
    }

    private NetworkCreation createInternalNetwork() {
        NetworkConfig networkConfig = NetworkConfig.builder()
                .name(INTERNAL_NETWORK_NAME)
                .driver(BRIDGE_DRIVER)
                .attachable(true)
                .build();

        NetworkCreation networkCreation = null;
        try {
            networkCreation = dockerClient.createNetwork(networkConfig);
        } catch (DockerException | InterruptedException e) {
            e.printStackTrace();
        }

        return networkCreation;
    }

    private Volume createVolume() {
        Volume volume = Volume.builder()
                .driver("local")
                .mountpoint(MOUNTPOINT)
                .scope("local")
                .name(DB_VOLUME_NAME)
                .build();

        try {
            return dockerClient.createVolume(volume);
        } catch (DockerException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private ContainerCreation createStorageContainer(String imageId, String networkName, String volumeName) {

        ContainerCreation containerCreation = null;
        try {
            EndpointConfig endpointConfig = EndpointConfig.builder()
                    .aliases(ImmutableList.of(DATABASE_CONTAINER_NETWORK_ALIAS))
                    .build();

            ContainerConfig.NetworkingConfig networkingConfig = ContainerConfig.NetworkingConfig.create(ImmutableMap.of(networkName, endpointConfig));

            ContainerConfig containerConfig = ContainerConfig.builder()
                    .networkingConfig(networkingConfig)
                    .domainname(DATABASE_CONTAINER_NETWORK_ALIAS)
                    .hostname(DATABASE_CONTAINER_NETWORK_ALIAS)
                    .exposedPorts(DATABASE_PORT)
                    .image(imageId)
                    .addVolume(volumeName)
                    .build();

            containerCreation = dockerClient.createContainer(containerConfig);
            logger.info("Container id : containerId = {}", containerCreation);
        } catch (DockerException | InterruptedException e) {
            e.printStackTrace();
        }

        return containerCreation;
    }

    private void addCurrentContainerToInternalNetwork(String networkName) {
        EndpointConfig hostEndpointConfig = EndpointConfig.builder()
                .aliases(ImmutableList.of("node"))
                .build();

        NetworkConnection networkConnection = NetworkConnection.builder()
                .containerId(currentContainerId)
                .endpointConfig(hostEndpointConfig)
                .build();
        try {
            dockerClient.connectToNetwork(networkName, networkConnection);
        } catch (DockerException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void viewDockerVersionForDebug() {
        String apiVersion = null;
        try {
            apiVersion = dockerClient.version().apiVersion();
        } catch (DockerException | InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("docker version: apiversion = {}", apiVersion);
    }

    private String getInitDbScript() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(PATH_TO_INIT_SQL_SCRIPT)) {
            return IOUtils.toString(inputStream, Charsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Init db sql script cannot be loaded");
        }
    }

    public Context getContext() {
        return context;
    }
}
