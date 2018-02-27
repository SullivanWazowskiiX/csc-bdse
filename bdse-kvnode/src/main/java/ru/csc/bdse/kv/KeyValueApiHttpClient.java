package ru.csc.bdse.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import ru.csc.bdse.util.Constants;
import ru.csc.bdse.util.Encoding;
import ru.csc.bdse.util.Require;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Http client for storage unit.
 *
 * @author semkagtn
 */
public class KeyValueApiHttpClient implements KeyValueApi {

    private final String baseUrl;
    private final RestTemplate rest = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KeyValueApiHttpClient(final String baseUrl) {
        Require.nonEmpty(baseUrl, "empty base url");
        this.baseUrl = baseUrl;
    }

    @Override
    public void put(String key, byte[] value) {
        Require.nonNull(key, "null key");
        Require.nonNull(value, "null value");

        final String url = baseUrl + "/key-value/" + key;
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.PUT, value);
        if (responseEntity.getStatusCode() != HttpStatus.OK) {
            throw new RuntimeException("Response error: " + responseEntity);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        Require.nonNull(key, "null key");

        final String url = baseUrl + "/key-value/" + key;
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.GET, Constants.EMPTY_BYTE_ARRAY);
        switch (responseEntity.getStatusCode()) {
            case OK:
                return Optional.of(responseEntity.getBody());
            case NOT_FOUND:
                return Optional.empty();
            default:
                throw new RuntimeException("Response error: " + responseEntity);
        }
    }

    @Override
    public Set<String> getKeys(String prefix) {
        Require.nonNull(prefix, "null prefix");

        final String url = baseUrl + "/key-value?prefix=" + Encoding.encodeUrl(prefix);
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.GET, Constants.EMPTY_BYTE_ARRAY);
        if (responseEntity.getStatusCode() == HttpStatus.OK) {
            return new HashSet<>(Arrays.asList(readAs(responseEntity.getBody(), String[].class)));
        } else {
            throw new RuntimeException("Response error: " + responseEntity);
        }
    }

    @Override
    public void delete(String key) {
        Require.nonNull(key, "null key");

        final String url = baseUrl + "/key-value/" + key;
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.DELETE, Constants.EMPTY_BYTE_ARRAY);
        if (responseEntity.getStatusCode() != HttpStatus.OK) {
            throw new RuntimeException("Response error: " + responseEntity);
        }
    }

    @Override
    public Set<NodeInfo> getInfo() {
        final String url = baseUrl + "/info";
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.GET, Constants.EMPTY_BYTE_ARRAY);
        if (responseEntity.getStatusCode() == HttpStatus.OK) {
            return new HashSet<>(Arrays.asList(readAs(responseEntity.getBody(), NodeInfo[].class)));
        } else {
            throw new RuntimeException("Response error: " + responseEntity);
        }
    }

    @Override
    public void action(String node, NodeAction action) {
        Require.nonNull(node, "null node");
        Require.nonNull(action, "null action");

        final String url = baseUrl + "/action/" + node + "/" + action;
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.POST, Constants.EMPTY_BYTE_ARRAY);
        if (responseEntity.getStatusCode() != HttpStatus.OK) {
            throw new RuntimeException("Response error: " + responseEntity);
        }
    }

    private ResponseEntity<byte[]> request(final String url,
                                           final HttpMethod method,
                                           final byte[] body) {
        try {
            return rest.exchange(url, method, new HttpEntity<>(body), byte[].class);
        } catch (HttpClientErrorException e) {
            return new ResponseEntity<>(Constants.EMPTY_BYTE_ARRAY, e.getStatusCode());
        }
    }

    private <T> T readAs(byte[] src, Class<T> valueType) {
        try {
            return objectMapper.readValue(src, valueType);
        } catch (Exception e) {
            throw new RuntimeException("Response error: " + e.getMessage());
        }
    }
}
