package ru.csc.bdse;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import ru.csc.bdse.kv.PostgresNode;


@RunWith(SpringRunner.class)
@SpringBootTest
@Ignore
public class ApplicationTests {

	@Autowired
	PostgresNode postgresNode;

	@Test
	public void contextLoads() {
		postgresNode.put("lalsa", "dasd".getBytes());
	}
}
