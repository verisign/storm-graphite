package com.verisign.storm.metrics.reporters;

import com.verisign.storm.metrics.reporters.graphite.GraphiteReporter;
import com.verisign.storm.metrics.util.ConnectionFailureException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;

public class GraphiteSocketSettingsTest {

    private InetSocketAddress graphiteSocketAddress;
    private ServerSocketChannel graphiteServer;

    @BeforeTest
    public void setUp() throws IOException {
        String graphiteHost = "127.0.0.1";
        int graphitePort = 2003;

        graphiteSocketAddress = new InetSocketAddress(graphiteHost, graphitePort);

        graphiteServer = ServerSocketChannel.open();
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (graphiteServer != null && graphiteServer.isOpen()) {
            graphiteServer.close();
        }
    }

    @Test
    public void testConnectTimeout() throws IOException {
        // Backlog of one socket
        graphiteServer.socket().bind(graphiteSocketAddress, 1);
        graphiteServer.configureBlocking(false);

        graphiteServer.accept();

        // Take the only available socket
        new Socket().connect(graphiteServer.socket().getLocalSocketAddress());

        HashMap<String, Object> reporterConfig = new HashMap<String, Object>();

        reporterConfig.put(GraphiteReporter.GRAPHITE_HOST_OPTION, graphiteSocketAddress.getHostName());
        reporterConfig.put(GraphiteReporter.GRAPHITE_PORT_OPTION, String.valueOf(graphiteSocketAddress.getPort()));
        reporterConfig.put(GraphiteReporter.GRAPHITE_CONNECT_TIMEOUT, 1);

        GraphiteReporter graphiteReporter = new GraphiteReporter();
        graphiteReporter.prepare(reporterConfig);

        try {
            graphiteReporter.connect();
            Assert.fail();
        } catch (ConnectionFailureException expected) {

        } finally {
            graphiteReporter.disconnect();
        }


    }
}
