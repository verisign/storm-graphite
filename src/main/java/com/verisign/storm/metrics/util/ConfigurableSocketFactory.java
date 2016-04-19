package com.verisign.storm.metrics.util;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

public class ConfigurableSocketFactory extends SocketFactory {

    private int connectTimeout = 0;
    private int readTimeout = 0;

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        Socket socket = createConfiguredSocket();

        socket.connect(new InetSocketAddress(host, port), connectTimeout);

        return socket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        Socket socket = createConfiguredSocket();

        socket.connect(new InetSocketAddress(host, port), connectTimeout);

        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        Socket socket = createConfiguredSocket();

        socket.bind(new InetSocketAddress(localHost, localPort));
        socket.connect(new InetSocketAddress(host, port), connectTimeout);

        return socket;
    }

    @Override
    public Socket createSocket(InetAddress host, int port, InetAddress localHost, int localPort) throws IOException {
        Socket socket = createConfiguredSocket();

        socket.bind(new InetSocketAddress(localHost, localPort));
        socket.connect(new InetSocketAddress(host, port), connectTimeout);

        return socket;
    }

    private Socket createConfiguredSocket() throws SocketException {
        Socket socket = new Socket();
        socket.setSoTimeout(readTimeout);
        return socket;
    }
}
