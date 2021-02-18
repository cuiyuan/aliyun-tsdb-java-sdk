package com.aliyun.hitsdb.client.telnet;

import com.aliyun.hitsdb.client.value.request.Point;
import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * @author cuiyuan
 * @date 2021/2/18 4:25 下午
 */
public class TelnetConnection {
    private static Logger LOG = LoggerFactory.getLogger(TelnetConnection.class);
    private TelnetClient telnet;
    private String host;
    private int port;
    private InputStream in;
    private PrintStream out;

    public TelnetConnection(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        TelnetClient conn = new TelnetClient();
//        conn.setDefaultTimeout(5000);
//        conn.setConnectTimeout(5000);
        conn.connect(host, port);
//        conn.setSoTimeout(5000);
        conn.setKeepAlive(true);
        this.telnet = conn;
        this.in = this.telnet.getInputStream();
        this.out = new PrintStream(this.telnet.getOutputStream());
        LOG.info("telnet connect successfully");
    }

    public void close() throws IOException {
        this.telnet.disconnect();
    }

    private String readResponse() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        return String.valueOf(reader.readLine());
    }

    public String put(Collection<Point> points) throws IOException {
        for (int i = 0; i < 3; i++) {
            try {
                return writePoints(points);
            } catch (Throwable e) {
                LOG.error("put error occurred", e);
                try {
                    connect();
                } catch (Throwable e1) {
                    LOG.error("connect error occurred", e);
                }
            }
        }
        return "error";
    }

    private String writePoints(Collection<Point> points) throws IOException {
        for (Point point : points) {
            StringBuilder tagset = new StringBuilder();
            for (Map.Entry<String, String> entry : point.getTags().entrySet()) {
                tagset.append(String.format("%s=%s ", entry.getKey(), entry.getValue()));
            }
            String commandLine = String.format("put %s %d %s %s",
                    point.getMetric(), point.getTimestamp(), point.getValue(), tagset.toString());
            this.out.println(commandLine);
        }

        this.out.flush();
        try {
            String response = readResponse();
            LOG.debug("put result: {}", response);
            if ("null".equals(response)) { //服务端连接断开的时候，response为"null"
                throw new IOException("response null");
            }
            return response;
        } catch (IOException ex) {
            LOG.error("IOException occurred while read: ", ex);
            throw ex;
        }
    }


    public String put(Point... points) throws IOException {
        return put(Arrays.asList(points));
    }
}
