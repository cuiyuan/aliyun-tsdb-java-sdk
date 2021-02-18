package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.telnet.TelnetConnection;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;
import com.aliyun.hitsdb.client.value.request.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author cuiyuan
 * @date 2021/2/18 3:46 下午
 */
public class BatchTelnetPutRunnable extends AbstractBatchPutRunnable implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchTelnetPutRunnable.class);

    /**
     * 批量提交回调
     */
    private final AbstractBatchPutCallback<?> batchPutCallback;

    private final TelnetConnection telnetConnection;

    public BatchTelnetPutRunnable(DataQueue dataQueue, HttpClient httpclient, Config config, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        super(dataQueue, httpclient, countDownLatch, config, rateLimiter);
        this.batchPutCallback = config.getBatchPutCallback();
        try {
            this.telnetConnection = new TelnetConnection(httpclient.getHost(),httpclient.getPort());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {

        Point waitPoint = null;
        boolean readyClose = false;
        int waitTimeLimit = batchPutTimeLimit / 3;

        while (true) {
            if (readyClose && waitPoint == null) {
                break;
            }

            long t0 = System.currentTimeMillis();
            List<Point> pointList = new ArrayList<Point>(batchSize);
            if (waitPoint != null) {
                pointList.add(waitPoint);
                waitPoint = null;
            }

            for (int i = pointList.size(); i < batchSize; i++) {
                try {
                    Point point = dataQueue.receiveTelnetPoint(waitTimeLimit);
                    if (point != null) {
                        if (this.rateLimiter != null) {
                            this.rateLimiter.acquire();
                        }
                        pointList.add(point);
                    }
                    long t1 = System.currentTimeMillis();
                    if (t1 - t0 > batchPutTimeLimit) {
                        break;
                    }
                } catch (InterruptedException e) {
                    readyClose = true;
                    LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                    break;
                }
            }

            if (pointList.size() == 0 && !readyClose) {
                try {
                    Point newPoint = dataQueue.receiveTelnetPoint();
                    waitPoint = newPoint;
                    continue;
                } catch (InterruptedException e) {
                    readyClose = true;
                    LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                }
            }

            if (pointList.size() == 0) {
                continue;
            }

            try {
                this.telnetConnection.put(pointList);
            } catch (IOException e) {
                LOGGER.error("ERROR occurred while telnet put", e);
            }
        }

        if (readyClose) {
            this.countDownLatch.countDown();
        }
    }
}
