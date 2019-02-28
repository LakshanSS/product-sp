package org.wso2.sp.scenario.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ResultContainer {
    private static Logger log = LoggerFactory.getLogger(ResultContainer.class);
    private int eventCount;
    private List<String> results;
    private CountDownLatch latch;
    private int timeout = 90;


    public ResultContainer(int expectedEventCount) {
        eventCount = 0;
        results = new ArrayList<>(expectedEventCount);
        latch = new CountDownLatch(expectedEventCount);
    }

    public ResultContainer(int expectedEventCount, int timeoutInSeconds) {
        eventCount = 0;
        results = new ArrayList<>(expectedEventCount);
        latch = new CountDownLatch(expectedEventCount);
        timeout = timeoutInSeconds;
    }

    public void eventReceived(String message) {
        eventCount++;
        results.add(message.toString());
        latch.countDown();
    }

    public void waitForResult() {
        try {
            latch.await(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Boolean assertMessageContent(String content) {
        try {
            if (latch.await(timeout, TimeUnit.SECONDS)) {
                for (String message : results) {
                    if (message.contains(content)) {
                        return true;
                    }
                }
                return false;
            } else {
                log.error("Expected number of results not received. Only received " + eventCount + " events.");
                return false;
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
