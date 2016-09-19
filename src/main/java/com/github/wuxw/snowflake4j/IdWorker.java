package com.github.wuxw.snowflake4j;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;


/**
 * Snowflake idworker ,from twitter snowflake project .
 */
public class IdWorker {

    protected static final Logger LOGGER = LoggerFactory.getLogger(IdWorker.class);
    private long workerId;
    private long datacenterId;
    private long sequence = 0L;

    private long twepoch = 1451577600000L;

    private long workerIdBits = 5L;
    private long datacenterIdBits = 5L;
    private long maxWorkerId = ~(-1L << workerIdBits);
    private long maxDatacenterId = ~(-1L << datacenterIdBits);
    private long sequenceBits = 12L;

    private long workerIdShift = sequenceBits;
    private long datacenterIdShift = sequenceBits + workerIdBits;
    private long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
    private long sequenceMask = ~(-1L << sequenceBits);

    private long lastTimestamp = -1L;

    private Random random = new Random();

    /**
     *
     */
    public IdWorker() {
        this.workerId = -1;
        this.datacenterId = -1;
    }

    /**
     * @param workerId 10 bits workerId，from 0 to 1023
     */
    public IdWorker(long workerId) {
        this((workerId & ~(-1 << 5)), workerId >> 5);
    }

    /**
     * @param workerId     5 bits worker id ,from 0 to 31
     * @param datacenterId 5 bits data center id ,from 0 to 31
     */
    public IdWorker(long workerId, long datacenterId) {
        // sanity check for workerId
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        if (datacenterId > maxDatacenterId || datacenterId < 0) {
            throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
        }
        this.workerId = workerId;
        this.datacenterId = datacenterId;
        LOGGER.info(String.format("worker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d", timestampLeftShift, datacenterIdBits, workerIdBits, sequenceBits, workerId));
    }

    public synchronized long nextId() {
        long timestamp = timeGen();

        if (timestamp < lastTimestamp) {
            LOGGER.error(String.format("clock is moving backwards.  Rejecting requests until %d.", lastTimestamp));
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
//            sequence = 0L;//this is twitter version
            sequence = random.nextInt(10);// optimize the random number of 0~9 to prevent the id distribution is not balanced
        }

        lastTimestamp = timestamp;

        long identity;
        if (datacenterId != -1 && workerId != -1) {
            identity = (datacenterId << datacenterIdShift) | (workerId << workerIdShift);
        } else {
            //get id from InstanceIdManager
            identity = InstanceIdProxy.getInstanceId();
        }

        return ((timestamp - twepoch) << timestampLeftShift) | identity | sequence;
    }

    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }


}
