/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.VoltDB;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.FixedDBBPool;

/**
 * Receives snapshot data from a replica. This is used on a rejoining partition.
 */
public class StreamSnapshotDataReceiver extends StreamSnapshotBase
implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    /*
     * element is a pair of <sourceHSId, blockData>. The hsId should remain the
     * same for the length of the data transfer process for this partition.
     */
    private final LinkedBlockingQueue<Pair<Long, Pair<Long, BBContainer>>> m_queue =
            new LinkedBlockingQueue<Pair<Long, Pair<Long, BBContainer>>>();

    final static long WATCHDOG_PERIOD_S = 5;

    class SiteTrackingData {
        long bytesSince = 0;
        long totalBytes = 0;
    }

    final Map<String, SiteTrackingData> m_transferTracking = new HashMap<String, SiteTrackingData>();

    private final Mailbox m_mb;
    private final FixedDBBPool m_bufferPool;
    private volatile boolean m_closed = false;

    public StreamSnapshotDataReceiver(Mailbox mb, FixedDBBPool bufferPool) {
        super();
        m_mb = mb;
        m_bufferPool = bufferPool;

        // start a periodic task to look for timed out connections
        VoltDB.instance().scheduleWork(new Watchdog(), WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);
    }

    public void close() {
        m_closed = true;
    }

    /**
     * Get the next message from queue.
     *
     * @return null if the queue is empty.
     */
    public Pair<Long, Pair<Long, BBContainer>> poll() {
        return m_queue.poll();
    }

    /**
     * Get the next message from the queue. This blocks until one is available.
     *
     * @return
     * @throws InterruptedException
     */
    public Pair<Long, Pair<Long, BBContainer>> take() throws InterruptedException {
        return m_queue.take();
    }

    public int size() {
        return m_queue.size();
    }

    @Override
    public void run() {
        BlockingQueue<BBContainer> bufferQueue =
            m_bufferPool.getQueue(SnapshotSiteProcessor.m_snapshotBufferLength);
        BlockingQueue<BBContainer> compressionBufferQueue =
            m_bufferPool.getQueue(SnapshotSiteProcessor.m_snapshotBufferCompressedLen);

        try {
            while (true) {
                BBContainer container = null;
                BBContainer compressionBufferC = null;
                ByteBuffer compressionBuffer = null;
                boolean success = false;

                try {
                    VoltMessage msg = m_mb.recvBlocking();
                    if (msg == null) {
                        // If interrupted, break
                        break;
                    }

                    assert(msg instanceof RejoinDataMessage);
                    RejoinDataMessage dataMsg = (RejoinDataMessage) msg;
                    byte[] data = dataMsg.getData();

                    String trackingKey =
                            CoreUtils.hsIdToString(dataMsg.m_sourceHSId) +
                            "=>" +
                            CoreUtils.hsIdToString(dataMsg.getTargetId());

                    synchronized (m_transferTracking) {
                        SiteTrackingData trackingData = m_transferTracking.get(trackingKey);
                        if (trackingData != null) {
                            trackingData.bytesSince += data.length;
                            trackingData.totalBytes += data.length;
                        }
                        else {
                            trackingData = new SiteTrackingData();
                            trackingData.bytesSince = trackingData.totalBytes = data.length;
                            m_transferTracking.put(trackingKey, trackingData);
                        }
                    }

                    rejoinLog.info(String.format("Added %d bytes to transferTracking for %s.", data.length, trackingKey));

                    // Only grab the buffer from the pool after receiving a message from the
                    // mailbox. If the buffer is grabbed before receiving the message,
                    // this thread could hold on to a buffer it may not need and other receivers
                    // will be blocked if the pool has no more buffers left.
                    container = bufferQueue.take();
                    ByteBuffer messageBuffer = container.b();
                    messageBuffer.clear();

                    compressionBufferC = compressionBufferQueue.take();
                    compressionBuffer = compressionBufferC.b();
                    compressionBuffer.clear();
                    compressionBuffer.limit(data.length);
                    compressionBuffer.put(data);
                    compressionBuffer.flip();
                    int uncompressedSize =
                            CompressionService.decompressBuffer(
                                    compressionBuffer,
                                    messageBuffer);
                    messageBuffer.limit(uncompressedSize);
                    m_queue.offer(Pair.of(dataMsg.m_sourceHSId, Pair.of(dataMsg.getTargetId(), container)));
                    success = true;
                } finally {
                    if (!success && container != null) {
                        container.discard();
                    }
                    if (compressionBuffer != null) {
                        compressionBufferC.discard();
                    }
                }
            }
        } catch (IOException e) {
            /*
             * Wait until the last message is delivered and then wait some more
             * so it can be processed so that closed can be set and the
             * exception suppressed.
             */
            try {
                while (!m_queue.isEmpty()) {
                    Thread.sleep(50);
                }
                Thread.sleep(300);
            } catch (InterruptedException e2) {}
            if (m_closed) {
                return;
            }
            rejoinLog.error("Error reading a message from a recovery stream.", e);
        } catch (InterruptedException e) {
            return;
        }
    }

    /**
     * Task run every so often to look for writes that haven't been acked
     * in writeTimeout time.
     */
    class Watchdog implements Runnable {

        @Override
        public synchronized void run() {

            try {
                Set<String> zeroPairs = new HashSet<>();

                synchronized (m_transferTracking) {
                    rejoinLog.info("Running StreamSnapshotDataReceiver Watchdog with tracking set size " + String.valueOf(m_transferTracking.size()));

                    for (Entry<String, SiteTrackingData> e : m_transferTracking.entrySet()) {
                        if (e.getValue().bytesSince == 0) {
                            zeroPairs.add(e.getKey() + ":" + String.valueOf(e.getValue().totalBytes));
                        }
                        else {
                            rejoinLog.info(String.format(
                                    "Rejoin snapshot for %s received %d bytes in the past %d seconds (%d recieved in total so far).",
                                    e.getKey(),
                                    e.getValue().bytesSince,
                                    WATCHDOG_PERIOD_S,
                                    e.getValue().totalBytes));
                            e.getValue().bytesSince = 0;
                        }

                    }
                }

                // print sites that didn't send data
                if (!zeroPairs.isEmpty()) {
                    String list = StringUtils.join(zeroPairs, ", ");
                    rejoinLog.info(String.format(
                            "Rejoin snapshot received no data in the past %d seconds from %s.",
                            WATCHDOG_PERIOD_S,
                            list));
                }

                // schedule to run again
                VoltDB.instance().scheduleWork(new Watchdog(), WATCHDOG_PERIOD_S, -1, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                rejoinLog.error("Watchdog threw", e);
                throw e;
            }
        }
    }

}
