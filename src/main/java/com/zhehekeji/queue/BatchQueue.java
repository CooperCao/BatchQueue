package com.zhehekeji.queue;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


public class BatchQueue<T> {
    private static final String TAG = "BatchQueue";
    /**
     * 默认间隔处理队列时间，ms
     */
    private static final int DEFAULT_TIME = 5000;
    /**
     * 默认队列处理长度
     */
    private static final int DEFAULT_COUNT = 200;

    private final long mIntervalTime;
    private final int mHandleLength;
    private final QueueProcess<T> mQueueProcess;
    private final BlockingQueue<T> mQueue = new ArrayBlockingQueue<>(5000);
    private List<T> mDataList;

    private static final int TIMEOUT = 100;
    private Thread mThread;
    private volatile boolean mIsStart = false;

    /**
     * 设置默认的队列处理时间和数量
     *
     * @param process the process
     */
    public BatchQueue(QueueProcess<T> process) {
        this(DEFAULT_TIME, DEFAULT_COUNT, process);
    }

    /**
     * 可以设置队列的处理的间隔时间和处理长度
     *
     * @param intervalTime      the interval time
     * @param handleQueueLength the handle queue length
     * @param process           the process
     */
    public BatchQueue(int intervalTime, int handleQueueLength, QueueProcess<T> process) {
        mQueueProcess = Optional.of(process).get();
        mIntervalTime = intervalTime;
        mHandleLength = handleQueueLength;
        mDataList = new ArrayList<>(mHandleLength);
    }

    public void add(T t) {
        if (mIsStart && t != null) {
            try {
                mQueue.put(t);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public synchronized void destroy() {
        if (mThread != null) {
            mThread.interrupt();
        }
        mIsStart = false;
        mQueue.clear();
    }

    public synchronized void start() {
        if (!mIsStart) {
            mThread = new Thread(new MainLoop(), TAG);
            mThread.start();
            mIsStart = true;
        }
    }

    /**
     * The interface Queue process.
     *
     * @param <T> the type parameter
     */
    public interface QueueProcess<T> {
        /**
         * Process data.
         *
         * @param list the list
         */
        void processData(List<T> list);
    }

    private class MainLoop implements Runnable {
        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            while (mIsStart) {
                try {
                    T t = mQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);
                    if (null != t) {
                        mDataList.add(t);
                    }
                    if (mDataList.size() >= mHandleLength) {
                        startTime = callBack(mDataList);
                        continue;
                    }
                    if (System.currentTimeMillis() - startTime > mIntervalTime
                            && mDataList.size() > 0) {
                        startTime = callBack(mDataList);
                    }
                } catch (InterruptedException ignored) {

                }
            }
            mQueue.clear();
        }

        private long callBack(List<T> dataList) {
            try {
                mQueueProcess.processData(dataList);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                mDataList = new ArrayList<>(mHandleLength);
            }
            return System.currentTimeMillis();
        }
    }
}
