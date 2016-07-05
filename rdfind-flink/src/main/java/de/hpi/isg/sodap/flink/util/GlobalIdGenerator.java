package de.hpi.isg.sodap.flink.util;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * This generator helps to assign unique IDs among several Flink task managers.
 *
 * Created by sebastian.kruse on 11.02.2015.
 */
@SuppressWarnings("serial")
public class GlobalIdGenerator implements Serializable {

    /**
     * The first ID to be generated.
     */
    private int startFrom = 0;

    /**
     * The next ID to be released.
     */
    private long nextId;

    /**
     * The offset to the next ID within the current context.
     */
    private int stride = -1;

    public GlobalIdGenerator(int startFrom) {
        this.startFrom = startFrom;
    }

    /**
     * Sets up the generator within the current function. Should be called when the function is already deployed
     * on the Flink cluster.
     */
    public void initialize(final RuntimeContext runtimeContext) {
        this.nextId = this.startFrom + runtimeContext.getIndexOfThisSubtask();
        this.stride = runtimeContext.getNumberOfParallelSubtasks();
    }

    /**
     * Yields the next ID.
     * @return the next globally unique ID
     */
    public int yield() {
        if (this.stride == -1) {
            throw new IllegalStateException("The ID generator has not been initialized.");
        }

        final int result = (int) this.nextId;
        this.nextId += this.stride;
        return result;
    }

    /**
     * Yields the next ID.
     * @return the next globally unique ID
     */
    public long yieldLong() {
        if (this.stride == -1) {
            throw new IllegalStateException("The ID generator has not been initialized.");
        }

        final long result = this.nextId;
        this.nextId += this.stride;
        return result;
    }
}
