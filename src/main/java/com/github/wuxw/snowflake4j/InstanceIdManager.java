package com.github.wuxw.snowflake4j;

import java.io.IOException;

/**
 * Interface of instance id manager
 * Created by wuxinw on 2016/9/14.
 */
public interface InstanceIdManager {

    /**
     * Gets the max number of instances.
     *
     * @return the max number of instances.
     */
    int getMaxNumberOfInstances();

    /**
     * Attempts to get an instance id.
     *
     * @return If successful the return value will be greater than or equal 0. If
     * not successful < 0.
     * @throws IOException
     */
    int tryToGetId() throws Exception;

    int getCurrentId();

    InstanceIdManager releaseId();
}
