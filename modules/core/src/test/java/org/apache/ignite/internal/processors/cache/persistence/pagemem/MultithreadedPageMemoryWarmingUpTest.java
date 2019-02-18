package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;

/**
 *
 */
public class MultithreadedPageMemoryWarmingUpTest extends PageMemoryWarmingUpTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(1024 * 1024 * 1024)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(maxMemorySize)
                .setInitialSize(maxMemorySize)
                .setPersistenceEnabled(true)
                .setWarmingUpEnabled(true)
                .setWaitWarmingUpOnStart(waitWarmingUpOnStart)
                .setWarmingUpRuntimeDumpDelay(warmingUpRuntimeDumpDelay)
                .setWarmingUpMultithreadedEnabled(true)
            );

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }
}
