/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.util.collections;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith( Parameterized.class )
public class TestDiskCheckerCheckDiskFull {

    private static ArrayList<File>      tempDirs = new ArrayList<>();

    // Test Parameters
    private float                       diskUsageThreshold = 0.99f;
    private float                       diskUsageWarnThreshold = 0.99f;
	private Object                      expectedResult;
	
    // Data Structure instances
	private DiskCheckerExtended         diskChecker;

    @Parameterized.Parameters
	public static Collection<Object[]> testParameters() throws Exception {
		return Arrays.asList(new Object[][] {

			//  Test Suite (1)
            //  { dir,   expectedResult }
            { 0.0,     Exception.class },
            { 0.2,     Exception.class },
            { 0.8,     Exception.class },
            { 1.0,     Exception.class },
            { 0.2,     Exception.class }

		});
	}
    
    
    
    @Before
    public void configure(){
        diskChecker = new DiskCheckerExtended( diskUsageThreshold, diskUsageWarnThreshold );
    }


    @Test //(expected = DiskOutOfSpaceException.class)
    public void testCheckDiskFull() throws IOException {
        File file = createTempDir("DiskCheck", "test");
        long usableSpace = file.getUsableSpace();
        long totalSpace = file.getTotalSpace();
        float threshold = minMaxThreshold( ( 1f - ( (float) usableSpace / (float) totalSpace ) ) - ( 1.0f - diskUsageThreshold ) );

        diskChecker.setDiskSpaceThreshold(threshold, threshold);
        diskChecker.checkDiskFull(file);
    }



    private static File createTempDir( String prefix, String suffix ) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }


    private static float minMaxThreshold(float threshold) {
        final float minThreshold = 0.0000001f;
        final float maxThreshold = 0.999999f;

        threshold = Math.min(threshold, maxThreshold);
        threshold = Math.max(threshold, minThreshold);
        return threshold;
    }



    // Extend the class DiskChecker to let the sut-methods to be accessible from the outside, for testing purpose.
    public class DiskCheckerExtended extends DiskChecker{

        private final Logger LOG = LoggerFactory.getLogger( DiskCheckerExtended.class );

        private float diskUsageThreshold;
        private float diskUsageWarnThreshold;


        public DiskCheckerExtended(float threshold, float warnThreshold) {
            super(threshold, warnThreshold);
        }


        private void validateThreshold(float diskSpaceThreshold, float diskSpaceWarnThreshold) {
            if (diskSpaceThreshold <= 0 || diskSpaceThreshold >= 1 || diskSpaceWarnThreshold - diskSpaceThreshold > 1e-6) {
                throw new IllegalArgumentException("Disk space threashold: "
                        + diskSpaceThreshold + " and warn threshold: " + diskSpaceWarnThreshold
                        + " are not valid. Should be > 0 and < 1 and diskSpaceThreshold >= diskSpaceWarnThreshold");
            }
        }


        public float checkDiskFull(File dir) throws DiskOutOfSpaceException, DiskWarnThresholdException {
            if (null == dir) {
                return 0f;
            }
            if (dir.exists()) {
                long usableSpace = dir.getUsableSpace();
                long totalSpace = dir.getTotalSpace();
                float free = (float) usableSpace / (float) totalSpace;
                float used = 1f - free;
                if (used > diskUsageThreshold) {
                    LOG.error("Space left on device {} : {}, Used space fraction: {} > threshold {}.",
                            dir, usableSpace, used, diskUsageThreshold);
                    throw new DiskOutOfSpaceException("Space left on device "
                            + usableSpace + " Used space fraction:" + used + " > threshold " + diskUsageThreshold, used);
                }
                // Warn should be triggered only if disk usage threshold doesn't trigger first.
                if (used > diskUsageWarnThreshold) {
                    LOG.warn("Space left on device {} : {}, Used space fraction: {} > WarnThreshold {}.",
                            dir, usableSpace, used, diskUsageWarnThreshold);
                    throw new DiskWarnThresholdException("Space left on device:"
                            + usableSpace + " Used space fraction:" + used + " > WarnThreshold:" + diskUsageWarnThreshold,
                            used);
                }
                return used;
            } else {
                return checkDiskFull( dir.getParentFile() );
            }
        }
        
        public void setDiskSpaceThreshold( float diskSpaceThreshold, float diskUsageWarnThreshold ) {
            validateThreshold(diskSpaceThreshold, diskUsageWarnThreshold);
            this.diskUsageThreshold = diskSpaceThreshold;
            this.diskUsageWarnThreshold = diskUsageWarnThreshold;
        }
    }

}
