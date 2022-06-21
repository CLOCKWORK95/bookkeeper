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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

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

    private static ArrayList<File>      directories = new ArrayList<>();

    // Test Parameters
    private File                        dir;
    private Object                      expectedResult;
	
    // Data Structure instances
	private DiskCheckerExtended         diskChecker;
    private float                       diskUsageThreshold = 0.99f;
    private float                       diskUsageWarnThreshold = 0.99f;
    private long                        usableDiskSpace;
    private long                        totalDiskSpace;



    @Parameterized.Parameters
	public static Collection<Object[]> testParameters() throws Exception {
		return Arrays.asList(new Object[][] {

			//  Test Suite (1)
            // pensa bene ai parametri!
            //  { dir,      diskUsageThreshold,     diskWarnUsageThreshold,     expectedResult }
            { getDir( "Diskchecker", null ),        Exception.class },
            { getDir( "testFile", null ),           Exception.class },
            { getDir( "invalidPath", null ),        Exception.class },
            { getDir( "Diskchecker", null ),        Exception.class },
            { getDir( "Diskchecker", null ),        Exception.class }

		});
	}



    public TestDiskCheckerCheckDiskFull( File dir, Object expectedResult ){
        this.dir = dir;
        this.expectedResult = expectedResult;
    }
    

    
    @Before
    public void configure() throws IOException {

        diskChecker = new DiskCheckerExtended( diskUsageThreshold, diskUsageWarnThreshold );
        
        File directory = addTempDir("DiskChecker", null);
        
        File file = addTempFileWithWrite( directory, "testFile" );

        totalDiskSpace = file.getTotalSpace();

        usableDiskSpace = file.getUsableSpace();


    }



    @Test //(expected = DiskOutOfSpaceException.class)
    public void testCheckDiskFull() throws IOException {
        
        float threshold = minMaxThreshold( ( 1f - ( (float) usableDiskSpace / (float) totalDiskSpace ) ) - ( 1.0f - diskUsageThreshold ) );
        diskChecker.setDiskSpaceThreshold( threshold, threshold );
        try{

            diskChecker.checkDiskFull( dir );
            
        } catch( Exception e ){

            Assert.assertEquals( expectedResult, e.getClass() );

        }
        
    }


    private static File getDir( String prefix, String suffix ) throws IOException {
        File dir = IOUtils.createTempDir( prefix, suffix );
        return dir;
    }


    private static File addTempDir( String prefix, String suffix ) throws IOException {
        File dir = IOUtils.createTempDir( prefix, suffix );
        directories.add( dir );
        return dir;
    }

    private static File addTempFileWithWrite( File directory , String filename ) throws IOException {
        File placeHolder = new File( directory, filename );      
        FileOutputStream placeHolderStream = new FileOutputStream( placeHolder );
        placeHolderStream.write( new byte[ 100 * 1024 ] );
        placeHolderStream.close();
        return placeHolder;
    }


    private static float minMaxThreshold(float threshold) {
        final float minThreshold = 0.0000001f;
        final float maxThreshold = 0.999999f;

        threshold = Math.min(threshold, maxThreshold);
        threshold = Math.max(threshold, minThreshold);
        return threshold;
    }



    // Extend the class DiskChecker to let the sut-methods to be accessible from this package, just for testing purpose.
    // This solution makes the test be valid only until the real class remains unchanged. 
    // The decision is taken only for educational purpose and to apply the methods learnt in classes, 
    // it should not be intended to be usable in "real" testing environments.
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
