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
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.bookkeeper.util.DiskChecker.DiskWarnThresholdException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestDiskCheckerCheckDiskFull {

    // Test Parameters
    private File                        dir;
    private float                       diskUsageThreshold;
    private float                       diskUsageWarnThreshold;
    private Object                      expectedResult;
	
    // Data Structure instances
	private DiskCheckerExtended         diskChecker;


    @Parameterized.Parameters
	public static Collection<Object[]> testParameters() throws Exception {
		return Arrays.asList(new Object[][] {

			//  Test Suite (1)
            //  {dirPath,      diskUsageThreshold,     diskWarnUsageThreshold,     expectedResult}
            {getDir("Diskchecker"),            0.01f,     0.01f,           DiskOutOfSpaceException.class},
            {getDir("testFile"),               0.99f,     0.99f,           true},
            {getDirInvalid("invalidInput"),    0.99f,     0.99f,           false},
            {getDir("Diskchecker"),            0.99f,     0.01f,           DiskWarnThresholdException.class},
            {getDir(null),                     0.99f,     0.99f,           false}

		});
	}



    public TestDiskCheckerCheckDiskFull(File dir, float diskUsageThreshold, float diskUsageWarnThreshold, Object expectedResult){
        this.dir = dir;
        this.diskUsageThreshold = diskUsageThreshold;
        this.diskUsageWarnThreshold = diskUsageWarnThreshold;
        this.expectedResult = expectedResult;
   }
    

    
    @Before
    public void configure() throws IOException {
        this.diskChecker = new DiskCheckerExtended(this.diskUsageThreshold, this.diskUsageWarnThreshold);
        this.diskChecker.setDiskSpaceThresholdVisible(diskUsageThreshold, diskUsageWarnThreshold);
   } 



    @Test 
    public void testCheckDiskFull() throws IOException {
        
        try{
            
            float usedSpace = diskChecker.checkDiskFullVisible(dir);
            Assert.assertEquals(expectedResult, (usedSpace > 0f && usedSpace < 1f));
            
       } catch(Exception e){

            Assert.assertEquals(expectedResult, e.getClass());

       }
        
   }



    private static File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        File placeHolder = File.createTempFile("testFile", null, dir);      
        FileOutputStream placeHolderStream = new FileOutputStream(placeHolder);
        placeHolderStream.write(new byte[ 100 * 1024 ]);
        placeHolderStream.close();
        return dir;
   }


 
    private static Object getDir(Object dirPath) throws IOException {
        if (dirPath == null) return null;
        File dir = createTempDir((String) dirPath, "test");
        return dir;

   } 

    private static Object getDirInvalid(Object dirPath) throws IOException{
        // returns a File Object which is not a directory, but a temporary file with not declared parent dir.
        File dir = new File((String) dirPath);
        return dir;
   }




    // Extend the class DiskChecker to let the sut-methods to be accessible from this package, just for testing purpose.
    // This solution makes the test be valid only until the real class remains unchanged. 
    // The decision is taken only for educational purpose and to apply the methods learnt in classes, 
    // it should not be intended to be usable in "real" testing environments.
    public class DiskCheckerExtended extends DiskChecker{

        private final Logger LOG = LoggerFactory.getLogger(DiskCheckerExtended.class);

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


        public float checkDiskFullVisible(File dir) throws DiskOutOfSpaceException, DiskWarnThresholdException {
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
                return checkDiskFullVisible(dir.getParentFile());
           }
       }
        
        public void setDiskSpaceThresholdVisible(float diskSpaceThreshold, float diskUsageWarnThreshold) {
            validateThreshold(diskSpaceThreshold, diskUsageWarnThreshold);
            this.diskUsageThreshold = diskSpaceThreshold;
            this.diskUsageWarnThreshold = diskUsageWarnThreshold;
       }
   }

}
