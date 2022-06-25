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
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.DiskChecker.DiskErrorException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith( Parameterized.class )
public class TestDiskCheckerCheckDir {

    // Test Parameters
    private String      dirPath;
    private boolean     isReadable;
    private boolean     isWritable;
    private boolean     isDirectory;
    private Object      expectedResult;

    // Data Structures
    private DiskChecker diskChecker;
    private File        dir;


    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        return Arrays.asList( new Object[][]{
            // dirpath      isReadable      isWritable      isDirectory     expectedResult
            // Test Suite (1)
            {    null,          true,       true,       true,       NullPointerException.class },
            {   "checkdir",     true,       true,       true,       true },
            {   "checkdir",     false,      true,       true,       DiskErrorException.class },
            {   "checkdir",     true,       false,      true,       DiskErrorException.class },
            {   "checkdir",     true,       true,       false,      DiskErrorException.class }

        });
    }


    public TestDiskCheckerCheckDir( String dirPath, boolean isReadable, boolean isWritable, boolean isDirectory, Object expectedResult ){
        this.dirPath = dirPath;
        this.isReadable = isReadable;
        this.isWritable = isWritable;
        this.isDirectory = isDirectory;
        this.expectedResult = expectedResult;
    }



    @Before 
    public void configure() throws IOException {    
        diskChecker = new DiskChecker( 0.99f, 0.99f );
        this.dir = directorySetup( this.dirPath );

    }


    public File directorySetup( String dirPath ) throws IOException {

        if ( dirPath == null ) {
            return null;
        }

        File parent = IOUtils.createTempDir( dirPath, null );
        File dir = parent;
        if ( !this.isDirectory ) {
            File child = File.createTempFile( dirPath, null, parent );
            dir = child;
        } 
        if ( !this.isReadable ){
            dir.setReadable( false );
        }
        if ( !this.isWritable ){
            dir.setWritable( false );
        }
        return dir;

    }



    @Test
    public void testCheckDir(){
        try{
            float used = diskChecker.checkDir(dir);
            Assert.assertTrue( used > 0f && used < 1f );
        } catch ( Exception e ){
            Assert.assertEquals( expectedResult, e.getClass() );
        } 
    }
    

}
