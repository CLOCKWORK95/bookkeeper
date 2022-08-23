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

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDiskCheckerSetThresholds {

    // Test Parameters
    private float                       diskUsageThreshold;
    private float                       diskUsageWarnThreshold;
	private Object                      expectedResult;
	
    // Data Structure instances
    private ArrayList<Float>            values;

	@Parameterized.Parameters
	public static Collection<Object[]> testParameters() throws Exception {
		return Arrays.asList(new Object[][] {

			//  Test Suite Minimale
            //  {diskUsageThreshold,   diskUsageWarnThreshold,   expectedResult}
            {0.0,      0.0,     IllegalArgumentException.class},
            {0.2,      0.2,     expected((float) 0.2, (float) 0.2)},
            {0.8,      0.2,     expected((float) 0.8, (float) 0.2)},
            {1.0,      1.0,     IllegalArgumentException.class},
            {0.2,      0.4,     IllegalArgumentException.class}

		});
	}
	

	public TestDiskCheckerSetThresholds(double diskUsageThreshold, double diskUsageWarnThreshold, Object expectedResult){
        this.diskUsageThreshold = (float) diskUsageThreshold;
        this.diskUsageWarnThreshold = (float) diskUsageWarnThreshold;
        this.expectedResult = expectedResult;
	}



	@Before
	public void configure() throws GeneralSecurityException {

        values = new ArrayList<>();
        values.add(diskUsageThreshold);
        values.add(diskUsageWarnThreshold );

	}


	@Test
	public void testSetThresholds() {

		try {

            new DiskChecker(diskUsageThreshold, diskUsageWarnThreshold);
            Assert.assertEquals(expectedResult, values);

		} catch (Exception e){
			Assert.assertEquals(expectedResult, e.getClass());
		}
      
	}



    private static ArrayList<Float> expected(float diskUsageThreshold, float diskUsageWarnThreshold){
        ArrayList<Float> expected = new ArrayList<>();
        expected.add(diskUsageThreshold);
        expected.add(diskUsageWarnThreshold);
        return expected;
   }


}  