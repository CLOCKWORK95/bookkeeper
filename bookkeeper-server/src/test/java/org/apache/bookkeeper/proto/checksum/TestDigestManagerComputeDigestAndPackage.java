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

package org.apache.bookkeeper.proto.checksum;

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.lang.NegativeArraySizeException;

@RunWith(Parameterized.class)
public class TestDigestManagerComputeDigestAndPackage {

    // Test Parameters
    private DigestType      digestType;
	private static long     lastAddConfirmed;
	private static long     entryId;
	private static long     length;
    private ByteBuf         data;
    private boolean         useV2Protocol;
	private Object          expectedResult;
	
    // Data Structure instances
    private DigestManager   digestManager;
	

	@Parameterized.Parameters
	public static Collection<Object[]> testParameters() throws Exception {
		return Arrays.asList(new Object[][] {

			//  Test Suite (1)
            //  {digest type , lastAddConfirmed,     entryID,    length,     data,     useV2Protocol,   expectedResult }
            {DigestType.HMAC,      1, 2, 1,     getEntry(1),    true,        expectedHeader(1, 2, 1, 1)    },
            {DigestType.CRC32,     0, 0, 0,     getEntry(0),    true,        expectedHeader(1, 0, 0, 0)    },
            {DigestType.CRC32C,   -1, 0, 0,     null,                   false,      NullPointerException.class },
            {DigestType.DUMMY,     0, 1, -1,    getEntry(-1),           false,       expectedHeader(1, 1, 0, -1) }

		});
	}
	

	public TestDigestManagerComputeDigestAndPackage(DigestType digestType, long lastAddConfirmed, long entryId, long length,  ByteBuf data, boolean useV2Protocol, Object expectedResult){
		this.digestType = digestType;
        TestDigestManagerComputeDigestAndPackage.lastAddConfirmed = lastAddConfirmed;
		TestDigestManagerComputeDigestAndPackage.entryId = entryId;
		TestDigestManagerComputeDigestAndPackage.length = length;
        this.data = data;
		this.useV2Protocol = useV2Protocol;
        this.expectedResult = expectedResult;
	}

	@Before
	public void configure() throws GeneralSecurityException {
        digestManager = DigestManager.instantiate(1, "password".getBytes(), digestType, UnpooledByteBufAllocator.DEFAULT, useV2Protocol);
	}


	@Test
	public void testComputeDigestAndPackage(){

		try {
			ByteBufList sut = digestManager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, data);
            ArrayList<Long> header = new ArrayList<>();
            int i = 0;
            while (i < 4){
                try{
                    long component = sut.getBuffer(0).readLong();
                    header.add(component);
                    i++;
                }catch (IndexOutOfBoundsException e){
                    break;
                }
            }
            // Assert that the header of the package is consistent to the input informations.
            Assert.assertEquals(expectedResult, header);			

            Assert.assertEquals(data.readableBytes(), sut.getBuffer(1).readableBytes());


		} catch (Exception e){
			Assert.assertEquals(expectedResult, e.getClass());
		}
      
	}


    private static ByteBuf getEntry(long length){
        
        try {
            byte[] entryPayload = new byte[ (int) length ];
            ByteBuf validEntry = Unpooled.buffer(512);
            validEntry.writeBytes(entryPayload);
            return validEntry;
        } catch (NegativeArraySizeException e){
            byte[] entryPayload = new byte[0];
            ByteBuf validEntry = Unpooled.buffer(512);
            validEntry.writeBytes(entryPayload);
            return validEntry;
        }

    }


    private static ArrayList<Long> expectedHeader(long ledgerId, long entryId, long lastAddConfirmed, long length){
        ArrayList<Long> expectedHeader = new ArrayList<>();
        expectedHeader.add(ledgerId);
        expectedHeader.add(entryId);
        expectedHeader.add(lastAddConfirmed);
        expectedHeader.add(length);
        return expectedHeader;
    }



}  