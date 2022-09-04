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
import java.util.List;

import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager.RecoveryData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.util.ByteBufList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;


@RunWith( Parameterized.class )
public class TestDigestManagerVerifyDigestAndReturnLastConfirmed {

    // Test Parameters
    private ByteBuf                     dataReceived;
    private long                        lastAddConfirmed;
    private long                        length;
    private Object                      expectedResult;

    // Data Structures
    private static DigestManager        digestManager;
    private static final long           ledgerId = 1;
    private static final long           entryId = 1;
    private static final DigestType     digestType = DigestType.HMAC;
    private static final String         text = "text";
    private List<Long>                  res = new ArrayList<>();

    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        return Arrays.asList( new Object[][]{
            //  lastAddConfirmed,   length,        
            {-1,    5,  resultList(-1, 5) },
            {0,     0,  resultList(0, 0) },
            {1,     5,  resultList(1, 5) },
            {1,    -1,  resultList(1, -1) },
        });
    }

    public TestDigestManagerVerifyDigestAndReturnLastConfirmed(long lastAddConfirmed, long length, Object expectedResult){
        this.lastAddConfirmed = lastAddConfirmed;
        this.length = length;
        this.expectedResult = expectedResult;
    }

    @Before
    public void configure() throws GeneralSecurityException{
        digestManager = DigestManager.instantiate(ledgerId, "password".getBytes(), digestType, UnpooledByteBufAllocator.DEFAULT, false);
        this.dataReceived = dataReceived(lastAddConfirmed, length);
    }


    @Test
    public void testVerifyDigestAndReturnLastConfirmed(){
        try{
            RecoveryData recoveryData = digestManager.verifyDigestAndReturnLastConfirmed(dataReceived);
            res.add( recoveryData.getLastAddConfirmed());
            res.add(recoveryData.getLength());
            Assert.assertEquals(expectedResult, res);

        } catch ( Exception e ){
            Assert.assertEquals(expectedResult, e.getClass());
        }

    }

    @After
    public void tearDown(){
        res.clear();
    }


    private static ByteBuf dataReceived(long lastAddConfirmed, long length) throws GeneralSecurityException {
		DigestManager   digest = DigestManager.instantiate(ledgerId, "password".getBytes(), digestType, UnpooledByteBufAllocator.DEFAULT, false);     
		ByteBuf         payload = payloadBuffer();
        ByteBufList     byteBufList = digest.computeDigestAndPackageForSending(entryId, lastAddConfirmed,  length,  payload);
		return ByteBufList.coalesce(byteBufList);
	}

    private static ByteBuf payloadBuffer(){
        byte[]  data = text.getBytes();
        ByteBuf payloadBuffer = Unpooled.buffer(128);
        payloadBuffer.writeBytes(data);
        return  payloadBuffer;      
    }

    private static List<Long> resultList( long lastAddConfirmed, long length ){
        ArrayList<Long> list = new ArrayList<>();
        list.add(lastAddConfirmed);
        list.add(length);
        return list;
    }


    
}
