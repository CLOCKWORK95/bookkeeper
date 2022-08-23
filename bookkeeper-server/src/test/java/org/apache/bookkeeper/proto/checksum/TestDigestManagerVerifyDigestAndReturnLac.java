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
import java.util.Arrays;
import java.util.Collection;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;



@RunWith(Parameterized.class)
public class TestDigestManagerVerifyDigestAndReturnLac {

    // Test Parameters
    private DigestType              digestType;
    private long                    ledgerId;
    private ByteBuf                 dataReceived;
    private Object                  expectedResult;

    // Data Structures
    private DigestManager           digestManager;


    @Parameterized.Parameters
    public static Collection<Object[]> parameters() throws GeneralSecurityException{
        return Arrays.asList(new Object[][]{
            // digestType       ledgerId    lac             dataReceived byte buffer              expectedResult
            // Test suite (1)
            {DigestType.HMAC,      1,     -1,      dataReceived(1, (long)-1, DigestType.HMAC),     (long)-1},
            {DigestType.CRC32,     1,      0,      dataReceived(1, 0, DigestType.DUMMY),       BKDigestMatchException.class},
            {DigestType.CRC32C,    1,      1,      dataReceived(1, 1, DigestType.CRC32C),      (long) 1},
            {DigestType.DUMMY,     1,      0,      dataReceived(0, 0, DigestType.DUMMY),       BKDigestMatchException.class},
            {DigestType.CRC32,     1,      0,      null,                                                             NullPointerException.class},
            // Control Flow Coverage
            {DigestType.HMAC,      1,      1,      dataReceivedWrongPassword(1, (long)1, DigestType.HMAC),     BKDigestMatchException.class}
       });
   }


    public TestDigestManagerVerifyDigestAndReturnLac(DigestType digestType, long ledgerId, long lac, ByteBuf dataReceived, Object expectedResult){
        this.digestType = digestType;
        this.ledgerId = ledgerId;
        this.dataReceived = dataReceived;
        this.expectedResult = expectedResult;
   }


    @Before
    public void configure() throws GeneralSecurityException {
        this.digestManager = DigestManager.instantiate(ledgerId, "password".getBytes(), digestType, UnpooledByteBufAllocator.DEFAULT, false); 
   }


    @Test
    public void testVerifyDigest(){
        try{
            long extractedResult = digestManager.verifyDigestAndReturnLac(dataReceived);
            Assert.assertEquals(expectedResult, extractedResult);
       } catch(Exception e){
            Assert.assertEquals(expectedResult, e.getClass());
       }
   }

    
    private static ByteBuf dataReceived(int declaredLedgerId, long lac, DigestType declaredDigestType) throws GeneralSecurityException {
		DigestManager   digest = DigestManager.instantiate(declaredLedgerId, "password".getBytes(), declaredDigestType, UnpooledByteBufAllocator.DEFAULT, false);     
        ByteBufList     byteBufList = digest.computeDigestAndPackageForSendingLac(lac);
		return          byteBufList.getBuffer(0);
	}

    private static ByteBuf dataReceivedWrongPassword(int declaredLedgerId, long lac, DigestType declaredDigestType) throws GeneralSecurityException {
		DigestManager   digest = DigestManager.instantiate(declaredLedgerId, "wrongPassword".getBytes(), declaredDigestType, UnpooledByteBufAllocator.DEFAULT, false);     
        ByteBufList     byteBufList = digest.computeDigestAndPackageForSendingLac(lac);
		return          byteBufList.getBuffer(0);
	}





    
}
