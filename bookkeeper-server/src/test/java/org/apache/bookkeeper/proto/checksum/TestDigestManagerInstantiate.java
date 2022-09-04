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
import java.util.Arrays;
import java.util.Collection;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;


@RunWith(Parameterized.class)
public class TestDigestManagerInstantiate {

    // Test parameters
    private long                        ledgerId;
    private byte[]                      password;
    private DigestType                  digestType;
    private ByteBufAllocator            allocator;
    private boolean                     useV2Protocol;
    private Object                      expectedResult;

    // Data Structures and constants
    private Object                      digestManager;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        return Arrays.asList(new Object[][] {

			//  Test Suite Minimale
            //  ledgerId        password        digestType            allocator                          useV2Protocol    expectedResult 
            {-1,    "password".getBytes(),      DigestType.HMAC,      UnpooledByteBufAllocator.DEFAULT,     false,              MacDigestManager.class},
            {0,     "password".getBytes(),      DigestType.CRC32,     UnpooledByteBufAllocator.DEFAULT,     true,               CRC32DigestManager.class},
            {1,     "password".getBytes(),      DigestType.CRC32C,    null,                                 false,              CRC32CDigestManager.class},
            {1,     "".getBytes(),              DigestType.DUMMY,     UnpooledByteBufAllocator.DEFAULT,     true,               DummyDigestManager.class},
            {1,     null,                       DigestType.HMAC,      UnpooledByteBufAllocator.DEFAULT,     false,              NullPointerException.class}

		});
    }

    public TestDigestManagerInstantiate(long ledgerId, byte[] password, DigestType digestType, ByteBufAllocator allocator, boolean useV2Protocol, Object expectedResult){
        this.ledgerId = ledgerId;
        this.password = password;
        this.digestType = digestType;
        this.allocator = allocator;
        this.useV2Protocol = useV2Protocol;
        this.expectedResult = expectedResult;
    }


    @Test
    public void testInstantiate(){
        try{
            digestManager = DigestManager.instantiate(ledgerId, password, digestType, allocator, useV2Protocol);     
            Assert.assertEquals(expectedResult, digestManager.getClass());
        } catch( Exception e ){
            Assert.assertEquals(expectedResult, e.getClass());
        }
    }

    
}
