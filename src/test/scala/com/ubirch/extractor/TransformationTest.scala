/*
 * Copyright (c) 2019 ubirch GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ubirch.extractor

import org.scalatest.{FlatSpec, Matchers}
import com.ubirch.extractor.Decode._

class TransformationTest extends FlatSpec with Matchers {

  "Message Payload Transformation" should "transform empty message" in {
    val result = transform("{}".getBytes)
    result.isSuccess shouldBe true
    result.get.toString should equal("ProtocolMessage(v=0x00,hint=0x00)")
  }
}
