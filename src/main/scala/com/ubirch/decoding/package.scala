package com.ubirch

package object decoding {

  val HARDWARE_ID_HEADER_KEY = "x-ubirch-hardware-id"

  //TODO: We should remove all hardcoded http codes from the niomon systems
  //Putting this here as a temporal solution
  val BAD_REQUEST = 400
  val UNAUTHORIZED = 401
  val PAYMENT_REQUIRED = 402
  val FORBIDDEN = 403
  val NOT_FOUND = 404

}
