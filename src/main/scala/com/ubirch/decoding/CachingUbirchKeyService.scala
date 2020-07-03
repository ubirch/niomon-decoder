package com.ubirch.decoding

import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.client.keyservice.UbirchKeyService
import com.ubirch.crypto.PubKey
import com.ubirch.niomon.base.NioMicroservice

/**
 * Represents a service for retrieving keys with a built-in cache
 * @param context Represents parts of the runtime exposed to the [[NioMicroserviceLogic]] instance.
 */
class CachingUbirchKeyService(context: NioMicroservice.Context) extends UbirchKeyService({
  val url = context.config.getString("ubirchKeyService.client.rest.host")
  if (url.startsWith("http://") || url.startsWith("https://")) url else s"http://$url"
}) with StrictLogging {

  lazy val getPublicKeysCached: UUID => List[PubKey] =
    context.cached(super.getPublicKey _).buildCache("public-keys-cache", shouldCache = _.nonEmpty)

  override def getPublicKey(uuid: UUID): List[PubKey] = getPublicKeysCached(uuid)
}
