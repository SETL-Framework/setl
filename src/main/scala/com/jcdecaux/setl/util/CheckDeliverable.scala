package com.jcdecaux.setl.util

import com.jcdecaux.setl.transformation.Factory

case class CheckDeliverable(
                             deliverableType: String,
                             deliveryId: String,
                             producer: Class[_],
                             consumer: Class[_ <: Factory[_]]
                           ) {

}
