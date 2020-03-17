package com.jcdecaux.setl.util

import com.jcdecaux.setl.transformation.Factory

case class ExpectedDeliverable(
                                deliverableType: String,
                                deliveryId: String,
                                producer: Class[_],
                                consumer: Class[_ <: Factory[_]]
                              ) {

}
