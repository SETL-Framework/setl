package io.github.setl.util

import io.github.setl.transformation.Factory

case class ExpectedDeliverable(deliverableType: String,
                               deliveryId: String,
                               producer: Class[_],
                               consumer: Class[_ <: Factory[_]]) {

}
