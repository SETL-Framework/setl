package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.exception.{AlreadyExistsException, InvalidDeliveryException}
import com.jcdecaux.datacorp.spark.internal.{HasUUIDRegistry, Logging}
import com.jcdecaux.datacorp.spark.storage.repository.SparkRepository
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory, FactoryDeliveryMetadata}
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * DeliverableDispatcher use a Directed Acyclic Graph (DAG) to transfer data between different stages.
  *
  * It can:
  * <ul>
  * <li>collect a [[com.jcdecaux.datacorp.spark.transformation.Deliverable]]
  * from a [[com.jcdecaux.datacorp.spark.transformation.Factory]]</li>
  * <li>find the right [[com.jcdecaux.datacorp.spark.transformation.Deliverable]]
  * from its pool and send it to a [[com.jcdecaux.datacorp.spark.transformation.Factory]]</li>
  * </ul>
  */
@InterfaceStability.Evolving
private[spark] class DeliverableDispatcher extends Logging with HasUUIDRegistry {

  private[workflow] val deliveries: ArrayBuffer[Deliverable[_]] = ArrayBuffer()
  private[this] var graph: DAG = _

  def setDataFlowGraph(graph: DAG): this.type = {
    this.graph = graph
    this
  }

  /**
    * Add a new Deliverable object into DeliverableDispatcher's delivery pool. <br>
    *
    * <b>Attention:</b> DeliverableDispatcher only guarantee that each deliverable object has different UUID.
    * In other word, use cannot call twice <code>setDelivery</code> with the same argument. However, it doesn't
    * check if multiple deliverables have the same data type with the same producer and the same consumer
    *
    * @param v : Deliverable
    * @return this DeliverableDispatcher
    */
  private[workflow] def setDelivery(v: Deliverable[_]): this.type = {
    log.debug(s"Add new delivery: ${v.payloadType}. Producer: ${v.producer.getSimpleName}")

    if (registerNewItem(v)) {
      deliveries.append(v)
    } else {
      throw new AlreadyExistsException(s"The current deliverable ${v.getUUID} already exists")
    }
    this
  }

  /**
    * Find the corresponding [[Deliverable]] from the pool with the given runtime Type information
    *
    * @param availableDeliverable an array of available deliverable
    * @return
    */
  @throws[InvalidDeliveryException]("find multiple matched deliveries")
  private[this] def getDelivery(availableDeliverable: Array[Deliverable[_]],
                                consumer: Class[_ <: Factory[_]],
                                producer: Class[_ <: Factory[_]]): Option[Deliverable[_]] = {
    availableDeliverable.length match {
      case 0 =>
        log.warn("Can not find any deliverable")
        None
      case 1 =>
        val deliverable = availableDeliverable.head
        if (deliverable.consumer.nonEmpty && !deliverable.consumer.contains(consumer)) {
          throw new InvalidDeliveryException(s"Can't find ${consumer.getSimpleName} in the consumer list")
        }
        log.debug("Find Deliverable")
        Some(availableDeliverable.head)
      case _ =>
        log.info("Find multiple Deliverable with same type. Try matching by producer")
        val matchedByProducer = availableDeliverable.filter(_.producer == producer)

        matchedByProducer.length match {
          case 0 =>
            log.warn("Can not find any deliverable that matches the producer")
            None
          case 1 =>
            log.debug("Find Deliverable")
            Some(matchedByProducer.head)
          case _ =>
            log.info("Find multiple Deliverable with same type and same producer Try matching by consumer")
            val matchedByConsumer = matchedByProducer
              .filter(_.consumer.nonEmpty)
              .filter(_.consumer.contains(consumer))

            matchedByConsumer.length match {
              case 0 =>
                log.warn("Can not find any deliverable that matches the consumer")
                None
              case 1 =>
                log.debug("Find Deliverable")
                Some(matchedByConsumer.head)
              case _ =>
                throw new InvalidDeliveryException("Find multiple deliveries having the same type, producer and consumer")
            }
        }
    }
  }

  /**
    * Get all the [[Deliverable]] by matching their payload's type with the given type
    *
    * @param deliveryType type of data
    * @return
    */
  private[workflow] def findDeliverableByType(deliveryType: ru.Type): Array[Deliverable[_]] = {
    deliveries.filter(_.hasSamePayloadType(deliveryType)).toArray
  }

  /**
    * Get all the Deliverable by matching the payload type name with the given name
    *
    * @param deliveryName string, type of data
    * @return
    */
  private[workflow] def findDeliverableByName(deliveryName: String): Array[Deliverable[_]] = {
    deliveries.filter(_.hasSamePayloadType(deliveryName)).toArray
  }

  /**
    * Collect a [[Deliverable]] from a [[Factory]] and set the deliverable to this DeliverableDispatcher
    *
    * @param factory a factory object
    * @return
    */
  private[workflow] def collectDeliverable(factory: Factory[_]): this.type = {
    setDelivery(factory.getDelivery)
  }

  /**
    * Collect [[Deliverable]] from a stage and set those deliverable to this DeliverableDispatcher
    *
    * @param stage stage
    * @return
    */
  private[workflow] def collectDeliverable(stage: Stage): this.type = {
    stage.deliveries.foreach(setDelivery)
    this
  }

  /**
    * Used only for testing
    */
  private[workflow] def testDispatch(factory: Factory[_]): this.type = {
    val setters = FactoryDeliveryMetadata
      .builder()
      .setFactory(factory)
      .getOrCreate()

    dispatch(factory, setters)
  }

  /**
    * For the given factory, find its setters from the DAG and dispatch deliveries
    *
    * @param factory a Factory[A] object
    * @return
    */
  @throws[NoSuchElementException]("Cannot find any matched delivery")
  @throws[InvalidDeliveryException]("Find multiple matched deliveries")
  private[workflow] def dispatch(factory: Factory[_]): this.type = {
    val deliveryMetadata = this.graph.findDeliveryMetadata(factory)
    dispatch(factory, deliveryMetadata)
  }

  /**
    * For a given dataset runtime type, for example: org.apache.spark.sql.Dataset[T], get the type T and
    * return the name of the corresponding SparkRepository[T]
    *
    * @param datasetType runtime type of a dataset
    * @return a string like com.jcdecaux.datacorp.spark.storage.repository.SparkRepository[T]
    */
  private[workflow] def getRepositoryNameFromDataset(datasetType: ru.Type): String = {
    val sparkRepo = ru.typeOf[SparkRepository[_]].toString.dropRight(3)
    val thisArgType = datasetType.toString.split("\\[")(1).dropRight(1)
    s"$sparkRepo[$thisArgType]"
  }

  /**
    * Dispatch the right deliverable object to the corresponding methods
    * (denoted by the @Delivery annotation) of a factory
    *
    * @param factory target factory
    * @return this type
    */
  @throws[NoSuchElementException]
  private[this] def dispatch(factory: Factory[_], deliveries: Iterable[FactoryDeliveryMetadata]): this.type = {
    deliveries
      .foreach {
        deliveryMeta =>
          // Loop through the type of all arguments of a method and get the Deliverable that correspond to the type
          val args = deliveryMeta.argTypes.map {
            argType =>
              log.debug(s"Look for available ${simpleTypeNameOf(argType)} in delivery pool")
              val availableDeliverable = findDeliverableByType(argType)

              val finalDelivery = getDelivery(
                availableDeliverable = availableDeliverable,
                consumer = factory.getClass,
                producer = deliveryMeta.producer
              ) match {
                case Some(deliverable) =>
                  // If we find some delivery, then return it if non null
                  require(deliverable.payload != null, "Deliverable is null")
                  Some(deliverable)

                case _ =>
                  /*
                   if there's no delivery, then check if autoLoad is set to true.
                   if true, then we will try to find the corresponding repository of the input type and load the data
                  */
                  val isDataset = argType.toString.startsWith(ru.typeOf[Dataset[_]].toString.dropRight(2)) // check if the input argType is a dataset

                  if (deliveryMeta.autoLoad && isDataset) {
                    val repoName = getRepositoryNameFromDataset(argType)
                    log.info("Find auto load enabled, looking for repository")

                    val availableRepos = findDeliverableByName(repoName)
                    val matchedRepo = getDelivery(availableRepos, factory.getClass, classOf[External])

                    matchedRepo match {
                      case Some(deliverable) =>
                        val repo = deliverable.payload.asInstanceOf[SparkRepository[_]]
                        val data = if (deliveryMeta.condition != "") {
                          repo.findAll().filter(deliveryMeta.condition)
                        } else {
                          repo.findAll()
                        }
                        Option(new Deliverable(data))
                      case _ =>
                        if (!deliveryMeta.optional) {
                          // throw exception if there's no deliverable and the current delivery is not optional
                          throw new NoSuchElementException(s"Can not find $repoName from DispatchManager")
                        }
                        None
                    }
                  } else {
                    None
                  }
              }

              finalDelivery match {
                case Some(deliverable) => deliverable
                case _ =>
                  if (!deliveryMeta.optional) {
                    throw new NoSuchElementException(s"Can not find type $argType from DispatchManager")
                  } else {
                    Deliverable.empty()
                  }
              }
          }

          if (args.exists(_.isEmpty)) {
            log.warn(s"No deliverable was found for optional input ${deliveryMeta.name}")
          } else {
            log.debug(s"Dispatch by calling ${factory.getClass.getSimpleName}.${deliveryMeta.name}")
            val factorySetter = factory.getClass.getMethod(deliveryMeta.name, args.map(_.classInfo): _*)
            factorySetter.invoke(factory, args.map(_.get.asInstanceOf[Object]): _*)
          }
      }
    this
  }

  private[this] val simpleTypeNameOf: ru.Type => String = {
    rType => rType.toString.split("\\[").map(_.split("\\.").last).mkString("[")
  }

}
