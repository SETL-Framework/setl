package com.jcdecaux.setl.workflow

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.exception.{AlreadyExistsException, InvalidDeliveryException}
import com.jcdecaux.setl.internal.{HasRegistry, Logging}
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.{Deliverable, Factory, FactoryDeliveryMetadata}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.reflect.runtime.{universe => ru}

/**
 * DeliverableDispatcher use a Directed Acyclic Graph (DAG) to transfer data between different stages.
 *
 * It can:
 * <ul>
 * <li>collect a [[com.jcdecaux.setl.transformation.Deliverable]]
 * from a [[com.jcdecaux.setl.transformation.Factory]]</li>
 * <li>find the right [[com.jcdecaux.setl.transformation.Deliverable]]
 * from its pool and send it to a [[com.jcdecaux.setl.transformation.Factory]]</li>
 * </ul>
 */
@InterfaceStability.Evolving
private[setl] class DeliverableDispatcher extends Logging
  with HasRegistry[Deliverable[_]] {

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
  private[workflow] def addDeliverable(v: Deliverable[_]): this.type = {
    log.debug(s"Add new delivery: ${v.payloadType}. Producer: ${v.producer.getSimpleName}")
    registerNewItem(v)
    this
  }

  /**
   * Find the corresponding [[Deliverable]] from the pool with the given runtime Type information
   *
   * @param availableDeliverable an array of available deliverable
   * @return
   */
  @throws[InvalidDeliveryException]("find multiple matched deliveries")
  private[this] def getDeliverable(availableDeliverable: Array[Deliverable[_]],
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
    findDeliverableBy(x => x.hasSamePayloadType(deliveryType))
  }

  private[workflow] def findDeliverableBy(condition: Deliverable[_] => Boolean): Array[Deliverable[_]] = {
    this.getRegistry.values.filter(d => condition(d)).toArray
  }

  /**
   * Get all the Deliverable by matching the payload type name with the given name
   *
   * @param deliveryName string, type of data
   * @return
   */
  private[workflow] def findDeliverableByName(deliveryName: String): Array[Deliverable[_]] = {
    findDeliverableBy(x => x.hasSamePayloadType(deliveryName))
  }

  /**
   * Collect a [[Deliverable]] from a [[Factory]] and set the deliverable to this DeliverableDispatcher
   *
   * @param factory a factory object
   * @return
   */
  private[workflow] def collectDeliverable(factory: Factory[_]): this.type = {
    addDeliverable(factory.getDelivery)
  }

  /**
   * Collect [[Deliverable]] from a stage and set those deliverable to this DeliverableDispatcher
   *
   * @param stage stage
   * @return
   */
  private[workflow] def collectDeliverable(stage: Stage): this.type = {
    stage.deliverable.foreach(addDeliverable)
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
   * @return a string like com.jcdecaux.setl.storage.repository.SparkRepository[T]
   */
  private[workflow] def getRepositoryNameFromDataset(datasetType: ru.Type): String = {
    val sparkRepo = ru.typeOf[SparkRepository[_]].toString.dropRight(3)
    val thisArgType = datasetType.toString.split("\\[")(1).dropRight(1)
    s"$sparkRepo[$thisArgType]"
  }

  /**
   * If the deliverable object is a Dataset and if a condition is defined in the @Delivery annotation, then filter
   * the input Dataset with this condition. Otherwise, return the input deliverable
   *
   * @param condition   condition string
   * @param deliverable deliverable object
   * @tparam D input type
   * @return `Option[Deliverable[D]]`
   */
  private[this] def handleDeliveryCondition[D: ru.TypeTag](condition: String,
                                                           deliverable: Deliverable[D]): Option[Deliverable[D]] = {
    if (condition != "" && deliverable.payloadClass.isAssignableFrom(classOf[Dataset[Row]])) {
      log.debug("Find data frame filtering condition to be applied")
      Some(
        new Deliverable(
          deliverable
            .getPayload
            .asInstanceOf[Dataset[Row]]
            .filter(condition)
            .asInstanceOf[D]
        )
      )
    } else {
      Option(deliverable)
    }
  }

  /**
   * For a given type, if it is a Dataset of type `T`, then find its corresponding `SparkRepository[T]` and use it
   * to load data from the data store.
   *
   * @param argType    input type
   * @param deliveryId delivery id
   * @param consumer   consumer of the input
   * @param condition  loading condition
   * @param optional   if set to true, then no exception will be thrown when there is no available SparkRepository,
   *                   false otherwise
   * @return `Option[Deliverable[T]]`
   */
  private[this] def handleAutoLoad(argType: ru.Type,
                                   deliveryId: String,
                                   consumer: Factory[_],
                                   condition: String,
                                   optional: Boolean): Option[Deliverable[_]] = {
    // check if the input argType is a dataset
    val isDataset = argType.toString.startsWith(ru.typeOf[Dataset[_]].toString.dropRight(2))

    if (isDataset) {
      val repoName = getRepositoryNameFromDataset(argType)
      log.info("Auto load enabled, looking for repository")

      val availableRepos = findDeliverableBy {
        deliverable => deliverable.hasSamePayloadType(repoName) && deliverable.deliveryId == deliveryId
      }

      val matchedRepo = getDeliverable(availableRepos, consumer.getClass, classOf[External])

      matchedRepo match {
        case Some(deliverable) =>
          val repo = deliverable.getPayload.asInstanceOf[SparkRepository[_]]
          val data = if (condition != "") {
            repo.findAll().filter(condition)
          } else {
            repo.findAll()
          }
          Option(new Deliverable(data))
        case _ =>
          if (!optional) {
            // throw exception if there's no deliverable and the current delivery is not optional
            throw new NoSuchElementException(s"Can not find $repoName from DeliverableDispatcher")
          }
          None
      }
    } else {
      None
    }
  }

  /**
   * Dispatch the right deliverable object to the corresponding methods
   * (denoted by the @Delivery annotation) of a factory
   *
   * @param consumer target factory
   * @return this type
   */
  @throws[NoSuchElementException]
  private[this] def dispatch(consumer: Factory[_], deliveries: Iterable[FactoryDeliveryMetadata]): this.type = {
    deliveries
      .foreach {
        deliveryMeta =>
          log.debug(s"Delivery name: ${deliveryMeta.name}")
          // Loop through the type of all arguments of a method and get the Deliverable that correspond to the type
          val args = deliveryMeta.argTypes.map {
            argType =>
              log.debug(s"Look for available ${simpleTypeNameOf(argType)} in delivery pool")

              val availableDeliverable = findDeliverableBy {
                deliverable => deliverable.hasSamePayloadType(argType) && deliverable.deliveryId == deliveryMeta.id
              }

              val finalDeliverable = getDeliverable(
                availableDeliverable = availableDeliverable,
                consumer = consumer.getClass,
                producer = deliveryMeta.producer
              ) match {
                case Some(deliverable) =>
                  // If we find some delivery, then return it if non null
                  handleDeliveryCondition(deliveryMeta.condition, deliverable)

                case _ =>
                  /*
                   if there's no delivery, then check if autoLoad is set to true.
                   if true, then we will try to find the corresponding repository of the input type and load the data
                  */
                  if (deliveryMeta.autoLoad) {
                    handleAutoLoad(
                      argType,
                      deliveryMeta.id,
                      consumer,
                      deliveryMeta.condition,
                      deliveryMeta.optional
                    )
                  } else {
                    None
                  }
              }

              finalDeliverable match {
                case Some(deliverable) => deliverable
                case _ =>
                  if (!deliveryMeta.optional) {
                    throw new NoSuchElementException(s"Can not find type $argType from DeliverableDispatcher")
                  } else {
                    Deliverable.empty()
                  }
              }
          }

          if (args.exists(_.isEmpty)) {
            log.warn(s"No deliverable was found for optional input ${deliveryMeta.name}")
          } else {
            deliveryMeta.deliverySetter match {
              case Left(field) =>
                log.debug(s"Set value of ${consumer.getClass.getSimpleName}.${deliveryMeta.name}")
                field.setAccessible(true)
                field.set(consumer, args.head.getPayload.asInstanceOf[Object])
              case Right(method) =>
                log.debug(s"Invoke ${consumer.getClass.getSimpleName}.${deliveryMeta.name}")
                method.setAccessible(true)
                method.invoke(consumer, args.map(_.getPayload.asInstanceOf[Object]): _*)
            }
          }
      }
    this
  }

  private[this] val simpleTypeNameOf: ru.Type => String = {
    rType => rType.toString.split("\\[").map(_.split("\\.").last).mkString("[")
  }

}
