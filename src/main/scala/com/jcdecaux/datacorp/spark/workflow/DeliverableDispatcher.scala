package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.{Delivery, InterfaceStability}
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

  private[workflow] val deliverablePool: ArrayBuffer[Deliverable[_]] = ArrayBuffer()
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

    if (registerNewItem(v)) {
      deliverablePool.append(v)
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
    deliverablePool.filter(d => condition(d)).toArray
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

                    val availableRepos = findDeliverableBy {
                      deliverable => deliverable.hasSamePayloadType(repoName) && deliverable.deliveryId == deliveryMeta.id
                    }

                    val matchedRepo = getDeliverable(availableRepos, consumer.getClass, classOf[External])

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
                          throw new NoSuchElementException(s"Can not find $repoName from DeliverableDispatcher")
                        }
                        None
                    }
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
            log.debug(s"Invoke ${consumer.getClass.getSimpleName}.${deliveryMeta.name}")
            val consumerClass = consumer.getClass
            val consumerSetter = try {
              consumerClass.getMethod(deliveryMeta.name, args.map(_.payloadClass): _*)
            } catch {
              case _: NoSuchMethodException =>
                log.debug("Can't invoke setter method. Retry with assignable class")
                getMethodWithAssignableType(consumerClass, deliveryMeta, args)
            }

            consumerSetter.invoke(consumer, args.map(_.get.asInstanceOf[Object]): _*)
          }
      }
    this
  }

  /**
    * In the case where getMethod(name, argsParameterTypes) fails, this method will be called. It will match
    * the consumerClass's method by looking at the name and the parameter types, which are assignable from the
    * given arguments types. In other words, the parameters of the setter methods should be the super-class of
    * the input arguments.
    *
    * @param consumerClass    consumer factory
    * @param deliveryMetadata metadata of this delivery
    * @param deliverable      list of deliverable for this delivery
    * @return
    */
  private[this] def getMethodWithAssignableType(consumerClass: Class[_ <: Factory[_]],
                                                deliveryMetadata: FactoryDeliveryMetadata,
                                                deliverable: Seq[Deliverable[_]]): java.lang.reflect.Method = {
    val availableMethods = consumerClass.getMethods.filter { method =>

      val deliveryPresent = if (method.getName.endsWith("_$eq")) {
        // Do not check isAnnotationPresent here because the setter method
        // generated by the scala compiler doesn't have the annotation Delivery
        true
      } else {
        method.isAnnotationPresent(classOf[Delivery])
      }

      method.getName == deliveryMetadata.name && method.getParameterCount == deliverable.length && deliveryPresent
    }

    if (availableMethods.isEmpty) throw new NoSuchMethodException(s"No method ${deliveryMetadata.name} was found")
    log.debug(s"Find ${availableMethods.length} methods having the name ${deliveryMetadata.name}")

    val isAssignable: (Seq[Class[_]], Seq[Class[_]]) => Boolean = (paramTypes, argTypes) => {
      argTypes.zip(paramTypes).map {
        case (arg, param) => param.isAssignableFrom(arg)
      }.forall(_ == true)
    }

    val argTypes = deliverable.map(_.payloadClass)
    val matchedSetters = availableMethods.filter(method => isAssignable(method.getParameterTypes, argTypes))
    if (matchedSetters.length == 1) {
      matchedSetters.head
    } else {
      throw new NoSuchMethodException(s"Multiple methods (${deliveryMetadata.name}) was found.")
    }

  }

  private[this] val simpleTypeNameOf: ru.Type => String = {
    rType => rType.toString.split("\\[").map(_.split("\\.").last).mkString("[")
  }

}
