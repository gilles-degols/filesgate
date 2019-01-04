package net.degols.libs.filesgate.core.loadbalancing

import com.typesafe.config.Config
import net.degols.libs.cluster.balancing.{BasicLoadBalancerType, LoadBalancer}
import net.degols.libs.cluster.core.{Node, Worker, WorkerManager, WorkerType}
import net.degols.libs.cluster.messages._
import net.degols.libs.filesgate.pipeline.download.Download
import net.degols.libs.filesgate.pipeline.prestorage.PreStorage
import net.degols.libs.filesgate.pipeline.storage.Storage
import org.slf4j.LoggerFactory
import play.api.libs.json.JsObject

import scala.util.{Random, Try}

/**
  * Similar to the BasicLoadBalancerType but we make sure to colocate specific actors on the same node.
  * The FilesgateBalancer should be used for every actor related to filesgate (PipelineSteps, PipelineInstance, ...)
  * @param instances
  * @param instanceType
  */
@SerialVersionUID(1L)
case class FilesgateBalancerType(instances: Int, instanceType: InstanceType = ClusterInstance) extends LoadBalancerType {
  override def toString: String = {
    val location = if(instanceType == JVMInstance) "jvm" else "cluster"
    s"FilesgateBalancerType: $instances instances/$location"
  }
}

object FilesgateBalancerType{
  val CONFIGURATION_KEY = "filesgate"

  def loadFromConfig(config: Config): FilesgateBalancerType = {
    val rawInstanceType = Try{config.getString("instance-type")}.getOrElse("cluster")
    val instanceType = if(rawInstanceType == "jvm") JVMInstance else ClusterInstance
    FilesgateBalancerType(config.getInt("max-instances"), instanceType)
  }
}

class FilesgateBalancer extends LoadBalancer {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Simple wrapper to access balancer configuration
    * @param metadata
    */
  case class WorkerInfo(metadata: JsObject) {
    /**
      * Return the pipeline step type of the given WorkerType (if it exists)
      * @param workerType
      * @return
      */
    val stepType: Option[String] = (metadata \ "pipeline-step-type").asOpt[String]

    /**
      * Can be set by the developer, if they wish to only have x instances of the same PipelineStep on any given server for
      * a given pipeline instance.
      * Important: It does not allow to say "I want 1 source of pipeline ABC / node and I have 36 instances of that pipeline".
      * This is another parameter called maxWorkersPerNodeAndPipeline
      */
    val maxWorkersPerNodeAndPipelineInstance: Option[Int] = (metadata \ "max-workers-per-node-and-pipeline-instance").asOpt[Int]

    /**
      * Limit the number of workers for a given step, for all instances of a given pipeline on a given node. Useful
      * if you want to be sure to only have 1 source / (pipeline * node).
      * TODO: Not yet implemented
      */
    val maxWorkersPerNodeAndPipeline: Option[Int] = (metadata \ "max-workers-per-node-and-pipeline").asOpt[Int]
  }

  override def isLoadBalancerType(loadBalancerType: LoadBalancerType): Boolean = loadBalancerType.isInstanceOf[FilesgateBalancerType]

  override def hardWorkDistribution(workerType: WorkerType): Unit = {
    // TODO: Implement hard work distribution
    logger.warn("FilesgateBalancer - There is no hard work distribution in the FilesgateBalancer (not yet at least).")
  }

  override def softWorkDistribution(workerType: WorkerType): Unit = {
    val nodes = clusterManagement.cluster.nodesForWorkerType(workerType)
    val balancerType = workerType.workerTypeInfo.loadBalancerType.asInstanceOf[FilesgateBalancerType]
    val workerInfo = WorkerInfo(workerType.workerTypeInfo.metadata)

    if(nodes.isEmpty) {
      logger.error(s"The WorkerType $workerType has no nodes available, no work distribution possible.")
    } else {
      workerInfo.stepType match {
        case Some(stepType) =>
          logger.debug(s"FilesgateBalancer - Soft work distribution of ${workerType.id}, step is '${stepType}'")
          softWorkDistributionForStep(workerType, nodes, balancerType)
        case None =>
          logger.debug(s"FilesgateBalancer - Soft work distribution of ${workerType.id}")
          // The PipelineInstance is always cluster-based. But if we want to have only 1 PipelineInstance / server, it
          // generally means that we want 1 Source / server. In that case, we just need to configure it on the step-level directly with maxWorkersPerNodeAndPipeline.
          // Important note: If we want to allow non-cluster-based PipelineInstance (to automatically scale if a new node is detected),
          // we would need to modify multiple things, like integrate the node name in the worker name (disgusting obviously), ...
          softWorkDistributionPerCluster(workerType, nodes, balancerType)
      }
    }
  }

  /**
    * Work distribution for a step. For almost all steps, we simply use the basic cluster distribution (not the jvm one)
    * as it does not make sense to have a jvm distribution.
    * @param workerType
    * @param nodes
    * @param balancerType
    */
  private def softWorkDistributionForStep(workerType: WorkerType, nodes: List[Node], balancerType: FilesgateBalancerType): Unit = {
    val workerInfo = WorkerInfo(workerType.workerTypeInfo.metadata)
    val stepType = workerInfo.stepType.get
    val wantedInstances = balancerType.instances

    // We only allow the Download step to be put randomly by the soft-work distribution. Then, we try to put the related PreStorage
    // and Storage types on nodes having a download step.
    if(stepType == PreStorage.TYPE || stepType == Storage.TYPE) {
      softWorkDistributionPerClusterForCollocatedSteps(workerType, nodes, balancerType)
    } else {
      softWorkDistributionPerCluster(workerType, nodes, balancerType)
    }
  }

  private def softWorkDistributionPerJVM(workerType: WorkerType, nodes: List[Node], balancerType: FilesgateBalancerType): Unit = {
    val wantedInstances = balancerType.instances

    nodes.flatMap(_.workerManagers.filter(_.isUp))
      .foreach(workerManager => {
        val runningInstances = workerManager.workerTypes.find(_ == workerType).get.workers.filter(_.isUp)

        var i = runningInstances.size
        if(i < wantedInstances) {
          logger.info(s"Starting ${wantedInstances - i} instances of $workerType on $this")
        }
        while(i < wantedInstances) {
          workerManager.startWorker(context, workerType)
          i += 1
        }
      })
  }

  private def softWorkDistributionPerCluster(workerType: WorkerType, nodes: List[Node], balancerType: FilesgateBalancerType): Unit = {
    val wantedInstances = balancerType.instances

    val managerRunningInstances = managerAndRunningInstances(workerType)
    val runningInstances = managerRunningInstances.values.flatten.size
    if(managerRunningInstances.keys.isEmpty) {
      logger.warn(s"There is no WorkerManager available for $workerType, not possible to start the missing ${wantedInstances - wantedInstances} instances.")
    } else if(runningInstances < wantedInstances) {
      logger.info(s"Starting ${wantedInstances - runningInstances} instances of $workerType on various WorkerManagers.")
      // We try to distribute the load between managers. For now we simply choose managers at random (but those having less than the average number of instances)
      val averageWantedInstances = (wantedInstances + 1f) / managerRunningInstances.keys.size
      val availableManagers = managerRunningInstances.toList.filter(_._2.size < averageWantedInstances)
      if(availableManagers.isEmpty) {
        logger.error(s"No good WorkerManager found for $workerType...")
      } else {
        var i = runningInstances
        while(i < wantedInstances) {
          val workerManager = Random.shuffle(availableManagers).head
          workerManager._1.startWorker(context, workerType)
          i += 1
        }
      }
    }
  }

  /**
    * Work distribution for Download / PreStorage / Storage steps.
    * Those steps does not necessarily to exist (especially the PreStorage one).
    * We only allow the Download steps to be put on any node. But the PreStorage
    * @param workerType
    * @param nodes
    * @param balancerType
    */
  private def softWorkDistributionPerClusterForCollocatedSteps(workerType: WorkerType, nodes: List[Node], balancerType: FilesgateBalancerType): Unit = {
    val workerInfo = WorkerInfo(workerType.workerTypeInfo.metadata)
    val balancerType = workerType.workerTypeInfo.loadBalancerType.asInstanceOf[FilesgateBalancerType]
    val wantedInstances = balancerType.instances

    // Even if we do not have the various steps already started, we can get the configuration of WorkerType directly here
    val nodesByWorkerType = clusterManagement.cluster.nodesByWorkerType()
                                      .map(wt => wt._1 -> (WorkerInfo(wt._1.workerTypeInfo.metadata), wt._2))
                                      .filter(wt => wt._2._1.stepType.isDefined)
    // As key, we have the different worker type needed on each node. As value we have nodes where we can start the workers
    // but it does not mean that those nodes already contain some workers (!)
    val collocatedNodes: Map[WorkerType, (WorkerInfo, List[Node])] = nodesByWorkerType.filter(wt => List(Download.TYPE, PreStorage.TYPE, Storage.TYPE).contains(wt._2._1.stepType.get))
    val neededWorkerTypes = collocatedNodes.keys.toList
    logger.debug(s"Collocated worker types: ${collocatedNodes.keys.map(_.id)}")
    // We only keep nodes having all those steps, no need to keep the other nodes. Normally this is not needed as every jvm
    // of a filesgate can start all workers, but better be safe than sorry.
    val acceptedNodes: Map[Node, List[WorkerType]] = collocatedNodes.map(cnode => {cnode._2._2.map(node => (node, cnode._1))}).flatten.groupBy(_._1)
      .map(obj => obj._1 -> obj._2.map(_._2).toList)
      .filter(x => x._2.size == neededWorkerTypes.size)

    if(acceptedNodes.isEmpty) {
      logger.error(s"There is no node available with all WorkerTypes necessary for the collocated steps of ${workerType.id}.")
      return
    }

    // We get information about the running instances for the current worker type we have to distribute
    val managerRunningInstances: Map[WorkerManager, List[Worker]] = managerAndRunningInstances(workerType)
    val runningInstances = managerRunningInstances.values.flatten.size
    if(managerRunningInstances.keys.isEmpty) {
      logger.warn(s"There is no WorkerManager available for $workerType, not possible to start the missing ${wantedInstances - wantedInstances} instances.")
    } else if(runningInstances < wantedInstances) {
      logger.info(s"Trying to start ${wantedInstances - runningInstances} instances of $workerType on various WorkerManagers.")

      // We need to find the maximum scalability between steps, so we need to find the maximum number of sets we could have. The PGCD is too restrictive,
      // we just need to know the scalability of every step.
      // This number is the maximum scalability inside one pipeline instance for the collocated steps.
      val max_sets = neededWorkerTypes.map(_.workerTypeInfo.loadBalancerType.asInstanceOf[FilesgateBalancerType].instances).min
      logger.debug(s"Maximum scalability for collocated steps (download, prestorage, storage) is $max_sets nodes for pipeline '${workerType.workerTypeInfo.workerTypeId}'")

      // Ideal number of sets per node is limited to the number of nodes. It is also limited by the optional maxWorkersPerNodeAndPipelineInstance
      // because of that parameter (maxWorkersPerNodeAndPipelineInstance) we might not deploy completely all pipeline instances
      // on a small cluster (and this is normal !)
      val setsPerNode = math.ceil(max_sets / (acceptedNodes.keys.size * 1f))

      val wantedInstancesPerNode: Map[WorkerType, Int] = neededWorkerTypes.map(wt => {
        val totInstances = wt.workerTypeInfo.loadBalancerType.asInstanceOf[FilesgateBalancerType].instances
        val totInstancesByStep = workerInfo.maxWorkersPerNodeAndPipelineInstance match {
          case Some(limitedInstancePerNode) => math.min(math.ceil(totInstances / acceptedNodes.keys.size).toInt, limitedInstancePerNode)
          case None => math.ceil(totInstances / acceptedNodes.keys.size).toInt
        }

        wt -> totInstancesByStep
      }).toMap

      // We want to fill existing sets of collocated steps before creating new one, so we sort the nodes by the number of existing workers (simpler than to create sets of collocated steps)
      val orderedNodes = acceptedNodes.keys.toList.sortBy(n => acceptedNodes(n).flatMap(_.workers.filter(_.isUp)).size).reverse

      // For each node we make sure to have enough instances to create multiple sets, as simple as that
      var i = runningInstances
      orderedNodes.foreach(cnode => {
        // Available worker managers to start the wanted workerType
        val availableWorkerManagers = cnode.workerManagers.filter(_.workerTypes.exists(_ == workerType))
        if(availableWorkerManagers.isEmpty) {
          logger.error("No valid worker manager on the selected node. The developer made a mistake.")
        }

        var existingInstances = acceptedNodes(cnode).filter(_ == workerType).map(_.workers.filter(_.isUp).size).sum
        while(i < wantedInstances && existingInstances < wantedInstancesPerNode(workerType)) {
          // Ask the worker manager to start the worker
          val workerManager = Random.shuffle(availableWorkerManagers).head
          workerManager.startWorker(context, workerType)
          i += 1
          existingInstances += 1
        }
      })

      if(i < wantedInstances) {
        workerInfo.maxWorkersPerNodeAndPipeline match {
          case Some(limitedInstancesPerNode) =>
            logger.warn(s"No WorkerManager found to ${wantedInstances - i} missing instances, we just created ${i - runningInstances} instances. This might due to the limitation of ${limitedInstancesPerNode} step instances of ${workerInfo.stepType.get} / node.")
          case None =>
            logger.error(s"No WorkerManager found to ${wantedInstances - i} missing instances, we just created ${i - runningInstances} instances.")
        }
      } else {
        logger.info(s"We just created the ${i - runningInstances} missing instances.")
      }
    }
  }

  /**
    * Returning a map of workermanager and their related worker for a given WorkerType
    * @param workerType
    * @return
    */
  private def managerAndRunningInstances(workerType: WorkerType): Map[WorkerManager, List[Worker]] = {
    val nodes = clusterManagement.cluster.nodesForWorkerType(workerType)
    nodes.flatMap(node => node.workerManagers.filter(_.isUp))
      .map(workerManager => workerManager -> workerManager.workerTypes.filter(_ == workerType).flatMap(_.workers.filter(_.isUp))).toMap
  }
}
