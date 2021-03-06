package mesosphere.marathon
package core.launcher.impl

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.launcher.{InstanceOp, InstanceOpFactory}
import mesosphere.marathon.core.matcher.base.util.OfferOperationFactory
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.metrics.Metrics
import org.apache.mesos.{Protos => Mesos}

class InstanceOpFactoryHelper(
    private val metrics: Metrics,
    private val principalOpt: Option[String],
    private val roleOpt: Option[String]) {

  private[this] val offerOperationFactory = new OfferOperationFactory(metrics, principalOpt, roleOpt)

  def launchEphemeral(
    taskInfo: Mesos.TaskInfo,
    newTask: Task,
    instance: Instance): InstanceOp.LaunchTask = {

    assume(newTask.taskId.mesosTaskId == taskInfo.getTaskId, "marathon task id and mesos task id must be equal")

    def createOperations = Seq(offerOperationFactory.launch(taskInfo))

    val stateOp = InstanceUpdateOperation.LaunchEphemeral(instance)
    InstanceOp.LaunchTask(taskInfo, stateOp, oldInstance = None, createOperations)
  }

  def launchEphemeral(
    executorInfo: Mesos.ExecutorInfo,
    groupInfo: Mesos.TaskGroupInfo,
    launched: Instance.LaunchRequest): InstanceOp.LaunchTaskGroup = {

    assume(
      executorInfo.getExecutorId.getValue == launched.instance.instanceId.executorIdString,
      "marathon pod instance id and mesos executor id must be equal")

    def createOperations = Seq(offerOperationFactory.launch(executorInfo, groupInfo))

    val stateOp = InstanceUpdateOperation.LaunchEphemeral(launched.instance)
    InstanceOp.LaunchTaskGroup(executorInfo, groupInfo, stateOp, oldInstance = None, createOperations)
  }

  def launchOnReservation(
    taskInfo: Mesos.TaskInfo,
    newState: InstanceUpdateOperation.LaunchOnReservation,
    oldState: Instance): InstanceOp.LaunchTask = {

    assume(
      oldState.isReserved,
      "only a reserved instance can be re-launched")

    def createOperations = Seq(offerOperationFactory.launch(taskInfo))

    InstanceOp.LaunchTask(taskInfo, newState, Some(oldState), createOperations)
  }

  def launchOnReservation(
    executorInfo: Mesos.ExecutorInfo,
    groupInfo: Mesos.TaskGroupInfo,
    newState: InstanceUpdateOperation.LaunchOnReservation,
    oldState: Instance): InstanceOp.LaunchTaskGroup = {

    def createOperations = Seq(offerOperationFactory.launch(executorInfo, groupInfo))

    InstanceOp.LaunchTaskGroup(executorInfo, groupInfo, newState, Some(oldState), createOperations)
  }

  /**
    * Returns a set of operations to reserve ALL resources (cpu, mem, ports, disk, etc.) and then create persistent
    * volumes against them as needed
    */
  def reserveAndCreateVolumes(
    reservationLabels: ReservationLabels,
    newState: InstanceUpdateOperation.Reserve,
    resources: Seq[Mesos.Resource],
    localVolumes: Seq[InstanceOpFactory.OfferedVolume]): InstanceOp.ReserveAndCreateVolumes = {

    def createOperations =
      offerOperationFactory.reserve(reservationLabels, resources) ++
        offerOperationFactory.createVolumes(reservationLabels, localVolumes)

    InstanceOp.ReserveAndCreateVolumes(newState, resources, createOperations)
  }
}
