package mesosphere.marathon
package core.storage.store.impl.zk

import akka.Done
import akka.util.ByteString
import mesosphere.marathon.core.base.{LifecycleState, _}
import mesosphere.marathon.stream.Implicits._
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.api.{BackgroundPathable, Backgroundable, Pathable}
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.{ACL, Stat}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
  * Extensions to CuratorFramework that give a more friendly API to scala (everything executes in the background
  * and returns futures instead of the blocking form).
  *
  * While an implicit conversion is provided, the compiler doesn't appear to be able to resolve it automatically
  * if named parameters are given. Instead, it is advisable to create it explicitly.
  *
  * @param client The underlying Curator client.
  */
class RichCuratorFramework(val client: CuratorFramework) {

  def close(): Unit = synchronized {
    client.close()
  }

  def start(): Unit = synchronized {
    client.start()
  }

  def create(
    path: String,
    data: Option[ByteString] = None,
    compress: Boolean = false,
    `protected`: Boolean = false,
    acls: Seq[ACL] = Nil,
    createMode: CreateMode = CreateMode.PERSISTENT,
    creatingParentsIfNeeded: Boolean = false,
    creatingParentContainersIfNeeded: Boolean = false): Future[String] =
    build(client.create(), ZkFuture.create) { builder =>
      if (compress) builder.compressed()
      if (`protected`) builder.withProtection()
      if (creatingParentsIfNeeded) builder.creatingParentsIfNeeded()
      if (creatingParentContainersIfNeeded) builder.creatingParentContainersIfNeeded()
      if (acls.nonEmpty) builder.withACL(acls.asJava)
      builder.withMode(createMode)
      data.fold(builder.forPath(path)) { bytes =>
        builder.forPath(path, bytes.toArray)
      }
    }

  def delete(
    path: String,
    version: Option[Int] = None,
    guaranteed: Boolean = false,
    deletingChildrenIfNeeded: Boolean = false): Future[String] =
    build(client.delete(), ZkFuture.delete) { builder =>
      if (deletingChildrenIfNeeded) builder.deletingChildrenIfNeeded()
      if (guaranteed) builder.guaranteed()
      if (deletingChildrenIfNeeded) builder.deletingChildrenIfNeeded()
      version.foreach(builder.withVersion)
      builder.forPath(path)
    }

  def exists(
    path: String,
    creatingParentContainersIfNeeded: Boolean = false): Future[ExistsResult] =
    build(client.checkExists(), ZkFuture.exists) { builder =>
      if (creatingParentContainersIfNeeded) builder.creatingParentContainersIfNeeded()
      builder.forPath(path)
    }

  def data(
    path: String,
    decompressed: Boolean = false): Future[GetData] =
    build(client.getData, ZkFuture.data) { builder =>
      if (decompressed) builder.decompressed()
      builder.forPath(path)
    }

  def setData(
    path: String,
    data: ByteString,
    compressed: Boolean = false,
    version: Option[Int] = None): Future[SetData] =
    build(client.setData(), ZkFuture.setData) { builder =>
      version.foreach(builder.withVersion)
      if (compressed) builder.compressed()
      builder.forPath(path, data.toArray)
    }

  def children(path: String): Future[Children] =
    build(client.getChildren, ZkFuture.children) { builder =>
      builder.forPath(path)
    }

  def sync(path: String): Future[Option[Stat]] =
    build(client.sync(), ZkFuture.sync) { builder =>
      builder.forPath(path)
    }

  def acl(path: String): Future[Seq[ACL]] =
    build(client.getACL, ZkFuture.acl) { builder =>
      builder.forPath(path)
    }

  @SuppressWarnings(Array("AsInstanceOf"))
  def setAcl(path: String, acls: Seq[ACL],
    version: Option[Int] = None): Future[Done] = {
    val builder = client.setACL()
    // sadly, the builder doesn't export BackgroundPathable, but the impl is.
    build(builder.asInstanceOf[BackgroundPathable[_]], ZkFuture.setAcl) { _ =>
      version.foreach(builder.withVersion)
      builder.withACL(acls.asJava)
      // it doesn't export Pathable either?
      builder.asInstanceOf[Pathable[_]].forPath(path)
    }
  }

  private def build[A <: Backgroundable[_], B](builder: A, future: ZkFuture[B])(f: A => Unit): Future[B] = synchronized {
    if (client.getState() == CuratorFrameworkState.STOPPED) future.fail(new IllegalStateException("Curator connection to ZooKeeper has been stopped."))
    try {
      builder.inBackground(future)
      f(builder)
      future
    } catch {
      case NonFatal(e) =>
        future.fail(e)
    }
  }

  override def toString: String =
    s"CuratorFramework(${client.getZookeeperClient.getCurrentConnectionString}/${client.getNamespace})"

  /**
    * Block the current thread until Zookeeper connection is established or until configured zookeeper connection
    * timeout is surpassed . If Marathon is detected to be shutting down, then we abort immediately and throw an
    * InterruptedException.
    *
    * @param lifecycleState reference to interface to query Marathon's lifecycle state
    */
  @SuppressWarnings(Array("CatchFatal"))
  def blockUntilConnected(lifecycleState: LifecycleState, crashStrategy: CrashStrategy): Unit = {
    if (!lifecycleState.isRunning)
      throw new InterruptedException("Not waiting for connection to zookeeper; Marathon is shutting down")

    if (!client.blockUntilConnected(100, java.util.concurrent.TimeUnit.MILLISECONDS))
      crashStrategy.crash()
  }
}

object RichCuratorFramework {

  /**
    * Listen to connection state changes and suicide if the connection to ZooKeeper is lost.
    */
  object ConnectionLostListener extends ConnectionStateListener {
    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
      if (!newState.isConnected) {
        client.close()
        Runtime.getRuntime.asyncExit()(ExecutionContext.Implicits.global)
      }
    }
  }

  def apply(client: CuratorFramework): RichCuratorFramework = {
    client.getConnectionStateListenable().addListener(ConnectionLostListener)
    new RichCuratorFramework(client)
  }
}
