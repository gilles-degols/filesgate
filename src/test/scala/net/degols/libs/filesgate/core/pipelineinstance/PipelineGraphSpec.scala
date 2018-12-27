package net.degols.libs.filesgate.core.pipelineinstance

import java.io.File

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import net.degols.libs.cluster.messages.{BasicLoadBalancerType, ClusterInstance}
import net.degols.libs.filesgate.core.{PipelineStepRunning, PipelineStepStatus}
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.datasource.DataSource
import net.degols.libs.filesgate.pipeline.download.Download
import net.degols.libs.filesgate.pipeline.matcher.Matcher
import net.degols.libs.filesgate.pipeline.metadata.Metadata
import net.degols.libs.filesgate.pipeline.postmetadata.PostMetadata
import net.degols.libs.filesgate.pipeline.predownload.PreDownload
import net.degols.libs.filesgate.pipeline.premetadata.PreMetadata
import net.degols.libs.filesgate.pipeline.prestorage.PreStorage
import net.degols.libs.filesgate.pipeline.storage.Storage
import net.degols.libs.filesgate.utils.{FilesgateConfiguration, PipelineMetadata, Step}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import play.api.libs.json.{JsObject, Json}


class PipelineGraphSpec extends TestKit(ActorSystem("MySpec")) with MockitoSugar with ImplicitSender with WordSpecLike with PrivateMethodTester with BeforeAndAfter{

  var config: Config = ConfigFactory.load(ConfigFactory.parseFile(new File("application.conf")))
  var filesgateConfiguration: FilesgateConfiguration = new FilesgateConfiguration(config)

  // Default pipeline to work on. But, we might use another one depending on the use case
  var steps: List[Step] = List(
    Step(DataSource.TYPE, "Component:Package:test.datasource", BasicLoadBalancerType(1, ClusterInstance)),
    Step(Matcher.TYPE, "Component:Package:test.matcher", BasicLoadBalancerType(1, ClusterInstance)),
    Step(PreDownload.TYPE, "Component:Package:test.predownload", BasicLoadBalancerType(1, ClusterInstance)),
    Step(Download.TYPE, "Component:Package:test.download", BasicLoadBalancerType(1, ClusterInstance)),
    Step(PreStorage.TYPE, "Component:Package:test.prestorage", BasicLoadBalancerType(1, ClusterInstance)),
    Step(Storage.TYPE, "Component:Package:test.storage", BasicLoadBalancerType(1, ClusterInstance)),
    Step(PreMetadata.TYPE, "Component:Package:test.premetadata", BasicLoadBalancerType(1, ClusterInstance)),
    Step(Metadata.TYPE, "Component:Package:test.metadata", BasicLoadBalancerType(1, ClusterInstance)),
    Step(PostMetadata.TYPE, "Component:Package:test.postmetadata", BasicLoadBalancerType(1, ClusterInstance))
  )
  var pipelineMetadata: PipelineMetadata = PipelineMetadata("test", steps, 1)
  var pipelineSteps: Map[String, PipelineStepStatus] = constructPipelineSteps(steps)
  var pipelineGraph: PipelineGraph = new PipelineGraph(filesgateConfiguration)
  pipelineGraph.pipelineMetadata = pipelineMetadata
  pipelineGraph.stepStatus = pipelineSteps.values.toList

  var pipelineGraphMock: PipelineGraph = mock[PipelineGraph]

  before {
    // Basic source for test
    val iter: Iterator[FileMetadata] = (1 to 100).map(number => FileMetadata(s"http://localhost/img/$number")).toIterator
    val source: Source[FileMetadata, NotUsed] = Source.fromIterator(() => iter)
    when(pipelineGraphMock.loadSource(ArgumentMatchers.any[PipelineStepStatus])).thenReturn(Option(source))
  }

  def constructPipelineSteps(steps: List[Step]) = {
    steps.map(step => {
      val actorRef = system.actorOf(TestActors.echoActorProps)
      val state = PipelineStepRunning
      val status = PipelineStepStatus(step, "test-1", state)
      actorRef.toString() -> status
    }).toMap
  }

  "loadGraph" must {

  }

}
