package mesosphere.marathon
package api.v3

import akka.Done
import akka.stream.ActorMaterializer
import mesosphere.AkkaUnitTest
import mesosphere.marathon.api._
import mesosphere.marathon.api.v2.{AppHelpers, AppNormalization}
import mesosphere.marathon.api.v3.TemplateRepository.Versioned
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.plugin.auth.{Authenticator, Authorizer}
import mesosphere.marathon.raml.{App, Raml}
import mesosphere.marathon.state._
import mesosphere.marathon.test.{GroupCreation, JerseyTest}
import org.apache.zookeeper.KeeperException.NoNodeException
import play.api.libs.json._

import scala.collection.immutable.Seq
import scala.concurrent.Future

class TemplatesResourceTest extends AkkaUnitTest with GroupCreation with JerseyTest {

  case class Fixture(
      auth: TestAuthFixture = new TestAuthFixture,
      repository: TemplateRepository = mock[TemplateRepository]) {
    val config: AllConf = AllConf.withTestConfig()
    implicit val mat = ActorMaterializer()
    val templatesResource: TemplatesResource = new TemplatesResource(
      repository,
      system.eventStream,
      config,
      PluginManager.None
    )(auth.auth, auth.auth, ctx, mat)

    implicit val authenticator: Authenticator = auth.auth
    implicit val authorizer: Authorizer = auth.auth

    val normalizationConfig = AppNormalization.Configuration(config.defaultNetworkName.toOption, config.mesosBridgeName())
    implicit lazy val appDefinitionValidator = AppDefinition.validAppDefinition(config.availableFeatures)(PluginManager.None)

    implicit val validateAndNormalizeApp: Normalization[raml.App] =
      AppHelpers.appNormalization(config.availableFeatures, normalizationConfig)(AppNormalization.withCanonizedIds())

    def normalize(app: App): App = {
      val migrated = AppNormalization.forDeprecated(normalizationConfig).normalized(app)
      AppNormalization(normalizationConfig).normalized(migrated)
    }

    def normalizeAndConvert(app: App): AppDefinition = {
      val normalized = normalize(app)
      Raml.fromRaml(normalized)
    }

    def appToBytes(app: App) = {
      val normed = normalize(app)
      val body = Json.stringify(Json.toJson(normed)).getBytes("UTF-8")
      body
    }
  }

  "Templates resource" should {
    "create a new template successfully" in new Fixture {
      Given("a template")
      val app = App(id = "/app", cmd = Some("cmd"))
      val body = appToBytes(app)
      val template = normalizeAndConvert(app)

      repository.create(any) returns Future.successful(Versioned(template, 1))

      When("create is request is made")
      val response = asyncRequest { r =>
        templatesResource.create(body, auth.request, r)
      }

      Then("it is successful")
      response.getStatus should be(201)

      And("returned JSON contains the version of created template")

      JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(JsObject(List("version" -> JsString("1"))))
    }

    "find the latest template version" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.versions(any) returns Future.successful(Seq(1))
      repository.read(any, any) returns Future.successful(Versioned(template, 1))

      When("latest template version is requested")
      val response = asyncRequest { r =>
        templatesResource.latest(template.id.toString, auth.request, r)
      }

      Then("it is successful")
      response.getStatus should be(200)
    }

    "find a template with provided version" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.read(any, any) returns Future.successful(Versioned(template, 1))

      When("template version is requested")
      val response = asyncRequest { r =>
        templatesResource.version(template.id.toString, 1, auth.request, r)
      }

      Then("it is successful")
      response.getStatus should be(200)
    }

    "fail to get a template with a non-existing version" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.read(any, any) returns Future.failed(new NoNodeException("/templates/app/1"))

      When("a non-existing template version is requested")
      val response = asyncRequest { r =>
        templatesResource.version(template.id.toString, 1, auth.request, r)
      }

      Then("it should fail")
      response.getStatus should be(500)
    }

    "list all versions of the template" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.versions(any) returns Future.successful(Seq(1, 2, 3))

      val response = asyncRequest { r =>
        templatesResource.versions(template.id.toString, auth.request, r)
      }

      Then("it is successful")
      response.getStatus should be(200)

      And("the JSON is as expected")

      JsonTestHelper
        .assertThatJsonString(response.getEntity.asInstanceOf[String])
        .correspondsToJsonOf(JsObject(List("versions" -> JsArray(Seq(1, 2, 3).map(JsNumber(_))))))
    }

    "list versions of the template where non exist" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.versions(any) returns Future.successful(Seq.empty)

      val response = asyncRequest { r =>
        templatesResource.versions(template.id.toString, auth.request, r)
      }

      Then("it is successful")
      response.getStatus should be(200)

      And("the JSON is as expected")

      JsonTestHelper
        .assertThatJsonString(response.getEntity.asInstanceOf[String])
        .correspondsToJsonOf(JsObject(List("versions" -> JsArray())))
    }

    "list versions of a non-existing template" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.versions(any) returns Future.failed(new NoNodeException("/templates/app/1"))

      val response = asyncRequest { r =>
        templatesResource.versions(template.id.toString, auth.request, r)
      }

      Then("it should fail")
      response.getStatus should be(500)
    }

    "delete a template with a provided version" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.delete(any, any) returns Future.successful(Done)
      val response = asyncRequest { r =>
        templatesResource.delete(template.id.toString, 1, auth.request, r)
      }

      Then("it is successful")
      response.getStatus should be(200)
    }

    "delete template and all versions" in new Fixture {
      val app = App(id = "/app", cmd = Some("cmd"))
      val template = normalizeAndConvert(app)

      repository.delete(equalTo(template.id)) returns Future.successful(Done)
      val response = asyncRequest { r =>
        templatesResource.delete(template.id.toString, auth.request, r)
      }

      Then("it is successful")
      response.getStatus should be(200)
    }

    "access without authentication is denied" in new Fixture() {
      Given("an unauthenticated request")
      auth.authenticated = false
      val req = auth.request
      val app = """{"id":"/a/b/c","cmd":"foo","ports":[]}"""

      When("we try to add a template")
      val create = asyncRequest { r =>
        templatesResource.create(app.getBytes("UTF-8"), req, r)
      }
      Then("we receive a NotAuthenticated response")
      create.getStatus should be(auth.NotAuthenticatedStatus)

      When("we try to fetch a template")
      val show = asyncRequest { r =>
        templatesResource.latest("", req, r)
      }
      Then("we receive a NotAuthenticated response")
      show.getStatus should be(auth.NotAuthenticatedStatus)

      When("we try to delete a template")
      val delete = asyncRequest { r =>
        templatesResource.delete("", req, r)
      }

      Then("we receive a NotAuthenticated response")
      delete.getStatus should be(auth.NotAuthenticatedStatus)
    }
  }
}
