package mesosphere.marathon
package api.v3

import java.net.URI

import akka.event.EventStream
import akka.stream.Materializer
import akka.Done
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.container.{AsyncResponse, Suspended}
import javax.ws.rs.core.{Context, MediaType, Response}
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.v2.{AppHelpers, AppNormalization}
import mesosphere.marathon.api.v3.TemplateRepository.Versioned
import mesosphere.marathon.api.{AuthResource, RestResource}
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.raml.Raml
import mesosphere.marathon.state._
import org.glassfish.jersey.server.ManagedAsync
import play.api.libs.json.{JsString, Json}

import scala.async.Async._
import scala.concurrent.{ExecutionContext, Future}

@Path("v3/templates")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class TemplatesResource @Inject() (
    templateRepository: TemplateRepository,
    eventBus: EventStream,
    val config: MarathonConf,
    pluginManager: PluginManager)(implicit
    val authenticator: Authenticator,
    val authorizer: Authorizer,
    val executionContext: ExecutionContext,
    val mat: Materializer) extends RestResource with AuthResource {

  import AppHelpers._
  import Normalization._

  private implicit lazy val appDefinitionValidator = AppDefinition.validAppDefinition(config.availableFeatures)(pluginManager)

  private val normalizationConfig = AppNormalization.Configuration(
    config.defaultNetworkName.toOption,
    config.mesosBridgeName())

  private implicit val validateAndNormalizeApp: Normalization[raml.App] =
    appNormalization(config.availableFeatures, normalizationConfig)(AppNormalization.withCanonizedIds())

  @SuppressWarnings(Array("all")) // async/await
  @POST
  @ManagedAsync
  def create(
    body: Array[Byte],
    @Context req: HttpServletRequest,
    @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      implicit val identity = await(authenticatedAsync(req))

      val rawApp = Raml.fromRaml(Json.parse(body).as[raml.App].normalize)

      val app = validateOrThrow(rawApp)

      checkAuthorization(CreateRunSpec, rawApp)

      val Versioned(_, version) = await(templateRepository.create(app))

      Response
        .created(new URI(app.id.toString))
        .entity(jsonObjString("version" -> JsString(version.toString)))
        .build()
    }
  }

  @GET
  @Path("""{id:.+}/latest""")
  def latest(
    @PathParam("id") id: String,
    @Context req: HttpServletRequest,
    @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      implicit val identity = await(authenticatedAsync(req))

      // We need to be able to check authorization by PathId, not by RunSpec
      // checkAuthorization(ViewRunSpec, id)

      val versions: Seq[Int] = result(templateRepository.versions(PathId(id)))
      if (versions.isEmpty) {
        Response
          .ok()
          .entity(jsonArrString())
          .build()
      } else {
        val Versioned(template, _) = await(templateRepository.read(PathId(id), versions.max))

        checkAuthorization(ViewRunSpec, template)

        Response
          .ok()
          .entity(jsonObjString("template" -> template))
          .build()
      }
    }
  }

  @GET
  @Path("{id:.+}/versions")
  def versions(
    @PathParam("id") id: String,
    @Context req: HttpServletRequest,
    @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      implicit val identity = await(authenticatedAsync(req))

      val templateId = PathId(id)
      val versions = await(templateRepository.versions(templateId))

      // TODO (ad): Authorization via RunSpec PathId
      // checkAuthorization(ViewRunSpec, templateId)

      Response
        .ok()
        .entity(jsonObjString("versions" -> versions))
        .build()

    }
  }

  @GET
  @Path("{id:.+}/versions/{version}")
  def version(
    @PathParam("id") id: String,
    @PathParam("version") version: Int,
    @Context req: HttpServletRequest,
    @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      implicit val identity = await(authenticatedAsync(req))

      val Versioned(template, _) = await(templateRepository.read(PathId(id), version))

      checkAuthorization(ViewRunSpec, template)

      Response
        .ok()
        .entity(jsonObjString("template" -> template))
        .build()
    }
  }

  @SuppressWarnings(Array("all")) /* async/await */
  @DELETE
  @Path("""{id:.+}""")
  def delete(
    @PathParam("id") id: String,
    @Context req: HttpServletRequest,
    @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      implicit val identity = await(authenticatedAsync(req))

      val templateId = PathId(id)

      // TODO (ad): Authorization via RunSpec PathId
      // checkAuthorization(DeleteRunSpec, templateId)

      await(templateRepository.delete(templateId))

      Response
        .ok()
        .build()
    }
  }

  @SuppressWarnings(Array("all")) /* async/await */
  @DELETE
  @Path("{id:.+}/versions/{version}")
  def delete(
    @PathParam("id") id: String,
    @PathParam("version") version: Int,
    @Context req: HttpServletRequest,
    @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      implicit val identity = await(authenticatedAsync(req))

      val templateId = PathId(id)

      // TODO (ad): Authorization via RunSpec PathId
      // checkAuthorization(DeleteRunSpec, templateId)

      await(templateRepository.delete(templateId, version))

      Response
        .ok()
        .build()
    }
  }
}

trait TemplateRepository {

  import TemplateRepository._

  def create(template: Template): Future[Versioned]

  def read(pathId: PathId, version: Int): Future[Versioned]

  def delete(pathId: PathId, version: Int): Future[Done]

  def delete(pathId: PathId): Future[Done]

  def versions(pathId: PathId): Future[Seq[Int]]
}

object TemplateRepository {

  type Template = AppDefinition

  case class Versioned(template: Template, version: Int)
}