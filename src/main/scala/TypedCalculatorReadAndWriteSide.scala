import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.ClosedShape
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.{Done, NotUsed}
import scala.concurrent.ExecutionContext
import akka_typed.CalculatorRepository.{getlatestOffsetAndResult, initdatabase, updateresultAndOffset}
import akka_typed.TypedCalculatorWriteSide._
import com.typesafe.config.ConfigFactory
import slick.jdbc.GetResult

import scala.concurrent.Future

object akka_typed {
  trait CborSerialization
  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Double) extends Command
    case class Multiply(amount: Double) extends Command
    case class Divide(amount: Double) extends Command

    sealed trait Event
    case class Added(id: Int, amount: Double) extends Event
    case class Multiplied(id: Int, amount: Double) extends Event
    case class Divided(id: Int, amount: Double) extends Event

    final case class State(value: Double) extends CborSerialization{
      def add(amount: Double): State = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State = copy(value = value / amount)
    }

    case class Result(seqNum: Long, operation: Event, state: Double)

    object State{
      val empty=State(0)
    }

    def handleCommand(
                     persId: String,
                     state: State,
                     command: Command,
                     ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding for number: $amount and state is ${state.value} ")
          val added = Added(persId.toInt, amount)
          Effect.persist(added)
          .thenRun{
            x=> ctx.log.info(s"The state result is ${x.value}")
          }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiply for number: $amount and state is ${state.value} ")
          val multiplied = Multiplied(persId.toInt, amount)
          Effect.persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive divide for number: $amount and state is ${state.value} ")
          val divided = Divided(persId.toInt, amount)
          Effect.persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling Event added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling Event multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling Event divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }


  case class TypedCalculatorReadSide()(implicit system: ActorSystem[NotUsed], ec: ExecutionContext){
    implicit val session = initdatabase
//    implicit val materializer = system.classicSystem
    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)



    /*
    В read side приложения с архитектурой CQRS (объект TypedCalculatorReadSide в TypedCalculatorReadAndWriteSide.scala) необходимо разделить чтение событий, бизнес логику и запись в целевой получатель и сделать их асинхронными, т.е.
    1) Persistence Query должно находиться в Source
    2) Обновление состояния необходимо переместить в отдельный от записи в БД flow, замкнуть пуcтым sink

    Как делать!!!
    1. в типах int заменить на double
    2. добавить функцию updateState, в которой сделать паттерн матчинг для событий Added, Multiplied, Divided
    3. нужно создать graphDsl в котором builder.add(source)
    4. builder.add(Flow[EventEnvelope].map(e=> updateState(e.event, e.sequenceNr)))

     */

    /*
    spoiler
    def updateState(event: Any, seqNum:Long) : Result = {
    val newState = event match {
    case Added(_, amount) =>???
    case Multiplied(_, amount) =>???
    case Divided(_, amount) =>???

    val graph = GraphDSL.Builder[NotUsed] =>
    //1.
    val input = builder.add(source)
    val stateUpdate = builder.add(Flow[EventEnvelope].map(e=> updateState(...)))
    val localSaveOutput = builder.add(Sink.foreach[Result]{
    r=>
    lCr = r.state
    //logs

    })

    val dbSaveOutput = builder.add(
    Sink.sink[Result](r=>updateresultAndOffset)
    )
    надо разделить builder на 2 части
    далее надо сохранить flow(уже разделен на 2 части) в 1. localSaveOutput 2. dbSaveOutput
    закрываем граф и запускаем

     */

    def updateState(event: Any , seqNum: Long): Future[Result] = {
      getlatestOffsetAndResult.collect{case Some(value) => value}
        .map{case (_, latest) =>
          event match {
          case op@Added(_, amount) => Result(seqNum, op, latest + amount)
          case op@Multiplied(_, amount) => Result(seqNum, op, latest * amount)
          case op@Divided(_, amount) => Result(seqNum, op, latest / amount)
        }
      }

    }

    getlatestOffsetAndResult.collect{case Some(value) => value}
      .map { case (offset, _) =>
        val startOffset = if (offset == 1) 1 else offset + 1
        val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

        val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
          import GraphDSL.Implicits._

          val input = builder.add(source)
          val stateUpdate = builder.add(Flow[EventEnvelope].mapAsync[Result](10)(e => updateState(e.event,e.sequenceNr)))
          val localSaveOutput = builder.add(Sink.foreach[Result]{
            r =>
              r.operation match {
                case _: Added => println(s"Log from Added: ${r.state}")
                case _: Multiplied => println(s"Log from Multiplied: ${r.state}")
                case _: Divided => println(s"Log from Divided: ${r.state}")
              }})
          val dbSaveOutput = builder.add(
            Sink.foreach[Result](r => updateresultAndOffset(r.state, r.seqNum))
          )

          val broadcast = builder.add(Broadcast[Result](2))

          input.out ~> stateUpdate ~> broadcast.in

          broadcast.out(0) ~> localSaveOutput
          broadcast.out(1) ~> dbSaveOutput

          ClosedShape
        })
        graph.run()
      }
  }

  object CalculatorRepository {
    //homework, how to do
    //1. SlickSession здесь надо посмотреть документацию
    def createSession(implicit system: ActorSystem[NotUsed]): SlickSession = {
      val session: SlickSession = {
        SlickSession.forConfig(ConfigFactory.parseString(
          """
            |{
            |  connectionPool = "HikariCP" //use HikariCP for our connection pool
            |  dataSourceClass = "org.postgresql.ds.PGSimpleDataSource" //Simple datasource with no connection pooling. The connection pool has already been specified with HikariCP.
            |  properties = {
            |    serverName = "localhost"
            |    portNumber = "5432"
            |    databaseName = "demo"
            |    user = "docker"
            |    password = "docker"
            |  }
            |  numThreads = 10
            |}
            |""".stripMargin))
    }
      system.getWhenTerminated.thenRun(() => session.close())
      session
    }

    def initdatabase(implicit system: ActorSystem[NotUsed]): SlickSession = createSession

    // homework
    //case class Result(state: Double, offset: Long)
    // надо переделать getlatestOffsetAndResult
    // def getlatestOffsetAndResult: Result = {
    // val query = sql"select * from public.result where id = 1;".as[Double].headOption
    // надо будет создать future для db.run
    // с помощью await надо получить результат или прокинуть ошибку если результата нет

    def getlatestOffsetAndResult(implicit actorSystem: ActorSystem[NotUsed], session: SlickSession): Future[Option[(Long, Double)]] = {
      import session.profile.api._
      implicit val getUserResult: GetResult[Double] = GetResult(_.nextDouble())
      Slick
        .source(sql"""SELECT r.write_side_offset, r.calculated_value  FROM  public.result r where id = 1""".as[(Long,Double)])
        .runWith(Sink.headOption)
    }

    def updateresultAndOffset(calculated: Double, offset: Long)(implicit actorSystem: ActorSystem[NotUsed], session: SlickSession): Future[Done] = {
      import session.profile.api._
      Source(Seq((calculated: Double, offset: Long)))
        .runWith(Slick.sink{case (calculated, offset) =>
          sqlu"""UPDATE PUBLIC.RESULT
                 SET calculated_value = $calculated, write_side_offset = $offset
                 WHERE ID = 1"""})
    }
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeActorRef ! Add(10)
        writeActorRef ! Multiply(2)
        writeActorRef ! Divide(5)
        Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")
    implicit val executionContext = system.executionContext

    TypedCalculatorReadSide()
  }


}