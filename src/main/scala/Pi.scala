import akka.actor._
import akka.routing._
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

object Pi extends App {

	calculate(numWorkers = 4, numElems = 10000, numMsgs = 10000)

	sealed trait PiMessage
	case object Calculate extends PiMessage
	case class Work(start: Int, numElems: Int) extends PiMessage
	case class Result(value: Double) extends PiMessage
	case class PiApproximation(pi: Double, duration: Duration)

	//actors and messages

	def calculate(numWorkers: Int, numElems: Int, numMsgs: Int){
		//create Akka system
		val system = ActorSystem("PiSystem")

		//create result listener
		//when you create a top level actor,
		// you do "system.actorOf()"
		//
		//when you create child actors within another actor,
		//you do "context.actorOf()"
		val listener = system.actorOf(
			Props[PiListener], name = "listener")

		//create master
		val master = system.actorOf(
			Props(new PiMaster(
				numWorkers, numMsgs, numElems, listener)),
			name = "master")

		//send the initial Calculate message to the master actor
		//to kick off calculation
		master ! Calculate
	}	


	class PiWorker extends Actor {

		//method to receive messages
		def receive = {

			//when you receive a message that is of class "Work",
			//return a Result object.
			//the sender means you send the message back to sender
			//the "!" means you fire-and-forget, or send it async,
			//	no promises needed
			case Work(start, numElems) =>
				sender() ! Result(calculatePiFor(start, numElems))
			case _ 	=> println("PiWorker received unknown message")

			println("received")
		}

		def calculatePiFor(start: Int, numElems: Int): Double = {

		  var acc = 0.0
		  for (i <- start until (start + numElems)){
		  	acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1)
		  }
		  acc
		}
	}

	//the ActorRef passed in to the PiMaster actor will be used to report
	//the final answer
	class PiMaster(numWorkers: Int, numMsgs: Int, 
		numElems: Int, listener: ActorRef)
		extends Actor {



		var pi: Double = _
		var numResults: Int = _
		val start: Long = System.currentTimeMillis

		//handles routing with round robin.
		var router = {
			val routees = Vector.fill(numWorkers) {
				val r = context.actorOf(Props[PiWorker])
				context watch r
				ActorRefRoutee(r)
			}

			Router(RoundRobinRoutingLogic(), routees)
		}

		val workerRouter: ActorRef = context.actorOf(
			RoundRobinPool(numWorkers).props(Props[PiWorker]), 
			"workerRouter")

		// val workerRouter = context.actorOf(
		// 	Props[PiWorker].withRouter(RoundRobinRouter(numWorkers)),
		// 	name = "workerRouter")

		//handle message
		//needs to handle:
		//	a calculate message, which starts the calculation
		//		and sends Work messages to workers via the router
		//	a Result message, after the workers calculate the different
		//		parts of pi
		def receive = {
			//if received a Calculate message,
			//async send Work messages to workers using
			//	worker router
			case Calculate => {
				for(i <- 0 until numMsgs){
					workerRouter ! Work(i*numElems, numElems)
				}
			}

			//if receieved Result message, aggregate results.
			//if all workers are done, send result to listener.
			case Result(value) => {
				pi += value
				numResults += 1
				if(numResults == numMsgs){
					listener ! PiApproximation(pi, 
						duration = (System.currentTimeMillis-start).millis)

					//stops this actor and its (supervised) children
					context.stop(self)
				}
			}

			case _	=> println("PiMaster received unknown message")
		}
	}

	class PiListener extends Actor {
		def receive = {
			case PiApproximation(pi, duration) => {
				println("\n\tPi approximation: \t\t%s\n\tCalculation time: \t%s"
					.format(pi, duration))
	      		context.system.shutdown()
			}

			case _	=> println("received unknown message")
		}
	}
}