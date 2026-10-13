import beam_broker/broker
import beam_broker/consumer
import beam_broker/publisher
import beam_broker/router
import beam_broker/topic
import gleam/erlang/process
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import mist
import wisp
import wisp/wisp_mist

const topic_supervisor_name = "topic_supervisor"

const consumer_supervisor_name = "consumer_supervisor"

const publisher_supervisor_name = "publisher_supervisor"

const broker_name = "broker"

/// Start the main orchestrator
///
/// Registers all the supervisors and the control plane and starts
/// the core OTP program
///
pub fn start_orchestrator() {
  let topic_reference = process.new_name(topic_supervisor_name)
  let consumer_reference = process.new_name(consumer_supervisor_name)
  let publisher_reference = process.new_name(publisher_supervisor_name)
  let broker_reference = process.new_name(broker_name)

  let topic_supervisor =
    factory.worker_child(topic.start_actor)
    |> factory.named(topic_reference)
    |> factory.supervised

  let consumer_supervisor =
    factory.worker_child(consumer.start_actor)
    |> factory.named(consumer_reference)
    |> factory.supervised

  let publisher_supervisor =
    factory.worker_child(publisher.start_actor)
    |> factory.named(publisher_reference)
    |> factory.supervised

  let broker_args =
    broker.Args(
      name: broker_reference,
      topic: topic_reference,
      consumer: consumer_reference,
      publisher: publisher_reference,
    )

  let broker_worker =
    supervision.worker(fn() { broker.start_actor(broker_args) })

  let web_worker =
    supervision.worker(fn() {
      let handle_request = fn(request) {
        router.handle_request(request, broker_reference)
      }
      wisp_mist.handler(handle_request, wisp.random_string(64))
      |> mist.new
      |> mist.port(8000)
      |> mist.start
    })

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(topic_supervisor)
  |> supervisor.add(consumer_supervisor)
  |> supervisor.add(publisher_supervisor)
  |> supervisor.add(broker_worker)
  |> supervisor.add(web_worker)
  |> supervisor.start
}
